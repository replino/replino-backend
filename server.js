const express = require('express');
const cors = require('cors');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const fs = require('fs').promises;
const path = require('path');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const compression = require('compression');
const { createClient } = require('@supabase/supabase-js');
const jwt = require('jsonwebtoken');
const Redis = require('ioredis');
const cluster = require('cluster');
const os = require('os');
const { throttle } = require('lodash');
const pino = require('pino');
const pinoHttp = require('pino-http');

// Initialize logger
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      translateTime: 'SYS:standard',
      ignore: 'pid,hostname'
    }
  }
});

const app = express();
const PORT = process.env.PORT || 3001;

// Cluster mode for multi-core utilization
if (cluster.isMaster && process.env.NODE_ENV === 'production') {
  const numCPUs = os.cpus().length;
  logger.info(`Master ${process.pid} is running with ${numCPUs} workers`);
  
  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    logger.error(`Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
  
  return;
}

// Initialize Redis client with connection pooling
const redis = new Redis({
  host: process.env.REDIS_HOST || 'clean-panda-53790.upstash.io',
  port: process.env.REDIS_PORT || 6379,
  password: process.env.REDIS_PASSWORD || 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
  tls: {},
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
  maxRetriesPerRequest: 3
});

// Initialize Supabase client with connection pooling
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    persistSession: false,
    autoRefreshToken: false
  }
});

if (!supabaseUrl || !supabaseServiceKey) {
  logger.error('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
  process.exit(1);
}

// HTTP logger middleware
app.use(pinoHttp({ logger }));

// Security and performance middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'", "https://*.upstash.io", supabaseUrl]
    }
  }
}));
app.use(compression({
  level: 6,
  threshold: '10kb',
  filter: (req, res) => {
    if (req.headers['x-no-compression']) return false;
    return compression.filter(req, res);
  }
}));
app.use(cors({
  origin: process.env.NODE_ENV === 'production' 
    ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
    : ['http://localhost:5173', 'http://localhost:3000'],
  credentials: true,
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  maxAge: 86400
}));

// Rate limiting with Redis store
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  store: new rateLimit.RedisStore({
    sendCommand: (...args) => redis.sendCommand(args)
  })
});

// Stricter rate limiting for message sending
const sendLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => `${req.user.id}:${req.params.sessionCode}`,
  store: new rateLimit.RedisStore({
    sendCommand: (...args) => redis.sendCommand(args)
  })
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// In-memory storage for WhatsApp clients with TTL
const clients = new Map();
const SESSION_TTL = 30 * 60 * 1000; // 30 minutes

// Session cleanup interval
setInterval(() => {
  const now = Date.now();
  for (const [sessionCode, { lastActivity }] of clients.entries()) {
    if (now - lastActivity > SESSION_TTL) {
      logger.info(`Cleaning up inactive session: ${sessionCode}`);
      const client = clients.get(sessionCode).client;
      client.destroy().catch(err => logger.error(`Error cleaning up session ${sessionCode}:`, err));
      clients.delete(sessionCode);
      redis.del(`qr:${sessionCode}`).catch(err => logger.error('Redis del error:', err));
    }
  }
}, 60 * 1000); // Run every minute

// Ensure sessions directory exists
const SESSIONS_DIR = path.join(__dirname, 'sessions');

async function ensureSessionsDir() {
  try {
    await fs.access(SESSIONS_DIR);
  } catch {
    await fs.mkdir(SESSIONS_DIR, { recursive: true });
  }
}

// Initialize sessions directory on startup
ensureSessionsDir().catch(err => logger.error('Error creating sessions dir:', err));

// Middleware to verify Supabase JWT token with caching
async function verifyAuth(req, res, next) {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ 
        success: false, 
        error: 'Authorization token required' 
      });
    }

    const token = authHeader.substring(7);
    
    // Check Redis cache first
    const cacheKey = `auth:${token}`;
    const cachedUser = await redis.get(cacheKey);
    
    if (cachedUser) {
      req.user = JSON.parse(cachedUser);
      return next();
    }

    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error || !user) {
      return res.status(401).json({ 
        success: false, 
        error: 'Invalid or expired token' 
      });
    }

    // Cache valid token for 5 minutes
    await redis.setex(cacheKey, 300, JSON.stringify(user));
    req.user = user;
    next();
  } catch (error) {
    logger.error('Auth verification error:', error);
    res.status(401).json({ 
      success: false, 
      error: 'Authentication failed' 
    });
  }
}

// Enhanced session management with caching
async function updateSessionInDB(sessionCode, updates) {
  try {
    const cacheKey = `session:${sessionCode}`;
    const { error } = await supabase
      .from('sessions')
      .update({
        ...updates,
        updated_at: new Date().toISOString()
      })
      .eq('session_code', sessionCode);

    if (error) {
      logger.error('Error updating session in DB:', error);
      return;
    }

    // Invalidate cache
    await redis.del(cacheKey);
  } catch (error) {
    logger.error('Database update error:', error);
  }
}

async function getSessionFromDB(sessionCode, userId = null) {
  try {
    const cacheKey = `session:${sessionCode}`;
    const cachedSession = await redis.get(cacheKey);
    
    if (cachedSession) {
      const session = JSON.parse(cachedSession);
      if (!userId || session.user_id === userId) {
        return session;
      }
      return null;
    }
    
    let query = supabase
      .from('sessions')
      .select('*')
      .eq('session_code', sessionCode);
    
    if (userId) {
      query = query.eq('user_id', userId);
    }
    
    const { data, error } = await query.single();
    
    if (error || !data) {
      logger.error('Error fetching session from DB:', error);
      return null;
    }
    
    // Cache session for 5 minutes
    await redis.setex(cacheKey, 300, JSON.stringify(data));
    return data;
  } catch (error) {
    logger.error('Database fetch error:', error);
    return null;
  }
}

// Optimized phone number validation
const phoneRegex = /^\+[1-9]\d{9,14}$/;
function validatePhoneNumber(phone) {
  if (typeof phone !== 'string') return false;
  const cleaned = phone.replace(/[^\d+]/g, '');
  return phoneRegex.test(cleaned);
}

function formatPhoneNumber(phone) {
  let formatted = phone.replace(/[^\d+]/g, '');
  if (!formatted.startsWith('+')) {
    formatted = '+' + formatted;
  }
  return formatted.substring(1) + '@c.us';
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    activeSessions: clients.size,
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    supabaseConnected: !!supabase,
    redisConnected: redis.status === 'ready',
    workerId: cluster.worker?.id || 'master'
  });
});

// Get server statistics
app.get('/stats', verifyAuth, async (req, res) => {
  try {
    const [userSessions, redisInfo] = await Promise.all([
      supabase
        .from('sessions')
        .select('*', { count: 'exact', head: true })
        .eq('user_id', req.user.id),
      redis.info()
    ]);

    const stats = {
      activeSessions: clients.size,
      userSessions: userSessions.count || 0,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      timestamp: new Date().toISOString(),
      redisStatus: redis.status,
      redisMemory: redisInfo.match(/used_memory:\d+/)?.[0] || 'unknown',
      workerId: cluster.worker?.id || 'master'
    };
    
    res.json(stats);
  } catch (error) {
    logger.error('Error fetching stats:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to fetch statistics' 
    });
  }
});

// Initialize WhatsApp session with optimized QR generation
app.post('/init/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    // Check if session already exists and is connected
    if (clients.has(sessionCode)) {
      const { client } = clients.get(sessionCode);
      try {
        const state = await client.getState();
        
        if (state === 'CONNECTED') {
          await updateSessionInDB(sessionCode, { 
            status: 'connected',
            last_connected_at: new Date().toISOString()
          });
          
          return res.json({ 
            success: true, 
            message: 'Session already connected',
            status: 'connected'
          });
        }
      } catch (error) {
        // Client exists but not responsive, clean it up
        clients.delete(sessionCode);
        await redis.del(`qr:${sessionCode}`);
      }
    }

    // Update session status to connecting
    await updateSessionInDB(sessionCode, { 
      status: 'connecting',
      qr_code: null 
    });

    // Create new client with optimized configuration
    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: sessionCode,
        dataPath: SESSIONS_DIR
      }),
      puppeteer: {
        headless: true,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-accelerated-2d-canvas',
          '--no-first-run',
          '--no-zygote',
          '--single-process',
          '--disable-gpu',
          '--js-flags="--max-old-space-size=256"'
        ],
        executablePath: process.env.CHROME_BIN || undefined
      },
      webVersionCache: {
        type: 'remote',
        remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html',
      },
      qrTimeoutMs: 0,
      takeoverOnConflict: true,
      restartOnAuthFail: true,
      qrMaxRetries: 3
    });

    // Store client with last activity timestamp
    clients.set(sessionCode, { 
      client, 
      lastActivity: Date.now() 
    });

    // Throttled QR code handler to prevent excessive updates
    const handleQR = throttle(async (qr) => {
      logger.info(`QR Code generated for session ${sessionCode}`);
      
      try {
        // Generate QR code directly in terminal and as data URL
        qrcode.generate(qr, { small: true });
        
        const qrCodeImage = await new Promise((resolve) => {
          qrcode.toDataURL(qr, {
            width: 256,
            margin: 2,
            color: {
              dark: '#000000',
              light: '#FFFFFF'
            }
          }, (err, url) => {
            if (err) {
              logger.error('QR generation error:', err);
              resolve(null);
            } else {
              resolve(url);
            }
          });
        });

        if (qrCodeImage) {
          // Store in Redis cache for faster access
          await redis.setex(`qr:${sessionCode}`, 300, qrCodeImage);

          // Update session with QR code in database
          await updateSessionInDB(sessionCode, { 
            status: 'connecting',
            qr_code: qrCodeImage 
          });
        }
      } catch (error) {
        logger.error('Error handling QR code:', error);
      }
    }, 5000); // Throttle to once every 5 seconds

    client.on('qr', handleQR);

    client.on('loading_screen', async (percent, message) => {
      logger.info(`Loading screen: ${percent}% ${message || ''}`);
      if (percent === 100) {
        // QR code has been scanned - clear it
        await redis.del(`qr:${sessionCode}`);
        await updateSessionInDB(sessionCode, {
          qr_code: null,
          status: 'connecting'
        });
      }
    });

    client.on('authenticated', async () => {
      logger.info(`WhatsApp client ${sessionCode} authenticated`);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null
      });
    });

    client.on('auth_failure', async (msg) => {
      logger.error(`Authentication failed for session ${sessionCode}:`, msg);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'expired',
        qr_code: null 
      });
      
      clients.delete(sessionCode);
    });

    client.on('ready', async () => {
      logger.info(`WhatsApp client ${sessionCode} is ready!`);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null 
      });
    });

    client.on('disconnected', async (reason) => {
      logger.warn(`WhatsApp client ${sessionCode} disconnected:`, reason);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });
      
      clients.delete(sessionCode);
    });

    // Initialize the client with error handling
    logger.info(`Initializing WhatsApp client for session ${sessionCode}`);
    await client.initialize().catch(err => {
      logger.error(`Client initialization failed for ${sessionCode}:`, err);
      throw err;
    });

    res.json({ 
      success: true, 
      message: 'Session initialization started',
      sessionCode,
      status: 'connecting'
    });

  } catch (error) {
    logger.error(`Error initializing session ${sessionCode}:`, error);
    await redis.del(`qr:${sessionCode}`);
    
    await updateSessionInDB(sessionCode, { 
      status: 'error',
      qr_code: null 
    });
    
    res.status(500).json({ 
      success: false, 
      error: 'Failed to initialize session',
      details: error.message 
    });
  }
});

// Get QR code for session
app.get('/qrcode/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    // Check Redis cache first
    const cachedQR = await redis.get(`qr:${sessionCode}`);
    if (cachedQR) {
      return res.json({ 
        success: true, 
        qrCode: cachedQR
      });
    }

    // Verify session belongs to user and get QR code from database
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    if (!sessionData.qr_code) {
      return res.status(404).json({ 
        success: false, 
        error: 'QR code not available. Please initialize session first.' 
      });
    }

    res.json({ 
      success: true, 
      qrCode: sessionData.qr_code 
    });

  } catch (error) {
    logger.error(`Error getting QR code for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get QR code',
      details: error.message 
    });
  }
});

// Get session status with optimized checks
app.get('/status/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    const [sessionData, cachedQR] = await Promise.all([
      getSessionFromDB(sessionCode, userId),
      redis.get(`qr:${sessionCode}`)
    ]);

    if (!sessionData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    const hasClient = clients.has(sessionCode);
    let actualStatus = sessionData.status;
    let qrCode = cachedQR || sessionData.qr_code;

    if (hasClient) {
      try {
        const { client } = clients.get(sessionCode);
        const state = await client.getState();
        
        if (state === 'CONNECTED') {
          actualStatus = 'connected';
          qrCode = null;
        } else if (state === 'QR_SCAN_COMPLETE') {
          actualStatus = 'connecting';
          qrCode = null;
        }
        
        // Update last activity
        clients.get(sessionCode).lastActivity = Date.now();
        
        await updateSessionInDB(sessionCode, { 
          status: actualStatus,
          qr_code: qrCode,
          last_connected_at: actualStatus === 'connected' ? new Date().toISOString() : sessionData.last_connected_at
        });
      } catch (error) {
        logger.error('Error checking client state:', error);
        actualStatus = 'disconnected';
      }
    }
    
    res.json({ 
      success: true,
      connected: actualStatus === 'connected',
      status: actualStatus,
      lastConnected: sessionData.last_connected_at,
      hasClient,
      qrCode: actualStatus === 'connected' ? null : qrCode
    });

  } catch (error) {
    logger.error(`Error getting status for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get session status' 
    });
  }
});

// Send message to a single contact with optimized validation
app.post('/send/:sessionCode/:number/:message', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode, number, message } = req.params;
  const userId = req.user.id;
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    // Validate phone number before checking client
    if (!validatePhoneNumber(number)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Invalid phone number format' 
      });
    }

    const clientData = clients.get(sessionCode);
    if (!clientData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not connected. Please connect first.' 
      });
    }

    const { client } = clientData;
    
    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      return res.status(400).json({ 
        success: false, 
        error: 'WhatsApp client is not connected',
        state 
      });
    }

    const formattedNumber = formatPhoneNumber(number);
    const decodedMessage = decodeURIComponent(message);

    // Check if number exists on WhatsApp
    const isRegistered = await client.isRegisteredUser(formattedNumber);
    if (!isRegistered) {
      return res.status(400).json({ 
        success: false, 
        error: 'Number is not registered on WhatsApp' 
      });
    }

    // Send message
    const sentMessage = await client.sendMessage(formattedNumber, decodedMessage);
    
    // Update session last activity
    clientData.lastActivity = Date.now();
    await updateSessionInDB(sessionCode, { 
      last_connected_at: new Date().toISOString() 
    });
    
    res.json({ 
      success: true, 
      messageId: sentMessage.id._serialized,
      to: number,
      message: decodedMessage,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    logger.error(`Error sending message in session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send message',
      details: error.message 
    });
  }
});

// Optimized bulk message sending with batching
app.post('/send-bulk/:sessionCode', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode } = req.params;
  const { contacts, message, interval = 5000 } = req.body;
  const userId = req.user.id;
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    const clientData = clients.get(sessionCode);
    if (!clientData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not connected. Please connect first.' 
      });
    }

    const { client } = clientData;
    
    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      return res.status(400).json({ 
        success: false, 
        error: 'WhatsApp client is not connected',
        state 
      });
    }

    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ 
        success: false, 
        error: 'Contacts array is required and cannot be empty' 
      });
    }

    if (!message || message.trim().length === 0) {
      return res.status(400).json({ 
        success: false, 
        error: 'Message is required' 
      });
    }

    // Process contacts in batches for better performance
    const BATCH_SIZE = 10;
    const results = [];
    const maxInterval = Math.max(interval, 3000); // Minimum 3 seconds between batches
    
    for (let i = 0; i < contacts.length; i += BATCH_SIZE) {
      const batch = contacts.slice(i, i + BATCH_SIZE);
      const batchResults = await Promise.all(batch.map(async (contact) => {
        try {
          // Validate phone number
          if (!validatePhoneNumber(contact.phone)) {
            return {
              contact: contact.phone,
              status: 'failed',
              error: 'Invalid phone number format'
            };
          }

          const formattedNumber = formatPhoneNumber(contact.phone);
          
          // Check if number exists on WhatsApp
          const isRegistered = await client.isRegisteredUser(formattedNumber);
          if (!isRegistered) {
            return {
              contact: contact.phone,
              status: 'failed',
              error: 'Number not registered on WhatsApp'
            };
          }

          // Send message
          const sentMessage = await client.sendMessage(
            formattedNumber, 
            contact.message || message
          );
          
          return {
            contact: contact.phone,
            status: 'sent',
            messageId: sentMessage.id._serialized,
            timestamp: new Date().toISOString()
          };
        } catch (error) {
          logger.error(`Error sending to ${contact.phone}:`, error);
          return {
            contact: contact.phone,
            status: 'failed',
            error: error.message
          };
        }
      }));

      results.push(...batchResults);
      
      // Wait before sending next batch (except for the last one)
      if (i + BATCH_SIZE < contacts.length) {
        await new Promise(resolve => setTimeout(resolve, maxInterval));
      }
    }

    // Update session last activity
    clientData.lastActivity = Date.now();
    await updateSessionInDB(sessionCode, { 
      last_connected_at: new Date().toISOString() 
    });
    
    const successCount = results.filter(r => r.status === 'sent').length;
    const failedCount = results.filter(r => r.status === 'failed').length;
    
    res.json({ 
      success: true,
      summary: {
        total: contacts.length,
        sent: successCount,
        failed: failedCount
      },
      results
    });

  } catch (error) {
    logger.error(`Error in bulk send for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send bulk messages',
      details: error.message 
    });
  }
});

// Disconnect session with cleanup
app.post('/disconnect/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    const clientData = clients.get(sessionCode);
    
    if (clientData) {
      try {
        const { client } = clientData;
        await client.logout();
        await client.destroy();
      } catch (error) {
        logger.error(`Error destroying client ${sessionCode}:`, error);
      }
      clients.delete(sessionCode);
      await redis.del(`qr:${sessionCode}`);
    }
    
    // Update session status in database
    await updateSessionInDB(sessionCode, { 
      status: 'disconnected',
      qr_code: null 
    });

    res.json({ 
      success: true, 
      message: 'Session disconnected successfully' 
    });

  } catch (error) {
    logger.error(`Error disconnecting session ${sessionCode}:`, error);
    
    // Force cleanup
    clients.delete(sessionCode);
    await redis.del(`qr:${sessionCode}`);
    await updateSessionInDB(sessionCode, { 
      status: 'disconnected',
      qr_code: null 
    });
    
    res.json({ 
      success: true, 
      message: 'Session disconnected (forced cleanup)',
      warning: error.message 
    });
  }
});

// Get user's sessions with optimized query
app.get('/sessions', verifyAuth, async (req, res) => {
  const userId = req.user.id;
  
  try {
    const { data: sessions, error } = await supabase
      .from('sessions')
      .select('*')
      .eq('user_id', userId)
      .order('created_at', { ascending: false });

    if (error) {
      throw error;
    }

    const sessionsWithStatus = sessions.map(session => ({
      ...session,
      hasClient: clients.has(session.session_code),
      isActive: clients.has(session.session_code) && session.status === 'connected'
    }));
    
    res.json({ 
      success: true, 
      sessions: sessionsWithStatus,
      total: sessions.length 
    });

  } catch (error) {
    logger.error('Error fetching user sessions:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to fetch sessions' 
    });
  }
});

// Validate phone number endpoint
app.post('/validate-phone', (req, res) => {
  const { phone } = req.body;
  
  if (!phone) {
    return res.status(400).json({ 
      success: false, 
      error: 'Phone number is required' 
    });
  }

  const isValid = validatePhoneNumber(phone);
  const formatted = isValid ? formatPhoneNumber(phone) : null;
  
  res.json({ 
    success: true,
    valid: isValid,
    original: phone,
    formatted: formatted ? formatted.replace('@c.us', '') : null
  });
});

// Error handling middleware
app.use((error, req, res, next) => {
  logger.error('Unhandled error:', error);
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? error.message : undefined
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ 
    success: false, 
    error: 'Endpoint not found' 
  });
});

// Graceful shutdown with cleanup
async function gracefulShutdown() {
  logger.info('Shutdown signal received, shutting down gracefully...');
  
  const cleanupPromises = [];
  for (const [sessionCode, { client }] of clients.entries()) {
    cleanupPromises.push(
      client.destroy()
        .then(() => redis.del(`qr:${sessionCode}`))
        .then(() => updateSessionInDB(sessionCode, { 
          status: 'disconnected',
          qr_code: null 
        }))
        .catch(err => logger.error(`Error disconnecting session ${sessionCode}:`, err))
    );
  }
  
  await Promise.allSettled(cleanupPromises);
  await redis.quit();
  process.exit(0);
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Start server
app.listen(PORT, () => {
  logger.info(`🚀 WhatsApp Bulk Sender Backend running on port ${PORT}`);
  logger.info(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
  logger.info(`💾 Sessions directory: ${SESSIONS_DIR}`);
  logger.info(`🔗 Health check: http://localhost:${PORT}/health`);
  logger.info(`🗄️  Supabase connected: ${!!supabase}`);
  logger.info(`🔴 Redis connected: ${redis.status === 'ready'}`);
  logger.info(`👷 Worker ${cluster.worker?.id || 'master'} started`);
});

module.exports = app;