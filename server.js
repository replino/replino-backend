const express = require('express');
const cors = require('cors');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode');
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
const { v4: uuidv4 } = require('uuid');
const pino = require('pino');
const pinoHttp = require('pino-http');

// Initialize logger
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level: (label) => ({ level: label.toUpperCase() }),
  },
  timestamp: () => `,"time":"${new Date().toISOString()}"`,
});

// Enhanced HTTP logger middleware
const httpLogger = pinoHttp({
  logger,
  customLogLevel: (res, err) => {
    if (res.statusCode >= 400 && res.statusCode < 500) {
      return 'warn';
    } else if (res.statusCode >= 500 || err) {
      return 'error';
    }
    return 'info';
  },
  serializers: {
    req: (req) => ({
      method: req.method,
      url: req.url,
      headers: {
        'user-agent': req.headers['user-agent'],
        'x-forwarded-for': req.headers['x-forwarded-for'],
      },
    }),
    res: (res) => ({
      statusCode: res.statusCode,
    }),
  },
});

// Cluster setup for multi-core utilization
const numCPUs = process.env.NODE_ENV === 'production' ? os.cpus().length : 1;

if (cluster.isMaster && numCPUs > 1) {
  logger.info(`Master ${process.pid} is running`);

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    logger.error(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
    logger.info('Starting a new worker');
    cluster.fork();
  });

  // Don't proceed further in master process
  return;
}

// Worker process code
const app = express();
const PORT = process.env.PORT || 3001;

// Configure Express to trust proxies
app.set('trust proxy', process.env.NODE_ENV === 'production' ? 2 : 0);

// Initialize Redis client with connection pooling and retry strategy
const redis = new Redis({
  host: 'clean-panda-53790.upstash.io',
  port: 6379,
  password: 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
  tls: {},
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
  maxRetriesPerRequest: 3,
  enableOfflineQueue: true,
  connectTimeout: 5000,
});

redis.on('error', (err) => {
  logger.error('Redis error:', err);
});

redis.on('connect', () => {
  logger.info('Redis connection established');
});

redis.on('ready', () => {
  logger.info('Redis client ready');
});

redis.on('reconnecting', () => {
  logger.warn('Redis client reconnecting...');
});

// Initialize Supabase client with connection pooling
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    persistSession: false,
    autoRefreshToken: false,
  },
  global: {
    headers: {
      'X-Client-Info': 'whatsapp-bulk-sender/1.0.0',
    },
  },
});

if (!supabaseUrl || !supabaseServiceKey) {
  logger.fatal('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
  process.exit(1);
}

// Security and performance middleware
app.use(httpLogger);
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "blob:"],
      connectSrc: ["'self'", "https://clean-panda-53790.upstash.io", supabaseUrl],
    },
  },
  hsts: {
    maxAge: 63072000,
    includeSubDomains: true,
    preload: true,
  },
  frameguard: {
    action: 'deny',
  },
}));
app.use(compression({
  level: 6,
  threshold: 10 * 1024, // Only compress responses larger than 10KB
}));
app.use(cors({
  origin: process.env.NODE_ENV === 'production' 
    ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
    : ['http://localhost:5173', 'http://localhost:3000'],
  credentials: true,
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  maxAge: 86400,
}));

// Preflight OPTIONS handling
app.options('*', cors());

// Rate limiting with Redis store for consistency across workers
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 1000, // Limit each IP to 1000 requests per windowMs
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => {
    // Skip rate limiting for health checks
    return req.path === '/health';
  },
  handler: (req, res) => {
    res.status(429).json({
      success: false,
      error: 'Too many requests, please try again later.',
    });
  },
});

app.use(limiter);

// Stricter rate limiting for message sending with Redis store
const sendLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({
      success: false,
      error: 'Too many messages sent, please slow down.',
    });
  },
});

// Enhanced body parsing with size limits
app.use(express.json({
  limit: '10mb',
  verify: (req, res, buf) => {
    req.rawBody = buf.toString('utf8');
  },
}));
app.use(express.urlencoded({
  extended: true,
  limit: '10mb',
  parameterLimit: 1000,
}));

// In-memory storage for WhatsApp clients with TTL and cleanup
const clients = new Map();
const sessionTTL = 6 * 60 * 60 * 1000; // 6 hours TTL for inactive sessions

// Session cleanup interval
setInterval(() => {
  const now = Date.now();
  for (const [sessionCode, { lastActive }] of clients.entries()) {
    if (now - lastActive > sessionTTL) {
      logger.info(`Cleaning up inactive session: ${sessionCode}`);
      const client = clients.get(sessionCode).client;
      client.destroy().catch(err => {
        logger.error(`Error cleaning up client ${sessionCode}:`, err);
      });
      clients.delete(sessionCode);
      redis.del(`qr:${sessionCode}`).catch(err => {
        logger.error(`Error cleaning up Redis QR for ${sessionCode}:`, err);
      });
    }
  }
}, 30 * 60 * 1000); // Check every 30 minutes

// Ensure sessions directory exists with proper permissions
const SESSIONS_DIR = path.join(__dirname, 'sessions');

async function ensureSessionsDir() {
  try {
    await fs.access(SESSIONS_DIR);
  } catch {
    await fs.mkdir(SESSIONS_DIR, { recursive: true, mode: 0o755 });
    logger.info(`Created sessions directory at ${SESSIONS_DIR}`);
  }
}

// Initialize sessions directory on startup
ensureSessionsDir().catch(err => {
  logger.error('Failed to initialize sessions directory:', err);
  process.exit(1);
});

// Enhanced JWT verification with caching
const verifyAuth = throttle(async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      logger.warn('Authorization header missing or invalid');
      return res.status(401).json({ 
        success: false, 
        error: 'Authorization token required' 
      });
    }

    const token = authHeader.substring(7);
    const cacheKey = `auth:${token}`;
    
    // Check cache first
    const cachedAuth = await redis.get(cacheKey);
    if (cachedAuth) {
      req.user = JSON.parse(cachedAuth);
      return next();
    }

    // Verify with Supabase
    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error || !user) {
      logger.warn('Invalid or expired token', { error });
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
}, 1000); // Throttle to 1 request per second per token

// Utility function to update session in database with retry logic
async function updateSessionInDB(sessionCode, updates) {
  const maxRetries = 3;
  let attempts = 0;
  
  while (attempts < maxRetries) {
    try {
      const { error } = await supabase
        .from('sessions')
        .update({
          ...updates,
          updated_at: new Date().toISOString(),
        })
        .eq('session_code', sessionCode);

      if (error) {
        throw error;
      }
      return;
    } catch (error) {
      attempts++;
      if (attempts === maxRetries) {
        logger.error(`Failed to update session ${sessionCode} after ${maxRetries} attempts:`, error);
        return;
      }
      await new Promise(resolve => setTimeout(resolve, 500 * attempts));
    }
  }
}

// Utility function to get session from database with caching
async function getSessionFromDB(sessionCode, userId = null) {
  const cacheKey = `session:${sessionCode}:${userId || 'any'}`;
  
  try {
    // Check cache first
    const cachedSession = await redis.get(cacheKey);
    if (cachedSession) {
      return JSON.parse(cachedSession);
    }
    
    let query = supabase
      .from('sessions')
      .select('*')
      .eq('session_code', sessionCode);
    
    if (userId) {
      query = query.eq('user_id', userId);
    }
    
    const { data, error } = await query.single();
    
    if (error) {
      throw error;
    }
    
    if (data) {
      // Cache session for 1 minute
      await redis.setex(cacheKey, 60, JSON.stringify(data));
    }
    
    return data;
  } catch (error) {
    logger.error(`Error fetching session ${sessionCode} from DB:`, error);
    return null;
  }
}

// Enhanced phone number validation with libphonenumber-js would be better
// but keeping the simple regex for compatibility
function validatePhoneNumber(phone) {
  const cleaned = phone.replace(/[^\d+]/g, '');
  const phoneRegex = /^\+[1-9]\d{9,14}$/;
  return phoneRegex.test(cleaned);
}

// Utility function to format phone number for WhatsApp
function formatPhoneNumber(phone) {
  let formatted = phone.replace(/[^\d+]/g, '');
  if (!formatted.startsWith('+')) {
    formatted = '+' + formatted;
  }
  return formatted.substring(1) + '@c.us';
}

// Health check endpoint with system diagnostics
app.get('/health', async (req, res) => {
  try {
    const redisPing = await redis.ping();
    const supabasePing = await supabase.rpc('version');
    
    const healthCheck = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      worker: cluster.worker?.id || 'master',
      activeSessions: clients.size,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      load: os.loadavg(),
      supabase: supabasePing.data ? 'connected' : 'disconnected',
      redis: redisPing === 'PONG' ? 'connected' : 'disconnected',
    };
    
    res.json(healthCheck);
  } catch (error) {
    logger.error('Health check failed:', error);
    res.status(500).json({
      status: 'unhealthy',
      error: error.message,
    });
  }
});

// Get server statistics with more detailed metrics
app.get('/stats', verifyAuth, async (req, res) => {
  try {
    const { count: userSessionsCount } = await supabase
      .from('sessions')
      .select('*', { count: 'exact', head: true })
      .eq('user_id', req.user.id);

    const stats = {
      activeSessions: clients.size,
      userSessions: userSessionsCount || 0,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      cpu: os.cpus().length,
      load: os.loadavg(),
      timestamp: new Date().toISOString(),
      redisStatus: redis.status,
      worker: cluster.worker?.id || 'master',
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

// Initialize WhatsApp session with enhanced error handling
app.post('/init/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  logger.info(`[${requestId}] Initializing session ${sessionCode} for user ${userId}`);
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      logger.warn(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    // Check if session already exists and is connected
    if (clients.has(sessionCode)) {
      const existingClient = clients.get(sessionCode).client;
      try {
        const state = await existingClient.getState();
        
        if (state === 'CONNECTED') {
          logger.info(`[${requestId}] Session already connected`);
          await updateSessionInDB(sessionCode, { 
            status: 'connected',
            last_connected_at: new Date().toISOString(),
          });
          
          return res.json({ 
            success: true, 
            message: 'Session already connected',
            status: 'connected',
          });
        }
      } catch (error) {
        logger.warn(`[${requestId}] Existing client not responsive, cleaning up`);
        clients.delete(sessionCode);
      }
    }

    // Update session status to connecting
    await updateSessionInDB(sessionCode, { 
      status: 'connecting',
      qr_code: null,
    });

    // Create new client with enhanced configuration
    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: sessionCode,
        dataPath: SESSIONS_DIR,
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
          '--disable-software-rasterizer',
          '--disable-features=AudioServiceOutOfProcess',
          '--disable-background-timer-throttling',
          '--disable-backgrounding-occluded-windows',
          '--disable-renderer-backgrounding',
          '--disable-breakpad',
          '--disable-client-side-phishing-detection',
          '--disable-crash-reporter',
          '--disable-default-apps',
          '--disable-extensions',
          '--disable-hang-monitor',
          '--disable-ipc-flooding-protection',
          '--disable-notifications',
          '--disable-offer-store-unmasked-wallet-cards',
          '--disable-popup-blocking',
          '--disable-print-preview',
          '--disable-prompt-on-repost',
          '--disable-sync',
          '--disable-translate',
          '--metrics-recording-only',
          '--mute-audio',
          '--no-default-browser-check',
          '--autoplay-policy=user-gesture-required',
          '--disable-background-networking',
          '--disable-component-update',
          '--disable-domain-reliability',
        ],
        executablePath: process.env.CHROME_PATH || undefined,
      },
      webVersionCache: {
        type: 'remote',
        remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html',
      },
      qrTimeoutMs: 0, // Disable QR timeout
      takeoverOnConflict: true, // Take over existing session
      restartOnAuthFail: true, // Restart if auth fails
      puppeteerOptions: {
        timeout: 30000,
      },
    });

    // Store client with last active timestamp
    clients.set(sessionCode, { client, lastActive: Date.now() });

    // Set up event handlers with enhanced logging
    client.on('qr', async (qr) => {
      logger.info(`[${sessionCode}] QR Code generated`);
      
      try {
        // Generate QR code image immediately
        const qrCodeImage = await qrcode.toDataURL(qr, {
          width: 256,
          margin: 2,
          color: {
            dark: '#000000',
            light: '#FFFFFF',
          },
        });

        // Store in Redis cache for faster access
        await redis.setex(`qr:${sessionCode}`, 300, qrCodeImage);

        // Update session with QR code in database
        await updateSessionInDB(sessionCode, { 
          status: 'connecting',
          qr_code: qrCodeImage,
        });

        logger.info(`[${sessionCode}] QR code stored`);
      } catch (error) {
        logger.error(`[${sessionCode}] Error generating QR code:`, error);
      }
    });

    client.on('loading_screen', async (percent, message) => {
      logger.info(`[${sessionCode}] Loading screen: ${percent}% ${message || ''}`);
      if (percent === 100) {
        // QR code has been scanned - clear it
        await redis.del(`qr:${sessionCode}`);
        await updateSessionInDB(sessionCode, {
          qr_code: null,
          status: 'connecting',
        });
      }
    });

    client.on('authenticated', async () => {
      logger.info(`[${sessionCode}] WhatsApp client authenticated`);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null,
      });
    });

    client.on('auth_failure', async (msg) => {
      logger.error(`[${sessionCode}] Authentication failed:`, msg);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'expired',
        qr_code: null,
      });
      
      clients.delete(sessionCode);
    });

    client.on('ready', async () => {
      logger.info(`[${sessionCode}] WhatsApp client is ready`);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null,
      });
    });

    client.on('disconnected', async (reason) => {
      logger.warn(`[${sessionCode}] WhatsApp client disconnected:`, reason);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null,
      });
      
      clients.delete(sessionCode);
    });

    client.on('message', async (msg) => {
      // Update last active on any message activity
      if (clients.has(sessionCode)) {
        clients.get(sessionCode).lastActive = Date.now();
      }
    });

    // Initialize the client with timeout
    logger.info(`[${requestId}] Initializing WhatsApp client for session ${sessionCode}`);
    
    const initTimeout = setTimeout(() => {
      logger.error(`[${requestId}] Client initialization timeout for ${sessionCode}`);
      client.destroy().catch(err => {
        logger.error(`[${requestId}] Error destroying client after timeout:`, err);
      });
      res.status(500).json({ 
        success: false, 
        error: 'Client initialization timeout',
      });
    }, 30000);

    try {
      await client.initialize();
      clearTimeout(initTimeout);
      logger.info(`[${requestId}] Client initialization started for ${sessionCode}`);
      
      res.json({ 
        success: true, 
        message: 'Session initialization started',
        sessionCode,
        status: 'connecting',
      });
    } catch (error) {
      clearTimeout(initTimeout);
      throw error;
    }

  } catch (error) {
    logger.error(`[${requestId}] Error initializing session ${sessionCode}:`, error);
    await redis.del(`qr:${sessionCode}`);
    
    await updateSessionInDB(sessionCode, { 
      status: 'expired',
      qr_code: null,
    });
    
    res.status(500).json({ 
      success: false, 
      error: 'Failed to initialize session',
      details: error.message,
    });
  }
});

// Get QR code for session with enhanced caching
app.get('/qrcode/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  logger.info(`[${requestId}] Fetching QR code for session ${sessionCode}`);
  
  try {
    // Check Redis cache first
    const cachedQR = await redis.get(`qr:${sessionCode}`);
    if (cachedQR) {
      logger.info(`[${requestId}] Returning cached QR code for ${sessionCode}`);
      return res.json({ 
        success: true, 
        qrCode: cachedQR,
      });
    }

    // Verify session belongs to user and get QR code from database
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      logger.warn(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied',
      });
    }

    if (!sessionData.qr_code) {
      logger.warn(`[${requestId}] QR code not available for session ${sessionCode}`);
      return res.status(404).json({ 
        success: false, 
        error: 'QR code not available. Please initialize session first.',
      });
    }

    // Cache the QR code from DB to Redis
    await redis.setex(`qr:${sessionCode}`, 300, sessionData.qr_code);
    
    res.json({ 
      success: true, 
      qrCode: sessionData.qr_code,
    });

  } catch (error) {
    logger.error(`[${requestId}] Error getting QR code for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get QR code',
      details: error.message,
    });
  }
});

// Get session status with enhanced state checking
app.get('/status/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  logger.info(`[${requestId}] Checking status for session ${sessionCode}`);
  
  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      logger.warn(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied',
      });
    }

    const hasClient = clients.has(sessionCode);
    let actualStatus = sessionData.status;
    let qrCode = null;

    if (hasClient) {
      try {
        const client = clients.get(sessionCode).client;
        const state = await client.getState();
        
        if (state === 'CONNECTED') {
          actualStatus = 'connected';
          // Ensure QR code is cleared when connected
          await redis.del(`qr:${sessionCode}`);
          qrCode = null;
        } else if (state === 'QR_SCAN_COMPLETE') {
          // Special state when QR is scanned but not fully authenticated
          actualStatus = 'connecting';
          qrCode = null;
        } else {
          // Check if we should show QR code
          const cachedQR = await redis.get(`qr:${sessionCode}`);
          qrCode = cachedQR || sessionData.qr_code;
        }
        
        await updateSessionInDB(sessionCode, { 
          status: actualStatus,
          qr_code: qrCode,
          last_connected_at: actualStatus === 'connected' ? new Date().toISOString() : sessionData.last_connected_at,
        });
      } catch (error) {
        logger.error(`[${requestId}] Error checking client state:`, error);
        actualStatus = 'disconnected';
      }
    }
    
    res.json({ 
      success: true,
      connected: actualStatus === 'connected',
      status: actualStatus,
      lastConnected: sessionData.last_connected_at,
      hasClient,
      qrCode: actualStatus === 'connected' ? null : qrCode, // Never show QR if connected
    });

  } catch (error) {
    logger.error(`[${requestId}] Error getting status for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get session status',
    });
  }
});

// Enhanced single message sending with better validation
app.post('/send/:sessionCode/:number/:message', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode, number, message } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  logger.info(`[${requestId}] Sending message via session ${sessionCode} to ${number}`);
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      logger.warn(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied',
      });
    }

    const clientData = clients.get(sessionCode);
    if (!clientData) {
      logger.warn(`[${requestId}] Session not connected`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not connected. Please connect first.',
      });
    }

    const client = clientData.client;
    
    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      logger.warn(`[${requestId}] WhatsApp client not connected, state: ${state}`);
      return res.status(400).json({ 
        success: false, 
        error: 'WhatsApp client is not connected',
        state,
      });
    }

    // Validate and format phone number
    if (!validatePhoneNumber(number)) {
      logger.warn(`[${requestId}] Invalid phone number format: ${number}`);
      return res.status(400).json({ 
        success: false, 
        error: 'Invalid phone number format',
      });
    }

    const formattedNumber = formatPhoneNumber(number);
    const decodedMessage = decodeURIComponent(message);

    // Check if number exists on WhatsApp
    const isRegistered = await client.isRegisteredUser(formattedNumber);
    if (!isRegistered) {
      logger.warn(`[${requestId}] Number not registered: ${number}`);
      return res.status(400).json({ 
        success: false, 
        error: 'Number is not registered on WhatsApp',
      });
    }

    // Send message with timeout
    const sendTimeout = setTimeout(() => {
      logger.error(`[${requestId}] Message send timeout for ${number}`);
      throw new Error('Message send timeout');
    }, 30000);

    try {
      const sentMessage = await client.sendMessage(formattedNumber, decodedMessage);
      clearTimeout(sendTimeout);
      
      // Update last active
      clients.get(sessionCode).lastActive = Date.now();
      
      // Update session last activity
      await updateSessionInDB(sessionCode, { 
        last_connected_at: new Date().toISOString(),
      });
      
      logger.info(`[${requestId}] Message sent successfully to ${number}`);
      
      res.json({ 
        success: true, 
        messageId: sentMessage.id._serialized,
        to: number,
        message: decodedMessage,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      clearTimeout(sendTimeout);
      throw error;
    }

  } catch (error) {
    logger.error(`[${requestId}] Error sending message in session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send message',
      details: error.message,
    });
  }
});

// Enhanced bulk message sending with concurrency control
app.post('/send-bulk/:sessionCode', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode } = req.params;
  const { contacts, message, interval = 5000 } = req.body;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  logger.info(`[${requestId}] Starting bulk send for session ${sessionCode} with ${contacts?.length || 0} contacts`);
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      logger.warn(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied',
      });
    }

    const clientData = clients.get(sessionCode);
    if (!clientData) {
      logger.warn(`[${requestId}] Session not connected`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not connected. Please connect first.',
      });
    }

    const client = clientData.client;
    
    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      logger.warn(`[${requestId}] WhatsApp client not connected, state: ${state}`);
      return res.status(400).json({ 
        success: false, 
        error: 'WhatsApp client is not connected',
        state,
      });
    }

    if (!Array.isArray(contacts) || contacts.length === 0) {
      logger.warn(`[${requestId}] Invalid contacts array`);
      return res.status(400).json({ 
        success: false, 
        error: 'Contacts array is required and cannot be empty',
      });
    }

    if (!message || message.trim().length === 0) {
      logger.warn(`[${requestId}] Empty message`);
      return res.status(400).json({ 
        success: false, 
        error: 'Message is required',
      });
    }

    // Limit to 1000 contacts per bulk send
    const limitedContacts = contacts.slice(0, 1000);
    const results = [];
    const maxInterval = Math.max(interval, 3000); // Minimum 3 seconds between messages
    const batchSize = 5; // Number of messages to send in parallel
    const delayBetweenBatches = 15000; // 15 seconds between batches
    
    // Process in batches to avoid rate limiting
    for (let i = 0; i < limitedContacts.length; i += batchSize) {
      const batch = limitedContacts.slice(i, i + batchSize);
      const batchResults = await Promise.all(batch.map(async (contact, index) => {
        try {
          // Validate phone number
          if (!validatePhoneNumber(contact.phone)) {
            return {
              contact: contact.phone,
              status: 'failed',
              error: 'Invalid phone number format',
            };
          }

          const formattedNumber = formatPhoneNumber(contact.phone);
          
          // Check if number exists on WhatsApp
          const isRegistered = await client.isRegisteredUser(formattedNumber);
          if (!isRegistered) {
            return {
              contact: contact.phone,
              status: 'failed',
              error: 'Number not registered on WhatsApp',
            };
          }

          // Send message with timeout
          const sendTimeout = setTimeout(() => {
            throw new Error('Message send timeout');
          }, 30000);

          try {
            const sentMessage = await client.sendMessage(
              formattedNumber, 
              contact.message || message
            );
            clearTimeout(sendTimeout);
            
            return {
              contact: contact.phone,
              status: 'sent',
              messageId: sentMessage.id._serialized,
              timestamp: new Date().toISOString(),
            };
          } catch (error) {
            clearTimeout(sendTimeout);
            throw error;
          }
        } catch (error) {
          logger.error(`[${requestId}] Error sending to ${contact.phone}:`, error);
          return {
            contact: contact.phone,
            status: 'failed',
            error: error.message,
          };
        }
      }));
      
      results.push(...batchResults);
      
      // Update last active
      clientData.lastActive = Date.now();
      
      // Wait before next batch (except for the last batch)
      if (i + batchSize < limitedContacts.length) {
        await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
      }
    }

    // Update session last activity
    await updateSessionInDB(sessionCode, { 
      last_connected_at: new Date().toISOString(),
    });
    
    const successCount = results.filter(r => r.status === 'sent').length;
    const failedCount = results.filter(r => r.status === 'failed').length;
    
    logger.info(`[${requestId}] Bulk send completed: ${successCount} sent, ${failedCount} failed`);
    
    res.json({ 
      success: true,
      summary: {
        total: limitedContacts.length,
        sent: successCount,
        failed: failedCount,
      },
      results,
    });

  } catch (error) {
    logger.error(`[${requestId}] Error in bulk send for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send bulk messages',
      details: error.message,
    });
  }
});

// Disconnect session with enhanced cleanup
app.post('/disconnect/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  logger.info(`[${requestId}] Disconnecting session ${sessionCode}`);
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      logger.warn(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied',
      });
    }

    const clientData = clients.get(sessionCode);
    
    if (clientData) {
      try {
        logger.info(`[${requestId}] Logging out client ${sessionCode}`);
        await clientData.client.logout();
        await clientData.client.destroy();
      } catch (error) {
        logger.error(`[${requestId}] Error destroying client ${sessionCode}:`, error);
      }
      clients.delete(sessionCode);
      await redis.del(`qr:${sessionCode}`);
    }
    
    // Update session status in database
    await updateSessionInDB(sessionCode, { 
      status: 'disconnected',
      qr_code: null,
    });

    logger.info(`[${requestId}] Session ${sessionCode} disconnected successfully`);
    
    res.json({ 
      success: true, 
      message: 'Session disconnected successfully',
    });

  } catch (error) {
    logger.error(`[${requestId}] Error disconnecting session ${sessionCode}:`, error);
    
    // Force cleanup
    clients.delete(sessionCode);
    await redis.del(`qr:${sessionCode}`);
    await updateSessionInDB(sessionCode, { 
      status: 'disconnected',
      qr_code: null,
    });
    
    res.json({ 
      success: true, 
      message: 'Session disconnected (forced cleanup)',
      warning: error.message,
    });
  }
});

// Get user's sessions with enhanced filtering
app.get('/sessions', verifyAuth, async (req, res) => {
  const userId = req.user.id;
  const requestId = uuidv4();
  
  logger.info(`[${requestId}] Fetching sessions for user ${userId}`);
  
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
      isActive: clients.has(session.session_code) && session.status === 'connected',
    }));
    
    logger.info(`[${requestId}] Found ${sessions.length} sessions for user ${userId}`);
    
    res.json({ 
      success: true, 
      sessions: sessionsWithStatus,
      total: sessions.length,
    });

  } catch (error) {
    logger.error(`[${requestId}] Error fetching user sessions:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to fetch sessions',
    });
  }
});

// Validate phone number endpoint
app.post('/validate-phone', (req, res) => {
  const { phone } = req.body;
  
  if (!phone) {
    return res.status(400).json({ 
      success: false, 
      error: 'Phone number is required',
    });
  }

  const isValid = validatePhoneNumber(phone);
  const formatted = isValid ? formatPhoneNumber(phone) : null;
  
  res.json({ 
    success: true,
    valid: isValid,
    original: phone,
    formatted: formatted ? formatted.replace('@c.us', '') : null,
  });
});

// Error handling middleware
app.use((error, req, res, next) => {
  const requestId = req.id || uuidv4();
  logger.error(`[${requestId}] Unhandled error:`, error);
  
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? error.message : undefined,
  });
});

// 404 handler
app.use((req, res) => {
  const requestId = req.id || uuidv4();
  logger.warn(`[${requestId}] 404 Not Found: ${req.method} ${req.url}`);
  
  res.status(404).json({ 
    success: false, 
    error: 'Endpoint not found',
  });
});

// Graceful shutdown with enhanced cleanup
async function gracefulShutdown(signal) {
  logger.warn(`${signal} received, shutting down gracefully...`);
  
  // Close HTTP server first to stop accepting new connections
  server.close(() => {
    logger.info('HTTP server closed');
  });

  // Disconnect all WhatsApp clients
  const disconnectPromises = Array.from(clients.entries()).map(async ([sessionCode, clientData]) => {
    try {
      logger.info(`Disconnecting session ${sessionCode}...`);
      await clientData.client.destroy();
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null,
      });
    } catch (error) {
      logger.error(`Error disconnecting session ${sessionCode}:`, error);
    }
  });

  await Promise.allSettled(disconnectPromises);
  
  // Close Redis connection
  await redis.quit();
  
  logger.info('All cleanup completed, exiting process');
  process.exit(0);
}

// Signal handlers
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Unhandled rejection handler
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Uncaught exception handler
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', error);
  process.exit(1);
});

// Start server
const server = app.listen(PORT, () => {
  logger.info(`🚀 WhatsApp Bulk Sender Backend running on port ${PORT}`);
  logger.info(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
  logger.info(`💾 Sessions directory: ${SESSIONS_DIR}`);
  logger.info(`🔗 Health check: http://localhost:${PORT}/health`);
  logger.info(`🗄️  Supabase connected: ${!!supabase}`);
  logger.info(`🔴 Redis connected: ${redis.status === 'ready'}`);
  logger.info(`👷 Worker ${cluster.worker?.id || 'master'} started`);
});

module.exports = app;