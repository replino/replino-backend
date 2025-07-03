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
const { Worker } = require('worker_threads');
const { v4: uuidv4 } = require('uuid');
const pino = require('pino');

// Enhanced Configuration with environment validation
const config = {
  PORT: parseInt(process.env.PORT || '3001', 10),
  NODE_ENV: process.env.NODE_ENV || 'development',
  SESSIONS_DIR: path.join(__dirname, 'sessions'),
  QR_CODE_TTL: 300, // 5 minutes in seconds
  MESSAGE_INTERVAL: 3000, // Minimum 3 seconds between messages
  MAX_WORKERS: parseInt(process.env.MAX_WORKERS || os.cpus().length, 10),
  MAX_MESSAGES_PER_MINUTE: parseInt(process.env.MAX_MESSAGES_PER_MINUTE || '60', 10),
  REDIS_CONFIG: {
    host: process.env.REDIS_HOST || 'clean-panda-53790.upstash.io',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD || 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
    tls: process.env.REDIS_TLS === 'true' ? {} : undefined,
    connectTimeout: 10000,
    maxRetriesPerRequest: 1
  },
  PUPPETEER_CONFIG: {
    headless: process.env.PUPPETEER_HEADLESS !== 'false',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process',
      '--disable-gpu'
    ],
    executablePath: process.env.CHROME_PATH || undefined
  },
  SECURITY: {
    JWT_SECRET: process.env.JWT_SECRET || 'default-secret-change-me',
    SESSION_TIMEOUT: 3600 // 1 hour in seconds
  }
};

// Validate configuration
if (isNaN(config.PORT)) throw new Error('Invalid PORT configuration');
if (isNaN(config.MAX_WORKERS)) throw new Error('Invalid MAX_WORKERS configuration');
if (isNaN(config.MAX_MESSAGES_PER_MINUTE)) throw new Error('Invalid MAX_MESSAGES_PER_MINUTE configuration');

// Initialize Redis with enhanced error handling and connection pooling
const redis = new Redis(config.REDIS_CONFIG);
redis.on('error', (err) => console.error('Redis error:', err));
redis.on('connect', () => console.log('Redis connected'));
redis.on('ready', () => console.log('Redis ready'));
redis.on('close', () => console.log('Redis connection closed'));
redis.on('reconnecting', () => console.log('Redis reconnecting'));

// Initialize Supabase client with enhanced configuration
const supabase = createClient(
  process.env.SUPABASE_URL || 'https://your-supabase-url.supabase.co',
  process.env.SUPABASE_SERVICE_ROLE_KEY || 'your-service-role-key',
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      detectSessionInUrl: false
    }
  }
);

// Enhanced logger configuration
const logger = pino({
  level: config.NODE_ENV === 'production' ? 'info' : 'debug',
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      translateTime: 'SYS:standard',
      ignore: 'pid,hostname'
    }
  }
});

// Session management with enhanced error handling and cleanup
class SessionManager {
  constructor() {
    this.clients = new Map();
    this.qrGenerators = new Map();
    this.messageQueues = new Map();
    this.cleanupInterval = setInterval(() => this.cleanupStaleSessions(), 60000); // Cleanup every minute
  }

  async getClient(sessionCode) {
    return this.clients.get(sessionCode);
  }

  async addClient(sessionCode, client) {
    this.clients.set(sessionCode, client);
  }

  async removeClient(sessionCode) {
    const client = this.clients.get(sessionCode);
    if (client) {
      try {
        await client.destroy();
      } catch (error) {
        logger.error(`Error destroying client ${sessionCode}:`, error);
      }
    }
    this.clients.delete(sessionCode);
    this.qrGenerators.delete(sessionCode);
    this.messageQueues.delete(sessionCode);
  }

  async cleanupStaleSessions() {
    const now = Date.now();
    for (const [sessionCode, client] of this.clients.entries()) {
      try {
        const state = await client.getState();
        if (state !== 'CONNECTED') {
          logger.info(`Cleaning up stale session: ${sessionCode}`);
          await this.removeClient(sessionCode);
        }
      } catch (error) {
        logger.error(`Error checking state for session ${sessionCode}:`, error);
        await this.removeClient(sessionCode);
      }
    }
  }

  async shutdown() {
    clearInterval(this.cleanupInterval);
    await Promise.all(
      Array.from(this.clients.keys()).map(sessionCode => this.removeClient(sessionCode))
    );
  }
}

const sessionManager = new SessionManager();

// Initialize Express app with enhanced security
const app = express();

// Security and performance middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "blob:"],
      connectSrc: ["'self'", "https://*.upstash.io", process.env.SUPABASE_URL || 'https://your-supabase-url.supabase.co'],
      frameAncestors: ["'none'"],
      formAction: ["'self'"],
      upgradeInsecureRequests: []
    }
  },
  hsts: {
    maxAge: 63072000,
    includeSubDomains: true,
    preload: true
  },
  referrerPolicy: { policy: 'strict-origin-when-cross-origin' }
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
  origin: config.NODE_ENV === 'production' 
    ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
    : ['http://localhost:5173', 'http://localhost:3000'],
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
  maxAge: 86400
}));

// Rate limiting with enhanced protection
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 1000, // limit each IP to 1000 requests per windowMs
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => req.ip === '127.0.0.1' // skip for localhost
});

const sendLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: config.MAX_MESSAGES_PER_MINUTE,
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => req.user?.id || req.ip // limit by user ID or IP
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Ensure sessions directory exists with proper permissions
async function ensureSessionsDir() {
  try {
    await fs.access(config.SESSIONS_DIR);
  } catch {
    await fs.mkdir(config.SESSIONS_DIR, { recursive: true, mode: 0o700 });
    logger.info(`Created sessions directory at ${config.SESSIONS_DIR}`);
  }
}

// Initialize sessions directory on startup
ensureSessionsDir().catch(err => {
  logger.error('Failed to create sessions directory:', err);
  process.exit(1);
});

// QR Code generation with worker pool
const QRWorkerPool = (() => {
  const pool = [];
  const maxWorkers = 4;
  
  const createWorker = () => {
    const worker = new Worker(`
      const qrcode = require('qrcode');
      const { parentPort } = require('worker_threads');
      
      parentPort.on('message', async (qrData) => {
        try {
          const qrCodeImage = await qrcode.toDataURL(qrData, {
            width: 256,
            margin: 2,
            color: {
              dark: '#000000',
              light: '#FFFFFF'
            },
            errorCorrectionLevel: 'H'
          });
          parentPort.postMessage({ success: true, qrCodeImage });
        } catch (error) {
          parentPort.postMessage({ success: false, error: error.message });
        }
      });
    `, { eval: true });
    
    worker.on('error', (err) => logger.error('QR Worker error:', err));
    worker.on('exit', (code) => {
      if (code !== 0) logger.warn(`QR Worker stopped with exit code ${code}`);
    });
    
    return worker;
  };
  
  // Initialize workers
  for (let i = 0; i < maxWorkers; i++) {
    pool.push(createWorker());
  }
  
  return {
    generate: (qrData) => {
      return new Promise((resolve, reject) => {
        const worker = pool.pop() || createWorker();
        
        const onMessage = ({ success, qrCodeImage, error }) => {
          cleanup();
          if (success) {
            resolve(qrCodeImage);
          } else {
            reject(new Error(error));
          }
        };
        
        const onError = (err) => {
          cleanup();
          reject(err);
        };
        
        const onExit = (code) => {
          if (code !== 0) {
            cleanup();
            reject(new Error(`Worker stopped with exit code ${code}`));
          }
        };
        
        const cleanup = () => {
          worker.removeListener('message', onMessage);
          worker.removeListener('error', onError);
          worker.removeListener('exit', onExit);
          pool.push(worker);
        };
        
        worker.once('message', onMessage);
        worker.once('error', onError);
        worker.once('exit', onExit);
        
        worker.postMessage(qrData);
      });
    },
    shutdown: () => {
      pool.forEach(worker => worker.terminate());
    }
  };
})();

// Middleware to verify Supabase JWT token with enhanced security
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
    
    // Verify token structure first
    if (!token.match(/^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+$/)) {
      return res.status(401).json({ 
        success: false, 
        error: 'Invalid token format' 
      });
    }

    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error || !user) {
      logger.warn('Invalid or expired token attempt');
      return res.status(401).json({ 
        success: false, 
        error: 'Invalid or expired token' 
      });
    }

    // Additional security checks
    if (!user.email_verified && config.NODE_ENV === 'production') {
      return res.status(403).json({ 
        success: false, 
        error: 'Email verification required' 
      });
    }

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

// Enhanced database operations with retry logic
async function updateSessionInDB(sessionCode, updates) {
  const maxRetries = 3;
  let retries = 0;
  
  while (retries < maxRetries) {
    try {
      const { error } = await supabase
        .from('sessions')
        .update({
          ...updates,
          updated_at: new Date().toISOString()
        })
        .eq('session_code', sessionCode);

      if (error) throw error;
      return true;
    } catch (error) {
      retries++;
      logger.error(`Attempt ${retries} to update session failed:`, error.message);
      if (retries >= maxRetries) {
        logger.error('Max retries reached updating session in DB');
        return false;
      }
      await new Promise(resolve => setTimeout(resolve, 1000 * retries));
    }
  }
}

async function getSessionFromDB(sessionCode, userId = null) {
  const maxRetries = 3;
  let retries = 0;
  
  while (retries < maxRetries) {
    try {
      let query = supabase
        .from('sessions')
        .select('*')
        .eq('session_code', sessionCode);
      
      if (userId) {
        query = query.eq('user_id', userId);
      }
      
      const { data, error } = await query.single();
      
      if (error) throw error;
      return data;
    } catch (error) {
      retries++;
      logger.error(`Attempt ${retries} to fetch session failed:`, error.message);
      if (retries >= maxRetries) {
        logger.error('Max retries reached fetching session from DB');
        return null;
      }
      await new Promise(resolve => setTimeout(resolve, 1000 * retries));
    }
  }
}

// Enhanced phone number validation
function validatePhoneNumber(phone) {
  if (typeof phone !== 'string') return false;
  
  const cleaned = phone.replace(/[^\d+]/g, '');
  if (cleaned.length < 10 || cleaned.length > 16) return false;
  
  const phoneRegex = /^\+[1-9]\d{9,14}$/;
  return phoneRegex.test(cleaned);
}

function formatPhoneNumber(phone) {
  if (!validatePhoneNumber(phone)) {
    throw new Error('Invalid phone number format');
  }
  
  let formatted = phone.replace(/[^\d+]/g, '');
  if (!formatted.startsWith('+')) {
    formatted = '+' + formatted;
  }
  return formatted.substring(1) + '@c.us';
}

// Health check endpoint with more comprehensive checks
app.get('/health', async (req, res) => {
  try {
    const healthChecks = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      activeSessions: sessionManager.clients.size,
      uptime: process.uptime(),
      memoryUsage: process.memoryUsage(),
      loadAvg: os.loadavg(),
      workers: config.MAX_WORKERS,
      dependencies: {
        supabase: false,
        redis: false
      }
    };

    // Check Supabase connection
    try {
      const { data, error } = await supabase.rpc('version');
      if (!error) healthChecks.dependencies.supabase = true;
    } catch (e) {
      logger.error('Supabase health check failed:', e);
    }

    // Check Redis connection
    try {
      await redis.ping();
      healthChecks.dependencies.redis = true;
    } catch (e) {
      logger.error('Redis health check failed:', e);
    }

    // Determine overall status
    const allHealthy = Object.values(healthChecks.dependencies).every(Boolean);
    healthChecks.status = allHealthy ? 'healthy' : 'degraded';

    res.status(allHealthy ? 200 : 503).json(healthChecks);
  } catch (error) {
    logger.error('Health check failed:', error);
    res.status(500).json({
      status: 'unhealthy',
      error: 'Health check failed',
      details: error.message
    });
  }
});

// Get server statistics with enhanced data
app.get('/stats', verifyAuth, async (req, res) => {
  try {
    const { count: userSessionsCount } = await supabase
      .from('sessions')
      .select('*', { count: 'exact', head: true })
      .eq('user_id', req.user.id);

    const redisInfo = await redis.info();
    const memoryUsage = process.memoryUsage();
    const systemMemory = {
      total: os.totalmem(),
      free: os.freemem(),
      used: os.totalmem() - os.freemem()
    };

    const stats = {
      activeSessions: sessionManager.clients.size,
      userSessions: userSessionsCount || 0,
      uptime: process.uptime(),
      memory: {
        process: memoryUsage,
        system: systemMemory
      },
      timestamp: new Date().toISOString(),
      redis: {
        status: redis.status,
        memory: redisInfo.match(/used_memory:\d+/)?.[0] || 'unknown',
        connections: redisInfo.match(/connected_clients:\d+/)?.[0] || 'unknown'
      },
      loadAvg: os.loadavg(),
      workers: config.MAX_WORKERS
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
    const existingClient = await sessionManager.getClient(sessionCode);
    if (existingClient) {
      try {
        const state = await existingClient.getState();
        
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
        await sessionManager.removeClient(sessionCode);
      }
    }

    // Update session status to connecting
    await updateSessionInDB(sessionCode, { 
      status: 'connecting',
      qr_code: null 
    });

    // Create new client with enhanced configuration
    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: sessionCode,
        dataPath: config.SESSIONS_DIR,
        encrypt: true
      }),
      puppeteer: config.PUPPETEER_CONFIG,
      webVersionCache: {
        type: 'local',
        path: path.join(__dirname, 'wwebjs_cache'),
        strict: false
      },
      qrTimeoutMs: 60000,
      takeoverOnConflict: true,
      restartOnAuthFail: true,
      authTimeoutMs: 90000,
      qrMaxRetries: 5,
      logger: pino({ name: 'whatsapp-client' })
    });

    // Store client
    await sessionManager.addClient(sessionCode, client);

    // Set up event handlers with enhanced logging
    client.on('qr', async (qr) => {
      logger.info(`QR Code generated for session ${sessionCode}`);
      
      try {
        const qrCodeImage = await QRWorkerPool.generate(qr);
        
        await redis.setex(`qr:${sessionCode}`, config.QR_CODE_TTL, qrCodeImage);
        await updateSessionInDB(sessionCode, { 
          status: 'connecting',
          qr_code: qrCodeImage 
        });

        logger.info(`QR code stored for session ${sessionCode}`);
      } catch (error) {
        logger.error('Error generating QR code:', error);
      }
    });

    client.on('loading_screen', async (percent, message) => {
      logger.debug(`Loading screen: ${percent}% ${message || ''}`);
      if (percent === 100) {
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
      await sessionManager.removeClient(sessionCode);
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
      await sessionManager.removeClient(sessionCode);
    });

    // Initialize the client with timeout
    logger.info(`Initializing WhatsApp client for session ${sessionCode}`);
    const initTimeout = setTimeout(async () => {
      try {
        if (client.pupPage && !client.info) {
          logger.warn(`Timeout initializing session ${sessionCode}, forcing cleanup`);
          await sessionManager.removeClient(sessionCode);
          await updateSessionInDB(sessionCode, { 
            status: 'failed',
            qr_code: null 
          });
        }
      } catch (error) {
        logger.error(`Error during timeout cleanup for ${sessionCode}:`, error);
      }
    }, 120000); // 2 minutes timeout

    client.initialize().then(() => {
      clearTimeout(initTimeout);
    }).catch(error => {
      clearTimeout(initTimeout);
      logger.error(`Client initialization error for ${sessionCode}:`, error);
      sessionManager.removeClient(sessionCode);
      updateSessionInDB(sessionCode, { 
        status: 'failed',
        qr_code: null 
      });
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
    await sessionManager.removeClient(sessionCode);
    await updateSessionInDB(sessionCode, { 
      status: 'failed',
      qr_code: null 
    });
    
    res.status(500).json({ 
      success: false, 
      error: 'Failed to initialize session',
      details: error.message 
    });
  }
});

// Get QR code for session with caching
app.get('/qrcode/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    // Check Redis cache first
    const cachedQR = await redis.get(`qr:${sessionCode}`);
    if (cachedQR) {
      return res.json({ 
        success: true, 
        qrCode: cachedQR,
        cached: true
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

    // Cache the QR code from DB in Redis
    await redis.setex(`qr:${sessionCode}`, config.QR_CODE_TTL, sessionData.qr_code);
    
    res.json({ 
      success: true, 
      qrCode: sessionData.qr_code,
      cached: false
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

// Enhanced session status endpoint
app.get('/status/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    const hasClient = sessionManager.clients.has(sessionCode);
    let actualStatus = sessionData.status;
    let qrCode = null;

    if (hasClient) {
      try {
        const client = await sessionManager.getClient(sessionCode);
        const state = await client.getState();
        
        if (state === 'CONNECTED') {
          actualStatus = 'connected';
          await redis.del(`qr:${sessionCode}`);
          qrCode = null;
        } else if (state === 'QR_SCAN_COMPLETE') {
          actualStatus = 'connecting';
          qrCode = null;
        } else {
          const cachedQR = await redis.get(`qr:${sessionCode}`);
          qrCode = cachedQR || sessionData.qr_code;
        }
        
        await updateSessionInDB(sessionCode, { 
          status: actualStatus,
          qr_code: qrCode,
          last_connected_at: actualStatus === 'connected' ? new Date().toISOString() : sessionData.last_connected_at
        });
      } catch (error) {
        logger.error('Error checking client state:', error);
        actualStatus = 'disconnected';
        await sessionManager.removeClient(sessionCode);
      }
    }
    
    res.json({ 
      success: true,
      connected: actualStatus === 'connected',
      status: actualStatus,
      lastConnected: sessionData.last_connected_at,
      hasClient,
      qrCode: actualStatus === 'connected' ? null : qrCode,
      sessionInfo: {
        createdAt: sessionData.created_at,
        updatedAt: sessionData.updated_at,
        deviceInfo: sessionData.device_info
      }
    });

  } catch (error) {
    logger.error(`Error getting status for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get session status' 
    });
  }
});

// Send message to a single contact with enhanced validation
app.post('/send/:sessionCode', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode } = req.params;
  const { number, message } = req.body;
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

    const client = await sessionManager.getClient(sessionCode);
    if (!client) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not connected. Please connect first.' 
      });
    }

    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      return res.status(400).json({ 
        success: false, 
        error: 'WhatsApp client is not connected',
        state 
      });
    }

    // Validate inputs
    if (!number || !message) {
      return res.status(400).json({ 
        success: false, 
        error: 'Phone number and message are required' 
      });
    }

    if (!validatePhoneNumber(number)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Invalid phone number format. Please use international format (+1234567890)' 
      });
    }

    if (typeof message !== 'string' || message.trim().length === 0) {
      return res.status(400).json({ 
        success: false, 
        error: 'Message must be a non-empty string' 
      });
    }

    if (message.length > 4096) {
      return res.status(400).json({ 
        success: false, 
        error: 'Message too long (max 4096 characters)' 
      });
    }

    const formattedNumber = formatPhoneNumber(number);

    // Check if number exists on WhatsApp
    const isRegistered = await client.isRegisteredUser(formattedNumber);
    if (!isRegistered) {
      return res.status(400).json({ 
        success: false, 
        error: 'Number is not registered on WhatsApp' 
      });
    }

    // Send message with timeout
    const sendTimeout = setTimeout(async () => {
      logger.warn(`Timeout sending message to ${number}`);
    }, 30000); // 30 seconds timeout

    const sentMessage = await client.sendMessage(formattedNumber, message);
    clearTimeout(sendTimeout);
    
    // Update session last activity
    await updateSessionInDB(sessionCode, { 
      last_connected_at: new Date().toISOString() 
    });
    
    res.json({ 
      success: true, 
      messageId: sentMessage.id._serialized,
      to: number,
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

// Enhanced bulk message sending with progress tracking
app.post('/send-bulk/:sessionCode', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode } = req.params;
  const { contacts, message, interval = config.MESSAGE_INTERVAL } = req.body;
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

    const client = await sessionManager.getClient(sessionCode);
    if (!client) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not connected. Please connect first.' 
      });
    }

    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      return res.status(400).json({ 
        success: false, 
        error: 'WhatsApp client is not connected',
        state 
      });
    }

    // Validate inputs
    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ 
        success: false, 
        error: 'Contacts array is required and cannot be empty' 
      });
    }

    if (contacts.length > 1000) {
      return res.status(400).json({ 
        success: false, 
        error: 'Maximum 1000 contacts per batch' 
      });
    }

    if (!message || typeof message !== 'string' || message.trim().length === 0) {
      return res.status(400).json({ 
        success: false, 
        error: 'Message is required and must be a non-empty string' 
      });
    }

    if (message.length > 4096) {
      return res.status(400).json({ 
        success: false, 
        error: 'Message too long (max 4096 characters)' 
      });
    }

    // Validate each contact
    for (const contact of contacts) {
      if (!contact.phone || !validatePhoneNumber(contact.phone)) {
        return res.status(400).json({ 
          success: false, 
          error: `Invalid phone number: ${contact.phone}` 
        });
      }
    }

    // Generate a batch ID for tracking
    const batchId = uuidv4();
    
    // Store initial batch info in Redis
    await redis.hset(`batch:${batchId}`, {
      userId,
      sessionCode,
      total: contacts.length,
      progress: 0,
      startedAt: new Date().toISOString(),
      status: 'processing'
    });

    // Set expiration (24 hours)
    await redis.expire(`batch:${batchId}`, 86400);
    
    // Immediately respond that processing has started
    res.json({ 
      success: true,
      message: 'Bulk message processing started',
      batchId,
      totalContacts: contacts.length
    });

    // Process messages in background
    processBulkMessages(client, sessionCode, contacts, message, interval, batchId)
      .catch(error => {
        logger.error(`Error processing batch ${batchId}:`, error);
        redis.hset(`batch:${batchId}`, {
          status: 'failed',
          error: error.message
        });
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

// Enhanced bulk message processing
async function processBulkMessages(client, sessionCode, contacts, message, interval, batchId) {
  const results = [];
  const maxInterval = Math.max(interval, config.MESSAGE_INTERVAL);
  const startTime = Date.now();
  
  for (let i = 0; i < contacts.length; i++) {
    const contact = contacts[i];
    const contactStartTime = Date.now();
    
    try {
      const formattedNumber = formatPhoneNumber(contact.phone);
      
      // Check if number exists on WhatsApp
      const isRegistered = await client.isRegisteredUser(formattedNumber);
      if (!isRegistered) {
        results.push({
          contact: contact.phone,
          status: 'failed',
          error: 'Number not registered on WhatsApp',
          timestamp: new Date().toISOString()
        });
        continue;
      }

      // Use contact-specific message if provided, otherwise use default
      const messageToSend = contact.message || message;
      
      // Send message with timeout
      const sendTimeout = setTimeout(async () => {
        logger.warn(`Timeout sending to ${contact.phone}`);
      }, 30000);

      const sentMessage = await client.sendMessage(formattedNumber, messageToSend);
      clearTimeout(sendTimeout);
      
      results.push({
        contact: contact.phone,
        status: 'sent',
        messageId: sentMessage.id._serialized,
        timestamp: new Date().toISOString(),
        duration: Date.now() - contactStartTime
      });

      // Store progress in Redis
      await redis.hset(`batch:${batchId}`, {
        progress: i + 1,
        lastUpdated: new Date().toISOString()
      });

      // Store detailed results incrementally
      await redis.setex(`batch:results:${batchId}:${i}`, 86400, JSON.stringify(results[i]));

      // Wait before sending next message (except for the last one)
      if (i < contacts.length - 1) {
        await new Promise(resolve => setTimeout(resolve, maxInterval));
      }

    } catch (error) {
      logger.error(`Error sending to ${contact.phone}:`, error);
      results.push({
        contact: contact.phone,
        status: 'failed',
        error: error.message,
        timestamp: new Date().toISOString(),
        duration: Date.now() - contactStartTime
      });
      
      // Store failed attempt
      await redis.setex(`batch:results:${batchId}:${i}`, 86400, JSON.stringify(results[i]));
    }
  }

  // Update session last activity
  await updateSessionInDB(sessionCode, { 
    last_connected_at: new Date().toISOString() 
  });

  // Calculate statistics
  const successCount = results.filter(r => r.status === 'sent').length;
  const failedCount = results.filter(r => r.status === 'failed').length;
  const totalDuration = Date.now() - startTime;
  
  // Store final results
  await redis.hset(`batch:${batchId}`, {
    completed: true,
    successCount,
    failedCount,
    finishedAt: new Date().toISOString(),
    status: 'completed',
    totalDuration
  });

  // Store aggregated results (expire after 24 hours)
  await redis.setex(`batch:results:${batchId}`, 86400, JSON.stringify(results));
}

// Enhanced batch status endpoint
app.get('/batch-status/:batchId', verifyAuth, async (req, res) => {
  const { batchId } = req.params;
  const userId = req.user.id;
  
  try {
    const batchData = await redis.hgetall(`batch:${batchId}`);
    if (!batchData || Object.keys(batchData).length === 0) {
      return res.status(404).json({ 
        success: false, 
        error: 'Batch not found' 
      });
    }

    // Verify the batch belongs to the requesting user
    if (batchData.userId !== userId) {
      return res.status(403).json({ 
        success: false, 
        error: 'Access denied to this batch' 
      });
    }

    const progress = parseInt(batchData.progress || '0');
    const total = parseInt(batchData.total || '0');
    const successCount = parseInt(batchData.successCount || '0');
    const failedCount = parseInt(batchData.failedCount || '0');
    
    res.json({
      success: true,
      batchId,
      progress,
      total,
      percentage: total > 0 ? Math.round((progress / total) * 100) : 0,
      completed: batchData.completed === 'true',
      successCount,
      failedCount,
      startedAt: batchData.startedAt,
      lastUpdated: batchData.lastUpdated,
      finishedAt: batchData.finishedAt,
      totalDuration: batchData.totalDuration ? parseInt(batchData.totalDuration) : null,
      status: batchData.status || 'unknown'
    });
  } catch (error) {
    logger.error(`Error getting batch status ${batchId}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get batch status' 
    });
  }
});

// Get batch results
app.get('/batch-results/:batchId', verifyAuth, async (req, res) => {
  const { batchId } = req.params;
  const userId = req.user.id;
  
  try {
    // Verify batch ownership
    const batchOwner = await redis.hget(`batch:${batchId}`, 'userId');
    if (batchOwner !== userId) {
      return res.status(403).json({ 
        success: false, 
        error: 'Access denied to this batch' 
      });
    }

    // Try to get aggregated results first
    let results = await redis.get(`batch:results:${batchId}`);
    
    if (!results) {
      // Fallback to building results from individual items
      const batchData = await redis.hgetall(`batch:${batchId}`);
      const total = parseInt(batchData.total || '0');
      
      const resultKeys = [];
      for (let i = 0; i < total; i++) {
        resultKeys.push(`batch:results:${batchId}:${i}`);
      }
      
      const individualResults = await redis.mget(resultKeys);
      results = JSON.stringify(individualResults.map(r => JSON.parse(r || '{}')));
    }

    res.json({
      success: true,
      batchId,
      results: JSON.parse(results || '[]')
    });
  } catch (error) {
    logger.error(`Error getting batch results ${batchId}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get batch results' 
    });
  }
});

// Enhanced session disconnection
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

    await sessionManager.removeClient(sessionCode);
    await redis.del(`qr:${sessionCode}`);
    
    // Update session status in database
    await updateSessionInDB(sessionCode, { 
      status: 'disconnected',
      qr_code: null,
      last_disconnected_at: new Date().toISOString()
    });

    res.json({ 
      success: true, 
      message: 'Session disconnected successfully',
      sessionCode,
      disconnectedAt: new Date().toISOString()
    });

  } catch (error) {
    logger.error(`Error disconnecting session ${sessionCode}:`, error);
    
    // Force cleanup
    await sessionManager.removeClient(sessionCode);
    await redis.del(`qr:${sessionCode}`);
    await updateSessionInDB(sessionCode, { 
      status: 'disconnected',
      qr_code: null,
      last_disconnected_at: new Date().toISOString()
    });
    
    res.json({ 
      success: true, 
      message: 'Session disconnected (forced cleanup)',
      warning: error.message,
      sessionCode
    });
  }
});

// Enhanced session listing
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

    const sessionsWithStatus = await Promise.all(sessions.map(async session => {
      const hasClient = sessionManager.clients.has(session.session_code);
      let isActive = false;
      
      if (hasClient) {
        try {
          const client = await sessionManager.getClient(session.session_code);
          const state = await client.getState();
          isActive = state === 'CONNECTED';
        } catch (error) {
          logger.error(`Error checking state for session ${session.session_code}:`, error);
        }
      }
      
      return {
        ...session,
        hasClient,
        isActive
      };
    }));
    
    res.json({ 
      success: true, 
      sessions: sessionsWithStatus,
      total: sessions.length,
      activeCount: sessionsWithStatus.filter(s => s.isActive).length
    });

  } catch (error) {
    logger.error('Error fetching user sessions:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to fetch sessions' 
    });
  }
});

// Enhanced phone number validation endpoint
app.post('/validate-phone', verifyAuth, async (req, res) => {
  const { phone } = req.body;
  const { sessionCode } = req.query;
  
  if (!phone) {
    return res.status(400).json({ 
      success: false, 
      error: 'Phone number is required' 
    });
  }

  const isValid = validatePhoneNumber(phone);
  let isRegistered = false;
  let canSend = false;
  
  try {
    if (isValid && sessionCode) {
      const client = await sessionManager.getClient(sessionCode);
      if (client) {
        const state = await client.getState();
        if (state === 'CONNECTED') {
          const formattedNumber = formatPhoneNumber(phone);
          isRegistered = await client.isRegisteredUser(formattedNumber);
          canSend = isRegistered;
        }
      }
    }
  } catch (error) {
    logger.error('Error checking phone registration:', error);
  }
  
  res.json({ 
    success: true,
    valid: isValid,
    registered: isRegistered,
    canSend,
    original: phone,
    formatted: isValid ? formatPhoneNumber(phone).replace('@c.us', '') : null
  });
});

// Error handling middleware with enhanced logging
app.use((error, req, res, next) => {
  const errorId = uuidv4();
  logger.error(`Error ID: ${errorId}`, {
    message: error.message,
    stack: error.stack,
    url: req.originalUrl,
    method: req.method,
    ip: req.ip,
    headers: req.headers
  });
  
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error',
    errorId,
    details: config.NODE_ENV === 'development' ? error.message : undefined
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ 
    success: false, 
    error: 'Endpoint not found',
    path: req.path,
    method: req.method 
  });
});

// Enhanced graceful shutdown
async function gracefulShutdown(signal) {
  logger.info(`Received ${signal}, shutting down gracefully...`);
  
  try {
    // Stop accepting new connections
    server?.close();
    
    // Close session manager
    await sessionManager.shutdown();
    
    // Close Redis connection
    await redis.quit();
    
    // Close QR worker pool
    QRWorkerPool.shutdown();
    
    logger.info('Cleanup complete, exiting...');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown:', error);
    process.exit(1);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Start server with clustering
let server;

if (cluster.isPrimary && config.NODE_ENV === 'production') {
  logger.info(`Primary ${process.pid} is running`);
  
  // Fork workers
  for (let i = 0; i < config.MAX_WORKERS; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    logger.warn(`Worker ${worker.process.pid} died (${signal || code})`);
    if (!server?.closed) {
      cluster.fork(); // Replace the dead worker
    }
  });
} else {
  server = app.listen(config.PORT, () => {
    logger.info(`🚀 WhatsApp Bulk Sender Backend running on port ${config.PORT}`);
    logger.info(`📱 Environment: ${config.NODE_ENV}`);
    logger.info(`💾 Sessions directory: ${config.SESSIONS_DIR}`);
    logger.info(`🔗 Health check: http://localhost:${config.PORT}/health`);
    logger.info(`🗄️  Supabase connected: ${!!supabase}`);
    logger.info(`🔴 Redis connected: ${redis.status === 'ready'}`);
    logger.info(`👷 Worker ${process.pid} started`);
  });

  // Handle server errors
  server.on('error', (error) => {
    logger.error('Server error:', error);
    process.exit(1);
  });
}

module.exports = app;