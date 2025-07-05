require('dotenv').config();
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
const morgan = require('morgan');
const { v4: uuidv4 } = require('uuid');
const winston = require('winston');
const expressWinston = require('express-winston');
const { createBullBoard } = require('@bull-board/api');
const { BullAdapter } = require('@bull-board/api/bullAdapter');
const { ExpressAdapter } = require('@bull-board/express');
const Queue = require('bull');
const cluster = require('cluster');
const os = require('os');
const { throttle } = require('lodash');

// Constants
const PORT = process.env.PORT || 3001;
const SESSIONS_DIR = path.join(__dirname, 'sessions');
const MAX_MESSAGE_RATE = process.env.MAX_MESSAGE_RATE || 60; // Messages per minute
const CPU_COUNT = os.cpus().length;
const IS_PRODUCTION = process.env.NODE_ENV === 'production';
const BULL_REDIS_URL = process.env.BULL_REDIS_URL || process.env.REDIS_URL;

// Initialize logging
const logger = winston.createLogger({
  level: IS_PRODUCTION ? 'info' : 'debug',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'logs/error.log', level: 'error' }),
    new winston.transports.File({ filename: 'logs/combined.log' })
  ],
  exceptionHandlers: [
    new winston.transports.File({ filename: 'logs/exceptions.log' })
  ]
});

// Initialize Express
const app = express();

// Configure Express to trust proxies
app.set('trust proxy', IS_PRODUCTION ? 1 : 0);

// Initialize Redis client
const redis = new Redis(process.env.REDIS_URL || {
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT || 6379,
  password: process.env.REDIS_PASSWORD,
  tls: process.env.REDIS_TLS === 'true' ? {} : undefined,
  retryStrategy: (times) => {
    const delay = Math.min(times * 100, 5000);
    return delay;
  }
});

// Initialize Bull queue for background jobs
const messageQueue = new Queue('whatsapp-messages', BULL_REDIS_URL, {
  limiter: {
    max: MAX_MESSAGE_RATE,
    duration: 60 * 1000, // 1 minute
    groupKey: 'sessionCode'
  },
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: 100,
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 5000
    }
  }
});

// Initialize Bull Board for queue monitoring
if (process.env.ENABLE_QUEUE_MONITOR === 'true') {
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath('/admin/queues');

  createBullBoard({
    queues: [new BullAdapter(messageQueue)],
    serverAdapter
  });

  app.use('/admin/queues', serverAdapter.getRouter());
}

// Initialize Supabase client
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

if (!supabaseUrl || !supabaseServiceKey) {
  logger.error('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
  process.exit(1);
}

// Security and performance middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
      fontSrc: ["'self'", "https://fonts.gstatic.com"],
      imgSrc: ["'self'", "data:", "https://*"],
      connectSrc: ["'self'", "https://*"],
      frameSrc: ["'self'"]
    }
  },
  hsts: IS_PRODUCTION ? { maxAge: 63072000, includeSubDomains: true, preload: true } : false
}));

app.use(compression({
  level: 6,
  threshold: 10 * 1024 // Only compress responses larger than 10KB
}));

// CORS configuration
const corsOptions = {
  origin: IS_PRODUCTION 
    ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
    : ['http://localhost:5173', 'http://localhost:3000'],
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true,
  maxAge: 86400
};
app.use(cors(corsOptions));
app.options('*', cors(corsOptions));

// Request logging
app.use(morgan(IS_PRODUCTION ? 'combined' : 'dev', {
  stream: {
    write: (message) => logger.info(message.trim())
  }
}));

// Error logging middleware
app.use(expressWinston.errorLogger({
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'logs/errors.log' })
  ],
  format: winston.format.combine(
    winston.format.colorize(),
    winston.format.json()
  )
}));

// Rate limiting
const globalLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 1000, // Limit each IP to 1000 requests per windowMs
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => req.ip === '::ffff:127.0.0.1', // Skip for localhost
  keyGenerator: (req) => req.headers['x-real-ip'] || req.ip // Use X-Real-IP if behind proxy
});
app.use(globalLimiter);

// Stricter rate limiting for message sending
const sendLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: MAX_MESSAGE_RATE,
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => req.user?.id || req.ip // Rate limit by user if authenticated
});

// Body parser middleware with size limits
app.use(express.json({
  limit: '10mb',
  verify: (req, res, buf) => {
    req.rawBody = buf.toString();
  }
}));
app.use(express.urlencoded({
  extended: true,
  limit: '10mb',
  parameterLimit: 10000
}));

// In-memory storage for WhatsApp clients with cleanup
const clients = new Map();
const clientTimeouts = new Map();

// Session cleanup function
const cleanupClient = async (sessionCode) => {
  const client = clients.get(sessionCode);
  if (client) {
    try {
      logger.info(`Cleaning up inactive session: ${sessionCode}`);
      await client.destroy();
      clients.delete(sessionCode);
      clientTimeouts.delete(sessionCode);
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, {
        status: 'disconnected',
        qr_code: null
      });
    } catch (error) {
      logger.error(`Error cleaning up session ${sessionCode}:`, error);
    }
  }
};

// Schedule cleanup for inactive clients
const scheduleCleanup = (sessionCode) => {
  // Clear any existing timeout
  if (clientTimeouts.has(sessionCode)) {
    clearTimeout(clientTimeouts.get(sessionCode));
  }
  
  // Set new timeout (30 minutes of inactivity)
  const timeout = setTimeout(() => cleanupClient(sessionCode), 30 * 60 * 1000);
  clientTimeouts.set(sessionCode, timeout);
};

// Ensure sessions directory exists
async function ensureSessionsDir() {
  try {
    await fs.access(SESSIONS_DIR);
  } catch {
    await fs.mkdir(SESSIONS_DIR, { recursive: true });
    logger.info(`Created sessions directory at ${SESSIONS_DIR}`);
  }
}

// Initialize sessions directory on startup
ensureSessionsDir().catch(err => {
  logger.error('Failed to create sessions directory:', err);
  process.exit(1);
});

// Database Models
class SessionModel {
  static async create(sessionData) {
    const { data, error } = await supabase
      .from('sessions')
      .insert(sessionData)
      .select()
      .single();

    if (error) throw error;
    return data;
  }

  static async getByCode(sessionCode, userId = null) {
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
  }

  static async update(sessionCode, updates) {
    const { data, error } = await supabase
      .from('sessions')
      .update({
        ...updates,
        updated_at: new Date().toISOString()
      })
      .eq('session_code', sessionCode)
      .select()
      .single();

    if (error) throw error;
    return data;
  }

  static async listByUser(userId) {
    const { data, error } = await supabase
      .from('sessions')
      .select('*')
      .eq('user_id', userId)
      .order('created_at', { ascending: false });

    if (error) throw error;
    return data;
  }
}

// Utility functions
class WhatsAppUtils {
  static validatePhoneNumber(phone) {
    const cleaned = phone.replace(/[^\d+]/g, '');
    const phoneRegex = /^\+[1-9]\d{9,14}$/;
    return phoneRegex.test(cleaned);
  }

  static formatPhoneNumber(phone) {
    let formatted = phone.replace(/[^\d+]/g, '');
    if (!formatted.startsWith('+')) {
      formatted = '+' + formatted;
    }
    return formatted.substring(1) + '@c.us';
  }

  static async generateQRCode(qrData) {
    return qrcode.toDataURL(qrData, {
      width: 256,
      margin: 2,
      color: {
        dark: '#000000',
        light: '#FFFFFF'
      }
    });
  }
}

// Middleware to verify Supabase JWT token
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
    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error || !user) {
      return res.status(401).json({ 
        success: false, 
        error: 'Invalid or expired token' 
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

// API Controllers
class HealthController {
  static async check(req, res) {
    try {
      // Test database connection
      const { data: dbTest } = await supabase.rpc('version');
      
      // Test Redis connection
      await redis.ping();
      
      res.json({ 
        status: 'healthy', 
        timestamp: new Date().toISOString(),
        activeSessions: clients.size,
        uptime: process.uptime(),
        database: dbTest ? 'connected' : 'disconnected',
        redis: redis.status,
        memory: process.memoryUsage(),
        environment: process.env.NODE_ENV || 'development'
      });
    } catch (error) {
      logger.error('Health check failed:', error);
      res.status(500).json({ 
        status: 'unhealthy',
        error: error.message 
      });
    }
  }
}

class SessionController {
  static async init(req, res) {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    
    try {
      // Verify session belongs to user
      const sessionData = await SessionModel.getByCode(sessionCode, userId);
      if (!sessionData) {
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied' 
        });
      }

      // Check if session already exists and is connected
      if (clients.has(sessionCode)) {
        const existingClient = clients.get(sessionCode);
        try {
          const state = await existingClient.getState();
          
          if (state === 'CONNECTED') {
            await SessionModel.update(sessionCode, { 
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
          clientTimeouts.delete(sessionCode);
        }
      }

      // Update session status to connecting
      await SessionModel.update(sessionCode, { 
        status: 'connecting',
        qr_code: null 
      });

      // Create new client with session persistence
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
            '--use-gl=egl'
          ],
          executablePath: process.env.CHROME_BIN || undefined
        },
        webVersionCache: {
          type: 'remote',
          remotePath: `https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/${process.env.WA_VERSION || '2.2412.54'}.html`,
        },
        qrTimeoutMs: 0, // Disable QR timeout
        takeoverOnConflict: true, // Take over existing session
        restartOnAuthFail: true, // Restart if auth fails
        ffmpegPath: process.env.FFMPEG_PATH || 'ffmpeg'
      });

      // Store client
      clients.set(sessionCode, client);
      scheduleCleanup(sessionCode);

      // Set up event handlers with enhanced logging
      client.on('qr', async (qr) => {
        logger.info(`QR Code generated for session ${sessionCode}`);
        
        try {
          const qrCodeImage = await WhatsAppUtils.generateQRCode(qr);
          
          // Store in Redis cache for faster access
          await redis.setex(`qr:${sessionCode}`, 300, qrCodeImage);

          // Update session with QR code in database
          await SessionModel.update(sessionCode, { 
            status: 'connecting',
            qr_code: qrCodeImage 
          });

          logger.info(`QR code stored for session ${sessionCode}`);
        } catch (error) {
          logger.error('Error generating QR code:', error);
        }
      });

      client.on('loading_screen', async (percent, message) => {
        logger.info(`Loading screen: ${percent}% ${message || ''}`);
        if (percent === 100) {
          // QR code has been scanned - clear it
          await redis.del(`qr:${sessionCode}`);
          await SessionModel.update(sessionCode, {
            qr_code: null,
            status: 'connecting'
          });
        }
      });

      client.on('authenticated', async () => {
        logger.info(`WhatsApp client ${sessionCode} authenticated`);
        await redis.del(`qr:${sessionCode}`);
        
        await SessionModel.update(sessionCode, { 
          status: 'connected',
          last_connected_at: new Date().toISOString(),
          qr_code: null
        });
        
        scheduleCleanup(sessionCode);
      });

      client.on('auth_failure', async (msg) => {
        logger.error(`Authentication failed for session ${sessionCode}:`, msg);
        await redis.del(`qr:${sessionCode}`);
        
        await SessionModel.update(sessionCode, { 
          status: 'expired',
          qr_code: null 
        });
        
        clients.delete(sessionCode);
        clientTimeouts.delete(sessionCode);
      });

      client.on('ready', async () => {
        logger.info(`WhatsApp client ${sessionCode} is ready!`);
        await redis.del(`qr:${sessionCode}`);
        
        await SessionModel.update(sessionCode, { 
          status: 'connected',
          last_connected_at: new Date().toISOString(),
          qr_code: null 
        });
        
        scheduleCleanup(sessionCode);
      });

      client.on('disconnected', async (reason) => {
        logger.warn(`WhatsApp client ${sessionCode} disconnected:`, reason);
        await redis.del(`qr:${sessionCode}`);
        
        await SessionModel.update(sessionCode, { 
          status: 'disconnected',
          qr_code: null 
        });
        
        clients.delete(sessionCode);
        clientTimeouts.delete(sessionCode);
      });

      // Initialize the client
      logger.info(`Initializing WhatsApp client for session ${sessionCode}`);
      await client.initialize();
      logger.info(`Client initialization started for ${sessionCode}`);

      res.json({ 
        success: true, 
        message: 'Session initialization started',
        sessionCode,
        status: 'connecting'
      });

    } catch (error) {
      logger.error(`Error initializing session ${sessionCode}:`, error);
      await redis.del(`qr:${sessionCode}`);
      
      await SessionModel.update(sessionCode, { 
        status: 'expired',
        qr_code: null 
      });
      
      res.status(500).json({ 
        success: false, 
        error: 'Failed to initialize session',
        details: error.message 
      });
    }
  }

  static async getQRCode(req, res) {
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
      const sessionData = await SessionModel.getByCode(sessionCode, userId);
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
  }

  static async getStatus(req, res) {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    
    try {
      const sessionData = await SessionModel.getByCode(sessionCode, userId);
      if (!sessionData) {
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied' 
        });
      }

      const hasClient = clients.has(sessionCode);
      let actualStatus = sessionData.status;
      let qrCode = null;

      if (hasClient) {
        try {
          const client = clients.get(sessionCode);
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
          
          await SessionModel.update(sessionCode, { 
            status: actualStatus,
            qr_code: qrCode,
            last_connected_at: actualStatus === 'connected' ? new Date().toISOString() : sessionData.last_connected_at
          });
          
          scheduleCleanup(sessionCode);
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
        qrCode: actualStatus === 'connected' ? null : qrCode // Never show QR if connected
      });

    } catch (error) {
      logger.error(`Error getting status for session ${sessionCode}:`, error);
      res.status(500).json({ 
        success: false, 
        error: 'Failed to get session status' 
      });
    }
  }

  static async disconnect(req, res) {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    
    try {
      // Verify session belongs to user
      const sessionData = await SessionModel.getByCode(sessionCode, userId);
      if (!sessionData) {
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied' 
        });
      }

      const client = clients.get(sessionCode);
      
      if (client) {
        try {
          await client.logout();
          await client.destroy();
        } catch (error) {
          logger.error(`Error destroying client ${sessionCode}:`, error);
        }
        clients.delete(sessionCode);
        clientTimeouts.delete(sessionCode);
        await redis.del(`qr:${sessionCode}`);
      }
      
      // Update session status in database
      await SessionModel.update(sessionCode, { 
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
      clientTimeouts.delete(sessionCode);
      await redis.del(`qr:${sessionCode}`);
      await SessionModel.update(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });
      
      res.json({ 
        success: true, 
        message: 'Session disconnected (forced cleanup)',
        warning: error.message 
      });
    }
  }

  static async list(req, res) {
    const userId = req.user.id;
    
    try {
      const sessions = await SessionModel.listByUser(userId);

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
  }
}

class MessageController {
  static async send(req, res) {
    const { sessionCode, number, message } = req.params;
    const userId = req.user.id;
    
    try {
      // Verify session belongs to user
      const sessionData = await SessionModel.getByCode(sessionCode, userId);
      if (!sessionData) {
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied' 
        });
      }

      const client = clients.get(sessionCode);
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

      // Validate and format phone number
      if (!WhatsAppUtils.validatePhoneNumber(number)) {
        return res.status(400).json({ 
          success: false, 
          error: 'Invalid phone number format' 
        });
      }

      const formattedNumber = WhatsAppUtils.formatPhoneNumber(number);
      const decodedMessage = decodeURIComponent(message);

      // Check if number exists on WhatsApp
      const isRegistered = await client.isRegisteredUser(formattedNumber);
      if (!isRegistered) {
        return res.status(400).json({ 
          success: false, 
          error: 'Number is not registered on WhatsApp' 
        });
      }

      // Add job to queue with retry logic
      const job = await messageQueue.add('send-message', {
        sessionCode,
        number: formattedNumber,
        message: decodedMessage,
        userId
      }, {
        jobId: `${sessionCode}-${uuidv4()}`,
        removeOnComplete: true
      });

      res.json({ 
        success: true, 
        jobId: job.id,
        message: 'Message queued for sending',
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
  }

  static async sendBulk(req, res) {
    const { sessionCode } = req.params;
    const { contacts, message, interval = 5000 } = req.body;
    const userId = req.user.id;
    
    try {
      // Verify session belongs to user
      const sessionData = await SessionModel.getByCode(sessionCode, userId);
      if (!sessionData) {
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied' 
        });
      }

      const client = clients.get(sessionCode);
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

      // Create a bulk job ID for tracking
      const bulkJobId = uuidv4();
      const maxInterval = Math.max(interval, 3000); // Minimum 3 seconds between messages
      
      // Add each contact as a separate job with throttling
      const jobs = [];
      for (let i = 0; i < contacts.length; i++) {
        const contact = contacts[i];
        
        // Validate phone number
        if (!WhatsAppUtils.validatePhoneNumber(contact.phone)) {
          jobs.push({
            contact: contact.phone,
            status: 'failed',
            error: 'Invalid phone number format'
          });
          continue;
        }

        const formattedNumber = WhatsAppUtils.formatPhoneNumber(contact.phone);
        const messageToSend = contact.message || message;
        
        // Add job with delay to throttle sending
        const delay = i * maxInterval;
        
        const job = await messageQueue.add('send-message', {
          sessionCode,
          number: formattedNumber,
          message: messageToSend,
          userId,
          bulkJobId
        }, {
          jobId: `${sessionCode}-${bulkJobId}-${i}`,
          delay,
          removeOnComplete: true
        });

        jobs.push({
          contact: contact.phone,
          jobId: job.id,
          status: 'queued',
          timestamp: new Date().toISOString()
        });
      }

      res.json({ 
        success: true,
        bulkJobId,
        message: 'Bulk messages queued for sending',
        queued: jobs.length,
        total: contacts.length,
        jobs
      });

    } catch (error) {
      logger.error(`Error in bulk send for session ${sessionCode}:`, error);
      res.status(500).json({ 
        success: false, 
        error: 'Failed to queue bulk messages',
        details: error.message 
      });
    }
  }

  static async validatePhone(req, res) {
    const { phone } = req.body;
    
    if (!phone) {
      return res.status(400).json({ 
        success: false, 
        error: 'Phone number is required' 
      });
    }

    const isValid = WhatsAppUtils.validatePhoneNumber(phone);
    const formatted = isValid ? WhatsAppUtils.formatPhoneNumber(phone) : null;
    
    res.json({ 
      success: true,
      valid: isValid,
      original: phone,
      formatted: formatted ? formatted.replace('@c.us', '') : null
    });
  }
}

// Process messages from the queue
messageQueue.process('send-message', 5, async (job) => {
  const { sessionCode, number, message, userId } = job.data;
  logger.info(`Processing message job ${job.id} for session ${sessionCode}`);

  try {
    // Verify session belongs to user
    const sessionData = await SessionModel.getByCode(sessionCode, userId);
    if (!sessionData) {
      throw new Error('Session not found or access denied');
    }

    const client = clients.get(sessionCode);
    if (!client) {
      throw new Error('Session not connected');
    }

    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      throw new Error('WhatsApp client is not connected');
    }

    // Check if number exists on WhatsApp
    const isRegistered = await client.isRegisteredUser(number);
    if (!isRegistered) {
      throw new Error('Number is not registered on WhatsApp');
    }

    // Send message with retry
    const sentMessage = await client.sendMessage(number, message);
    
    // Update session last activity
    await SessionModel.update(sessionCode, { 
      last_connected_at: new Date().toISOString() 
    });
    
    scheduleCleanup(sessionCode);

    return {
      success: true,
      messageId: sentMessage.id._serialized,
      to: number,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    logger.error(`Error processing message job ${job.id}:`, error);
    throw error; // Will trigger retry
  }
});

// Routes
app.get('/health', HealthController.check);
app.get('/stats', verifyAuth, HealthController.check);

// Session routes
app.post('/init/:sessionCode', verifyAuth, SessionController.init);
app.get('/qrcode/:sessionCode', verifyAuth, SessionController.getQRCode);
app.get('/status/:sessionCode', verifyAuth, SessionController.getStatus);
app.post('/disconnect/:sessionCode', verifyAuth, SessionController.disconnect);
app.get('/sessions', verifyAuth, SessionController.list);

// Message routes
app.post('/send/:sessionCode/:number/:message', verifyAuth, sendLimiter, MessageController.send);
app.post('/send-bulk/:sessionCode', verifyAuth, sendLimiter, MessageController.sendBulk);
app.post('/validate-phone', verifyAuth, MessageController.validatePhone);

// Error handling middleware
app.use((error, req, res, next) => {
  logger.error('Unhandled error:', error);
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error',
    details: !IS_PRODUCTION ? error.message : undefined
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ 
    success: false, 
    error: 'Endpoint not found' 
  });
});

// Graceful shutdown
async function shutdown() {
  logger.info('Shutdown signal received, shutting down gracefully...');
  
  // Close all WhatsApp clients
  for (const [sessionCode, client] of clients.entries()) {
    try {
      logger.info(`Disconnecting session ${sessionCode}...`);
      await client.destroy();
      await redis.del(`qr:${sessionCode}`);
      await SessionModel.update(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });
    } catch (error) {
      logger.error(`Error disconnecting session ${sessionCode}:`, error);
    }
  }
  
  // Close Redis connection
  try {
    await redis.quit();
  } catch (error) {
    logger.error('Error closing Redis connection:', error);
  }
  
  // Close message queue
  try {
    await messageQueue.close();
  } catch (error) {
    logger.error('Error closing message queue:', error);
  }
  
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Start server
if (cluster.isMaster && IS_PRODUCTION) {
  logger.info(`Master ${process.pid} is running`);
  
  // Fork workers
  for (let i = 0; i < CPU_COUNT; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    logger.warn(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
    logger.info('Forking a new worker...');
    cluster.fork();
  });
} else {
  app.listen(PORT, () => {
    logger.info(`🚀 WhatsApp Bulk Sender Backend running on port ${PORT}`);
    logger.info(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
    logger.info(`💾 Sessions directory: ${SESSIONS_DIR}`);
    logger.info(`🔗 Health check: http://localhost:${PORT}/health`);
    logger.info(`🗄️  Supabase connected: ${!!supabase}`);
    logger.info(`🔴 Redis connected: ${redis.status}`);
    logger.info(`📊 Bull queue dashboard: http://localhost:${PORT}/admin/queues`);
    logger.info(`👷 Worker ${process.pid} started`);
  });
}

module.exports = app;