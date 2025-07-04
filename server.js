require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const fs = require('fs').promises;
const path = require('path');
const { default: RedisStore } = require('rate-limit-redis');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const compression = require('compression');
const { createClient } = require('@supabase/supabase-js');
const jwt = require('jsonwebtoken');
const Redis = require('ioredis');
const winston = require('winston');
const cluster = require('cluster');
const os = require('os');
const { v4: uuidv4 } = require('uuid');
const morgan = require('morgan');

// Initialize logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [new winston.transports.Console()]
});

// Cluster mode for multi-core processing
if (cluster.isMaster && process.env.NODE_ENV === 'production') {
  const numCPUs = os.cpus().length;
  logger.info(`Master ${process.pid} is running with ${numCPUs} workers`);

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    logger.error(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
    logger.info('Starting a new worker');
    cluster.fork();
  });
} else {
  const app = express();
  const PORT = process.env.PORT || 3001;

  // Configure Express to trust proxies
  app.set('trust proxy', process.env.NODE_ENV === 'production' ? ['loopback', 'linklocal', 'uniquelocal'] : false);

  // Initialize Redis client
  const redis = new Redis({
    host: process.env.REDIS_HOST || 'clean-panda-53790.upstash.io',
    port: process.env.REDIS_PORT || 6379,
    password: process.env.REDIS_PASSWORD || 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
    tls: process.env.REDIS_TLS === 'true' ? {} : undefined,
    retryStrategy: (times) => Math.min(times * 50, 2000),
    maxRetriesPerRequest: 3
  });

  // Initialize Supabase client
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const supabase = createClient(supabaseUrl, supabaseServiceKey, {
    auth: { persistSession: false }
  });

  if (!supabaseUrl || !supabaseServiceKey) {
    logger.error('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
    process.exit(1);
  }

  // Rate limiters
  const globalRateLimiter = rateLimit({
    store: new RedisStore({
      sendCommand: (...args) => redis.call(...args),
      prefix: 'rl_global:'
    }),
    windowMs: 15 * 60 * 1000,
    max: 1000,
    message: 'Too many requests from this IP, please try again later.',
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => {
      const forwarded = req.headers['x-forwarded-for'];
      return forwarded ? forwarded.split(',')[0] : req.ip;
    }
  });

  const sendLimiter = rateLimit({
    store: new RedisStore({
      sendCommand: (...args) => redis.call(...args),
      prefix: 'rl_send:'
    }),
    windowMs: 60 * 1000,
    max: 60,
    message: 'Too many messages sent, please slow down.',
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => req.user?.id || 'unknown'
  });

  // Security and performance middleware
  app.use(helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", "'unsafe-inline'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", "data:"],
        connectSrc: ["'self'"]
      }
    }
  }));
  
  app.use(compression());
  app.use(cors({
    origin: process.env.NODE_ENV === 'production' 
      ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
      : ['http://localhost:5173', 'http://localhost:3000'],
    credentials: true
  }));
  
  app.use(globalRateLimiter);
  app.use(express.json({ limit: '10mb' }));
  app.use(express.urlencoded({ extended: true, limit: '10mb' }));

  // Request ID middleware
  app.use((req, res, next) => {
    req.id = req.headers['x-request-id'] || uuidv4();
    res.setHeader('X-Request-ID', req.id);
    next();
  });

  // Request logging
  app.use(morgan('combined', {
    stream: {
      write: (message) => logger.info(message.trim())
    }
  }));

  // In-memory storage for WhatsApp clients
  const clients = new Map();
  const clientTTL = new Map();

  // Session cleanup interval
  setInterval(() => {
    const now = Date.now();
    for (const [sessionCode, ttl] of clientTTL.entries()) {
      if (now > ttl) {
        const client = clients.get(sessionCode);
        if (client) {
          client.destroy().catch(err => {
            logger.error(`Error cleaning up client ${sessionCode}:`, err);
          });
        }
        clients.delete(sessionCode);
        clientTTL.delete(sessionCode);
        redis.del(`qr:${sessionCode}`).catch(err => {
          logger.error(`Error cleaning up QR for ${sessionCode}:`, err);
        });
        logger.info(`Cleaned up expired session ${sessionCode}`);
      }
    }
  }, 60000); // Run every minute

  // Ensure sessions directory exists
  const SESSIONS_DIR = path.join(__dirname, 'sessions');

  async function ensureSessionsDir() {
    try {
      await fs.access(SESSIONS_DIR);
    } catch {
      await fs.mkdir(SESSIONS_DIR, { recursive: true });
    }
  }

  // Initialize sessions directory
  ensureSessionsDir().catch(err => {
    logger.error('Failed to initialize sessions directory:', err);
    process.exit(1);
  });

  // Middleware to verify Supabase JWT token
  async function verifyAuth(req, res, next) {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        logger.warn('Authorization token missing', { requestId: req.id });
        return res.status(401).json({ 
          success: false, 
          error: 'Authorization token required',
          requestId: req.id
        });
      }

      const token = authHeader.substring(7);
      const cacheKey = `auth:${token}`;
      const cachedUser = await redis.get(cacheKey);
      
      if (cachedUser) {
        req.user = JSON.parse(cachedUser);
        return next();
      }

      const { data: { user }, error } = await supabase.auth.getUser(token);
      
      if (error || !user) {
        logger.warn('Invalid or expired token', { requestId: req.id });
        return res.status(401).json({ 
          success: false, 
          error: 'Invalid or expired token',
          requestId: req.id
        });
      }

      await redis.setex(cacheKey, 300, JSON.stringify(user));
      req.user = user;
      next();
    } catch (error) {
      logger.error('Auth verification error:', { error, requestId: req.id });
      res.status(401).json({ 
        success: false, 
        error: 'Authentication failed',
        requestId: req.id
      });
    }
  }

  // Utility functions
  async function updateSessionInDB(sessionCode, updates) {
    const maxRetries = 3;
    let attempts = 0;
    
    while (attempts < maxRetries) {
      try {
        const { error } = await supabase
          .from('sessions')
          .update({
            ...updates,
            updated_at: new Date().toISOString()
          })
          .eq('session_code', sessionCode);

        if (error) throw error;
        return;
      } catch (error) {
        attempts++;
        if (attempts === maxRetries) {
          logger.error('Failed to update session in DB after retries:', { 
            error, 
            sessionCode,
            attempts 
          });
          throw error;
        }
        await new Promise(resolve => setTimeout(resolve, 100 * attempts));
      }
    }
  }

  async function getSessionFromDB(sessionCode, userId = null) {
    const cacheKey = `session:${sessionCode}`;
    
    try {
      const cachedSession = await redis.get(cacheKey);
      if (cachedSession) {
        const session = JSON.parse(cachedSession);
        if (!userId || session.user_id === userId) {
          return session;
        }
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
        throw error || new Error('Session not found');
      }
      
      await redis.setex(cacheKey, 60, JSON.stringify(data));
      return data;
    } catch (error) {
      logger.error('Error fetching session from DB:', { 
        error, 
        sessionCode,
        userId 
      });
      throw error;
    }
  }

  function validatePhoneNumber(phone) {
    if (typeof phone !== 'string') return false;
    const cleaned = phone.replace(/[^\d+]/g, '');
    const phoneRegex = /^\+[1-9]\d{9,14}$/;
    return phoneRegex.test(cleaned) && cleaned.length >= 10 && cleaned.length <= 16;
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

  // Health check endpoint
  app.get('/health', async (req, res) => {
    try {
      const redisPing = await redis.ping();
      const supabasePing = await supabase.rpc('ping');
      
      res.json({ 
        status: 'healthy',
        timestamp: new Date().toISOString(),
        worker: cluster.worker?.id || 'single',
        redis: redisPing === 'PONG',
        supabase: !supabasePing.error
      });
    } catch (error) {
      logger.error('Health check failed:', error);
      res.status(500).json({
        status: 'unhealthy',
        error: 'Health check failed'
      });
    }
  });

  // Initialize WhatsApp session
  app.post('/init/:sessionCode', verifyAuth, async (req, res) => {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    
    try {
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        logger.warn('Session not found or access denied', { sessionCode, userId });
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId: req.id
        });
      }

      if (clients.has(sessionCode)) {
        const existingClient = clients.get(sessionCode);
        try {
          const state = await existingClient.getState();
          
          if (state === 'CONNECTED') {
            await updateSessionInDB(sessionCode, { 
              status: 'connected',
              last_connected_at: new Date().toISOString()
            });
            
            clientTTL.set(sessionCode, Date.now() + 3600000);
            return res.json({ 
              success: true, 
              message: 'Session already connected',
              status: 'connected',
              requestId: req.id
            });
          }
        } catch (error) {
          clients.delete(sessionCode);
          clientTTL.delete(sessionCode);
          await redis.del(`qr:${sessionCode}`);
        }
      }

      await updateSessionInDB(sessionCode, { 
        status: 'connecting',
        qr_code: null 
      });

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
            '--disable-gpu'
          ]
        },
        webVersionCache: {
          type: 'remote',
          remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html',
        },
        qrTimeoutMs: 0,
        takeoverOnConflict: true,
        restartOnAuthFail: true
      });

      clients.set(sessionCode, client);
      clientTTL.set(sessionCode, Date.now() + 3600000);

      client.on('qr', async (qr) => {
        logger.info(`QR Code generated for session ${sessionCode}`);
        try {
          const qrCodeImage = await qrcode.toDataURL(qr, {
            width: 256,
            margin: 2,
            color: {
              dark: '#000000',
              light: '#FFFFFF'
            }
          });

          await redis.setex(`qr:${sessionCode}`, 300, qrCodeImage);
          await updateSessionInDB(sessionCode, { 
            status: 'connecting',
            qr_code: qrCodeImage 
          });
        } catch (error) {
          logger.error('Error generating QR code:', { error, sessionCode });
        }
      });

      client.on('loading_screen', async (percent, message) => {
        if (percent === 100) {
          await redis.del(`qr:${sessionCode}`);
          await updateSessionInDB(sessionCode, {
            qr_code: null,
            status: 'connecting'
          });
        }
      });

      client.on('authenticated', async () => {
        await redis.del(`qr:${sessionCode}`);
        await updateSessionInDB(sessionCode, { 
          status: 'connected',
          last_connected_at: new Date().toISOString(),
          qr_code: null
        });
        clientTTL.set(sessionCode, Date.now() + 3600000);
      });

      client.on('auth_failure', async (msg) => {
        logger.error(`Authentication failed for session ${sessionCode}:`, msg);
        await redis.del(`qr:${sessionCode}`);
        await updateSessionInDB(sessionCode, { 
          status: 'expired',
          qr_code: null 
        });
        clients.delete(sessionCode);
        clientTTL.delete(sessionCode);
      });

      client.on('ready', async () => {
        await redis.del(`qr:${sessionCode}`);
        await updateSessionInDB(sessionCode, { 
          status: 'connected',
          last_connected_at: new Date().toISOString(),
          qr_code: null 
        });
        clientTTL.set(sessionCode, Date.now() + 3600000);
      });

      client.on('disconnected', async (reason) => {
        logger.warn(`WhatsApp client ${sessionCode} disconnected:`, reason);
        await redis.del(`qr:${sessionCode}`);
        await updateSessionInDB(sessionCode, { 
          status: 'disconnected',
          qr_code: null 
        });
        clients.delete(sessionCode);
        clientTTL.delete(sessionCode);
      });

      client.on('error', async (error) => {
        logger.error(`WhatsApp client ${sessionCode} error:`, error);
        await redis.del(`qr:${sessionCode}`);
        await updateSessionInDB(sessionCode, { 
          status: 'error',
          qr_code: null 
        });
        clients.delete(sessionCode);
        clientTTL.delete(sessionCode);
      });

      await client.initialize();
      res.json({ 
        success: true, 
        message: 'Session initialization started',
        sessionCode,
        status: 'connecting',
        requestId: req.id
      });

    } catch (error) {
      logger.error(`Error initializing session ${sessionCode}:`, { error, requestId: req.id });
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { 
        status: 'error',
        qr_code: null 
      });
      clients.delete(sessionCode);
      clientTTL.delete(sessionCode);
      
      res.status(500).json({ 
        success: false, 
        error: 'Failed to initialize session',
        details: error.message,
        requestId: req.id
      });
    }
  });

  // Get QR code for session
  app.get('/qrcode/:sessionCode', verifyAuth, async (req, res) => {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    
    try {
      const cachedQR = await redis.get(`qr:${sessionCode}`);
      if (cachedQR) {
        return res.json({ 
          success: true, 
          qrCode: cachedQR,
          requestId: req.id
        });
      }

      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        logger.warn('Session not found or access denied', { sessionCode, userId });
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId: req.id
        });
      }

      if (!sessionData.qr_code) {
        return res.status(404).json({ 
          success: false, 
          error: 'QR code not available. Please initialize session first.',
          requestId: req.id
        });
      }

      res.json({ 
        success: true, 
        qrCode: sessionData.qr_code,
        requestId: req.id
      });

    } catch (error) {
      logger.error(`Error getting QR code for session ${sessionCode}:`, { error, requestId: req.id });
      res.status(500).json({ 
        success: false, 
        error: 'Failed to get QR code',
        details: error.message,
        requestId: req.id
      });
    }
  });

  // Get session status
  app.get('/status/:sessionCode', verifyAuth, async (req, res) => {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    
    try {
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        logger.warn('Session not found or access denied', { sessionCode, userId });
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId: req.id
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
          clientTTL.set(sessionCode, Date.now() + 3600000);
        } catch (error) {
          logger.error('Error checking client state:', { error, sessionCode });
          actualStatus = 'disconnected';
        }
      }
      
      res.json({ 
        success: true,
        connected: actualStatus === 'connected',
        status: actualStatus,
        lastConnected: sessionData.last_connected_at,
        hasClient,
        qrCode: actualStatus === 'connected' ? null : qrCode,
        requestId: req.id
      });

    } catch (error) {
      logger.error(`Error getting status for session ${sessionCode}:`, { error, requestId: req.id });
      res.status(500).json({ 
        success: false, 
        error: 'Failed to get session status',
        requestId: req.id
      });
    }
  });

  // Send message to a single contact
  app.post('/send/:sessionCode/:number/:message', verifyAuth, sendLimiter, async (req, res) => {
    const { sessionCode, number, message } = req.params;
    const userId = req.user.id;
    
    try {
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        logger.warn('Session not found or access denied', { sessionCode, userId });
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId: req.id
        });
      }

      const client = clients.get(sessionCode);
      if (!client) {
        return res.status(404).json({ 
          success: false, 
          error: 'Session not connected. Please connect first.',
          requestId: req.id
        });
      }

      const state = await client.getState();
      if (state !== 'CONNECTED') {
        return res.status(400).json({ 
          success: false, 
          error: 'WhatsApp client is not connected',
          state,
          requestId: req.id
        });
      }

      if (!validatePhoneNumber(number)) {
        return res.status(400).json({ 
          success: false, 
          error: 'Invalid phone number format. Must be in international format (e.g. +1234567890)',
          requestId: req.id
        });
      }

      const formattedNumber = formatPhoneNumber(number);
      const decodedMessage = decodeURIComponent(message);

      if (decodedMessage.length > 4096) {
        return res.status(400).json({ 
          success: false, 
          error: 'Message too long. Maximum 4096 characters allowed.',
          requestId: req.id
        });
      }

      let isRegistered;
      try {
        isRegistered = await Promise.race([
          client.isRegisteredUser(formattedNumber),
          new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout checking number')), 10000))
        ]);
      } catch (checkError) {
        logger.error('Error checking number registration:', { error: checkError, number, sessionCode });
        return res.status(400).json({ 
          success: false, 
          error: 'Failed to verify number. Please try again.',
          details: checkError.message,
          requestId: req.id
        });
      }

      if (!isRegistered) {
        return res.status(400).json({ 
          success: false, 
          error: 'Number is not registered on WhatsApp',
          requestId: req.id
        });
      }

      let sentMessage;
      try {
        sentMessage = await Promise.race([
          client.sendMessage(formattedNumber, decodedMessage),
          new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout sending message')), 30000))
        ]);
      } catch (sendError) {
        logger.error('Error sending message:', { error: sendError, number, sessionCode });
        return res.status(500).json({ 
          success: false, 
          error: 'Failed to send message',
          details: sendError.message,
          requestId: req.id
        });
      }
      
      await updateSessionInDB(sessionCode, { 
        last_connected_at: new Date().toISOString() 
      });
      clientTTL.set(sessionCode, Date.now() + 3600000);
      
      res.json({ 
        success: true, 
        messageId: sentMessage.id._serialized,
        to: number,
        message: decodedMessage,
        timestamp: new Date().toISOString(),
        requestId: req.id
      });

    } catch (error) {
      logger.error(`Error sending message in session ${sessionCode}:`, { error, requestId: req.id });
      res.status(500).json({ 
        success: false, 
        error: 'Failed to send message',
        details: error.message,
        requestId: req.id
      });
    }
  });

  // Send bulk messages
  app.post('/send-bulk/:sessionCode', verifyAuth, sendLimiter, async (req, res) => {
    const { sessionCode } = req.params;
    const { contacts, message, interval = 5000 } = req.body;
    const userId = req.user.id;
    
    try {
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        logger.warn('Session not found or access denied', { sessionCode, userId });
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId: req.id
        });
      }

      const client = clients.get(sessionCode);
      if (!client) {
        return res.status(404).json({ 
          success: false, 
          error: 'Session not connected. Please connect first.',
          requestId: req.id
        });
      }

      const state = await client.getState();
      if (state !== 'CONNECTED') {
        return res.status(400).json({ 
          success: false, 
          error: 'WhatsApp client is not connected',
          state,
          requestId: req.id
        });
      }

      if (!Array.isArray(contacts) || contacts.length === 0) {
        return res.status(400).json({ 
          success: false, 
          error: 'Contacts array is required and cannot be empty',
          requestId: req.id
        });
      }

      if (contacts.length > 1000) {
        return res.status(400).json({ 
          success: false, 
          error: 'Maximum 1000 contacts per bulk send allowed',
          requestId: req.id
        });
      }

      if (!message || message.trim().length === 0) {
        return res.status(400).json({ 
          success: false, 
          error: 'Message is required',
          requestId: req.id
        });
      }

      if (message.length > 4096) {
        return res.status(400).json({ 
          success: false, 
          error: 'Message too long. Maximum 4096 characters allowed.',
          requestId: req.id
        });
      }

      const results = [];
      const maxInterval = Math.max(interval, 3000);
      const startTime = Date.now();
      
      const batchSize = 50;
      for (let i = 0; i < contacts.length; i += batchSize) {
        const batch = contacts.slice(i, i + batchSize);
        
        for (let j = 0; j < batch.length; j++) {
          const contact = batch[j];
          
          try {
            if (!contact || typeof contact !== 'object' || !contact.phone) {
              results.push({
                contact: 'invalid',
                status: 'failed',
                error: 'Invalid contact format'
              });
              continue;
            }

            if (!validatePhoneNumber(contact.phone)) {
              results.push({
                contact: contact.phone,
                status: 'failed',
                error: 'Invalid phone number format'
              });
              continue;
            }

            const formattedNumber = formatPhoneNumber(contact.phone);
            const contactMessage = contact.message || message;
            
            if (contactMessage.length > 4096) {
              results.push({
                contact: contact.phone,
                status: 'failed',
                error: 'Message too long'
              });
              continue;
            }
            
            let isRegistered;
            try {
              isRegistered = await Promise.race([
                client.isRegisteredUser(formattedNumber),
                new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout checking number')), 10000))
              ]);
            } catch (checkError) {
              logger.error('Error checking number registration in bulk:', { 
                error: checkError, 
                number: contact.phone,
                sessionCode 
              });
              results.push({
                contact: contact.phone,
                status: 'failed',
                error: 'Failed to verify number'
              });
              continue;
            }

            if (!isRegistered) {
              results.push({
                contact: contact.phone,
                status: 'failed',
                error: 'Number not registered on WhatsApp'
              });
              continue;
            }

            let sentMessage;
            try {
              sentMessage = await Promise.race([
                client.sendMessage(formattedNumber, contactMessage),
                new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout sending message')), 30000))
              ]);
              
              results.push({
                contact: contact.phone,
                status: 'sent',
                messageId: sentMessage.id._serialized,
                timestamp: new Date().toISOString()
              });
            } catch (sendError) {
              logger.error('Error sending message in bulk:', { 
                error: sendError, 
                number: contact.phone,
                sessionCode 
              });
              results.push({
                contact: contact.phone,
                status: 'failed',
                error: 'Failed to send message'
              });
            }

            if (j < batch.length - 1) {
              await new Promise(resolve => setTimeout(resolve, maxInterval));
            }

          } catch (error) {
            logger.error(`Error processing contact ${contact.phone}:`, { 
              error, 
              sessionCode,
              requestId: req.id 
            });
            results.push({
              contact: contact.phone,
              status: 'failed',
              error: error.message
            });
          }
        }
      }

      await updateSessionInDB(sessionCode, { 
        last_connected_at: new Date().toISOString() 
      });
      clientTTL.set(sessionCode, Date.now() + 3600000);
      
      const successCount = results.filter(r => r.status === 'sent').length;
      const failedCount = results.filter(r => r.status === 'failed').length;
      const duration = (Date.now() - startTime) / 1000;
      
      logger.info(`Bulk send completed for session ${sessionCode}`, {
        total: contacts.length,
        sent: successCount,
        failed: failedCount,
        duration: `${duration}s`,
        requestId: req.id
      });
      
      res.json({ 
        success: true,
        summary: {
          total: contacts.length,
          sent: successCount,
          failed: failedCount,
          duration: `${duration}s`
        },
        results,
        requestId: req.id
      });

    } catch (error) {
      logger.error(`Error in bulk send for session ${sessionCode}:`, { error, requestId: req.id });
      res.status(500).json({ 
        success: false, 
        error: 'Failed to send bulk messages',
        details: error.message,
        requestId: req.id
      });
    }
  });

  // Disconnect session
  app.post('/disconnect/:sessionCode', verifyAuth, async (req, res) => {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    
    try {
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        logger.warn('Session not found or access denied', { sessionCode, userId });
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId: req.id
        });
      }

      const client = clients.get(sessionCode);
      
      if (client) {
        try {
          await client.logout();
          await client.destroy();
        } catch (error) {
          logger.error(`Error destroying client ${sessionCode}:`, { error, requestId: req.id });
        }
        clients.delete(sessionCode);
        clientTTL.delete(sessionCode);
        await redis.del(`qr:${sessionCode}`);
      }
      
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });

      logger.info(`Session disconnected successfully: ${sessionCode}`, { requestId: req.id });
      
      res.json({ 
        success: true, 
        message: 'Session disconnected successfully',
        requestId: req.id
      });

    } catch (error) {
      logger.error(`Error disconnecting session ${sessionCode}:`, { error, requestId: req.id });
      
      clients.delete(sessionCode);
      clientTTL.delete(sessionCode);
      await redis.del(`qr:${sessionCode}`).catch(err => {
        logger.error('Error cleaning up QR in disconnect:', { error: err, sessionCode });
      });
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      }).catch(err => {
        logger.error('Error updating session in disconnect:', { error: err, sessionCode });
      });
      
      res.json({ 
        success: true, 
        message: 'Session disconnected (forced cleanup)',
        warning: error.message,
        requestId: req.id
      });
    }
  });

  // Get user's sessions
  app.get('/sessions', verifyAuth, async (req, res) => {
    const userId = req.user.id;
    const { page = 1, limit = 10 } = req.query;
    const offset = (page - 1) * limit;
    
    try {
      const { data: sessions, count, error } = await supabase
        .from('sessions')
        .select('*', { count: 'exact' })
        .eq('user_id', userId)
        .order('created_at', { ascending: false })
        .range(offset, offset + limit - 1);

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
        pagination: {
          total: count || 0,
          page: parseInt(page),
          limit: parseInt(limit),
          totalPages: Math.ceil((count || 0) / limit)
        },
        requestId: req.id
      });

    } catch (error) {
      logger.error('Error fetching user sessions:', { error, userId, requestId: req.id });
      res.status(500).json({ 
        success: false, 
        error: 'Failed to fetch sessions',
        requestId: req.id
      });
    }
  });

  // Validate phone number
  app.post('/validate-phone', async (req, res) => {
    const { phone } = req.body;
    
    if (!phone) {
      return res.status(400).json({ 
        success: false, 
        error: 'Phone number is required',
        requestId: req.id
      });
    }

    try {
      const isValid = validatePhoneNumber(phone);
      const formatted = isValid ? formatPhoneNumber(phone) : null;
      
      res.json({ 
        success: true,
        valid: isValid,
        original: phone,
        formatted: formatted ? formatted.replace('@c.us', '') : null,
        requestId: req.id
      });
    } catch (error) {
      res.json({ 
        success: false,
        valid: false,
        original: phone,
        error: error.message,
        requestId: req.id
      });
    }
  });

  // Error handling middleware
  app.use((error, req, res, next) => {
    logger.error('Unhandled error:', { 
      error, 
      requestId: req.id,
      path: req.path,
      method: req.method
    });
    
    res.status(500).json({ 
      success: false, 
      error: 'Internal server error',
      requestId: req.id,
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  });

  // 404 handler
  app.use((req, res) => {
    res.status(404).json({ 
      success: false, 
      error: 'Endpoint not found',
      requestId: req.id
    });
  });

  // Graceful shutdown
  async function gracefulShutdown(signal) {
    logger.info(`${signal} received, shutting down gracefully...`);
    
    // Stop accepting new connections
    server.close(() => {
      logger.info('HTTP server closed');
    });
    
    // Close all WhatsApp clients
    const cleanupPromises = [];
    for (const [sessionCode, client] of clients.entries()) {
      cleanupPromises.push(
        (async () => {
          try {
            logger.info(`Disconnecting session ${sessionCode}...`);
            await client.destroy();
            await redis.del(`qr:${sessionCode}`);
            await updateSessionInDB(sessionCode, { 
              status: 'disconnected',
              qr_code: null 
            });
          } catch (error) {
            logger.error(`Error disconnecting session ${sessionCode}:`, error);
          }
        })()
      );
    }
    
    await Promise.allSettled(cleanupPromises);
    
    // Close Redis connection
    await redis.quit();
    
    logger.info('Graceful shutdown complete');
    process.exit(0);
  }

  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));

  // Start server
  const server = app.listen(PORT, '0.0.0.0', () => {
    logger.info(`🚀 WhatsApp Bulk Sender Backend running on port ${PORT}`);
    logger.info(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
    logger.info(`💾 Sessions directory: ${SESSIONS_DIR}`);
    logger.info(`🔗 Health check: http://localhost:${PORT}/health`);
    logger.info(`🗄️  Supabase connected: ${!!supabase}`);
    logger.info(`🔴 Redis connected: ${redis.status === 'ready'}`);
    if (cluster.worker) {
      logger.info(`👷 Worker ${cluster.worker.id} started`);
    }
  });

  // Handle server errors
  server.on('error', (error) => {
    logger.error('Server error:', error);
    process.exit(1);
  });
}