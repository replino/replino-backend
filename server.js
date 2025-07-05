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
const morgan = require('morgan');
const { v4: uuidv4 } = require('uuid');
const { throttle } = require('lodash');
const { performance } = require('perf_hooks');
const { Worker } = require('worker_threads');

// Load environment variables early
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;
const numCPUs = process.env.NODE_ENV === 'production' ? os.cpus().length : 1;

// Cluster mode for production
if (cluster.isMaster && process.env.NODE_ENV === 'production') {
  console.log(`Master ${process.pid} is running`);

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
    console.log('Forking a new worker...');
    cluster.fork();
  });
} else {
  // Configure Express to trust proxies
  app.set('trust proxy', process.env.NODE_ENV === 'production' ? 2 : 0);

  // Initialize Redis client with connection pooling and retry strategy
  const redis = new Redis({
    host: process.env.REDIS_HOST || 'clean-panda-53790.upstash.io',
    port: process.env.REDIS_PORT || 6379,
    password: process.env.REDIS_PASSWORD || 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
    tls: process.env.REDIS_TLS === 'true' ? {} : undefined,
    retryStrategy: (times) => {
      const delay = Math.min(times * 50, 2000);
      return delay;
    },
    maxRetriesPerRequest: 3,
    enableOfflineQueue: true,
    connectTimeout: 10000
  });

  // Redis error handling
  redis.on('error', (err) => {
    console.error('Redis error:', err);
  });

  redis.on('connect', () => {
    console.log('Redis connected successfully');
  });

  // Initialize Supabase client with connection pooling
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  
  if (!supabaseUrl || !supabaseServiceKey) {
    console.error('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
    process.exit(1);
  }

  const supabase = createClient(supabaseUrl, supabaseServiceKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false
    },
    global: {
      headers: {
        'x-connection-pool': 'whatsapp-bulk-sender'
      }
    }
  });

  // Security and performance middleware
  app.use(helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", "'unsafe-inline'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", "data:", "blob:"],
        connectSrc: ["'self'", supabaseUrl, process.env.REDIS_HOST || 'clean-panda-53790.upstash.io'],
        frameSrc: ["'self'"],
        objectSrc: ["'none'"],
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
    threshold: 10 * 1024, // Compress responses larger than 10KB
    filter: (req, res) => {
      if (req.headers['x-no-compression']) {
        return false;
      }
      return compression.filter(req, res);
    }
  }));

  // Enhanced CORS configuration
  const corsOptions = {
    origin: process.env.NODE_ENV === 'production' 
      ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
      : ['http://localhost:5173', 'http://localhost:3000'],
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept', 'Origin'],
    maxAge: 86400,
    preflightContinue: false,
    optionsSuccessStatus: 204
  };
  app.use(cors(corsOptions));

  // Pre-flight requests
  app.options('*', cors(corsOptions));

  // Request logging
  app.use(morgan(process.env.NODE_ENV === 'production' ? 'combined' : 'dev', {
    skip: (req) => req.path === '/health' // Skip health checks in logs
  }));

  // Rate limiting with Redis store for distributed environments
  const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 1000, // Limit each IP to 1000 requests per windowMs
    message: 'Too many requests from this IP, please try again later.',
    standardHeaders: true,
    legacyHeaders: false,
    validate: { trustProxy: true },
    skip: (req) => req.ip === '127.0.0.1' // Skip rate limiting for localhost
  });

  app.use(limiter);

  // Stricter rate limiting for message sending with Redis
  const sendLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 100, // Limit to 100 messages per minute
    message: 'Too many messages sent, please slow down.',
    standardHeaders: true,
    legacyHeaders: false,
    validate: { trustProxy: true },
    keyGenerator: (req) => {
      return `${req.user?.id || req.ip}:${req.params.sessionCode}`;
    }
  });

  // Body parsing with increased limits
  app.use(express.json({
    limit: '50mb',
    verify: (req, res, buf, encoding) => {
      try {
        JSON.parse(buf.toString('utf-8'));
      } catch (e) {
        res.status(400).json({ success: false, error: 'Invalid JSON payload' });
      }
    }
  }));

  app.use(express.urlencoded({
    extended: true,
    limit: '50mb',
    parameterLimit: 10000
  }));

  // Session management with enhanced cleanup
  const SESSIONS_DIR = path.join(__dirname, 'sessions');
  const clients = new Map();
  const sessionCleanupInterval = setInterval(cleanupInactiveSessions, 3600000); // Cleanup every hour

  async function cleanupInactiveSessions() {
    const now = Date.now();
    const threshold = 24 * 60 * 60 * 1000; // 24 hours
    
    for (const [sessionCode, client] of clients.entries()) {
      try {
        const sessionData = await getSessionFromDB(sessionCode);
        if (!sessionData || (now - new Date(sessionData.last_connected_at).getTime() > threshold)) {
          await client.destroy();
          clients.delete(sessionCode);
          await redis.del(`qr:${sessionCode}`);
          console.log(`Cleaned up inactive session: ${sessionCode}`);
        }
      } catch (error) {
        console.error(`Error cleaning up session ${sessionCode}:`, error);
      }
    }
  }

  // Ensure sessions directory exists with proper permissions
  async function ensureSessionsDir() {
    try {
      await fs.access(SESSIONS_DIR);
      // Verify directory permissions
      const stats = await fs.stat(SESSIONS_DIR);
      if (!stats.isDirectory()) {
        throw new Error('Sessions path is not a directory');
      }
    } catch (error) {
      await fs.mkdir(SESSIONS_DIR, { recursive: true, mode: 0o755 });
      console.log(`Created sessions directory at ${SESSIONS_DIR}`);
    }
  }

  // Initialize sessions directory on startup
  ensureSessionsDir().catch(err => {
    console.error('Failed to initialize sessions directory:', err);
    process.exit(1);
  });

  // Enhanced auth verification with caching
  async function verifyAuth(req, res, next) {
    const requestId = uuidv4();
    const startTime = performance.now();
    
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        console.warn(`[${requestId}] No auth token provided`);
        return res.status(401).json({ 
          success: false, 
          error: 'Authorization token required',
          requestId
        });
      }

      const token = authHeader.substring(7);
      
      // Check Redis cache first
      const cacheKey = `auth:${token}`;
      const cachedUser = await redis.get(cacheKey);
      
      if (cachedUser) {
        req.user = JSON.parse(cachedUser);
        console.log(`[${requestId}] Authenticated via cache in ${performance.now() - startTime}ms`);
        return next();
      }

      // Verify with Supabase
      const { data: { user }, error } = await supabase.auth.getUser(token);
      
      if (error || !user) {
        console.warn(`[${requestId}] Invalid auth token`);
        return res.status(401).json({ 
          success: false, 
          error: 'Invalid or expired token',
          requestId
        });
      }

      // Cache the user for 5 minutes
      await redis.setex(cacheKey, 300, JSON.stringify(user));
      
      req.user = user;
      console.log(`[${requestId}] Authenticated via Supabase in ${performance.now() - startTime}ms`);
      next();
    } catch (error) {
      console.error(`[${requestId}] Auth verification error:`, error);
      res.status(401).json({ 
        success: false, 
        error: 'Authentication failed',
        requestId
      });
    }
  }

  // Enhanced session management with caching
  async function updateSessionInDB(sessionCode, updates) {
    const cacheKey = `session:${sessionCode}`;
    
    try {
      const { error } = await supabase
        .from('sessions')
        .update({
          ...updates,
          updated_at: new Date().toISOString()
        })
        .eq('session_code', sessionCode);

      if (error) {
        console.error('Error updating session in DB:', error);
        throw error;
      }

      // Invalidate cache
      await redis.del(cacheKey);
      return true;
    } catch (error) {
      console.error('Database update error:', error);
      throw error;
    }
  }

  async function getSessionFromDB(sessionCode, userId = null) {
    const cacheKey = `session:${sessionCode}`;
    
    try {
      // Check cache first
      const cachedSession = await redis.get(cacheKey);
      if (cachedSession) {
        const session = JSON.parse(cachedSession);
        if (!userId || session.user_id === userId) {
          return session;
        }
      }

      // Build query
      let query = supabase
        .from('sessions')
        .select('*')
        .eq('session_code', sessionCode);
      
      if (userId) {
        query = query.eq('user_id', userId);
      }
      
      const { data, error } = await query.single();
      
      if (error) {
        console.error('Error fetching session from DB:', error);
        throw error;
      }
      
      if (data) {
        // Cache for 5 minutes
        await redis.setex(cacheKey, 300, JSON.stringify(data));
      }
      
      return data;
    } catch (error) {
      console.error('Database fetch error:', error);
      throw error;
    }
  }

  // Enhanced phone number validation
  function validatePhoneNumber(phone) {
    if (typeof phone !== 'string') return false;
    
    const cleaned = phone.replace(/[^\d+]/g, '');
    const phoneRegex = /^\+[1-9]\d{9,14}$/;
    return phoneRegex.test(cleaned);
  }

  function formatPhoneNumber(phone) {
    let formatted = phone.replace(/[^\d+]/g, '');
    if (!formatted.startsWith('+')) {
      formatted = '+' + formatted;
    }
    return formatted.substring(1) + '@c.us';
  }

  // Health check endpoint with detailed diagnostics
  app.get('/health', async (req, res) => {
    const healthCheck = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      system: {
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        cpu: process.cpuUsage(),
        load: os.loadavg()
      },
      services: {
        supabase: 'ok',
        redis: redis.status === 'ready' ? 'ok' : 'down',
        sessions: {
          active: clients.size,
          directory: SESSIONS_DIR
        }
      },
      environment: process.env.NODE_ENV || 'development',
      worker: cluster.worker ? cluster.worker.id : 'master'
    };

    try {
      // Verify Supabase connection
      const { error } = await supabase.from('sessions').select('*').limit(1);
      if (error) {
        healthCheck.services.supabase = 'error';
        healthCheck.services.supabaseError = error.message;
      }
    } catch (error) {
      healthCheck.services.supabase = 'error';
      healthCheck.services.supabaseError = error.message;
    }

    res.json(healthCheck);
  });

  // Enhanced server statistics
  app.get('/stats', verifyAuth, async (req, res) => {
    try {
      const requestStart = performance.now();
      
      // Parallel requests
      const [userSessions, systemStats] = await Promise.all([
        supabase
          .from('sessions')
          .select('*', { count: 'exact', head: true })
          .eq('user_id', req.user.id),
        redis.info()
      ]);

      const memoryUsage = process.memoryUsage();
      const cpuUsage = process.cpuUsage();
      
      const stats = {
        requestId: uuidv4(),
        processingTime: performance.now() - requestStart,
        activeSessions: clients.size,
        userSessions: userSessions.count || 0,
        system: {
          uptime: process.uptime(),
          memory: {
            rss: memoryUsage.rss,
            heapTotal: memoryUsage.heapTotal,
            heapUsed: memoryUsage.heapUsed,
            external: memoryUsage.external,
            arrayBuffers: memoryUsage.arrayBuffers
          },
          cpu: {
            user: cpuUsage.user,
            system: cpuUsage.system
          },
          load: os.loadavg()
        },
        redis: {
          status: redis.status,
          info: systemStats.split('\r\n').reduce((acc, line) => {
            if (line && !line.startsWith('#')) {
              const [key, value] = line.split(':');
              acc[key] = value;
            }
            return acc;
          }, {})
        },
        timestamp: new Date().toISOString()
      };
      
      res.json(stats);
    } catch (error) {
      console.error('Error fetching stats:', error);
      res.status(500).json({ 
        success: false, 
        error: 'Failed to fetch statistics',
        requestId: uuidv4()
      });
    }
  });

  // Initialize WhatsApp session with enhanced error handling
  app.post('/init/:sessionCode', verifyAuth, async (req, res) => {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    const requestId = uuidv4();
    
    try {
      // Verify session belongs to user
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        console.warn(`[${requestId}] Session not found or access denied: ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId
        });
      }

      // Check if session already exists and is connected
      if (clients.has(sessionCode)) {
        const existingClient = clients.get(sessionCode);
        try {
          const state = await existingClient.getState();
          
          if (state === 'CONNECTED') {
            await updateSessionInDB(sessionCode, { 
              status: 'connected',
              last_connected_at: new Date().toISOString()
            });
            
            console.log(`[${requestId}] Using existing connected session: ${sessionCode}`);
            return res.json({ 
              success: true, 
              message: 'Session already connected',
              status: 'connected',
              requestId
            });
          }
        } catch (error) {
          // Client exists but not responsive, clean it up
          console.warn(`[${requestId}] Cleaning up unresponsive client for session: ${sessionCode}`);
          clients.delete(sessionCode);
          await redis.del(`qr:${sessionCode}`);
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
          dataPath: SESSIONS_DIR,
          encrypt: true
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
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-features=site-per-process,TranslateUI,BlinkGenPropertyTrees',
            '--enable-features=NetworkService,NetworkServiceInProcess',
            '--window-size=1920,1080'
          ],
          executablePath: process.env.CHROME_BIN || undefined,
          ignoreHTTPSErrors: true,
          timeout: 30000
        },
        webVersionCache: {
          type: 'remote',
          remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html',
        },
        qrTimeoutMs: 0, // Disable QR timeout
        takeoverOnConflict: true, // Take over existing session
        restartOnAuthFail: true, // Restart if auth fails
        puppeteerProduct: 'chrome',
        ffmpegPath: process.env.FFMPEG_PATH || 'ffmpeg',
        bypassCSP: true,
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      });

      // Store client
      clients.set(sessionCode, client);

      // Throttled QR code generation to prevent flooding
      const throttledQRHandler = throttle(async (qr) => {
        console.log(`[${requestId}] QR Code generated for session ${sessionCode}`);
        
        try {
          // Generate QR code image immediately
          const qrCodeImage = await qrcode.toDataURL(qr, {
            width: 256,
            margin: 2,
            color: {
              dark: '#000000',
              light: '#FFFFFF'
            }
          });

          // Store in Redis cache for faster access
          await redis.setex(`qr:${sessionCode}`, 300, qrCodeImage);

          // Update session with QR code in database
          await updateSessionInDB(sessionCode, { 
            status: 'connecting',
            qr_code: qrCodeImage 
          });

          console.log(`[${requestId}] QR code stored for session ${sessionCode}`);
        } catch (error) {
          console.error(`[${requestId}] Error generating QR code:`, error);
        }
      }, 5000, { leading: true, trailing: false }); // Throttle to max once every 5 seconds

      // Set up event handlers with enhanced logging
      client.on('qr', throttledQRHandler);

      client.on('loading_screen', async (percent, message) => {
        console.log(`[${requestId}] Loading screen: ${percent}% ${message || ''}`);
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
        console.log(`[${requestId}] WhatsApp client ${sessionCode} authenticated`);
        await redis.del(`qr:${sessionCode}`);
        
        await updateSessionInDB(sessionCode, { 
          status: 'connected',
          last_connected_at: new Date().toISOString(),
          qr_code: null
        });
      });

      client.on('auth_failure', async (msg) => {
        console.error(`[${requestId}] Authentication failed for session ${sessionCode}:`, msg);
        await redis.del(`qr:${sessionCode}`);
        
        await updateSessionInDB(sessionCode, { 
          status: 'expired',
          qr_code: null 
        });
        
        clients.delete(sessionCode);
      });

      client.on('ready', async () => {
        console.log(`[${requestId}] WhatsApp client ${sessionCode} is ready!`);
        await redis.del(`qr:${sessionCode}`);
        
        await updateSessionInDB(sessionCode, { 
          status: 'connected',
          last_connected_at: new Date().toISOString(),
          qr_code: null 
        });
      });

      client.on('disconnected', async (reason) => {
        console.log(`[${requestId}] WhatsApp client ${sessionCode} disconnected:`, reason);
        await redis.del(`qr:${sessionCode}`);
        
        await updateSessionInDB(sessionCode, { 
          status: 'disconnected',
          qr_code: null 
        });
        
        clients.delete(sessionCode);
      });

      // Error handling
      client.on('error', async (error) => {
        console.error(`[${requestId}] WhatsApp client error for session ${sessionCode}:`, error);
        await redis.del(`qr:${sessionCode}`);
        
        await updateSessionInDB(sessionCode, { 
          status: 'error',
          qr_code: null,
          error_message: error.message 
        });
        
        clients.delete(sessionCode);
      });

      // Initialize the client with timeout
      console.log(`[${requestId}] Initializing WhatsApp client for session ${sessionCode}`);
      
      const initTimeout = setTimeout(async () => {
        if (!client.info) {
          console.error(`[${requestId}] Client initialization timeout for session ${sessionCode}`);
          await redis.del(`qr:${sessionCode}`);
          await updateSessionInDB(sessionCode, { 
            status: 'timeout',
            qr_code: null 
          });
          clients.delete(sessionCode);
        }
      }, 60000); // 1 minute timeout

      await client.initialize();
      clearTimeout(initTimeout);
      
      console.log(`[${requestId}] Client initialization started for ${sessionCode}`);

      res.json({ 
        success: true, 
        message: 'Session initialization started',
        sessionCode,
        status: 'connecting',
        requestId
      });

    } catch (error) {
      console.error(`[${requestId}] Error initializing session ${sessionCode}:`, error);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'error',
        qr_code: null,
        error_message: error.message 
      });
      
      res.status(500).json({ 
        success: false, 
        error: 'Failed to initialize session',
        details: error.message,
        requestId
      });
    }
  });

  // Get QR code for session with caching
  app.get('/qrcode/:sessionCode', verifyAuth, async (req, res) => {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    const requestId = uuidv4();
    
    try {
      // Check Redis cache first
      const cachedQR = await redis.get(`qr:${sessionCode}`);
      if (cachedQR) {
        console.log(`[${requestId}] Serving QR code from cache for session ${sessionCode}`);
        return res.json({ 
          success: true, 
          qrCode: cachedQR,
          cached: true,
          requestId
        });
      }

      // Verify session belongs to user and get QR code from database
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        console.warn(`[${requestId}] Session not found or access denied: ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId
        });
      }

      if (!sessionData.qr_code) {
        console.log(`[${requestId}] No QR code available for session ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'QR code not available. Please initialize session first.',
          requestId
        });
      }

      // Cache the QR code for faster subsequent access
      await redis.setex(`qr:${sessionCode}`, 300, sessionData.qr_code);
      
      console.log(`[${requestId}] Serving QR code from DB for session ${sessionCode}`);
      res.json({ 
        success: true, 
        qrCode: sessionData.qr_code,
        cached: false,
        requestId
      });

    } catch (error) {
      console.error(`[${requestId}] Error getting QR code for session ${sessionCode}:`, error);
      res.status(500).json({ 
        success: false, 
        error: 'Failed to get QR code',
        details: error.message,
        requestId
      });
    }
  });

  // Get session status with enhanced state checking
  app.get('/status/:sessionCode', verifyAuth, async (req, res) => {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    const requestId = uuidv4();
    
    try {
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        console.warn(`[${requestId}] Session not found or access denied: ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId
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
          
          await updateSessionInDB(sessionCode, { 
            status: actualStatus,
            qr_code: qrCode,
            last_connected_at: actualStatus === 'connected' ? new Date().toISOString() : sessionData.last_connected_at
          });
        } catch (error) {
          console.error(`[${requestId}] Error checking client state for session ${sessionCode}:`, error);
          actualStatus = 'disconnected';
        }
      }
      
      console.log(`[${requestId}] Status check for session ${sessionCode}: ${actualStatus}`);
      res.json({ 
        success: true,
        connected: actualStatus === 'connected',
        status: actualStatus,
        lastConnected: sessionData.last_connected_at,
        hasClient,
        qrCode: actualStatus === 'connected' ? null : qrCode, // Never show QR if connected
        requestId
      });

    } catch (error) {
      console.error(`[${requestId}] Error getting status for session ${sessionCode}:`, error);
      res.status(500).json({ 
        success: false, 
        error: 'Failed to get session status',
        requestId
      });
    }
  });

  // Send message to a single contact with enhanced validation
  app.post('/send/:sessionCode/:number/:message', verifyAuth, sendLimiter, async (req, res) => {
    const { sessionCode, number, message } = req.params;
    const userId = req.user.id;
    const requestId = uuidv4();
    
    try {
      // Verify session belongs to user
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        console.warn(`[${requestId}] Session not found or access denied: ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId
        });
      }

      const client = clients.get(sessionCode);
      if (!client) {
        console.warn(`[${requestId}] Session not connected: ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'Session not connected. Please connect first.',
          requestId
        });
      }

      // Check if client is ready
      const state = await client.getState();
      if (state !== 'CONNECTED') {
        console.warn(`[${requestId}] Client not connected: ${sessionCode} (state: ${state})`);
        return res.status(400).json({ 
          success: false, 
          error: 'WhatsApp client is not connected',
          state,
          requestId
        });
      }

      // Validate and format phone number
      if (!validatePhoneNumber(number)) {
        console.warn(`[${requestId}] Invalid phone number format: ${number}`);
        return res.status(400).json({ 
          success: false, 
          error: 'Invalid phone number format',
          requestId
        });
      }

      const formattedNumber = formatPhoneNumber(number);
      const decodedMessage = decodeURIComponent(message);

      // Check if number exists on WhatsApp
      const isRegistered = await client.isRegisteredUser(formattedNumber);
      if (!isRegistered) {
        console.warn(`[${requestId}] Number not registered: ${number}`);
        return res.status(400).json({ 
          success: false, 
          error: 'Number is not registered on WhatsApp',
          requestId
        });
      }

      // Send message with timeout
      const sendTimeout = setTimeout(() => {
        console.error(`[${requestId}] Message send timeout for ${number}`);
      }, 30000); // 30 second timeout

      const sentMessage = await client.sendMessage(formattedNumber, decodedMessage);
      clearTimeout(sendTimeout);
      
      // Update session last activity
      await updateSessionInDB(sessionCode, { 
        last_connected_at: new Date().toISOString(),
        last_activity: 'message_sent',
        last_activity_at: new Date().toISOString()
      });
      
      console.log(`[${requestId}] Message sent to ${number} via session ${sessionCode}`);
      res.json({ 
        success: true, 
        messageId: sentMessage.id._serialized,
        to: number,
        message: decodedMessage,
        timestamp: new Date().toISOString(),
        requestId
      });

    } catch (error) {
      console.error(`[${requestId}] Error sending message in session ${sessionCode}:`, error);
      res.status(500).json({ 
        success: false, 
        error: 'Failed to send message',
        details: error.message,
        requestId
      });
    }
  });

  // Enhanced bulk message sending with worker threads
  app.post('/send-bulk/:sessionCode', verifyAuth, sendLimiter, async (req, res) => {
    const { sessionCode } = req.params;
    const { contacts, message, interval = 5000 } = req.body;
    const userId = req.user.id;
    const requestId = uuidv4();
    
    try {
      // Verify session belongs to user
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        console.warn(`[${requestId}] Session not found or access denied: ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId
        });
      }

      const client = clients.get(sessionCode);
      if (!client) {
        console.warn(`[${requestId}] Session not connected: ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'Session not connected. Please connect first.',
          requestId
        });
      }

      // Check if client is ready
      const state = await client.getState();
      if (state !== 'CONNECTED') {
        console.warn(`[${requestId}] Client not connected: ${sessionCode} (state: ${state})`);
        return res.status(400).json({ 
          success: false, 
          error: 'WhatsApp client is not connected',
          state,
          requestId
        });
      }

      if (!Array.isArray(contacts) || contacts.length === 0) {
        console.warn(`[${requestId}] Empty contacts array`);
        return res.status(400).json({ 
          success: false, 
          error: 'Contacts array is required and cannot be empty',
          requestId
        });
      }

      if (!message || message.trim().length === 0) {
        console.warn(`[${requestId}] Empty message`);
        return res.status(400).json({ 
          success: false, 
          error: 'Message is required',
          requestId
        });
      }

      // Limit the number of contacts per request
      const MAX_CONTACTS = 1000;
      if (contacts.length > MAX_CONTACTS) {
        console.warn(`[${requestId}] Too many contacts: ${contacts.length}`);
        return res.status(400).json({ 
          success: false, 
          error: `Too many contacts. Maximum ${MAX_CONTACTS} per request.`,
          requestId
        });
      }

      // Create a bulk send job ID for tracking
      const jobId = uuidv4();
      
      // Immediately respond that the job is being processed
      res.json({ 
        success: true,
        jobId,
        message: 'Bulk send job started. Results will be processed in the background.',
        totalContacts: contacts.length,
        requestId
      });

      // Process the bulk send in the background using a worker thread
      const worker = new Worker(path.join(__dirname, 'bulk-send-worker.js'), {
        workerData: {
          jobId,
          sessionCode,
          contacts,
          message,
          interval: Math.max(interval, 3000), // Minimum 3 seconds between messages
          requestId,
          userId
        }
      });

      worker.on('message', (message) => {
        console.log(`[${requestId}] Worker message:`, message);
      });

      worker.on('error', (error) => {
        console.error(`[${requestId}] Worker error:`, error);
      });

      worker.on('exit', (code) => {
        if (code !== 0) {
          console.error(`[${requestId}] Worker stopped with exit code ${code}`);
        }
      });

    } catch (error) {
      console.error(`[${requestId}] Error in bulk send for session ${sessionCode}:`, error);
      res.status(500).json({ 
        success: false, 
        error: 'Failed to initiate bulk send',
        details: error.message,
        requestId
      });
    }
  });

  // Disconnect session with enhanced cleanup
  app.post('/disconnect/:sessionCode', verifyAuth, async (req, res) => {
    const { sessionCode } = req.params;
    const userId = req.user.id;
    const requestId = uuidv4();
    
    try {
      // Verify session belongs to user
      const sessionData = await getSessionFromDB(sessionCode, userId);
      if (!sessionData) {
        console.warn(`[${requestId}] Session not found or access denied: ${sessionCode}`);
        return res.status(404).json({ 
          success: false, 
          error: 'Session not found or access denied',
          requestId
        });
      }

      const client = clients.get(sessionCode);
      
      if (client) {
        try {
          console.log(`[${requestId}] Disconnecting session: ${sessionCode}`);
          await client.logout();
          await client.destroy();
        } catch (error) {
          console.error(`[${requestId}] Error destroying client ${sessionCode}:`, error);
        }
        clients.delete(sessionCode);
        await redis.del(`qr:${sessionCode}`);
      }
      
      // Update session status in database
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null,
        last_disconnected_at: new Date().toISOString()
      });

      console.log(`[${requestId}] Session disconnected successfully: ${sessionCode}`);
      res.json({ 
        success: true, 
        message: 'Session disconnected successfully',
        requestId
      });

    } catch (error) {
      console.error(`[${requestId}] Error disconnecting session ${sessionCode}:`, error);
      
      // Force cleanup
      clients.delete(sessionCode);
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
        requestId
      });
    }
  });

  // Get user's sessions with pagination
  app.get('/sessions', verifyAuth, async (req, res) => {
    const userId = req.user.id;
    const { page = 1, limit = 20 } = req.query;
    const requestId = uuidv4();
    
    try {
      const { data: sessions, error, count } = await supabase
        .from('sessions')
        .select('*', { count: 'exact' })
        .eq('user_id', userId)
        .order('created_at', { ascending: false })
        .range((page - 1) * limit, page * limit - 1);

      if (error) {
        throw error;
      }

      const sessionsWithStatus = sessions.map(session => ({
        ...session,
        hasClient: clients.has(session.session_code),
        isActive: clients.has(session.session_code) && session.status === 'connected'
      }));
      
      console.log(`[${requestId}] Fetched ${sessions.length} sessions for user ${userId}`);
      res.json({ 
        success: true, 
        sessions: sessionsWithStatus,
        total: count,
        page: parseInt(page),
        limit: parseInt(limit),
        requestId
      });

    } catch (error) {
      console.error(`[${requestId}] Error fetching user sessions:`, error);
      res.status(500).json({ 
        success: false, 
        error: 'Failed to fetch sessions',
        requestId
      });
    }
  });

  // Validate phone number endpoint with enhanced response
  app.post('/validate-phone', (req, res) => {
    const { phone } = req.body;
    const requestId = uuidv4();
    
    if (!phone) {
      console.warn(`[${requestId}] No phone number provided`);
      return res.status(400).json({ 
        success: false, 
        error: 'Phone number is required',
        requestId
      });
    }

    const isValid = validatePhoneNumber(phone);
    const formatted = isValid ? formatPhoneNumber(phone) : null;
    
    console.log(`[${requestId}] Phone validation: ${phone} - ${isValid ? 'valid' : 'invalid'}`);
    res.json({ 
      success: true,
      valid: isValid,
      original: phone,
      formatted: formatted ? formatted.replace('@c.us', '') : null,
      internationalFormat: isValid ? phone.replace(/[^\d+]/g, '') : null,
      requestId
    });
  });

  // Error handling middleware with request tracking
  app.use((error, req, res, next) => {
    const requestId = req.headers['x-request-id'] || uuidv4();
    console.error(`[${requestId}] Unhandled error:`, error);
    
    res.status(500).json({ 
      success: false, 
      error: 'Internal server error',
      requestId,
      details: process.env.NODE_ENV === 'development' ? {
        message: error.message,
        stack: error.stack,
        type: error.constructor.name
      } : undefined
    });
  });

  // 404 handler with request tracking
  app.use((req, res) => {
    const requestId = uuidv4();
    console.warn(`[${requestId}] 404 Not Found: ${req.method} ${req.originalUrl}`);
    
    res.status(404).json({ 
      success: false, 
      error: 'Endpoint not found',
      requestId,
      path: req.path,
      method: req.method
    });
  });

  // Graceful shutdown with enhanced cleanup
  async function gracefulShutdown(signal) {
    console.log(`${signal} received, shutting down gracefully...`);
    
    // Stop accepting new connections
    server.close(async () => {
      console.log('HTTP server closed');
      
      // Disconnect all WhatsApp clients
      const disconnectPromises = Array.from(clients.entries()).map(async ([sessionCode, client]) => {
        try {
          console.log(`Disconnecting session ${sessionCode}...`);
          await client.destroy();
          await redis.del(`qr:${sessionCode}`);
          await updateSessionInDB(sessionCode, { 
            status: 'disconnected',
            qr_code: null,
            last_disconnected_at: new Date().toISOString()
          });
        } catch (error) {
          console.error(`Error disconnecting session ${sessionCode}:`, error);
        }
      });
      
      await Promise.all(disconnectPromises);
      
      // Clear intervals
      clearInterval(sessionCleanupInterval);
      
      // Close Redis connection
      await redis.quit();
      console.log('Redis connection closed');
      
      process.exit(0);
    });
    
    // Force shutdown after timeout
    setTimeout(() => {
      console.error('Could not close connections in time, forcefully shutting down');
      process.exit(1);
    }, 10000);
  }

  // Handle shutdown signals
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));

  // Start server
  const server = app.listen(PORT, () => {
    console.log(`🚀 WhatsApp Bulk Sender Backend running on port ${PORT}`);
    console.log(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`💾 Sessions directory: ${SESSIONS_DIR}`);
    console.log(`🔗 Health check: http://localhost:${PORT}/health`);
    console.log(`🗄️  Supabase connected: ${!!supabase}`);
    console.log(`🔴 Redis connected: ${redis.status === 'ready'}`);
    console.log(`👷 Worker ${cluster.worker ? cluster.worker.id : 'master'} ready`);
  });

  // Handle server errors
  server.on('error', (error) => {
    console.error('Server error:', error);
    process.exit(1);
  });

  // Handle unhandled promise rejections
  process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  });

  // Handle uncaught exceptions
  process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    process.exit(1);
  });
}

// Worker thread implementation for bulk sending (bulk-send-worker.js)
// This would be a separate file in your project
/*
const { parentPort, workerData } = require('worker_threads');
const { Client, LocalAuth } = require('whatsapp-web.js');
const path = require('path');

async function processBulkSend() {
  const { jobId, sessionCode, contacts, message, interval, requestId, userId } = workerData;
  
  try {
    // Initialize client (similar to your main server code)
    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: sessionCode,
        dataPath: path.join(__dirname, 'sessions')
      }),
      puppeteer: {
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
      }
    });

    // Wait for client to be ready
    await client.initialize();

    const results = [];
    
    for (const contact of contacts) {
      try {
        // Process each contact (similar to your existing logic)
        // ...
        results.push({ /* success/failure result *\/ });
        
        // Wait between messages
        await new Promise(resolve => setTimeout(resolve, interval));
      } catch (error) {
        results.push({ /* error result *\/ });
      }
    }

    // Send results back to main thread
    parentPort.postMessage({ 
      jobId,
      success: true,
      results
    });

    await client.destroy();
  } catch (error) {
    parentPort.postMessage({
      jobId,
      success: false,
      error: error.message
    });
  }
}

processBulkSend();
*/

module.exports = app;