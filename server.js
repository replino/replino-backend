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

// Cluster mode for multi-core utilization
if (cluster.isMaster && process.env.NODE_ENV === 'production') {
  const numCPUs = Math.max(1, os.cpus().length - 1); // Leave one core free
  
  console.log(`Master ${process.pid} is running`);
  console.log(`Forking ${numCPUs} workers`);

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died with code ${code}. Restarting...`);
    cluster.fork();
  });
  
  return;
}

const app = express();
const PORT = process.env.PORT || 3001;

// Configure Express to trust proxies
app.set('trust proxy', process.env.NODE_ENV === 'production' ? 2 : 0);

// Enhanced Redis connection with robust error handling and reconnection
class RedisConnection {
  constructor() {
    this.client = null;
    this.isConnected = false;
    this.initialize();
  }

  initialize() {
    this.client = new Redis({
      host: process.env.REDIS_HOST || 'clean-panda-53790.upstash.io',
      port: process.env.REDIS_PORT || 6379,
      password: process.env.REDIS_PASSWORD || 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
      tls: process.env.REDIS_TLS === 'true' ? {} : undefined,
      retryStrategy: (times) => {
        const delay = Math.min(times * 100, 5000); // Max 5 second delay
        return delay;
      },
      maxRetriesPerRequest: null, // Set to null to disable retry limit
      enableOfflineQueue: false, // Disable offline queue to prevent request buildup
      reconnectOnError: (err) => {
        // Only reconnect on these specific errors
        const targetErrors = ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'NR_CLOSED'];
        if (targetErrors.includes(err.code)) {
          return true;
        }
        return false;
      },
      showFriendlyErrorStack: true,
      connectTimeout: 10000, // 10 second connection timeout
      commandTimeout: 5000, // 5 second command timeout
      keepAlive: 10000 // 10 second keepalive
    });

    this.client.on('connect', () => {
      console.log('Redis connection established');
      this.isConnected = true;
    });

    this.client.on('ready', () => {
      console.log('Redis client ready');
      this.isConnected = true;
    });

    this.client.on('error', (err) => {
      console.error('Redis error:', err.message);
      this.isConnected = false;
    });

    this.client.on('end', () => {
      console.log('Redis connection closed');
      this.isConnected = false;
    });

    this.client.on('reconnecting', (delay) => {
      console.log(`Attempting to reconnect to Redis in ${delay}ms`);
    });
  }

  async get(key) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }
    try {
      return await this.client.get(key);
    } catch (err) {
      console.error('Redis get error:', err.message);
      throw err;
    }
  }

  async setex(key, ttl, value) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }
    try {
      return await this.client.setex(key, ttl, value);
    } catch (err) {
      console.error('Redis setex error:', err.message);
      throw err;
    }
  }

  async del(key) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }
    try {
      return await this.client.del(key);
    } catch (err) {
      console.error('Redis del error:', err.message);
      throw err;
    }
  }

  async quit() {
    try {
      await this.client.quit();
    } catch (err) {
      console.error('Redis quit error:', err.message);
    }
  }
}

// Initialize Redis connection
const redis = new RedisConnection();

// Initialize Supabase client with connection pooling
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    autoRefreshToken: true,
    persistSession: true,
    detectSessionInUrl: false
  },
  global: {
    headers: {
      'x-connection-pool': `whatsapp-bulk-sender-${process.pid}`
    }
  }
});

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
  process.exit(1);
}

// Security and performance middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "blob:"],
      connectSrc: ["'self'", "https://*.supabase.co", "https://*.upstash.io"],
      frameSrc: ["'self'"],
      objectSrc: ["'none'"]
    }
  },
  hsts: {
    maxAge: 63072000, // 2 years
    includeSubDomains: true,
    preload: true
  },
  referrerPolicy: { policy: 'strict-origin-when-cross-origin' }
}));

app.use(compression({
  level: 6,
  threshold: 10 * 1024, // Compress responses larger than 10KB
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
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
  maxAge: 86400
}));

// Preflight OPTIONS handling
app.options('*', cors());

// Rate limiting with Redis store for cluster consistency
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
      error: 'Too many requests',
      retryAfter: req.rateLimit.resetTime
    });
  }
});

app.use(limiter);

// Stricter rate limiting for message sending with Redis store
const sendLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 60, // Limit each user to 60 messages per minute
  keyGenerator: (req) => {
    // Rate limit by user ID when authenticated
    return req.user ? req.user.id : req.ip;
  },
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({
      success: false,
      error: 'Too many messages sent',
      retryAfter: req.rateLimit.resetTime
    });
  }
});

// Body parsing with size limits
app.use(express.json({
  limit: '10mb',
  verify: (req, res, buf) => {
    req.rawBody = buf.toString();
  }
}));

app.use(express.urlencoded({
  extended: true,
  limit: '10mb',
  parameterLimit: 1000
}));

// In-memory storage for WhatsApp clients with TTL
const clients = new Map();
const CLIENT_TTL = 6 * 60 * 60 * 1000; // 6 hours

// Session cleanup interval
setInterval(() => {
  const now = Date.now();
  for (const [sessionCode, { lastActive }] of clients.entries()) {
    if (now - lastActive > CLIENT_TTL) {
      const client = clients.get(sessionCode).client;
      client.destroy().catch(() => {});
      clients.delete(sessionCode);
      redis.del(`qr:${sessionCode}`).catch(() => {});
      console.log(`Cleaned up inactive session: ${sessionCode}`);
    }
  }
}, 30 * 60 * 1000); // Run every 30 minutes

// Ensure sessions directory exists
const SESSIONS_DIR = path.join(__dirname, 'sessions');

async function ensureSessionsDir() {
  try {
    await fs.access(SESSIONS_DIR);
  } catch {
    await fs.mkdir(SESSIONS_DIR, { recursive: true });
    await fs.chmod(SESSIONS_DIR, 0o777); // Ensure write permissions
  }
}

// Initialize sessions directory on startup
ensureSessionsDir().catch(err => {
  console.error('Failed to initialize sessions directory:', err);
  process.exit(1);
});

// Middleware to verify Supabase JWT token with caching
async function verifyAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ 
      success: false, 
      error: 'Authorization token required' 
    });
  }

  const token = authHeader.substring(7);
  const cacheKey = `auth:${token}`;

  try {
    // Check cache first
    try {
      const cachedUser = await redis.get(cacheKey);
      if (cachedUser) {
        req.user = JSON.parse(cachedUser);
        return next();
      }
    } catch (redisError) {
      console.error('Redis cache check failed, proceeding without cache:', redisError.message);
    }

    // Verify token with Supabase
    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error || !user) {
      return res.status(401).json({ 
        success: false, 
        error: 'Invalid or expired token' 
      });
    }

    // Cache the user for 5 minutes
    try {
      await redis.setex(cacheKey, 300, JSON.stringify(user));
    } catch (redisError) {
      console.error('Failed to cache user in Redis:', redisError.message);
    }
    
    req.user = user;
    next();
  } catch (error) {
    console.error('Auth verification error:', error);
    res.status(401).json({ 
      success: false, 
      error: 'Authentication failed' 
    });
  }
}

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
          updated_at: new Date().toISOString()
        })
        .eq('session_code', sessionCode);

      if (!error) return true;
      
      attempts++;
      if (attempts < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, 500 * attempts));
      }
    } catch (error) {
      attempts++;
      if (attempts < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, 500 * attempts));
      }
    }
  }
  
  console.error('Failed to update session in DB after retries:', sessionCode);
  return false;
}

// Utility function to get session from database with caching
async function getSessionFromDB(sessionCode, userId = null) {
  const cacheKey = `session:${sessionCode}:${userId || 'any'}`;
  
  try {
    // Check cache first
    try {
      const cachedSession = await redis.get(cacheKey);
      if (cachedSession) return JSON.parse(cachedSession);
    } catch (redisError) {
      console.error('Redis cache check failed, proceeding without cache:', redisError.message);
    }
    
    let query = supabase
      .from('sessions')
      .select('*')
      .eq('session_code', sessionCode);
    
    if (userId) {
      query = query.eq('user_id', userId);
    }
    
    const { data, error } = await query.single();
    
    if (error || !data) return null;
    
    // Cache the session for 1 minute
    try {
      await redis.setex(cacheKey, 60, JSON.stringify(data));
    } catch (redisError) {
      console.error('Failed to cache session in Redis:', redisError.message);
    }
    
    return data;
  } catch (error) {
    console.error('Database fetch error:', error);
    return null;
  }
}

// Optimized phone number validation
function validatePhoneNumber(phone) {
  if (typeof phone !== 'string') return false;
  
  // Remove all non-digit characters except leading +
  const cleaned = phone.replace(/[^\d+]/g, '');
  
  // Validate international format
  return /^\+[1-9]\d{9,14}$/.test(cleaned);
}

// Optimized phone number formatting
function formatPhoneNumber(phone) {
  // Remove all non-digit characters except leading +
  let formatted = phone.replace(/[^\d+]/g, '');
  
  // Ensure it starts with +
  if (!formatted.startsWith('+')) {
    formatted = '+' + formatted;
  }
  
  // Remove any trailing @c.us if present
  formatted = formatted.replace(/@c\.us$/, '');
  
  return formatted.substring(1) + '@c.us';
}

// Optimized QR code generation with caching
const generateQRCode = throttle(async (qrData) => {
  try {
    return await qrcode.toDataURL(qrData, {
      width: 256,
      margin: 2,
      color: {
        dark: '#000000',
        light: '#FFFFFF'
      },
      errorCorrectionLevel: 'H'
    });
  } catch (error) {
    console.error('QR generation error:', error);
    throw error;
  }
}, 100); // Throttle to max 10 QR generations per second

// Health check endpoint with system diagnostics
app.get('/health', async (req, res) => {
  try {
    const [redisStatus, supabaseStatus, memoryUsage] = await Promise.all([
      (async () => {
        try {
          await redis.get('healthcheck');
          return 'connected';
        } catch {
          return 'disconnected';
        }
      })(),
      supabase.from('sessions').select('*', { count: 'exact', head: true }).limit(1)
        .then(() => 'connected')
        .catch(() => 'disconnected'),
      process.memoryUsage()
    ]);
    
    res.json({ 
      status: 'healthy',
      worker: process.pid,
      timestamp: new Date().toISOString(),
      activeSessions: clients.size,
      uptime: process.uptime(),
      memory: {
        rss: memoryUsage.rss / 1024 / 1024,
        heapTotal: memoryUsage.heapTotal / 1024 / 1024,
        heapUsed: memoryUsage.heapUsed / 1024 / 1024,
        external: memoryUsage.external / 1024 / 1024
      },
      redis: redisStatus,
      supabase: supabaseStatus,
      load: os.loadavg()
    });
  } catch (error) {
    res.status(500).json({
      status: 'unhealthy',
      error: error.message
    });
  }
});

// Get server statistics with more detailed metrics
app.get('/stats', verifyAuth, async (req, res) => {
  try {
    const [
      userSessionsCount,
      memoryUsage
    ] = await Promise.all([
      supabase
        .from('sessions')
        .select('*', { count: 'exact', head: true })
        .eq('user_id', req.user.id),
      process.memoryUsage()
    ]);

    const stats = {
      activeSessions: clients.size,
      userSessions: userSessionsCount.count || 0,
      uptime: process.uptime(),
      memory: {
        rss: memoryUsage.rss / 1024 / 1024,
        heapTotal: memoryUsage.heapTotal / 1024 / 1024,
        heapUsed: memoryUsage.heapUsed / 1024 / 1024,
        external: memoryUsage.external / 1024 / 1024
      },
      cpu: process.cpuUsage(),
      load: os.loadavg(),
      timestamp: new Date().toISOString(),
      redis: redis.isConnected ? 'connected' : 'disconnected',
      worker: process.pid
    };
    
    res.json({ success: true, stats });
  } catch (error) {
    console.error('Error fetching stats:', error);
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
  
  console.log(`[${requestId}] Initializing session ${sessionCode} for user ${userId}`);
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      console.log(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    // Check if session already exists and is connected
    if (clients.has(sessionCode)) {
      const { client, lastActive } = clients.get(sessionCode);
      
      try {
        const state = await client.getState();
        
        if (state === 'CONNECTED') {
          console.log(`[${requestId}] Session already connected`);
          
          await updateSessionInDB(sessionCode, { 
            status: 'connected',
            last_connected_at: new Date().toISOString()
          });
          
          // Update last active time
          clients.set(sessionCode, { client, lastActive: Date.now() });
          
          return res.json({ 
            success: true, 
            message: 'Session already connected',
            status: 'connected'
          });
        }
      } catch (error) {
        console.log(`[${requestId}] Existing client not responsive, cleaning up`);
        // Client exists but not responsive, clean it up
        try {
          await client.destroy();
        } catch (e) {}
        clients.delete(sessionCode);
        try {
          await redis.del(`qr:${sessionCode}`);
        } catch (e) {}
      }
    }

    // Update session status to connecting
    await updateSessionInDB(sessionCode, { 
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
          '--disable-software-rasterizer',
          '--disable-background-timer-throttling',
          '--disable-backgrounding-occluded-windows',
          '--disable-renderer-backgrounding'
        ],
        executablePath: process.env.CHROME_PATH || undefined
      },
      webVersionCache: {
        type: 'remote',
        remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html',
      },
      qrTimeoutMs: 0, // Disable QR timeout
      takeoverOnConflict: true, // Take over existing session
      restartOnAuthFail: true, // Restart if auth fails
      puppeteerOptions: {
        timeout: 60000 // Increase timeout to 60 seconds
      }
    });

    // Store client with last active time
    clients.set(sessionCode, { client, lastActive: Date.now() });

    // Set up event handlers with enhanced logging
    client.on('qr', async (qr) => {
      console.log(`[${sessionCode}] QR Code generated`);
      
      try {
        // Generate QR code image immediately
        const qrCodeImage = await generateQRCode(qr);

        // Store in Redis cache for faster access
        try {
          await redis.setex(`qr:${sessionCode}`, 300, qrCodeImage);
        } catch (redisError) {
          console.error(`[${sessionCode}] Failed to cache QR code:`, redisError.message);
        }

        // Update session with QR code in database
        await updateSessionInDB(sessionCode, { 
          status: 'connecting',
          qr_code: qrCodeImage 
        });

        console.log(`[${sessionCode}] QR code stored`);
      } catch (error) {
        console.error(`[${sessionCode}] QR generation error:`, error);
      }
    });

    client.on('loading_screen', async (percent, message) => {
      console.log(`[${sessionCode}] Loading: ${percent}% ${message || ''}`);
      if (percent === 100) {
        // QR code has been scanned - clear it
        try {
          await redis.del(`qr:${sessionCode}`);
        } catch (e) {}
        await updateSessionInDB(sessionCode, {
          qr_code: null,
          status: 'connecting'
        });
      }
    });

    client.on('authenticated', async () => {
      console.log(`[${sessionCode}] Authenticated`);
      try {
        await redis.del(`qr:${sessionCode}`);
      } catch (e) {}
      
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null
      });
      
      // Update last active time
      clients.set(sessionCode, { client, lastActive: Date.now() });
    });

    client.on('auth_failure', async (msg) => {
      console.error(`[${sessionCode}] Authentication failed:`, msg);
      try {
        await redis.del(`qr:${sessionCode}`);
      } catch (e) {}
      
      await updateSessionInDB(sessionCode, { 
        status: 'expired',
        qr_code: null 
      });
      
      clients.delete(sessionCode);
    });

    client.on('ready', async () => {
      console.log(`[${sessionCode}] Client ready`);
      try {
        await redis.del(`qr:${sessionCode}`);
      } catch (e) {}
      
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null 
      });
      
      // Update last active time
      clients.set(sessionCode, { client, lastActive: Date.now() });
    });

    client.on('disconnected', async (reason) => {
      console.log(`[${sessionCode}] Disconnected:`, reason);
      try {
        await redis.del(`qr:${sessionCode}`);
      } catch (e) {}
      
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });
      
      clients.delete(sessionCode);
    });

    // Initialize the client with timeout
    console.log(`[${requestId}] Initializing WhatsApp client`);
    
    const initializationTimeout = setTimeout(async () => {
      console.error(`[${requestId}] Initialization timeout for session ${sessionCode}`);
      try {
        await client.destroy();
      } catch (e) {}
      clients.delete(sessionCode);
      try {
        await redis.del(`qr:${sessionCode}`);
      } catch (e) {}
      
      await updateSessionInDB(sessionCode, { 
        status: 'failed',
        qr_code: null 
      });
    }, 60000); // 60 seconds timeout

    try {
      await client.initialize();
      clearTimeout(initializationTimeout);
      console.log(`[${requestId}] Client initialization started`);
      
      res.json({ 
        success: true, 
        message: 'Session initialization started',
        sessionCode,
        status: 'connecting'
      });
    } catch (error) {
      clearTimeout(initializationTimeout);
      throw error;
    }

  } catch (error) {
    console.error(`[${requestId}] Initialization error:`, error);
    try {
      await redis.del(`qr:${sessionCode}`);
    } catch (e) {}
    
    await updateSessionInDB(sessionCode, { 
      status: 'failed',
      qr_code: null 
    });
    
    if (clients.has(sessionCode)) {
      try {
        await clients.get(sessionCode).client.destroy();
      } catch (e) {}
      clients.delete(sessionCode);
    }
    
    res.status(500).json({ 
      success: false, 
      error: 'Failed to initialize session',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Get QR code for session with caching
app.get('/qrcode/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  console.log(`[${requestId}] Fetching QR code for session ${sessionCode}`);
  
  try {
    // Check Redis cache first
    let cachedQR;
    try {
      cachedQR = await redis.get(`qr:${sessionCode}`);
      if (cachedQR) {
        console.log(`[${requestId}] Returning cached QR code`);
        return res.json({ 
          success: true, 
          qrCode: cachedQR
        });
      }
    } catch (redisError) {
      console.error(`[${requestId}] Redis cache check failed:`, redisError.message);
    }

    // Verify session belongs to user and get QR code from database
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      console.log(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    if (!sessionData.qr_code) {
      console.log(`[${requestId}] QR code not available`);
      return res.status(404).json({ 
        success: false, 
        error: 'QR code not available. Please initialize session first.' 
      });
    }

    // Cache the QR code in Redis
    try {
      await redis.setex(`qr:${sessionCode}`, 300, sessionData.qr_code);
    } catch (redisError) {
      console.error(`[${requestId}] Failed to cache QR code:`, redisError.message);
    }
    
    console.log(`[${requestId}] Returning QR code from DB`);
    res.json({ 
      success: true, 
      qrCode: sessionData.qr_code 
    });

  } catch (error) {
    console.error(`[${requestId}] QR code fetch error:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get QR code',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Get session status with enhanced state checking
app.get('/status/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  console.log(`[${requestId}] Checking status for session ${sessionCode}`);
  
  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      console.log(`[${requestId}] Session not found or access denied`);
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
        const { client } = clients.get(sessionCode);
        const state = await client.getState();
        
        if (state === 'CONNECTED') {
          actualStatus = 'connected';
          // Ensure QR code is cleared when connected
          try {
            await redis.del(`qr:${sessionCode}`);
          } catch (e) {}
          qrCode = null;
          
          // Update last active time
          clients.set(sessionCode, { client, lastActive: Date.now() });
        } else if (state === 'QR_SCAN_COMPLETE') {
          // Special state when QR is scanned but not fully authenticated
          actualStatus = 'connecting';
          qrCode = null;
        } else {
          // Check if we should show QR code
          try {
            const cachedQR = await redis.get(`qr:${sessionCode}`);
            qrCode = cachedQR || sessionData.qr_code;
          } catch (e) {
            qrCode = sessionData.qr_code;
          }
        }
        
        await updateSessionInDB(sessionCode, { 
          status: actualStatus,
          qr_code: qrCode,
          last_connected_at: actualStatus === 'connected' ? new Date().toISOString() : sessionData.last_connected_at
        });
      } catch (error) {
        console.error(`[${requestId}] Client state check error:`, error);
        actualStatus = 'disconnected';
        
        // Clean up if client is not responsive
        try {
          const { client } = clients.get(sessionCode);
          await client.destroy();
        } catch (e) {}
        clients.delete(sessionCode);
        try {
          await redis.del(`qr:${sessionCode}`);
        } catch (e) {}
      }
    }
    
    console.log(`[${requestId}] Returning status: ${actualStatus}`);
    res.json({ 
      success: true,
      connected: actualStatus === 'connected',
      status: actualStatus,
      lastConnected: sessionData.last_connected_at,
      hasClient,
      qrCode: actualStatus === 'connected' ? null : qrCode // Never show QR if connected
    });

  } catch (error) {
    console.error(`[${requestId}] Status check error:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get session status',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Send message to a single contact with enhanced validation
app.post('/send/:sessionCode/:number/:message', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode, number, message } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  console.log(`[${requestId}] Sending message via session ${sessionCode}`);
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      console.log(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    const clientData = clients.get(sessionCode);
    if (!clientData) {
      console.log(`[${requestId}] Session not connected`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not connected. Please connect first.' 
      });
    }

    const { client } = clientData;
    
    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      console.log(`[${requestId}] Client not connected, state: ${state}`);
      return res.status(400).json({ 
        success: false, 
        error: 'WhatsApp client is not connected',
        state 
      });
    }

    // Validate and format phone number
    if (!validatePhoneNumber(number)) {
      console.log(`[${requestId}] Invalid phone number: ${number}`);
      return res.status(400).json({ 
        success: false, 
        error: 'Invalid phone number format' 
      });
    }

    const formattedNumber = formatPhoneNumber(number);
    const decodedMessage = decodeURIComponent(message);

    // Check if number exists on WhatsApp
    const isRegistered = await client.isRegisteredUser(formattedNumber);
    if (!isRegistered) {
      console.log(`[${requestId}] Number not registered: ${number}`);
      return res.status(400).json({ 
        success: false, 
        error: 'Number is not registered on WhatsApp' 
      });
    }

    // Send message with timeout
    const sendTimeout = setTimeout(() => {
      console.error(`[${requestId}] Message send timeout for ${number}`);
      throw new Error('Message send timeout');
    }, 30000); // 30 seconds timeout

    try {
      const sentMessage = await client.sendMessage(formattedNumber, decodedMessage);
      clearTimeout(sendTimeout);
      
      // Update session last activity
      await updateSessionInDB(sessionCode, { 
        last_connected_at: new Date().toISOString() 
      });
      
      // Update last active time
      clients.set(sessionCode, { client, lastActive: Date.now() });
      
      console.log(`[${requestId}] Message sent to ${number}`);
      res.json({ 
        success: true, 
        messageId: sentMessage.id._serialized,
        to: number,
        message: decodedMessage,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      clearTimeout(sendTimeout);
      throw error;
    }

  } catch (error) {
    console.error(`[${requestId}] Message send error:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send message',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Send bulk messages with optimized sending
app.post('/send-bulk/:sessionCode', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode } = req.params;
  const { contacts, message, interval = 5000 } = req.body;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  console.log(`[${requestId}] Starting bulk send for ${contacts.length} contacts`);
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      console.log(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    const clientData = clients.get(sessionCode);
    if (!clientData) {
      console.log(`[${requestId}] Session not connected`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not connected. Please connect first.' 
      });
    }

    const { client } = clientData;
    
    // Check if client is ready
    const state = await client.getState();
    if (state !== 'CONNECTED') {
      console.log(`[${requestId}] Client not connected, state: ${state}`);
      return res.status(400).json({ 
        success: false, 
        error: 'WhatsApp client is not connected',
        state 
      });
    }

    if (!Array.isArray(contacts) || contacts.length === 0) {
      console.log(`[${requestId}] Invalid contacts array`);
      return res.status(400).json({ 
        success: false, 
        error: 'Contacts array is required and cannot be empty' 
      });
    }

    if (!message || message.trim().length === 0) {
      console.log(`[${requestId}] Empty message`);
      return res.status(400).json({ 
        success: false, 
        error: 'Message is required' 
      });
    }

    // Limit the number of contacts per request
    const MAX_CONTACTS = 1000;
    if (contacts.length > MAX_CONTACTS) {
      console.log(`[${requestId}] Too many contacts: ${contacts.length}`);
      return res.status(400).json({ 
        success: false, 
        error: `Too many contacts. Maximum ${MAX_CONTACTS} per request.` 
      });
    }

    const results = [];
    const maxInterval = Math.max(interval, 3000); // Minimum 3 seconds between messages
    const batchSize = 5; // Number of messages to send in parallel
    const totalBatches = Math.ceil(contacts.length / batchSize);
    
    for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
      const batchStart = batchIndex * batchSize;
      const batchEnd = Math.min(batchStart + batchSize, contacts.length);
      const batch = contacts.slice(batchStart, batchEnd);
      
      // Process batch in parallel
      const batchResults = await Promise.all(batch.map(async (contact, index) => {
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

          // Send message with timeout
          const sendTimeout = setTimeout(() => {
            console.error(`[${requestId}] Message send timeout for ${contact.phone}`);
            throw new Error('Message send timeout');
          }, 30000); // 30 seconds timeout

          try {
            const msgContent = contact.message || message;
            const sentMessage = await client.sendMessage(formattedNumber, msgContent);
            clearTimeout(sendTimeout);
            
            return {
              contact: contact.phone,
              status: 'sent',
              messageId: sentMessage.id._serialized,
              timestamp: new Date().toISOString()
            };
          } catch (error) {
            clearTimeout(sendTimeout);
            throw error;
          }
        } catch (error) {
          console.error(`[${requestId}] Error sending to ${contact.phone}:`, error);
          return {
            contact: contact.phone,
            status: 'failed',
            error: error.message
          };
        }
      }));
      
      results.push(...batchResults);
      
      // Wait before sending next batch (except for the last batch)
      if (batchIndex < totalBatches - 1) {
        await new Promise(resolve => setTimeout(resolve, maxInterval));
      }
    }

    // Update session last activity
    await updateSessionInDB(sessionCode, { 
      last_connected_at: new Date().toISOString() 
    });
    
    // Update last active time
    clients.set(sessionCode, { client, lastActive: Date.now() });
    
    const successCount = results.filter(r => r.status === 'sent').length;
    const failedCount = results.filter(r => r.status === 'failed').length;
    
    console.log(`[${requestId}] Bulk send completed: ${successCount} sent, ${failedCount} failed`);
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
    console.error(`[${requestId}] Bulk send error:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send bulk messages',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Disconnect session with enhanced cleanup
app.post('/disconnect/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  const requestId = uuidv4();
  
  console.log(`[${requestId}] Disconnecting session ${sessionCode}`);
  
  try {
    // Verify session belongs to user
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      console.log(`[${requestId}] Session not found or access denied`);
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or access denied' 
      });
    }

    const clientData = clients.get(sessionCode);
    
    if (clientData) {
      const { client } = clientData;
      try {
        console.log(`[${requestId}] Logging out client`);
        await client.logout();
        await client.destroy();
        console.log(`[${requestId}] Client destroyed`);
      } catch (error) {
        console.error(`[${requestId}] Error destroying client:`, error);
        try {
          await client.destroy();
        } catch (e) {}
      }
      clients.delete(sessionCode);
      try {
        await redis.del(`qr:${sessionCode}`);
      } catch (e) {}
    }
    
    // Update session status in database
    await updateSessionInDB(sessionCode, { 
      status: 'disconnected',
      qr_code: null 
    });

    console.log(`[${requestId}] Session disconnected successfully`);
    res.json({ 
      success: true, 
      message: 'Session disconnected successfully' 
    });

  } catch (error) {
    console.error(`[${requestId}] Disconnection error:`, error);
    
    // Force cleanup
    clients.delete(sessionCode);
    try {
      await redis.del(`qr:${sessionCode}`);
    } catch (e) {}
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

// Get user's sessions with caching
app.get('/sessions', verifyAuth, async (req, res) => {
  const userId = req.user.id;
  const requestId = uuidv4();
  
  console.log(`[${requestId}] Fetching sessions for user ${userId}`);
  
  try {
    const cacheKey = `user_sessions:${userId}`;
    
    // Check cache first
    try {
      const cachedSessions = await redis.get(cacheKey);
      if (cachedSessions) {
        console.log(`[${requestId}] Returning cached sessions`);
        return res.json({ 
          success: true, 
          sessions: JSON.parse(cachedSessions),
          cached: true
        });
      }
    } catch (redisError) {
      console.error(`[${requestId}] Redis cache check failed:`, redisError.message);
    }

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
    
    // Cache the sessions for 1 minute
    try {
      await redis.setex(cacheKey, 60, JSON.stringify(sessionsWithStatus));
    } catch (redisError) {
      console.error(`[${requestId}] Failed to cache sessions:`, redisError.message);
    }
    
    console.log(`[${requestId}] Returning ${sessions.length} sessions`);
    res.json({ 
      success: true, 
      sessions: sessionsWithStatus,
      total: sessions.length,
      cached: false
    });

  } catch (error) {
    console.error(`[${requestId}] Sessions fetch error:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to fetch sessions',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Validate phone number endpoint with caching
app.post('/validate-phone', async (req, res) => {
  const { phone } = req.body;
  
  if (!phone) {
    return res.status(400).json({ 
      success: false, 
      error: 'Phone number is required' 
    });
  }

  const cacheKey = `phone_validation:${phone}`;
  
  try {
    // Check cache first
    try {
      const cachedValidation = await redis.get(cacheKey);
      if (cachedValidation) {
        return res.json(JSON.parse(cachedValidation));
      }
    } catch (redisError) {
      console.error('Redis cache check failed:', redisError.message);
    }

    const isValid = validatePhoneNumber(phone);
    const formatted = isValid ? formatPhoneNumber(phone) : null;
    
    const result = { 
      success: true,
      valid: isValid,
      original: phone,
      formatted: formatted ? formatted.replace('@c.us', '') : null
    };

    // Cache the result for 1 hour
    try {
      await redis.setex(cacheKey, 3600, JSON.stringify(result));
    } catch (redisError) {
      console.error('Failed to cache phone validation:', redisError.message);
    }
    
    res.json(result);
  } catch (error) {
    console.error('Phone validation error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to validate phone number' 
    });
  }
});

// Error handling middleware
app.use((error, req, res, next) => {
  const requestId = req.headers['x-request-id'] || uuidv4();
  console.error(`[${requestId}] Unhandled error:`, error);
  
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error',
    requestId,
    details: process.env.NODE_ENV === 'development' ? error.message : undefined
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ 
    success: false, 
    error: 'Endpoint not found',
    path: req.path
  });
});

// Graceful shutdown handler
async function gracefulShutdown(signal) {
  console.log(`\n${signal} received, shutting down gracefully...`);
  
  // Close all WhatsApp clients
  const disconnectPromises = [];
  for (const [sessionCode, { client }] of clients.entries()) {
    disconnectPromises.push(
      (async () => {
        try {
          console.log(`Disconnecting session ${sessionCode}...`);
          await client.destroy();
          try {
            await redis.del(`qr:${sessionCode}`);
          } catch (e) {}
          await updateSessionInDB(sessionCode, { 
            status: 'disconnected',
            qr_code: null 
          });
        } catch (error) {
          console.error(`Error disconnecting session ${sessionCode}:`, error);
        }
      })()
    );
  }
  
  await Promise.all(disconnectPromises);
  
  // Close Redis connection
  try {
    await redis.quit();
  } catch (error) {
    console.error('Error closing Redis connection:', error);
  }
  
  console.log('Cleanup complete. Exiting...');
  process.exit(0);
}

// Handle shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Handle uncaught exceptions and rejections
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  // Attempt to log the error before exiting
  setTimeout(() => process.exit(1), 1000);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Start server
const server = app.listen(PORT, () => {
  console.log(`🚀 WhatsApp Bulk Sender Backend running on port ${PORT} (Worker ${process.pid})`);
  console.log(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`💾 Sessions directory: ${SESSIONS_DIR}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/health`);
});

// Handle server errors
server.on('error', (error) => {
  if (error.code === 'EADDRINUSE') {
    console.error(`Port ${PORT} is already in use`);
    process.exit(1);
  } else {
    console.error('Server error:', error);
  }
});

// Handle process exit
process.on('exit', (code) => {
  console.log(`Process exiting with code ${code}`);
});

module.exports = app;