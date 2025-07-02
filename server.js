const express = require('express');
const cors = require('cors');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const fs = require('fs').promises;
const path = require('path');
const { rateLimit } = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
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
const { Worker } = require('worker_threads');
const { LRUCache } = require('lru-cache'); // Fixed import

// Initialize logger with production optimizations
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level: (label) => ({ level: label.toUpperCase() }),
  },
  timestamp: () => `,"time":"${new Date().toISOString()}"`,
  ...(process.env.NODE_ENV === 'production' ? {} : {
    transport: {
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:standard',
        ignore: 'pid,hostname'
      }
    }
  })
});

const app = express();
const PORT = process.env.PORT || 3001;

// Enhanced cluster mode with zero-downtime restarts
if (cluster.isMaster && process.env.NODE_ENV === 'production') {
  const numCPUs = Math.max(os.cpus().length, 2); // At least 2 workers
  logger.info(`Master ${process.pid} is running with ${numCPUs} workers`);
  
  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    const exitCode = worker.process.exitCode;
    logger.error(`Worker ${worker.process.pid} died (Code: ${exitCode}, Signal: ${signal}). Restarting...`);
    const newWorker = cluster.fork();
    logger.info(`Started new worker ${newWorker.process.pid}`);
  });
  
  // Zero-downtime restarts
  process.on('SIGUSR2', () => {
    const workers = Object.values(cluster.workers);
    
    const restartWorker = (index) => {
      if (index >= workers.length) return;
      
      const worker = workers[index];
      logger.info(`Restarting worker ${worker.process.pid}`);
      
      worker.once('exit', () => {
        const newWorker = cluster.fork();
        newWorker.once('listening', () => {
          restartWorker(index + 1);
        });
      });
      
      worker.disconnect();
    };
    
    restartWorker(0);
  });
  
  return;
}

// Enhanced Redis client with connection pooling and failover
const redis = new Redis.Cluster(
  process.env.REDIS_CLUSTER_NODES 
    ? process.env.REDIS_CLUSTER_NODES.split(',').map(node => {
        const [host, port] = node.split(':');
        return { host, port: port || 6379 };
      })
    : [{
        host: process.env.REDIS_HOST || 'clean-panda-53790.upstash.io',
        port: process.env.REDIS_PORT || 6379,
        password: process.env.REDIS_PASSWORD || 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
        tls: {}
      }],
  {
    scaleReads: 'slave',
    redisOptions: {
      maxRetriesPerRequest: 3,
      retryStrategy: (times) => Math.min(times * 50, 2000),
      enableOfflineQueue: false,
      connectTimeout: 10000,
      commandTimeout: 5000
    }
  }
);

// Redis error handling
redis.on('error', (err) => {
  if (err.code === 'ECONNREFUSED') {
    logger.error('Redis connection refused, please check Redis server');
  } else {
    logger.error('Redis error:', err);
  }
});

// Initialize Supabase client with enhanced configuration
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    persistSession: false,
    autoRefreshToken: false
  },
  db: {
    schema: 'public',
  },
  global: {
    headers: {
      'X-Client-Info': 'whatsapp-bulk-sender/1.0'
    }
  }
});

if (!supabaseUrl || !supabaseServiceKey) {
  logger.error('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
  process.exit(1);
}

// HTTP logger middleware with production optimizations
app.use(pinoHttp({
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
      hostname: req.hostname,
      remoteAddress: req.ip,
      userAgent: req.headers['user-agent']
    }),
    res: (res) => ({
      statusCode: res.statusCode
    })
  }
}));

// Security and performance middleware with enhanced CSP
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https:"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'", "https://*.upstash.io", supabaseUrl, "wss:"],
      fontSrc: ["'self'", "https:", "data:"],
      objectSrc: ["'none'"],
      upgradeInsecureRequests: []
    }
  },
  hsts: {
    maxAge: 63072000,
    includeSubDomains: true,
    preload: true
  },
  frameguard: {
    action: 'deny'
  }
}));

app.use(compression({
  level: 6,
  threshold: '10kb',
  filter: (req, res) => {
    if (req.headers['x-no-compression']) return false;
    return compression.filter(req, res);
  },
  brotli: {
    enabled: true,
    zlib: {}
  }
}));

app.use(cors({
  origin: process.env.NODE_ENV === 'production' 
    ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
    : ['http://localhost:5173', 'http://localhost:3000'],
  credentials: true,
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
  exposedHeaders: ['X-RateLimit-Limit', 'X-RateLimit-Remaining', 'X-RateLimit-Reset'],
  maxAge: 86400
}));

// Enhanced rate limiting with Redis cluster support
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  store: new RedisStore({
    sendCommand: (...args) => redis.sendCommand(...args),
    prefix: 'rl:'
  }),
  skip: (req) => {
    // Skip rate limiting for health checks
    return req.path === '/health';
  }
});

// Stricter rate limiting for message sending with Redis cluster
const sendLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    const sessionCode = req.params.sessionCode || req.body.sessionCode;
    return `${req.user.id}:${sessionCode}`;
  },
  store: new RedisStore({
    sendCommand: (...args) => redis.sendCommand(...args),
    prefix: 'rl_msg:'
  })
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Enhanced session management with LRU cache for active clients
const clients = new LRUCache({
  max: 5000, // Maximum number of active sessions
  ttl: 30 * 60 * 1000, // 30 minutes TTL
  dispose: async (client, sessionCode) => {
    try {
      logger.info(`LRU disposing session: ${sessionCode}`);
      await client.destroy();
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });
    } catch (err) {
      logger.error(`Error disposing session ${sessionCode}:`, err);
    }
  }
});

// Session cleanup interval
setInterval(() => {
  const now = Date.now();
  for (const [sessionCode, clientData] of clients) {
    try {
      if (now - clientData.lastActivity > 30 * 60 * 1000) {
        logger.info(`Cleaning up inactive session: ${sessionCode}`);
        clients.delete(sessionCode);
      }
    } catch (err) {
      logger.error(`Error in session cleanup for ${sessionCode}:`, err);
    }
  }
}, 5 * 60 * 1000); // Run every 5 minutes

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

// Middleware to verify Supabase JWT token with enhanced caching
const authCache = new LRUCache({
  max: 10000,
  ttl: 5 * 60 * 1000 // 5 minutes
});

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
    const cacheKey = `auth:${token}`;
    
    // Check memory cache first
    if (authCache.has(cacheKey)) {
      req.user = authCache.get(cacheKey);
      return next();
    }

    // Check Redis cache
    const cachedUser = await redis.get(cacheKey);
    if (cachedUser) {
      const user = JSON.parse(cachedUser);
      authCache.set(cacheKey, user);
      req.user = user;
      return next();
    }

    // Verify with Supabase
    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error || !user) {
      return res.status(401).json({ 
        success: false, 
        error: 'Invalid or expired token' 
      });
    }

    // Cache in both memory and Redis
    authCache.set(cacheKey, user);
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
const sessionCache = new LRUCache({
  max: 10000,
  ttl: 5 * 60 * 1000 // 5 minutes
});

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
      return false;
    }

    // Invalidate caches
    sessionCache.delete(cacheKey);
    await redis.del(cacheKey);
    return true;
  } catch (error) {
    logger.error('Database update error:', error);
    return false;
  }
}

async function getSessionFromDB(sessionCode, userId = null) {
  try {
    const cacheKey = `session:${sessionCode}`;
    
    // Check memory cache first
    if (sessionCache.has(cacheKey)) {
      const session = sessionCache.get(cacheKey);
      if (!userId || session.user_id === userId) {
        return session;
      }
      return null;
    }
    
    // Check Redis cache
    const cachedSession = await redis.get(cacheKey);
    if (cachedSession) {
      const session = JSON.parse(cachedSession);
      sessionCache.set(cacheKey, session);
      if (!userId || session.user_id === userId) {
        return session;
      }
      return null;
    }
    
    // Query database
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
    
    // Cache in both memory and Redis
    sessionCache.set(cacheKey, data);
    await redis.setex(cacheKey, 300, JSON.stringify(data));
    return data;
  } catch (error) {
    logger.error('Database fetch error:', error);
    return null;
  }
}

// Optimized phone number validation with precompiled regex
const phoneRegex = /^\+[1-9]\d{9,14}$/;
const phoneCache = new LRUCache({
  max: 100000,
  ttl: 24 * 60 * 60 * 1000 // 24 hours
});

function validatePhoneNumber(phone) {
  if (typeof phone !== 'string') return false;
  
  const cacheKey = `phone:${phone}`;
  if (phoneCache.has(cacheKey)) {
    return phoneCache.get(cacheKey);
  }
  
  const cleaned = phone.replace(/[^\d+]/g, '');
  const isValid = phoneRegex.test(cleaned);
  phoneCache.set(cacheKey, isValid);
  return isValid;
}

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
    const [redisPing, supabasePing, memoryUsage] = await Promise.all([
      redis.ping().then(() => true).catch(() => false),
      supabase.rpc('version').then(() => true).catch(() => false),
      process.memoryUsage()
    ]);
    
    res.json({ 
      status: 'healthy',
      system: {
        platform: process.platform,
        arch: process.arch,
        nodeVersion: process.version,
        memory: {
          rss: (memoryUsage.rss / 1024 / 1024).toFixed(2) + 'MB',
          heapTotal: (memoryUsage.heapTotal / 1024 / 1024).toFixed(2) + 'MB',
          heapUsed: (memoryUsage.heapUsed / 1024 / 1024).toFixed(2) + 'MB',
          external: (memoryUsage.external / 1024 / 1024).toFixed(2) + 'MB'
        },
        cpuUsage: process.cpuUsage(),
        uptime: process.uptime()
      },
      services: {
        redis: redisPing,
        supabase: supabasePing,
        activeSessions: clients.size,
        workerId: cluster.worker?.id || 'master'
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error('Health check failed:', error);
    res.status(500).json({ 
      status: 'unhealthy',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Get server statistics with enhanced metrics
app.get('/stats', verifyAuth, async (req, res) => {
  try {
    const [userSessions, redisInfo, systemStats] = await Promise.all([
      supabase
        .from('sessions')
        .select('status, created_at', { count: 'exact', head: true })
        .eq('user_id', req.user.id),
      redis.info(),
      {
        memory: process.memoryUsage(),
        cpu: process.cpuUsage(),
        uptime: process.uptime(),
        loadavg: os.loadavg()
      }
    ]);

    const stats = {
      user: {
        id: req.user.id,
        sessions: userSessions.count || 0
      },
      system: {
        activeSessions: clients.size,
        memory: {
          rss: (systemStats.memory.rss / 1024 / 1024).toFixed(2) + 'MB',
          heapUsed: (systemStats.memory.heapUsed / 1024 / 1024).toFixed(2) + 'MB'
        },
        cpu: {
          user: systemStats.cpu.user / 1000 + 'ms',
          system: systemStats.cpu.system / 1000 + 'ms'
        },
        load: systemStats.loadavg.map(v => v.toFixed(2)),
        uptime: systemStats.uptime
      },
      redis: {
        status: redis.status,
        memory: redisInfo.match(/used_memory:\d+/)?.[0] || 'unknown',
        connections: redisInfo.match(/connected_clients:\d+/)?.[0] || 'unknown'
      },
      worker: cluster.worker?.id || 'master',
      timestamp: new Date().toISOString()
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

// QR code generation worker pool
const QR_WORKER_PATH = path.join(__dirname, 'qr-worker.js');
const qrWorkers = new Map();

function getQRWorker() {
  if (!qrWorkers.has(cluster.worker?.id || 'master')) {
    const worker = new Worker(QR_WORKER_PATH, {
      workerData: { workerId: cluster.worker?.id || 'master' }
    });
    
    worker.on('error', (err) => {
      logger.error(`QR Worker error: ${err.message}`);
    });
    
    worker.on('exit', (code) => {
      if (code !== 0) {
        logger.error(`QR Worker stopped with exit code ${code}`);
        qrWorkers.delete(cluster.worker?.id || 'master');
      }
    });
    
    qrWorkers.set(cluster.worker?.id || 'master', worker);
  }
  
  return qrWorkers.get(cluster.worker?.id || 'master');
}

// Initialize WhatsApp session with optimized QR generation
app.post('/init/:sessionCode', verifyAuth, limiter, async (req, res) => {
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
      const client = clients.get(sessionCode).client;
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

    // Throttled QR code handler using worker thread
    const handleQR = throttle(async (qr) => {
      logger.info(`QR Code generated for session ${sessionCode}`);
      
      try {
        // Generate QR code in worker thread
        const worker = getQRWorker();
        const qrCodeImage = await new Promise((resolve, reject) => {
          const messageHandler = (message) => {
            if (message.sessionCode === sessionCode) {
              worker.off('message', messageHandler);
              if (message.error) {
                reject(new Error(message.error));
              } else {
                resolve(message.qrCode);
              }
            }
          };
          
          worker.on('message', messageHandler);
          worker.postMessage({ 
            qr, 
            sessionCode,
            workerId: cluster.worker?.id || 'master'
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

// Get QR code for session with caching
app.get('/qrcode/:sessionCode', verifyAuth, limiter, async (req, res) => {
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

    // Cache in Redis
    await redis.setex(`qr:${sessionCode}`, 300, sessionData.qr_code);

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
app.get('/status/:sessionCode', verifyAuth, limiter, async (req, res) => {
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

// Optimized bulk message sending with batching and progress tracking
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

    // Create a progress tracker in Redis
    const progressKey = `bulk:${sessionCode}:${Date.now()}`;
    await redis.set(progressKey, JSON.stringify({
      total: contacts.length,
      completed: 0,
      failed: 0,
      startedAt: new Date().toISOString()
    }));

    // Start processing in background
    processBulkMessages(client, contacts, message, interval, progressKey, sessionCode)
      .catch(err => logger.error(`Bulk send error for ${progressKey}:`, err));

    res.json({ 
      success: true,
      message: 'Bulk send started',
      progressKey,
      total: contacts.length,
      startedAt: new Date().toISOString()
    });

  } catch (error) {
    logger.error(`Error starting bulk send for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to start bulk messages',
      details: error.message 
    });
  }
});

// Background bulk message processing
async function processBulkMessages(client, contacts, message, interval, progressKey, sessionCode) {
  const BATCH_SIZE = 10;
  const maxInterval = Math.max(interval, 3000); // Minimum 3 seconds between batches
  const results = [];
  
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
    
    // Update progress
    const completed = results.length;
    const failed = results.filter(r => r.status === 'failed').length;
    
    await redis.set(progressKey, JSON.stringify({
      total: contacts.length,
      completed,
      failed,
      startedAt: new Date().toISOString(),
      lastUpdated: new Date().toISOString()
    }));
    
    // Wait before sending next batch (except for the last one)
    if (i + BATCH_SIZE < contacts.length) {
      await new Promise(resolve => setTimeout(resolve, maxInterval));
    }
  }

  // Final update
  const failed = results.filter(r => r.status === 'failed').length;
  await redis.setex(progressKey, 3600, JSON.stringify({
    total: contacts.length,
    completed: results.length,
    failed,
    startedAt: new Date().toISOString(),
    finishedAt: new Date().toISOString(),
    status: 'completed',
    results
  }));

  // Update session last activity
  if (clients.has(sessionCode)) {
    clients.get(sessionCode).lastActivity = Date.now();
    await updateSessionInDB(sessionCode, { 
      last_connected_at: new Date().toISOString() 
    });
  }
}

// Get bulk send progress
app.get('/bulk-progress/:progressKey', verifyAuth, async (req, res) => {
  const { progressKey } = req.params;
  
  try {
    const progressData = await redis.get(progressKey);
    if (!progressData) {
      return res.status(404).json({ 
        success: false, 
        error: 'Progress data not found or expired' 
      });
    }
    
    const progress = JSON.parse(progressData);
    res.json({ 
      success: true,
      ...progress
    });
  } catch (error) {
    logger.error(`Error getting progress for ${progressKey}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get progress',
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
  
  // Close QR workers
  qrWorkers.forEach(worker => {
    worker.postMessage({ command: 'shutdown' });
    worker.terminate();
  });

  // Cleanup all sessions
  const cleanupPromises = [];
  for (const [sessionCode, clientData] of clients) {
    cleanupPromises.push(
      clientData.client.destroy()
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