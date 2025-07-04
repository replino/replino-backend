const express = require('express');
const cors = require('cors');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const fs = require('fs').promises;
const path = require('path');
const rateLimit = require('express-rate-limit');
const { rateLimit: redisRateLimit } = require('express-rate-limit/redis');
const helmet = require('helmet');
const compression = require('compression');
const { createClient } = require('@supabase/supabase-js');
const Redis = require('ioredis');
const cluster = require('cluster');
const os = require('os');
const { createBullBoard } = require('@bull-board/api');
const { BullAdapter } = require('@bull-board/api/bullAdapter');
const { ExpressAdapter } = require('@bull-board/express');
const Queue = require('bull');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 3001;
const numCPUs = process.env.NODE_ENV === 'production' ? os.cpus().length : 1;

// Cluster setup for multi-core utilization
if (cluster.isPrimary && process.env.NODE_ENV === 'production') {
  console.log(`Master ${process.pid} is running`);
  
  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
    cluster.fork();
  });
  
  return;
}

// Configure Express to trust proxies safely
const proxySetting = process.env.TRUST_PROXY || 'loopback';
app.set('trust proxy', proxySetting);

// Initialize Redis client for distributed systems
const redisConfig = {
  host: process.env.REDIS_HOST || 'clean-panda-53790.upstash.io',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD || 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
  tls: process.env.REDIS_TLS === 'true' ? {} : undefined,
  enableOfflineQueue: false,
  maxRetriesPerRequest: null
};

const redis = new Redis(redisConfig);
redis.on('error', (err) => console.error('Redis error:', err));

// Initialize Supabase client
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('❌ Missing Supabase configuration');
  process.exit(1);
}

// Initialize Bull queues
const messageQueue = new Queue('whatsapp-messages', {
  redis: redisConfig,
  limiter: {
    max: 30,
    duration: 60000
  }
});

// Bull Board setup for queue monitoring
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

createBullBoard({
  queues: [new BullAdapter(messageQueue)],
  serverAdapter
});

// Security and performance middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https://qrcode.com"]
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

// Rate limiting using Redis for distributed systems
const limiter = rateLimit({
  store: new redisRateLimit(redis),
  windowMs: 15 * 60 * 1000,
  max: 1000,
  message: 'Too many requests, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  validate: { trustProxy: false }
});

app.use(limiter);

// Stricter rate limiting for message sending
const sendLimiter = rateLimit({
  store: new redisRateLimit(redis),
  windowMs: 60 * 1000,
  max: 60,
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  validate: { trustProxy: false }
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use('/admin/queues', serverAdapter.getRouter());

// Distributed session management using Redis
const clients = new Map();

// Ensure sessions directory exists
const SESSIONS_DIR = path.join(__dirname, 'sessions');

async function ensureSessionsDir() {
  try {
    await fs.access(SESSIONS_DIR);
  } catch {
    await fs.mkdir(SESSIONS_DIR, { recursive: true });
  }
}

ensureSessionsDir();

// Enhanced auth middleware with caching
async function verifyAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Authorization token required' });
  }

  const token = authHeader.substring(7);
  
  try {
    // Check Redis cache first
    const cachedUser = await redis.get(`auth:${token}`);
    if (cachedUser) {
      req.user = JSON.parse(cachedUser);
      return next();
    }

    // Verify with Supabase
    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error || !user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    // Cache valid token for 5 minutes
    await redis.setex(`auth:${token}`, 300, JSON.stringify(user));
    req.user = user;
    next();
  } catch (error) {
    console.error('Auth verification error:', error);
    res.status(401).json({ error: 'Authentication failed' });
  }
}

// Utility functions with Redis caching
async function updateSessionInDB(sessionCode, updates) {
  try {
    const { error } = await supabase
      .from('sessions')
      .update({ ...updates, updated_at: new Date().toISOString() })
      .eq('session_code', sessionCode);

    if (error) console.error('DB update error:', error);
  } catch (error) {
    console.error('DB error:', error);
  }
}

async function getSessionFromDB(sessionCode, userId = null) {
  try {
    const cacheKey = `session:${sessionCode}`;
    const cachedSession = await redis.get(cacheKey);
    if (cachedSession) return JSON.parse(cachedSession);

    let query = supabase
      .from('sessions')
      .select('*')
      .eq('session_code', sessionCode);
    
    if (userId) query = query.eq('user_id', userId);
    
    const { data, error } = await query.single();
    
    if (error) {
      console.error('DB fetch error:', error);
      return null;
    }
    
    // Cache session for 1 minute
    if (data) await redis.setex(cacheKey, 60, JSON.stringify(data));
    return data;
  } catch (error) {
    console.error('DB error:', error);
    return null;
  }
}

// Phone number utilities
function validatePhoneNumber(phone) {
  const cleaned = phone.replace(/[^\d+]/g, '');
  return /^\+[1-9]\d{9,14}$/.test(cleaned);
}

function formatPhoneNumber(phone) {
  const formatted = phone.replace(/[^\d+]/g, '').replace(/^\+?/, '+');
  return formatted.substring(1) + '@c.us';
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    activeSessions: clients.size,
    uptime: process.uptime(),
    worker: cluster.worker ? cluster.worker.id : 'single',
    memory: process.memoryUsage()
  });
});

// Initialize WhatsApp session (optimized)
app.post('/init/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    // Check existing session
    const existingClient = clients.get(sessionCode);
    if (existingClient) {
      try {
        const state = await existingClient.getState();
        if (state === 'CONNECTED') {
          await updateSessionInDB(sessionCode, { 
            status: 'connected',
            last_connected_at: new Date().toISOString()
          });
          return res.json({ status: 'connected', message: 'Session already active' });
        }
      } catch (error) {
        clients.delete(sessionCode);
      }
    }

    // Update session status
    await updateSessionInDB(sessionCode, { 
      status: 'connecting',
      qr_code: null 
    });

    // Create new client
    const client = new Client({
      authStrategy: new LocalAuth({ clientId: sessionCode, dataPath: SESSIONS_DIR }),
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
        remotePath: `https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/${process.env.WA_VERSION || '2.2412.54'}.html`,
      },
      qrTimeoutMs: 0,
      takeoverOnConflict: true,
      restartOnAuthFail: true
    });

    clients.set(sessionCode, client);

    // Event handlers
    client.on('qr', async (qr) => {
      try {
        const qrCodeImage = await qrcode.toDataURL(qr, { width: 256, margin: 2 });
        await redis.setex(`qr:${sessionCode}`, 300, qrCodeImage);
        await updateSessionInDB(sessionCode, { qr_code: qrCodeImage });
      } catch (error) {
        console.error('QR generation error:', error);
      }
    });

    client.on('loading_screen', async (percent) => {
      if (percent === 100) {
        await redis.del(`qr:${sessionCode}`);
        await updateSessionInDB(sessionCode, { qr_code: null });
      }
    });

    client.on('authenticated', async () => {
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString()
      });
    });

    client.on('auth_failure', async (msg) => {
      console.error(`Auth failed: ${sessionCode} - ${msg}`);
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { status: 'expired' });
      clients.delete(sessionCode);
    });

    client.on('ready', async () => {
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString()
      });
    });

    client.on('disconnected', async (reason) => {
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { status: 'disconnected' });
      clients.delete(sessionCode);
    });

    await client.initialize();
    res.json({ status: 'connecting', message: 'Initialization started' });

  } catch (error) {
    console.error(`Init error: ${sessionCode} - ${error.message}`);
    await redis.del(`qr:${sessionCode}`);
    await updateSessionInDB(sessionCode, { status: 'error' });
    res.status(500).json({ error: 'Initialization failed' });
  }
});

// Queue-based message sending
messageQueue.process(async (job) => {
  const { sessionCode, number, message } = job.data;
  try {
    const client = clients.get(sessionCode);
    if (!client) throw new Error('Session not connected');

    const state = await client.getState();
    if (state !== 'CONNECTED') throw new Error('WhatsApp client not ready');

    if (!validatePhoneNumber(number)) throw new Error('Invalid phone number');
    
    const formattedNumber = formatPhoneNumber(number);
    const isRegistered = await client.isRegisteredUser(formattedNumber);
    if (!isRegistered) throw new Error('Number not registered');

    const sentMessage = await client.sendMessage(formattedNumber, message);
    await updateSessionInDB(sessionCode, { last_connected_at: new Date().toISOString() });
    
    return { 
      success: true, 
      messageId: sentMessage.id._serialized,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error(`Queue error: ${sessionCode} - ${number} - ${error.message}`);
    throw error;
  }
});

// Send message endpoint using queue
app.post('/send/:sessionCode/:number/:message', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode, number, message } = req.params;
  const userId = req.user.id;
  const decodedMessage = decodeURIComponent(message);

  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) return res.status(404).json({ error: 'Session not found' });

    const job = await messageQueue.add({
      sessionCode,
      number,
      message: decodedMessage,
      userId
    }, {
      jobId: uuidv4(),
      timeout: 30000,
      attempts: 3
    });

    res.json({ 
      success: true, 
      jobId: job.id,
      message: 'Message queued for sending'
    });
  } catch (error) {
    console.error(`Send error: ${sessionCode} - ${error.message}`);
    res.status(500).json({ error: 'Failed to queue message' });
  }
});

// Bulk message sending using queue
app.post('/send-bulk/:sessionCode', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode } = req.params;
  const { contacts, message } = req.body;
  const userId = req.user.id;

  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) return res.status(404).json({ error: 'Session not found' });

    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ error: 'Invalid contacts data' });
    }

    const jobIds = [];
    for (const contact of contacts) {
      const job = await messageQueue.add({
        sessionCode,
        number: contact.phone,
        message: contact.message || message,
        userId
      }, {
        jobId: uuidv4(),
        timeout: 30000,
        attempts: 3,
        delay: contact.delay || 0
      });
      jobIds.push(job.id);
    }

    res.json({ 
      success: true, 
      total: contacts.length,
      jobIds,
      message: 'Messages queued for sending'
    });
  } catch (error) {
    console.error(`Bulk send error: ${sessionCode} - ${error.message}`);
    res.status(500).json({ error: 'Failed to queue bulk messages' });
  }
});

// Disconnect session
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

    const client = clients.get(sessionCode);
    
    if (client) {
      try {
        await client.logout();
        await client.destroy();
      } catch (error) {
        console.error(`Error destroying client ${sessionCode}:`, error);
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
    console.error(`Error disconnecting session ${sessionCode}:`, error);
    
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

// Get user's sessions
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
    console.error('Error fetching user sessions:', error);
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
  console.error('Unhandled error:', error);
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

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully...');
  
  for (const [sessionCode, client] of clients.entries()) {
    try {
      console.log(`Disconnecting session ${sessionCode}...`);
      await client.destroy();
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });
    } catch (error) {
      console.error(`Error disconnecting session ${sessionCode}:`, error);
    }
  }
  
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully...');
  
  for (const [sessionCode, client] of clients.entries()) {
    try {
      console.log(`Disconnecting session ${sessionCode}...`);
      await client.destroy();
      await redis.del(`qr:${sessionCode}`);
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });
    } catch (error) {
      console.error(`Error disconnecting session ${sessionCode}:`, error);
    }
  }
  
  process.exit(0);
});

// Start server
// Start server
if (!cluster.worker || cluster.worker.id === 1) {
  app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`👷 Worker: ${cluster.worker ? cluster.worker.id : 'single'}`);
    console.log(`🔗 Health: http://localhost:${PORT}/health`);
    console.log(`📊 Queues: http://localhost:${PORT}/admin/queues`);
  });
}

module.exports = app;