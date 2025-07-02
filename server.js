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

// Configuration
const config = {
  PORT: process.env.PORT || 3001,
  NODE_ENV: process.env.NODE_ENV || 'development',
  SESSIONS_DIR: path.join(__dirname, 'sessions'),
  QR_CODE_TTL: 300, // 5 minutes
  MESSAGE_INTERVAL: 3000, // Minimum 3 seconds between messages
  MAX_WORKERS: process.env.MAX_WORKERS || os.cpus().length,
  REDIS_CONFIG: {
    host: 'clean-panda-53790.upstash.io',
    port: 6379,
    password: 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
    tls: {},
    connectTimeout: 10000,
    maxRetriesPerRequest: 1
  },
  PUPPETEER_CONFIG: {
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
  }
};

// Initialize Redis with connection pooling
const redis = new Redis(config.REDIS_CONFIG);

// Initialize Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
  process.exit(1);
}

// Session management with LRU cache
class SessionManager {
  constructor() {
    this.clients = new Map();
    this.qrGenerators = new Map();
    this.messageQueues = new Map();
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
        console.error(`Error destroying client ${sessionCode}:`, error);
      }
    }
    this.clients.delete(sessionCode);
    this.qrGenerators.delete(sessionCode);
    this.messageQueues.delete(sessionCode);
  }

  async getQRGenerator(sessionCode) {
    return this.qrGenerators.get(sessionCode);
  }

  async addQRGenerator(sessionCode, generator) {
    this.qrGenerators.set(sessionCode, generator);
  }

  async getMessageQueue(sessionCode) {
    if (!this.messageQueues.has(sessionCode)) {
      this.messageQueues.set(sessionCode, []);
    }
    return this.messageQueues.get(sessionCode);
  }
}

const sessionManager = new SessionManager();

// Initialize Express app
const app = express();

// Security and performance middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "blob:"],
      connectSrc: ["'self'", "https://*.upstash.io", process.env.SUPABASE_URL]
    }
  }
}));

app.use(compression());
app.use(cors({
  origin: config.NODE_ENV === 'production' 
    ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
    : ['http://localhost:5173', 'http://localhost:3000'],
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => req.ip === '::ffff:127.0.0.1' // Skip for localhost
});

app.use(limiter);

// Stricter rate limiting for message sending
const sendLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Ensure sessions directory exists
async function ensureSessionsDir() {
  try {
    await fs.access(config.SESSIONS_DIR);
  } catch {
    await fs.mkdir(config.SESSIONS_DIR, { recursive: true });
  }
}

// Initialize sessions directory on startup
ensureSessionsDir().catch(err => {
  console.error('Failed to create sessions directory:', err);
  process.exit(1);
});

// QR Code generation in worker thread
async function generateQRCode(qrData) {
  return new Promise((resolve, reject) => {
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
            }
          });
          parentPort.postMessage({ success: true, qrCodeImage });
        } catch (error) {
          parentPort.postMessage({ success: false, error: error.message });
        }
      });
    `, { eval: true });

    worker.on('message', ({ success, qrCodeImage, error }) => {
      worker.terminate();
      if (success) {
        resolve(qrCodeImage);
      } else {
        reject(new Error(error));
      }
    });

    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });

    worker.postMessage(qrData);
  });
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
    console.error('Auth verification error:', error);
    res.status(401).json({ 
      success: false, 
      error: 'Authentication failed' 
    });
  }
}

// Utility function to update session in database
async function updateSessionInDB(sessionCode, updates) {
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
      return false;
    }
    return true;
  } catch (error) {
    console.error('Database update error:', error);
    return false;
  }
}

// Utility function to get session from database
async function getSessionFromDB(sessionCode, userId = null) {
  try {
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
      return null;
    }
    
    return data;
  } catch (error) {
    console.error('Database fetch error:', error);
    return null;
  }
}

// Utility function to validate phone number
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

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    activeSessions: sessionManager.clients.size,
    uptime: process.uptime(),
    supabaseConnected: !!supabase,
    redisConnected: redis.status === 'ready',
    memoryUsage: process.memoryUsage(),
    loadAvg: os.loadavg(),
    workers: config.MAX_WORKERS
  });
});

// Get server statistics
app.get('/stats', verifyAuth, async (req, res) => {
  try {
    const { count: userSessionsCount } = await supabase
      .from('sessions')
      .select('*', { count: 'exact', head: true })
      .eq('user_id', req.user.id);

    const stats = {
      activeSessions: sessionManager.clients.size,
      userSessions: userSessionsCount || 0,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      timestamp: new Date().toISOString(),
      redisStatus: redis.status,
      loadAvg: os.loadavg(),
      workers: config.MAX_WORKERS
    };
    
    res.json(stats);
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to fetch statistics' 
    });
  }
});

// Initialize WhatsApp session with improved QR generation
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

    // Create new client with session persistence
    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: sessionCode,
        dataPath: config.SESSIONS_DIR
      }),
      puppeteer: config.PUPPETEER_CONFIG,
      webVersionCache: {
        type: 'remote',
        remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html',
      },
      qrTimeoutMs: 0, // Disable QR timeout
      takeoverOnConflict: true, // Take over existing session
      restartOnAuthFail: true // Restart if auth fails
    });

    // Store client
    await sessionManager.addClient(sessionCode, client);

    // Set up event handlers with enhanced logging
    client.on('qr', async (qr) => {
      console.log(`QR Code generated for session ${sessionCode}`);
      
      try {
        // Generate QR code image in a worker thread
        const qrCodeImage = await generateQRCode(qr);
        
        // Store in Redis cache for faster access
        await redis.setex(`qr:${sessionCode}`, config.QR_CODE_TTL, qrCodeImage);

        // Update session with QR code in database
        await updateSessionInDB(sessionCode, { 
          status: 'connecting',
          qr_code: qrCodeImage 
        });

        console.log(`QR code stored for session ${sessionCode}`);
      } catch (error) {
        console.error('Error generating QR code:', error);
      }
    });

    client.on('loading_screen', async (percent, message) => {
      console.log(`Loading screen: ${percent}% ${message || ''}`);
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
      console.log(`WhatsApp client ${sessionCode} authenticated`);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null
      });
    });

    client.on('auth_failure', async (msg) => {
      console.error(`Authentication failed for session ${sessionCode}:`, msg);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'expired',
        qr_code: null 
      });
      
      await sessionManager.removeClient(sessionCode);
    });

    client.on('ready', async () => {
      console.log(`WhatsApp client ${sessionCode} is ready!`);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null 
      });
    });

    client.on('disconnected', async (reason) => {
      console.log(`WhatsApp client ${sessionCode} disconnected:`, reason);
      await redis.del(`qr:${sessionCode}`);
      
      await updateSessionInDB(sessionCode, { 
        status: 'disconnected',
        qr_code: null 
      });
      
      await sessionManager.removeClient(sessionCode);
    });

    // Initialize the client in a non-blocking way
    console.log(`Initializing WhatsApp client for session ${sessionCode}`);
    client.initialize().catch(error => {
      console.error(`Client initialization error for ${sessionCode}:`, error);
    });

    res.json({ 
      success: true, 
      message: 'Session initialization started',
      sessionCode,
      status: 'connecting'
    });

  } catch (error) {
    console.error(`Error initializing session ${sessionCode}:`, error);
    await redis.del(`qr:${sessionCode}`);
    await sessionManager.removeClient(sessionCode);
    
    await updateSessionInDB(sessionCode, { 
      status: 'expired',
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
    console.error(`Error getting QR code for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get QR code',
      details: error.message 
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
        console.error('Error checking client state:', error);
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
    console.error(`Error getting status for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get session status' 
    });
  }
});

// Send message to a single contact
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

    // Validate and format phone number
    if (!validatePhoneNumber(number)) {
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
      return res.status(400).json({ 
        success: false, 
        error: 'Number is not registered on WhatsApp' 
      });
    }

    // Send message
    const sentMessage = await client.sendMessage(formattedNumber, decodedMessage);
    
    // Update session last activity
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
    console.error(`Error sending message in session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send message',
      details: error.message 
    });
  }
});

// Send bulk messages with improved queue management
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

    // Generate a batch ID for tracking
    const batchId = uuidv4();
    
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
        console.error(`Error processing batch ${batchId}:`, error);
      });
    
  } catch (error) {
    console.error(`Error in bulk send for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send bulk messages',
      details: error.message 
    });
  }
});

// Background processing of bulk messages
async function processBulkMessages(client, sessionCode, contacts, message, interval, batchId) {
  const results = [];
  const maxInterval = Math.max(interval, config.MESSAGE_INTERVAL); // Minimum interval
  
  for (let i = 0; i < contacts.length; i++) {
    const contact = contacts[i];
    
    try {
      // Validate phone number
      if (!validatePhoneNumber(contact.phone)) {
        results.push({
          contact: contact.phone,
          status: 'failed',
          error: 'Invalid phone number format'
        });
        continue;
      }

      const formattedNumber = formatPhoneNumber(contact.phone);
      
      // Check if number exists on WhatsApp
      const isRegistered = await client.isRegisteredUser(formattedNumber);
      if (!isRegistered) {
        results.push({
          contact: contact.phone,
          status: 'failed',
          error: 'Number not registered on WhatsApp'
        });
        continue;
      }

      // Send message
      const sentMessage = await client.sendMessage(formattedNumber, contact.message || message);
      
      results.push({
        contact: contact.phone,
        status: 'sent',
        messageId: sentMessage.id._serialized,
        timestamp: new Date().toISOString()
      });

      // Store progress in Redis
      await redis.hset(`batch:${batchId}`, {
        progress: i + 1,
        total: contacts.length,
        lastUpdated: new Date().toISOString()
      });

      // Wait before sending next message (except for the last one)
      if (i < contacts.length - 1) {
        await new Promise(resolve => setTimeout(resolve, maxInterval));
      }

    } catch (error) {
      console.error(`Error sending to ${contact.phone}:`, error);
      results.push({
        contact: contact.phone,
        status: 'failed',
        error: error.message
      });
    }
  }

  // Update session last activity
  await updateSessionInDB(sessionCode, { 
    last_connected_at: new Date().toISOString() 
  });

  // Store final results in Redis
  const successCount = results.filter(r => r.status === 'sent').length;
  const failedCount = results.filter(r => r.status === 'failed').length;
  
  await redis.hset(`batch:${batchId}`, {
    completed: true,
    successCount,
    failedCount,
    finishedAt: new Date().toISOString()
  });

  // Store detailed results (expire after 24 hours)
  await redis.setex(`batch:results:${batchId}`, 86400, JSON.stringify(results));
}

// Get batch status
app.get('/batch-status/:batchId', verifyAuth, async (req, res) => {
  const { batchId } = req.params;
  
  try {
    const batchData = await redis.hgetall(`batch:${batchId}`);
    if (!batchData || Object.keys(batchData).length === 0) {
      return res.status(404).json({ 
        success: false, 
        error: 'Batch not found' 
      });
    }

    res.json({
      success: true,
      batchId,
      progress: parseInt(batchData.progress || '0'),
      total: parseInt(batchData.total || '0'),
      completed: batchData.completed === 'true',
      successCount: parseInt(batchData.successCount || '0'),
      failedCount: parseInt(batchData.failedCount || '0'),
      lastUpdated: batchData.lastUpdated,
      finishedAt: batchData.finishedAt
    });
  } catch (error) {
    console.error(`Error getting batch status ${batchId}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get batch status' 
    });
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

    await sessionManager.removeClient(sessionCode);
    await redis.del(`qr:${sessionCode}`);
    
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
    await sessionManager.removeClient(sessionCode);
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
      hasClient: sessionManager.clients.has(session.session_code),
      isActive: sessionManager.clients.has(session.session_code) && session.status === 'connected'
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
    details: config.NODE_ENV === 'development' ? error.message : undefined
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
async function gracefulShutdown() {
  console.log('Shutdown signal received, shutting down gracefully...');
  
  for (const [sessionCode, client] of sessionManager.clients.entries()) {
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
  
  // Close Redis connection
  await redis.quit();
  
  process.exit(0);
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Start server
if (cluster.isPrimary && config.NODE_ENV === 'production') {
  console.log(`Primary ${process.pid} is running`);
  
  // Fork workers
  for (let i = 0; i < config.MAX_WORKERS; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
    cluster.fork(); // Replace the dead worker
  });
} else {
  app.listen(config.PORT, () => {
    console.log(`🚀 WhatsApp Bulk Sender Backend running on port ${config.PORT}`);
    console.log(`📱 Environment: ${config.NODE_ENV}`);
    console.log(`💾 Sessions directory: ${config.SESSIONS_DIR}`);
    console.log(`🔗 Health check: http://localhost:${config.PORT}/health`);
    console.log(`🗄️  Supabase connected: ${!!supabase}`);
    console.log(`🔴 Redis connected: ${redis.status === 'ready'}`);
    console.log(`👷 Worker ${process.pid} started`);
  });
}

module.exports = app; 