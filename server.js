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

const app = express();
const PORT = process.env.PORT || 3001;

// Configure Express to trust proxies
app.set('trust proxy', process.env.NODE_ENV === 'production' ? 1 : 0);

// Initialize Redis client for Upstash
const redis = new Redis({
  host: 'clean-panda-53790.upstash.io',
  port: 6379,
  password: 'AdIeAAIjcDE3ZDhjZTNlYTRmYWY0YTMxODNhZDc1MDVmZGQwNWVhOXAxMA',
  tls: {}
});

// Initialize Supabase client
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
  process.exit(1);
}

// Security and performance middleware
app.use(helmet());
app.use(compression());
app.use(cors({
  origin: process.env.NODE_ENV === 'production' 
    ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : ['https://your-frontend-domain.com'])
    : ['http://localhost:5173', 'http://localhost:3000'],
  credentials: true
}));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  validate: { trustProxy: true } // Explicitly enable trust proxy for rate limiter
});
app.use(limiter);

// Stricter rate limiting for message sending
const sendLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  validate: { trustProxy: true } // Explicitly enable trust proxy for rate limiter
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// In-memory storage for WhatsApp clients
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

// Initialize sessions directory on startup
ensureSessionsDir();

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
    }
  } catch (error) {
    console.error('Database update error:', error);
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
    activeSessions: clients.size,
    uptime: process.uptime(),
    supabaseConnected: !!supabase,
    redisConnected: redis.status === 'ready'
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
      activeSessions: clients.size,
      userSessions: userSessionsCount || 0,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      timestamp: new Date().toISOString(),
      redisStatus: redis.status
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

// Initialize WhatsApp session
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
      const existingClient = clients.get(sessionCode);
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
        clients.delete(sessionCode);
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
          '--disable-gpu'
        ]
      },
      webVersionCache: {
        type: 'remote',
        remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html',
      },
      qrTimeoutMs: 0, // Disable QR timeout
      takeoverOnConflict: true, // Take over existing session
      restartOnAuthFail: true // Restart if auth fails
    });

    // Store client
    clients.set(sessionCode, client);

    // Set up event handlers with enhanced logging
    client.on('qr', async (qr) => {
      console.log(`QR Code generated for session ${sessionCode}`);
      console.log(`QR data length: ${qr.length}`);
      
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
      
      clients.delete(sessionCode);
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
      
      clients.delete(sessionCode);
    });

    // Initialize the client
    console.log(`Initializing WhatsApp client for session ${sessionCode}`);
    await client.initialize();
    console.log(`Client initialization started for ${sessionCode}`);

    res.json({ 
      success: true, 
      message: 'Session initialization started',
      sessionCode,
      status: 'connecting'
    });

  } catch (error) {
    console.error(`Error initializing session ${sessionCode}:`, error);
    await redis.del(`qr:${sessionCode}`);
    
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

// Send bulk messages
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

    const results = [];
    const maxInterval = Math.max(interval, 3000); // Minimum 3 seconds between messages
    
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
    console.error(`Error in bulk send for session ${sessionCode}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send bulk messages',
      details: error.message 
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
app.listen(PORT, () => {
  console.log(`🚀 WhatsApp Bulk Sender Backend running on port ${PORT}`);
  console.log(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`💾 Sessions directory: ${SESSIONS_DIR}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/health`);
  console.log(`🗄️  Supabase connected: ${!!supabase}`);
  console.log(`🔴 Redis connected: ${redis.status === 'ready'}`);
});

module.exports = app;   