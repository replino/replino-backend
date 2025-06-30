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

const app = express();
const PORT = process.env.PORT || 3001;

// Enhanced configuration
const SESSIONS_DIR = path.join(__dirname, 'sessions');
const MAX_QR_GENERATION_ATTEMPTS = 5;
const QR_GENERATION_TIMEOUT = 30000; // 30 seconds
const SESSION_CLEANUP_INTERVAL = 3600000; // 1 hour
const WEB_VERSION = '2.2412.54';

// Initialize Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Missing Supabase configuration');
  process.exit(1);
}

// Session storage
const clients = new Map();
const qrCodeCache = new Map();
const sessionTimestamps = new Map();

// Security and performance middleware
app.use(helmet());
app.use(compression());
app.use(cors({
  origin: process.env.NODE_ENV === 'production' 
    ? (process.env.FRONTEND_URLS ? process.env.FRONTEND_URLS.split(',') : [])
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
});
app.use(limiter);

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
    await fs.access(SESSIONS_DIR);
  } catch {
    await fs.mkdir(SESSIONS_DIR, { recursive: true });
  }
}

// Session cleanup
async function cleanupOldSessions() {
  try {
    const now = Date.now();
    const threshold = 24 * 60 * 60 * 1000; // 24 hours
    
    for (const [sessionCode, timestamp] of sessionTimestamps) {
      if (now - timestamp > threshold) {
        const client = clients.get(sessionCode);
        if (client) {
          try {
            await client.destroy();
          } catch (error) {
            console.error(`Error destroying old session ${sessionCode}:`, error);
          }
        }
        clients.delete(sessionCode);
        qrCodeCache.delete(sessionCode);
        sessionTimestamps.delete(sessionCode);
        
        // Delete session files
        try {
          const sessionPath = path.join(SESSIONS_DIR, `session-${sessionCode}`);
          await fs.rm(sessionPath, { recursive: true, force: true });
        } catch (error) {
          console.error(`Error deleting session files for ${sessionCode}:`, error);
        }
      }
    }
  } catch (error) {
    console.error('Error during session cleanup:', error);
  }
}

// Auth middleware
async function verifyAuth(req, res, next) {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, error: 'Authorization token required' });
    }

    const token = authHeader.substring(7);
    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error || !user) {
      return res.status(401).json({ success: false, error: 'Invalid or expired token' });
    }

    req.user = user;
    next();
  } catch (error) {
    res.status(401).json({ success: false, error: 'Authentication failed' });
  }
}

// Database functions
async function updateSessionInDB(sessionCode, updates) {
  try {
    const { error } = await supabase
      .from('sessions')
      .update({ ...updates, updated_at: new Date().toISOString() })
      .eq('session_code', sessionCode);

    if (error) console.error('Error updating session in DB:', error);
  } catch (error) {
    console.error('Database update error:', error);
  }
}

async function getSessionFromDB(sessionCode, userId = null) {
  try {
    let query = supabase
      .from('sessions')
      .select('*')
      .eq('session_code', sessionCode);
    
    if (userId) query = query.eq('user_id', userId);
    
    const { data, error } = await query.single();
    return error ? null : data;
  } catch (error) {
    console.error('Database fetch error:', error);
    return null;
  }
}

// Phone number validation
function validatePhoneNumber(phone) {
  const cleaned = phone.replace(/[^\d+]/g, '');
  return /^\+[1-9]\d{9,14}$/.test(cleaned);
}

function formatPhoneNumber(phone) {
  let formatted = phone.replace(/[^\d+]/g, '');
  if (!formatted.startsWith('+')) formatted = '+' + formatted;
  return formatted.substring(1) + '@c.us';
}

// QR code generation with retries
async function generateQRCodeWithRetry(qrData, attempts = 0) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), QR_GENERATION_TIMEOUT);

    const qrCodeImage = await qrcode.toDataURL(qrData, {
      width: 256,
      margin: 2,
      color: { dark: '#000000', light: '#FFFFFF' }
    }, { signal: controller.signal });

    clearTimeout(timeout);
    return qrCodeImage;
  } catch (error) {
    if (attempts < MAX_QR_GENERATION_ATTEMPTS) {
      return generateQRCodeWithRetry(qrData, attempts + 1);
    }
    throw error;
  }
}

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    activeSessions: clients.size,
    uptime: process.uptime(),
    supabaseConnected: !!supabase
  });
});

// Server stats
app.get('/stats', verifyAuth, async (req, res) => {
  try {
    const { count: userSessionsCount } = await supabase
      .from('sessions')
      .select('*', { count: 'exact', head: true })
      .eq('user_id', req.user.id);

    res.json({
      activeSessions: clients.size,
      userSessions: userSessionsCount || 0,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to fetch statistics' });
  }
});

// Initialize WhatsApp session
app.post('/init/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ success: false, error: 'Session not found or access denied' });
    }

    // Check existing session
    if (clients.has(sessionCode)) {
      const existingClient = clients.get(sessionCode);
      try {
        const state = await existingClient.getState();
        if (state === 'CONNECTED') {
          await updateSessionInDB(sessionCode, { 
            status: 'connected',
            last_connected_at: new Date().toISOString()
          });
          return res.json({ success: true, message: 'Session already connected', status: 'connected' });
        }
      } catch {
        clients.delete(sessionCode);
        qrCodeCache.delete(sessionCode);
      }
    }

    await updateSessionInDB(sessionCode, { status: 'connecting', qr_code: null });

    // Create new client with enhanced configuration
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
        remotePath: `https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/${WEB_VERSION}.html`,
      },
      takeoverOnConflict: true,
      restartOnAuthFail: true
    });

    clients.set(sessionCode, client);
    sessionTimestamps.set(sessionCode, Date.now());

    // Event handlers
    client.on('qr', async (qr) => {
      try {
        const qrCodeImage = await generateQRCodeWithRetry(qr);
        qrCodeCache.set(sessionCode, qrCodeImage);
        await updateSessionInDB(sessionCode, { status: 'connecting', qr_code: qrCodeImage });
      } catch (error) {
        console.error('Error generating QR code:', error);
      }
    });

    client.on('ready', async () => {
      qrCodeCache.delete(sessionCode);
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString(),
        qr_code: null 
      });
    });

    client.on('authenticated', async () => {
      qrCodeCache.delete(sessionCode);
      await updateSessionInDB(sessionCode, { 
        status: 'connected',
        last_connected_at: new Date().toISOString()
      });
    });

    client.on('auth_failure', async (msg) => {
      qrCodeCache.delete(sessionCode);
      await updateSessionInDB(sessionCode, { status: 'expired', qr_code: null });
      clients.delete(sessionCode);
    });

    client.on('disconnected', async (reason) => {
      qrCodeCache.delete(sessionCode);
      await updateSessionInDB(sessionCode, { status: 'disconnected', qr_code: null });
      clients.delete(sessionCode);
    });

    await client.initialize();
    res.json({ success: true, message: 'Session initialization started', sessionCode, status: 'connecting' });

  } catch (error) {
    qrCodeCache.delete(sessionCode);
    res.status(500).json({ success: false, error: 'Failed to initialize session', details: error.message });
  }
});

// Get QR code
app.get('/qrcode/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    if (qrCodeCache.has(sessionCode)) {
      return res.json({ success: true, qrCode: qrCodeCache.get(sessionCode) });
    }

    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ success: false, error: 'Session not found or access denied' });
    }

    if (!sessionData.qr_code) {
      return res.status(404).json({ success: false, error: 'QR code not available' });
    }

    res.json({ success: true, qrCode: sessionData.qr_code });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to get QR code', details: error.message });
  }
});

// Get session status
app.get('/status/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ success: false, error: 'Session not found or access denied' });
    }

    const hasClient = clients.has(sessionCode);
    let actualStatus = sessionData.status;

    if (hasClient) {
      try {
        const client = clients.get(sessionCode);
        const state = await client.getState();
        
        if (state === 'CONNECTED' && sessionData.status !== 'connected') {
          actualStatus = 'connected';
          await updateSessionInDB(sessionCode, { 
            status: 'connected',
            last_connected_at: new Date().toISOString()
          });
        } else if (state !== 'CONNECTED' && sessionData.status === 'connected') {
          actualStatus = 'disconnected';
          await updateSessionInDB(sessionCode, { status: 'disconnected' });
        }
      } catch (error) {
        actualStatus = 'disconnected';
      }
    }
    
    res.json({ 
      success: true,
      connected: actualStatus === 'connected',
      status: actualStatus,
      lastConnected: sessionData.last_connected_at,
      hasClient,
      qrCode: sessionData.qr_code
    });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to get session status' });
  }
});

// Send message
app.post('/send/:sessionCode/:number/:message', verifyAuth, sendLimiter, async (req, res) => {
  const { sessionCode, number, message } = req.params;
  const userId = req.user.id;
  
  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ success: false, error: 'Session not found or access denied' });
    }

    const client = clients.get(sessionCode);
    if (!client) {
      return res.status(404).json({ success: false, error: 'Session not connected' });
    }

    const state = await client.getState();
    if (state !== 'CONNECTED') {
      return res.status(400).json({ success: false, error: 'WhatsApp client is not connected', state });
    }

    if (!validatePhoneNumber(number)) {
      return res.status(400).json({ success: false, error: 'Invalid phone number format' });
    }

    const formattedNumber = formatPhoneNumber(number);
    const decodedMessage = decodeURIComponent(message);

    const isRegistered = await client.isRegisteredUser(formattedNumber);
    if (!isRegistered) {
      return res.status(400).json({ success: false, error: 'Number is not registered on WhatsApp' });
    }

    const sentMessage = await client.sendMessage(formattedNumber, decodedMessage);
    
    await updateSessionInDB(sessionCode, { last_connected_at: new Date().toISOString() });
    
    res.json({ 
      success: true, 
      messageId: sentMessage.id._serialized,
      to: number,
      message: decodedMessage,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to send message', details: error.message });
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
      return res.status(404).json({ success: false, error: 'Session not found or access denied' });
    }

    const client = clients.get(sessionCode);
    if (!client) {
      return res.status(404).json({ success: false, error: 'Session not connected' });
    }

    const state = await client.getState();
    if (state !== 'CONNECTED') {
      return res.status(400).json({ success: false, error: 'WhatsApp client is not connected', state });
    }

    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ success: false, error: 'Contacts array is required' });
    }

    if (!message || message.trim().length === 0) {
      return res.status(400).json({ success: false, error: 'Message is required' });
    }

    const results = [];
    const maxInterval = Math.max(interval, 3000);
    
    for (let i = 0; i < contacts.length; i++) {
      const contact = contacts[i];
      
      try {
        if (!validatePhoneNumber(contact.phone)) {
          results.push({ contact: contact.phone, status: 'failed', error: 'Invalid phone number format' });
          continue;
        }

        const formattedNumber = formatPhoneNumber(contact.phone);
        const isRegistered = await client.isRegisteredUser(formattedNumber);
        if (!isRegistered) {
          results.push({ contact: contact.phone, status: 'failed', error: 'Number not registered on WhatsApp' });
          continue;
        }

        const sentMessage = await client.sendMessage(formattedNumber, contact.message || message);
        results.push({
          contact: contact.phone,
          status: 'sent',
          messageId: sentMessage.id._serialized,
          timestamp: new Date().toISOString()
        });

        if (i < contacts.length - 1) {
          await new Promise(resolve => setTimeout(resolve, maxInterval));
        }
      } catch (error) {
        results.push({ contact: contact.phone, status: 'failed', error: error.message });
      }
    }

    await updateSessionInDB(sessionCode, { last_connected_at: new Date().toISOString() });
    
    const successCount = results.filter(r => r.status === 'sent').length;
    const failedCount = results.filter(r => r.status === 'failed').length;
    
    res.json({ 
      success: true,
      summary: { total: contacts.length, sent: successCount, failed: failedCount },
      results
    });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to send bulk messages', details: error.message });
  }
});

// Disconnect session
app.post('/disconnect/:sessionCode', verifyAuth, async (req, res) => {
  const { sessionCode } = req.params;
  const userId = req.user.id;
  
  try {
    const sessionData = await getSessionFromDB(sessionCode, userId);
    if (!sessionData) {
      return res.status(404).json({ success: false, error: 'Session not found or access denied' });
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
      qrCodeCache.delete(sessionCode);
      sessionTimestamps.delete(sessionCode);
    }
    
    await updateSessionInDB(sessionCode, { status: 'disconnected', qr_code: null });
    res.json({ success: true, message: 'Session disconnected successfully' });
  } catch (error) {
    clients.delete(sessionCode);
    qrCodeCache.delete(sessionCode);
    sessionTimestamps.delete(sessionCode);
    await updateSessionInDB(sessionCode, { status: 'disconnected', qr_code: null });
    res.json({ success: true, message: 'Session disconnected (forced cleanup)', warning: error.message });
  }
});

// Get user sessions
app.get('/sessions', verifyAuth, async (req, res) => {
  const userId = req.user.id;
  
  try {
    const { data: sessions, error } = await supabase
      .from('sessions')
      .select('*')
      .eq('user_id', userId)
      .order('created_at', { ascending: false });

    if (error) throw error;

    const sessionsWithStatus = sessions.map(session => ({
      ...session,
      hasClient: clients.has(session.session_code),
      isActive: clients.has(session.session_code) && session.status === 'connected'
    }));
    
    res.json({ success: true, sessions: sessionsWithStatus, total: sessions.length });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to fetch sessions' });
  }
});

// Validate phone number
app.post('/validate-phone', (req, res) => {
  const { phone } = req.body;
  
  if (!phone) {
    return res.status(400).json({ success: false, error: 'Phone number is required' });
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

// Error handling
app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? error.message : undefined
  });
});

app.use((req, res) => {
  res.status(404).json({ success: false, error: 'Endpoint not found' });
});

// Graceful shutdown
async function shutdown() {
  console.log('Shutting down gracefully...');
  
  for (const [sessionCode, client] of clients.entries()) {
    try {
      console.log(`Disconnecting session ${sessionCode}...`);
      await client.destroy();
      await updateSessionInDB(sessionCode, { status: 'disconnected', qr_code: null });
    } catch (error) {
      console.error(`Error disconnecting session ${sessionCode}:`, error);
    }
  }
  
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Initialize and start server
(async () => {
  await ensureSessionsDir();
  setInterval(cleanupOldSessions, SESSION_CLEANUP_INTERVAL);

  app.listen(PORT, () => {
    console.log(`🚀 WhatsApp Bulk Sender Backend running on port ${PORT}`);
    console.log(`📱 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`💾 Sessions directory: ${SESSIONS_DIR}`);
    console.log(`🔗 Health check: http://localhost:${PORT}/health`);
  });
})();

module.exports = app;