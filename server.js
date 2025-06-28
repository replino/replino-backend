const express = require('express');
const cors = require('cors');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const fs = require('fs').promises;
const path = require('path');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const compression = require('compression');

const app = express();
const PORT = process.env.PORT || 3001;

// Security and performance middleware
app.use(helmet());
app.use(compression());
app.use(cors({
  origin: process.env.NODE_ENV === 'production' 
    ? ['https://your-frontend-domain.com'] 
    : ['http://localhost:5173', 'http://localhost:3000'],
  credentials: true
}));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 1000, // limit each IP to 1000 requests per windowMs
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
});
app.use(limiter);

// Stricter rate limiting for message sending
const sendLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 60, // limit each IP to 60 message sends per minute
  message: 'Too many messages sent, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// In-memory storage for WhatsApp clients and session data
const clients = new Map();
const qrCodes = new Map();
const sessionStatus = new Map();

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

// Utility function to clean up inactive sessions
function cleanupInactiveSessions() {
  const now = Date.now();
  const INACTIVE_TIMEOUT = 30 * 60 * 1000; // 30 minutes

  for (const [sessionId, lastActivity] of sessionStatus.entries()) {
    if (now - lastActivity.timestamp > INACTIVE_TIMEOUT && lastActivity.status !== 'connected') {
      console.log(`Cleaning up inactive session: ${sessionId}`);
      
      if (clients.has(sessionId)) {
        const client = clients.get(sessionId);
        try {
          client.destroy();
        } catch (error) {
          console.error(`Error destroying client ${sessionId}:`, error);
        }
        clients.delete(sessionId);
      }
      
      qrCodes.delete(sessionId);
      sessionStatus.delete(sessionId);
    }
  }
}

// Run cleanup every 10 minutes
setInterval(cleanupInactiveSessions, 10 * 60 * 1000);

// Utility function to update session status
function updateSessionStatus(sessionId, status, data = {}) {
  sessionStatus.set(sessionId, {
    status,
    timestamp: Date.now(),
    ...data
  });
}

// Utility function to validate phone number
function validatePhoneNumber(phone) {
  // Remove all non-digits except leading +
  const cleaned = phone.replace(/[^\d+]/g, '');
  
  // Basic validation - should start with + and have 10-15 digits
  const phoneRegex = /^\+[1-9]\d{9,14}$/;
  return phoneRegex.test(cleaned);
}

// Utility function to format phone number for WhatsApp
function formatPhoneNumber(phone) {
  // Remove all non-digits except leading +
  let formatted = phone.replace(/[^\d+]/g, '');
  
  // Add + if not present
  if (!formatted.startsWith('+')) {
    formatted = '+' + formatted;
  }
  
  // Add @c.us suffix for WhatsApp
  return formatted.substring(1) + '@c.us';
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    activeSessions: clients.size,
    uptime: process.uptime()
  });
});

// Get server statistics
app.get('/stats', (req, res) => {
  const stats = {
    activeSessions: clients.size,
    totalSessions: sessionStatus.size,
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    timestamp: new Date().toISOString()
  };
  
  res.json(stats);
});

// Initialize WhatsApp session
app.post('/init/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  
  try {
    // Check if session already exists
    if (clients.has(sessionId)) {
      const existingClient = clients.get(sessionId);
      const state = await existingClient.getState();
      
      if (state === 'CONNECTED') {
        updateSessionStatus(sessionId, 'connected');
        return res.json({ 
          success: true, 
          message: 'Session already connected',
          status: 'connected'
        });
      }
    }

    // Create new client with session persistence
    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: sessionId,
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
      }
    });

    // Store client
    clients.set(sessionId, client);
    updateSessionStatus(sessionId, 'initializing');

    // Set up event handlers
    client.on('qr', (qr) => {
      console.log(`QR Code generated for session ${sessionId}`);
      qrCodes.set(sessionId, qr);
      updateSessionStatus(sessionId, 'qr_ready');
    });

    client.on('ready', () => {
      console.log(`WhatsApp client ${sessionId} is ready!`);
      updateSessionStatus(sessionId, 'connected');
      qrCodes.delete(sessionId); // Remove QR code once connected
    });

    client.on('authenticated', () => {
      console.log(`WhatsApp client ${sessionId} authenticated`);
      updateSessionStatus(sessionId, 'authenticated');
    });

    client.on('auth_failure', (msg) => {
      console.error(`Authentication failed for session ${sessionId}:`, msg);
      updateSessionStatus(sessionId, 'auth_failed', { error: msg });
    });

    client.on('disconnected', (reason) => {
      console.log(`WhatsApp client ${sessionId} disconnected:`, reason);
      updateSessionStatus(sessionId, 'disconnected', { reason });
      
      // Clean up
      clients.delete(sessionId);
      qrCodes.delete(sessionId);
    });

    client.on('message', (message) => {
      // Log incoming messages for debugging
      console.log(`Message received in session ${sessionId}:`, message.from);
    });

    // Initialize the client
    await client.initialize();

    res.json({ 
      success: true, 
      message: 'Session initialization started',
      sessionId,
      status: 'initializing'
    });

  } catch (error) {
    console.error(`Error initializing session ${sessionId}:`, error);
    updateSessionStatus(sessionId, 'error', { error: error.message });
    
    res.status(500).json({ 
      success: false, 
      error: 'Failed to initialize session',
      details: error.message 
    });
  }
});

// Get QR code for session
app.post('/qrcode', async (req, res) => {
  const { sessionId } = req.body;
  
  if (!sessionId) {
    return res.status(400).json({ 
      success: false, 
      error: 'Session ID is required' 
    });
  }

  try {
    const qr = qrCodes.get(sessionId);
    
    if (!qr) {
      return res.status(404).json({ 
        success: false, 
        error: 'QR code not available. Please initialize session first.' 
      });
    }

    // Generate QR code image
    const qrCodeImage = await qrcode.toDataURL(qr, {
      width: 256,
      margin: 2,
      color: {
        dark: '#000000',
        light: '#FFFFFF'
      }
    });

    res.json({ 
      success: true, 
      qrCode: qrCodeImage 
    });

  } catch (error) {
    console.error(`Error generating QR code for session ${sessionId}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to generate QR code',
      details: error.message 
    });
  }
});

// Get session status
app.get('/status/:sessionId', (req, res) => {
  const { sessionId } = req.params;
  
  const status = sessionStatus.get(sessionId);
  const hasClient = clients.has(sessionId);
  
  if (!status && !hasClient) {
    return res.json({ 
      connected: false, 
      status: 'not_initialized',
      message: 'Session not found'
    });
  }

  const currentStatus = status ? status.status : 'unknown';
  
  res.json({ 
    connected: currentStatus === 'connected',
    status: currentStatus,
    timestamp: status ? status.timestamp : null,
    hasClient
  });
});

// Send message to a single contact
app.post('/send/:sessionId/:number/:message', sendLimiter, async (req, res) => {
  const { sessionId, number, message } = req.params;
  
  try {
    const client = clients.get(sessionId);
    
    if (!client) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or not connected' 
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
    
    updateSessionStatus(sessionId, 'connected'); // Update last activity
    
    res.json({ 
      success: true, 
      messageId: sentMessage.id._serialized,
      to: number,
      message: decodedMessage,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error(`Error sending message in session ${sessionId}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send message',
      details: error.message 
    });
  }
});

// Send bulk messages
app.post('/send-bulk/:sessionId', sendLimiter, async (req, res) => {
  const { sessionId } = req.params;
  const { contacts, message, interval = 5000 } = req.body;
  
  try {
    const client = clients.get(sessionId);
    
    if (!client) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found or not connected' 
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

    updateSessionStatus(sessionId, 'connected'); // Update last activity
    
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
    console.error(`Error in bulk send for session ${sessionId}:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to send bulk messages',
      details: error.message 
    });
  }
});

// Disconnect session
app.post('/disconnect/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  
  try {
    const client = clients.get(sessionId);
    
    if (!client) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found' 
      });
    }

    await client.logout();
    await client.destroy();
    
    // Clean up
    clients.delete(sessionId);
    qrCodes.delete(sessionId);
    updateSessionStatus(sessionId, 'disconnected');

    res.json({ 
      success: true, 
      message: 'Session disconnected successfully' 
    });

  } catch (error) {
    console.error(`Error disconnecting session ${sessionId}:`, error);
    
    // Force cleanup even if there's an error
    clients.delete(sessionId);
    qrCodes.delete(sessionId);
    updateSessionStatus(sessionId, 'disconnected');
    
    res.json({ 
      success: true, 
      message: 'Session disconnected (forced cleanup)',
      warning: error.message 
    });
  }
});

// Get all active sessions
app.get('/sessions', (req, res) => {
  const sessions = Array.from(sessionStatus.entries()).map(([id, data]) => ({
    sessionId: id,
    status: data.status,
    timestamp: data.timestamp,
    hasClient: clients.has(id)
  }));
  
  res.json({ 
    success: true, 
    sessions,
    total: sessions.length 
  });
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
  
  // Disconnect all clients
  for (const [sessionId, client] of clients.entries()) {
    try {
      console.log(`Disconnecting session ${sessionId}...`);
      await client.destroy();
    } catch (error) {
      console.error(`Error disconnecting session ${sessionId}:`, error);
    }
  }
  
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully...');
  
  // Disconnect all clients
  for (const [sessionId, client] of clients.entries()) {
    try {
      console.log(`Disconnecting session ${sessionId}...`);
      await client.destroy();
    } catch (error) {
      console.error(`Error disconnecting session ${sessionId}:`, error);
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
});

module.exports = app;