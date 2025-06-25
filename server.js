// server.js

const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const cors = require('cors');
const {
  default: makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason
} = require('@whiskeysockets/baileys');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const SESSIONS_DIR = path.join(__dirname, 'sessions');
fs.ensureDirSync(SESSIONS_DIR);

const sessions = new Map();

// 🧠 Track internal connection status for each session
const sessionStatus = new Map();

async function createOrGetSession(sessionId) {
  if (sessions.has(sessionId)) return sessions.get(sessionId);

  const sessionPath = path.join(SESSIONS_DIR, sessionId);
  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
    syncFullHistory: false,
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;

    console.log(`[${sessionId}] connection.update`, update);

    if (qr) {
      sessionStatus.set(sessionId, 'pending_qr');
    }

    if (connection === 'open') {
      console.log(`✅ [${sessionId}] connected`);
      sessionStatus.set(sessionId, 'authenticated');
    }

    if (connection === 'close') {
      const shouldReconnect =
        (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut;

      if (shouldReconnect) {
        console.log(`🔁 [${sessionId}] reconnecting...`);
        sessions.delete(sessionId);
        createOrGetSession(sessionId);
      } else {
        console.log(`❌ [${sessionId}] logged out`);
        sessions.delete(sessionId);
        sessionStatus.set(sessionId, 'not_initialized');
        fs.removeSync(sessionPath);
      }
    }
  });

  sessions.set(sessionId, sock);
  sessionStatus.set(sessionId, 'initialized');

  return sock;
}

// ======================= ROUTES =======================

// POST /init/:sessionId
app.post('/init/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });

  try {
    await createOrGetSession(sessionId);
    res.json({ status: 'initialized' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to initialize session' });
  }
});

// POST /qrcode
app.post('/qrcode', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });

  try {
    const sock = await createOrGetSession(sessionId);

    if (sock.authState.creds?.registered) {
      return res.json({ qr: null, status: 'already_authenticated' });
    }

    let responded = false;

    const handler = (update) => {
      const { qr, connection } = update;

      if (qr && !responded) {
        responded = true;
        res.json({ qr, status: 'pending_qr' });
        cleanup();
      }

      if (connection === 'open' && !responded) {
        responded = true;
        res.json({ qr: null, status: 'connected' });
        cleanup();
      }
    };

    const cleanup = () => {
      sock.ev.off('connection.update', handler);
      clearTimeout(timeout);
    };

    sock.ev.on('connection.update', handler);

    const timeout = setTimeout(() => {
      if (!responded) {
        responded = true;
        res.status(504).json({ error: 'QR code generation timed out' });
        cleanup();
      }
    }, 15000);
  } catch (err) {
    console.error('QR Error:', err);
    res.status(500).json({ error: 'Internal server error generating QR' });
  }
});

// GET /status/:sessionId
app.get('/status/:sessionId', (req, res) => {
  const { sessionId } = req.params;

  const currentStatus = sessionStatus.get(sessionId);
  if (!sessions.has(sessionId)) {
    return res.json({ status: 'not_initialized' });
  }

  if (currentStatus === 'authenticated') {
    return res.json({ status: 'authenticated' });
  }

  if (currentStatus === 'pending_qr') {
    return res.json({ status: 'pending_qr' });
  }

  return res.json({ status: currentStatus || 'unknown' });
});

// POST /send/:sessionId/:number/:message
app.post('/send/:sessionId/:number/:message', async (req, res) => {
  const { sessionId, number, message } = req.params;

  if (!sessionId || !number || !message) {
    return res.status(400).json({ error: 'Missing parameters' });
  }

  try {
    const sock = sessions.get(sessionId) || await createOrGetSession(sessionId);

    if (!sock.authState.creds?.registered) {
      return res.status(403).json({ error: 'Session not authenticated' });
    }

    const jid = number.includes('@s.whatsapp.net') ? number : `${number}@s.whatsapp.net`;
    await sock.sendMessage(jid, { text: message });

    res.json({ status: 'sent' });
  } catch (err) {
    console.error(`Send Error [${sessionId}]:`, err);
    res.status(500).json({ error: 'Failed to send message' });
  }
});

// POST /disconnect/:sessionId
app.post('/disconnect/:sessionId', async (req, res) => {
  const { sessionId } = req.params;

  const sock = sessions.get(sessionId);
  if (!sock) return res.status(404).json({ error: 'Session not found' });

  try {
    await sock.logout();
    sessions.delete(sessionId);
    sessionStatus.set(sessionId, 'not_initialized');
    fs.removeSync(path.join(SESSIONS_DIR, sessionId));
    res.json({ status: 'disconnected' });
  } catch (err) {
    console.error(`Disconnect Error [${sessionId}]:`, err);
    res.status(500).json({ error: 'Failed to disconnect session' });
  }
});

// Default route
app.get('/', (req, res) => {
  res.send('✅ Backend is running.');
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
