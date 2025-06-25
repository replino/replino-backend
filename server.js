// server.js

const express = require('express');
const { default: makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, DisconnectReason } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode');
const fs = require('fs-extra');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

const SESSIONS_DIR = path.join(__dirname, 'sessions');
fs.ensureDirSync(SESSIONS_DIR);

const sessions = {};

async function createOrGetSession(sessionId) {
  if (sessions[sessionId]) return sessions[sessionId];

  const sessionPath = path.join(SESSIONS_DIR, sessionId);
  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const shouldReconnect = (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut;
      if (shouldReconnect) {
        createOrGetSession(sessionId);
      } else {
        delete sessions[sessionId];
        fs.removeSync(sessionPath);
      }
    }
  });

  sessions[sessionId] = sock;
  return sock;
}

// ========== ROUTES ==========

// POST /qrcode
// POST /qrcode
app.post('/qrcode', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });

  try {
    const sock = await createOrGetSession(sessionId);

    if (sock.authState.creds?.registered) {
      return res.json({ qrCode: null, status: 'already_authenticated' });
    }

    let responded = false;

    sock.ev.on('connection.update', async (update) => {
      if (update.qr && !responded) {
        responded = true;
        const qr = await qrcode.toDataURL(update.qr);
        return res.json({ qrCode: qr });
      }

      if (update.connection === 'open' && !responded) {
        responded = true;
        return res.json({ qrCode: null, status: 'authenticated' });
      }
    });

    // Fallback in case QR never arrives
    setTimeout(() => {
      if (!responded) {
        responded = true;
        return res.status(504).json({ error: 'QR code timeout' });
      }
    }, 15000);
  } catch (err) {
    console.error('QR Error:', err);
    return res.status(500).json({ error: 'Internal error generating QR' });
  }
});


// POST /send/:sessionId/:number/:message
app.post('/send/:sessionId/:number/:message', async (req, res) => {
  const { sessionId, number, message } = req.params;
  if (!sessionId || !number || !message) {
    return res.status(400).json({ error: 'Missing parameters' });
  }

  try {
    const sock = sessions[sessionId] || await createOrGetSession(sessionId);

    if (!sock.authState.creds?.registered) {
      return res.status(403).json({ error: 'Session not authenticated yet' });
    }

    const jid = number.includes('@s.whatsapp.net') ? number : `${number}@s.whatsapp.net`;
    await sock.sendMessage(jid, { text: message });

    return res.json({ status: 'sent' });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to send message' });
  }
});

// GET /status/:sessionId
app.get('/status/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  const sock = sessions[sessionId];
  if (!sock) return res.json({ status: 'not_initialized' });

  if (sock.authState.creds?.registered) {
    return res.json({ status: 'authenticated' });
  }

  return res.json({ status: 'pending_qr' });
});

// POST /init/:sessionId
app.post('/init/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  try {
    await createOrGetSession(sessionId);
    return res.json({ status: 'initialized' });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to initialize session' });
  }
});

// POST /disconnect/:sessionId
app.post('/disconnect/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  const sock = sessions[sessionId];
  if (!sock) return res.status(404).json({ error: 'Session not found' });

  try {
    await sock.logout();
    delete sessions[sessionId];
    fs.removeSync(path.join(SESSIONS_DIR, sessionId));
    return res.json({ status: 'disconnected' });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to disconnect session' });
  }
});

// Default route
app.get('/', (req, res) => {
  res.send('🚀 Replino WhatsApp Backend is running.');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`✅ Server running on http://localhost:${PORT}`);
});
