// server.js

const express = require('express');
const { default: makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode');
const fs = require('fs-extra');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

const SESSIONS_DIR = path.join(__dirname, 'sessions');
fs.ensureDirSync(SESSIONS_DIR);

const sessions = {}; // active socket connections

async function createOrGetSession(sessionId) {
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
      const shouldReconnect = (lastDisconnect?.error?.output?.statusCode !== 401);
      if (shouldReconnect) {
        createOrGetSession(sessionId);
      } else {
        delete sessions[sessionId];
      }
    }
  });

  sessions[sessionId] = sock;
  return sock;
}

// ========== ROUTES ==========

// Home route to avoid "Cannot GET /"
app.get('/', (req, res) => {
  res.send('✅ WhatsApp backend is running, test 1.');
});

// POST /qrcode – Get QR code for login
app.post('/qrcode', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });

  try {
    const sock = await createOrGetSession(sessionId);

    if (sock.authState.creds?.registered) {
      return res.json({ qrCode: null, status: 'already_authenticated' });
    }

    let responded = false;

    const timeout = setTimeout(() => {
      if (!responded) {
        responded = true;
        res.status(408).json({ error: 'QR code timeout' });
      }
    }, 10000); // 10 sec timeout

    sock.ev.on('connection.update', async (update) => {
      if (update.qr && !responded) {
        const qr = await qrcode.toDataURL(update.qr);
        clearTimeout(timeout);
        responded = true;
        res.json({ qrCode: qr });
      }
    });

  } catch (error) {
    console.error('QR Error:', error);
    return res.status(500).json({ error: 'Failed to generate QR' });
  }
});

// GET /send/:sessionId/:number/:message – Send WhatsApp message
app.get('/send/:sessionId/:number/:message', async (req, res) => {
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
    console.error('Send Error:', err);
    return res.status(500).json({ error: 'Failed to send message' });
  }
});

// ========== START SERVER ==========
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`✅ Server running on http://localhost:${PORT}`);
});
