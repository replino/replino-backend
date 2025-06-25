// server.js

const express = require('express');
const { Boom } = require('@hapi/boom');
const { default: makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, makeInMemoryStore } = require('@whiskeysockets/baileys');
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
      const shouldReconnect = (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut;
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

// POST /qrcode
app.post('/qrcode', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });

  const sock = await createOrGetSession(sessionId);

  if (sock.authState.creds?.registered) {
    return res.json({ qrCode: null, status: 'already_authenticated' });
  }

  sock.ev.once('connection.update', async (update) => {
    if (update.qr) {
      const qr = await qrcode.toDataURL(update.qr);
      return res.json({ qrCode: qr });
    }
  });
});

// GET /send/:sessionId/:number/:message
app.get('/send/:sessionId/:number/:message', async (req, res) => {
  const { sessionId, number, message } = req.params;

  if (!sessionId || !number || !message) {
    return res.status(400).json({ error: 'Missing parameters' });
  }

  const sock = sessions[sessionId] || await createOrGetSession(sessionId);

  if (!sock.authState.creds?.registered) {
    return res.status(403).json({ error: 'Session not authenticated yet' });
  }

  const jid = number.includes('@s.whatsapp.net') ? number : `${number}@s.whatsapp.net`;

  try {
    await sock.sendMessage(jid, { text: message });
    return res.json({ status: 'sent' });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to send message' });
  }
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`✅ Server running on http://localhost:${PORT}`);
});
