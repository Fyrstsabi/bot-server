const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 24790;
const LETTERS = 'ABCDEFGHJKMNPQRSTVWXYZ';

const server = http.createServer((req, res) => {
  // CORS Headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.url === '/api/news') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ items: [] }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Battle of Talingchan Relay Server Running (WebSocket active on same port)');
});

const wss = new WebSocketServer({ server });

const rooms = new Map(); // code -> client ws
const queue = [];        // array of client ws
const matches = new Map(); // matchId -> { host, guest }
let nextMatchId = 1;

function generateCode() {
  while (true) {
    let code = '';
    for (let i = 0; i < 5; i++) {
      code += LETTERS.charAt(Math.floor(Math.random() * LETTERS.length));
    }
    if (!rooms.has(code)) return code;
  }
}

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function leaveWaiting(ws) {
  const qIdx = queue.indexOf(ws);
  if (qIdx !== -1) queue.splice(qIdx, 1);

  if (ws.roomCode && rooms.get(ws.roomCode) === ws) {
    rooms.delete(ws.roomCode);
    ws.roomCode = null;
  }
}

function pair(host, guest) {
  leaveWaiting(host);
  leaveWaiting(guest);

  host.partner = guest;
  guest.partner = host;

  const matchId = nextMatchId++;
  const tokenHost = crypto.randomBytes(16).toString('hex');
  const tokenGuest = crypto.randomBytes(16).toString('hex');

  host.matchId = matchId;
  guest.matchId = matchId;

  send(host, { t: 'paired', host: true, token: tokenHost });
  send(guest, { t: 'paired', host: false, token: tokenGuest });
}

wss.on('connection', (ws) => {
  ws.partner = null;
  ws.roomCode = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString('utf8'));
    } catch (e) {
      // If client sends raw binary or non-json
      return;
    }

    if (!msg || typeof msg !== 'object') return;

    // If already paired with someone, relay messages directly
    if (ws.partner) {
      send(ws.partner, msg);
      if (msg.t === 'bye' || msg.t === 'forfeit') {
        const p = ws.partner;
        ws.partner = null;
        if (p) p.partner = null;
      }
      return;
    }

    const t = msg.t;
    if (t === 'room_create') {
      leaveWaiting(ws);
      const code = generateCode();
      rooms.set(code, ws);
      ws.roomCode = code;
      send(ws, { t: 'room', code });
    } else if (t === 'room_join') {
      const code = String(msg.code || '').trim().toUpperCase();
      const host = rooms.get(code);
      if (!host || host === ws || host.readyState !== WebSocket.OPEN) {
        send(ws, { t: 'error', why: `ไม่พบห้อง ${code}` });
      } else {
        pair(host, ws);
      }
    } else if (t === 'queue') {
      leaveWaiting(ws);
      const other = queue.shift();
      if (other && other.readyState === WebSocket.OPEN && other !== ws) {
        pair(other, ws);
      } else {
        queue.push(ws);
      }
    } else if (t === 'leave') {
      leaveWaiting(ws);
    } else if (t === 'count') {
      let playing = 0;
      for (const client of wss.clients) {
        if (client.partner) playing++;
      }
      send(ws, {
        t: 'count',
        online: wss.clients.size,
        queue: queue.length,
        playing: Math.floor(playing / 2)
      });
    }
  });

  ws.on('close', () => {
    leaveWaiting(ws);
    if (ws.partner) {
      send(ws.partner, { t: 'peer_left' });
      ws.partner.partner = null;
      ws.partner = null;
    }
  });

  ws.on('error', () => {
    leaveWaiting(ws);
  });
});

server.listen(PORT, () => {
  console.log(`Relay Server listening on port ${PORT}`);
});
