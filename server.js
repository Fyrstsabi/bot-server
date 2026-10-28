const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 24790;
const LETTERS = 'ABCDEFGHJKMNPQRSTVWXYZ';

// ─── Tower Leaderboard (in-memory, top 50 entries) ────────────────────────────
const TOWER_MAX_ENTRIES = 50;
const towerBoard = new Map(); // userId -> { name, floor, updated_at }

function getTowerLeaderboard() {
  const list = Array.from(towerBoard.values());
  list.sort((a, b) => b.floor - a.floor);
  return list.slice(0, TOWER_MAX_ENTRIES).map((e, i) => ({
    rank: i + 1,
    name: e.name,
    user: e.user,
    floor: e.floor,
    updated_at: e.updated_at,
  }));
}

function readBody(req) {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', chunk => { data += chunk; });
    req.on('end', () => {
      try { resolve(JSON.parse(data)); }
      catch { resolve({}); }
    });
  });
}

const server = http.createServer(async (req, res) => {
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

  // ── GET /api/tower/leaderboard — ดึงตารางอันดับ ──────────────────────────
  if (req.method === 'GET' && req.url === '/api/tower/leaderboard') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ leaderboard: getTowerLeaderboard() }));
    return;
  }

  // ── POST /api/tower/score — บันทึกคะแนน ─────────────────────────────────
  if (req.method === 'POST' && req.url === '/api/tower/score') {
    const body = await readBody(req);
    const name = String(body.name || '').trim().slice(0, 32);
    const user = String(body.user || '').trim().slice(0, 64);
    const floor = Math.max(1, Math.min(99999, parseInt(body.floor) || 1));

    if (!name || !user) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'missing name or user' }));
      return;
    }

    // Only update if new score is higher than existing
    const existing = towerBoard.get(user);
    if (!existing || floor > existing.floor) {
      towerBoard.set(user, {
        name,
        user,
        floor,
        updated_at: new Date().toISOString(),
      });
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, leaderboard: getTowerLeaderboard() }));
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
