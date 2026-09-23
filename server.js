// Coup online: serves the game page and relays messages between the host and guests of each room.
// The host's browser runs the game; this server only passes messages along and holds them
// for a little while when someone's connection drops (a phone switching apps, a tunnel, etc).

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PAGE = path.join(__dirname, 'index.html');
const HOST_GRACE_MS = 3 * 60 * 1000;   // how long a room waits for its host to come back
const GUEST_GRACE_MS = 2 * 60 * 1000;  // how long a seat waits for a guest to come back
const MAX_QUEUE = 3000;                // messages held for someone who is offline
const MAX_GUESTS = 5;
const CODE_ALPHABET = 'BCDFGHJKLMNPQRSTVWXZ';

const rooms = new Map(); // code -> { code, host, guests: Map(id -> member), nextId }

function newCode() {
  for (let tries = 0; tries < 50; tries++) {
    let code = '';
    for (let i = 0; i < 5; i++) code += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    if (!rooms.has(code)) return code;
  }
  throw new Error('no free room codes');
}
const newToken = () => crypto.randomBytes(18).toString('base64url');

function member(id) {
  return { id, token: newToken(), ws: null, queue: [], graceTimer: null };
}

function deliver(m, msg) {
  const text = JSON.stringify(msg);
  if (m.ws && m.ws.readyState === m.ws.OPEN) m.ws.send(text);
  else {
    m.queue.push(text);
    if (m.queue.length > MAX_QUEUE) m.queue.shift();
  }
}

function attach(m, ws) {
  if (m.ws && m.ws !== ws) { try { m.ws.close(4000, 'replaced'); } catch (e) { /* already closed */ } }
  clearTimeout(m.graceTimer);
  m.graceTimer = null;
  m.ws = ws;
  ws.member = m;
}

function flush(m) {
  const q = m.queue;
  m.queue = [];
  q.forEach((text) => m.ws.send(text));
}

function closeRoom(room, reason) {
  rooms.delete(room.code);
  clearTimeout(room.host.graceTimer);
  room.guests.forEach((g) => {
    clearTimeout(g.graceTimer);
    if (g.ws) { g.ws.send(JSON.stringify({ t: 'closed', reason })); g.ws.member = null; g.ws.room = null; }
  });
  if (room.host.ws) { room.host.ws.member = null; room.host.ws.room = null; }
}

function removeGuest(room, g, reason) {
  if (!room.guests.has(g.id)) return;
  clearTimeout(g.graceTimer);
  room.guests.delete(g.id);
  if (g.ws) { try { g.ws.send(JSON.stringify({ t: 'closed', reason })); } catch (e) { /* ignore */ } g.ws.member = null; g.ws.room = null; }
  deliver(room.host, { t: 'close', from: g.id });
}

function onDisconnect(ws) {
  const m = ws.member, room = ws.room;
  if (!m || !room || m.ws !== ws) return;
  m.ws = null;
  const isHost = m === room.host;
  m.graceTimer = setTimeout(() => {
    if (isHost) closeRoom(room, 'host-left');
    else removeGuest(room, m, 'timeout');
  }, isHost ? HOST_GRACE_MS : GUEST_GRACE_MS);
}

function handle(ws, msg) {
  const reply = (o) => ws.send(JSON.stringify(o));
  const m = ws.member, room = ws.room;

  switch (msg.t) {
    case 'ping': reply({ t: 'pong' }); return;

    case 'host': {
      if (m) return;
      const code = newCode();
      const r = { code, host: member('h'), guests: new Map(), nextId: 1 };
      rooms.set(code, r);
      attach(r.host, ws);
      ws.room = r;
      reply({ t: 'hosted', code, token: r.host.token });
      return;
    }

    case 'join': {
      if (m) return;
      const r = rooms.get(String(msg.code || '').toUpperCase());
      if (!r) { reply({ t: 'error', reason: 'no-room' }); return; }
      if (r.guests.size >= MAX_GUESTS) { reply({ t: 'error', reason: 'full' }); return; }
      const g = member('g' + r.nextId++);
      r.guests.set(g.id, g);
      attach(g, ws);
      ws.room = r;
      reply({ t: 'joined', code: r.code, id: g.id, token: g.token });
      deliver(r.host, { t: 'open', from: g.id });
      return;
    }

    case 'resume': {
      const r = rooms.get(String(msg.code || '').toUpperCase());
      const who = r && (r.host.token === msg.token ? r.host : [...r.guests.values()].find((g) => g.token === msg.token));
      if (!who) { reply({ t: 'error', reason: 'expired' }); return; }
      attach(who, ws);
      ws.room = r;
      reply({ t: 'resumed', id: who.id });
      flush(who);
      return;
    }

    case 'send': {
      if (!m || !room) return;
      if (m === room.host) {
        const g = room.guests.get(msg.to);
        if (g) deliver(g, { t: 'data', from: 'h', data: msg.data });
      } else {
        deliver(room.host, { t: 'data', from: m.id, data: msg.data });
      }
      return;
    }

    case 'kick': {
      if (!m || !room || m !== room.host) return;
      const g = room.guests.get(msg.id);
      if (g) removeGuest(room, g, 'kicked');
      return;
    }

    case 'leave': {
      if (!m || !room) return;
      if (m === room.host) closeRoom(room, 'host-left');
      else removeGuest(room, m, 'left');
      ws.member = null;
      ws.room = null;
      return;
    }
  }
}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); res.end(); return; }
  fs.readFile(PAGE, (err, buf) => {
    if (err) { res.writeHead(500); res.end('index.html not found'); return; }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    res.end(req.method === 'HEAD' ? undefined : buf);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });
wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (msg && typeof msg === 'object') handle(ws, msg);
  });
  ws.on('close', () => onDisconnect(ws));
  ws.on('error', () => { /* close follows */ });
});

// Drop sockets that stopped answering, so their seat's grace period can start.
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) { /* ignore */ }
  });
}, 30000);

server.listen(PORT, () => console.log(`Coup online on port ${PORT}`));
