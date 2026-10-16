// Servidor de salas para "La Casa de los Zombis".
// Hace 2 cosas: (1) sirve el juego (public/index.html) y (2) reenvía mensajes entre jugadores.
// El anfitrión sigue mandando en la partida: este servidor NO ejecuta el juego, solo reenvía.
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PEERS = 3000;            // conexiones totales a la vez
const MAX_PER_IP = 20;             // conexiones por IP
const MAX_LINKS = 8;               // conexiones por jugador (el anfitrión necesita 3)
const RATE_PER_SEC = 400;          // mensajes por segundo y jugador (límite anti-abuso)
const RATE_BURST = 800;
const HOST_ID = /^casazombis-[A-Z0-9]{4,8}$/;

// ---------- Web: sirve el juego ----------
const indexPath = path.join(__dirname, 'public', 'index.html');
let indexRaw = null, indexGz = null;
try { indexRaw = fs.readFileSync(indexPath); indexGz = zlib.gzipSync(indexRaw, { level: 9 }); }
catch (e) { console.error('No encuentro public/index.html'); }

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html') {
    if (!indexRaw) { res.writeHead(500); return res.end('Falta public/index.html'); }
    const gz = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
    res.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-cache',
      ...(gz ? { 'content-encoding': 'gzip' } : {})
    });
    return res.end(gz ? indexGz : indexRaw);
  }
  if (url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  res.writeHead(404, { 'content-type': 'text/plain' }); res.end('No encontrado');
});

// ---------- Salas: reenvío de mensajes ----------
const wss = new WebSocketServer({ server, maxPayload: 128 * 1024 });
const peers = new Map();   // id -> ws
const perIp = new Map();   // ip -> nº conexiones

function send(ws, o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }
function ipOf(req) { return ((req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || '?'; }

wss.on('connection', (ws, req) => {
  const ip = ipOf(req);
  const n = (perIp.get(ip) || 0) + 1;
  if (wss.clients.size > MAX_PEERS || n > MAX_PER_IP) { ws.close(1013, 'lleno'); return; }
  perIp.set(ip, n);

  ws.id = null; ws.links = new Set(); ws.alive = true; ws.tokens = RATE_BURST; ws.lastTok = Date.now();
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', (raw) => {
    ws.alive = true;
    const now = Date.now();
    ws.tokens = Math.min(RATE_BURST, ws.tokens + (now - ws.lastTok) / 1000 * RATE_PER_SEC); ws.lastTok = now;
    if (--ws.tokens < 0) { ws.close(1008, 'demasiados mensajes'); return; }

    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.k === 'reg') {
      if (ws.id) return;
      let id = m.id;
      if (id === undefined || id === null) id = 'p-' + crypto.randomBytes(8).toString('hex');
      else if (typeof id !== 'string' || !HOST_ID.test(id)) return send(ws, { k: 'err', type: 'invalid-id' });
      if (peers.has(id)) return send(ws, { k: 'err', type: 'unavailable-id' });
      ws.id = id; peers.set(id, ws);
      return send(ws, { k: 'open', id });
    }
    if (!ws.id) return;

    if (m.k === 'conn') {
      const to = m.to, t = (typeof to === 'string' && HOST_ID.test(to)) ? peers.get(to) : null;
      if (!t || t === ws || ws.links.size >= MAX_LINKS) return send(ws, { k: 'err', type: 'peer-unavailable', to });
      ws.links.add(to); t.links.add(ws.id);
      send(t, { k: 'incoming', from: ws.id });      // primero avisa al anfitrión...
      return send(ws, { k: 'copen', peer: to });     // ...y luego al que se une
    }
    if (m.k === 'data') {
      if (typeof m.to !== 'string' || !ws.links.has(m.to)) return;
      return send(peers.get(m.to), { k: 'data', from: ws.id, d: m.d });
    }
    if (m.k === 'close') {
      if (typeof m.to !== 'string' || !ws.links.has(m.to)) return;
      ws.links.delete(m.to);
      const t = peers.get(m.to);
      if (t) { t.links.delete(ws.id); send(t, { k: 'close', from: ws.id }); }
    }
  });

  ws.on('close', () => {
    const c = (perIp.get(ip) || 1) - 1; if (c <= 0) perIp.delete(ip); else perIp.set(ip, c);
    for (const other of ws.links) {
      const t = peers.get(other);
      if (t) { t.links.delete(ws.id); send(t, { k: 'close', from: ws.id }); }
    }
    if (ws.id && peers.get(ws.id) === ws) peers.delete(ws.id);
  });
  ws.on('error', () => {});
});

// Limpia conexiones muertas (cada 25 s)
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; try { ws.ping(); } catch (e) {}
  }
}, 25000);

server.listen(PORT, () => console.log('Servidor listo en el puerto ' + PORT));
