// Watch Party server: HTTP static files + WebSocket room engine.
// The server is the single source of truth for playback state. Every control
// action is serialized by Node's event loop, stamped with a sequence number
// and broadcast, so concurrent actions resolve deterministically (last write wins).
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUB = path.join(__dirname, 'public');
const MAX_MEMBERS = 50;
const RECONNECT_GRACE_MS = 15000; // time a dropped member keeps their seat (and host role)
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' };

const rooms = new Map();
const now = () => Date.now();
const uid = () => crypto.randomBytes(6).toString('hex');
const clean = (s, n) => String(s ?? '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n);

function newCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c;
  do { c = Array.from({ length: 5 }, () => A[(Math.random() * A.length) | 0]).join(''); } while (rooms.has(c));
  return c;
}

function createRoom() {
  const r = {
    code: newCode(), host: null, hostOnly: false, seq: 0,
    members: new Map(),
    pb: { src: null, title: '', playing: false, pos: 0, at: now() },
    playlist: [], polls: [], history: [], reactions: [], chat: [],
  };
  rooms.set(r.code, r);
  return r;
}

const curPos = (r) => (r.pb.playing ? r.pb.pos + (now() - r.pb.at) / 1000 : r.pb.pos);

function send(ws, obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function broadcast(r, obj, except) {
  const data = JSON.stringify(obj);
  for (const m of r.members.values()) if (m.ws !== except && m.ws && m.ws.readyState === 1) m.ws.send(data);
}
const sys = (r, text) => { const m = { t: 'sys', text, ts: now() }; r.chat.push(m); r.chat = r.chat.slice(-100); broadcast(r, m); };

function roomView(r) {
  return {
    code: r.code, host: r.host, hostOnly: r.hostOnly,
    members: [...r.members.values()].map(m => ({ id: m.id, name: m.name, online: m.online, drift: m.drift, rtt: m.rtt })),
    playlist: [...r.playlist].sort((a, b) => b.votes.length - a.votes.length || a.addedAt - b.addedAt),
    polls: r.polls.slice(-5), history: r.history.slice(-30),
  };
}
const pushRoom = (r) => broadcast(r, { t: 'room', room: roomView(r) });
const pushPb = (r, by, act, sent) => broadcast(r, { t: 'pb', seq: r.seq, pb: r.pb, by, act, sent, s: now() });

function setPb(r, patch, by, act, sent) {
  r.pb = { ...r.pb, ...patch, at: now() };
  r.seq++;
  pushPb(r, by, act, sent);
}

function loadVideo(r, src, title, by) {
  r.history.push({ src, title, at: now() });
  r.reactions = [];
  r.pb = { src, title, playing: false, pos: 0, at: now() };
  r.seq++;
  pushPb(r, by, 'load');
  // start playing together once everyone has had a moment to buffer
  setTimeout(() => { if (r.pb.src === src && !r.pb.playing && r.pb.pos === 0) setPb(r, { playing: true, pos: 0 }, 'server', 'play'); }, 1500);
  pushRoom(r);
}

function transferHost(r, to, why) {
  const m = r.members.get(to);
  if (!m) return;
  r.host = to;
  sys(r, `${m.name} is now the host${why ? ' (' + why + ')' : ''}`);
  pushRoom(r);
}

function removeMember(r, id) {
  const m = r.members.get(id);
  if (!m) return;
  r.members.delete(id);
  sys(r, `${m.name} left`);
  if (r.host === id) {
    const next = [...r.members.values()].find(x => x.online) || [...r.members.values()][0];
    if (next) transferHost(r, next.id, 'previous host left');
    else r.host = null;
  }
  if (r.members.size === 0) rooms.delete(r.code);
  else pushRoom(r);
}

function canControl(r, m) { return !r.hostOnly || r.host === m.id; }

// ---------- message handling ----------
function onMessage(ctx, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch { return; }
  if (msg.t === 'ping') return send(ctx.ws, { t: 'pong', c: msg.c, s: now() });

  if (msg.t === 'join') {
    let r = msg.room ? rooms.get(String(msg.room).toUpperCase()) : createRoom();
    if (!r) return send(ctx.ws, { t: 'error', error: 'Room not found. Check the code and try again.' });
    let m = msg.id && r.members.get(msg.id);
    const rejoin = !!m;
    if (!m) {
      if (r.members.size >= MAX_MEMBERS) return send(ctx.ws, { t: 'error', error: 'Room is full.' });
      m = { id: uid(), name: clean(msg.name, 24) || 'Guest', joinedAt: now(), drift: 0, rtt: 0 };
      r.members.set(m.id, m);
      if (!r.host) r.host = m.id;
    }
    if (m.ws && m.ws !== ctx.ws) { try { m.ws.close(); } catch {} } // stale socket from a dropped connection
    clearTimeout(m.dropTimer);
    m.ws = ctx.ws; m.online = true; ctx.room = r; ctx.id = m.id;
    send(ctx.ws, {
      t: 'joined', you: m.id, room: roomView(r), seq: r.seq, s: now(),
      pb: { ...r.pb, pos: curPos(r), at: now(), playing: r.pb.playing },  // late joiners get "now", not a stale position
      chat: r.chat, reactions: r.reactions.map(x => x.pos),
    });
    if (!rejoin) sys(r, `${m.name} joined`);
    pushRoom(r);
    return;
  }

  const r = ctx.room, m = r && r.members.get(ctx.id);
  if (!m) return;

  switch (msg.t) {
    case 'ctl': { // play | pause | seek
      if (!canControl(r, m)) return send(ctx.ws, { t: 'denied', text: 'Only the host can control playback.' });
      const pos = Math.max(0, Number(msg.pos) || 0);
      if (msg.act === 'play') setPb(r, { playing: true, pos }, m.id, 'play', msg.sent);
      else if (msg.act === 'pause') setPb(r, { playing: false, pos }, m.id, 'pause', msg.sent);
      else if (msg.act === 'seek') setPb(r, { pos }, m.id, 'seek', msg.sent);
      break;
    }
    case 'load': {
      if (!canControl(r, m)) return send(ctx.ws, { t: 'denied', text: 'Only the host can change the video.' });
      const src = clean(msg.src, 500);
      if (!/^https?:\/\//.test(src)) return;
      loadVideo(r, src, clean(msg.title, 120) || src, m.id);
      break;
    }
    case 'ended': { // whoever reports first wins; later duplicates are ignored because pb.src changes
      if (r.pb.src !== msg.src) break;
      const next = r.playlist.sort((a, b) => b.votes.length - a.votes.length || a.addedAt - b.addedAt)[0];
      if (next) { r.playlist = r.playlist.filter(x => x.id !== next.id); loadVideo(r, next.src, next.title, 'server'); }
      else setPb(r, { playing: false }, 'server', 'pause');
      break;
    }
    case 'chat': {
      const text = clean(msg.text, 500);
      if (!text) break;
      const now_ = now();
      m.recent = (m.recent || []).filter(t => now_ - t < 5000); // rate limit: 8 msgs / 5s
      if (m.recent.length >= 8) break;
      m.recent.push(now_);
      const c = { t: 'chat', id: uid(), from: m.id, name: m.name, text, ts: now_ };
      r.chat.push(c); r.chat = r.chat.slice(-100);
      broadcast(r, c);
      break;
    }
    case 'react': {
      const emoji = clean(msg.emoji, 8);
      if (!emoji) break;
      const pos = curPos(r);
      if (r.reactions.length < 5000) r.reactions.push({ pos });
      broadcast(r, { t: 'react', emoji, by: m.id, name: m.name, pos });
      break;
    }
    case 'pl_add': {
      const src = clean(msg.src, 500);
      if (!/^https?:\/\//.test(src) || r.playlist.some(x => x.src === src) || r.playlist.length >= 50) break;
      r.playlist.push({ id: uid(), src, title: clean(msg.title, 120) || src, by: m.name, votes: [m.id], addedAt: now() });
      pushRoom(r);
      break;
    }
    case 'pl_vote': {
      const it = r.playlist.find(x => x.id === msg.id);
      if (!it) break;
      const i = it.votes.indexOf(m.id);
      if (i >= 0) it.votes.splice(i, 1); else it.votes.push(m.id);
      pushRoom(r);
      break;
    }
    case 'pl_remove': {
      const it = r.playlist.find(x => x.id === msg.id);
      if (it && (r.host === m.id || it.by === m.name)) { r.playlist = r.playlist.filter(x => x !== it); pushRoom(r); }
      break;
    }
    case 'pl_play': {
      if (!canControl(r, m)) break;
      const it = r.playlist.find(x => x.id === msg.id);
      if (it) { r.playlist = r.playlist.filter(x => x !== it); loadVideo(r, it.src, it.title, m.id); }
      break;
    }
    case 'poll_new': {
      const q = clean(msg.q, 140);
      const opts = (msg.opts || []).slice(0, 8).map(o => ({ label: clean(o.label, 100), src: /^https?:\/\//.test(o.src || '') ? clean(o.src, 500) : null, votes: [] })).filter(o => o.label);
      if (!q || opts.length < 2) break;
      r.polls.push({ id: uid(), q, opts, by: m.name, open: true, createdAt: now() });
      pushRoom(r);
      break;
    }
    case 'poll_vote': {
      const p = r.polls.find(x => x.id === msg.id);
      if (!p || !p.open || !p.opts[msg.idx]) break;
      p.opts.forEach(o => { o.votes = o.votes.filter(v => v !== m.id); }); // one vote per person, changeable
      p.opts[msg.idx].votes.push(m.id);
      const online = [...r.members.values()].filter(x => x.online).length;
      if (p.opts.reduce((n, o) => n + o.votes.length, 0) >= online) closePoll(r, p);
      else pushRoom(r);
      break;
    }
    case 'poll_close': {
      const p = r.polls.find(x => x.id === msg.id);
      if (p && p.open && (r.host === m.id || p.by === m.name)) closePoll(r, p);
      break;
    }
    case 'transfer':
      if (r.host === m.id && r.members.has(msg.to)) transferHost(r, msg.to);
      break;
    case 'settings':
      if (r.host === m.id) { r.hostOnly = !!msg.hostOnly; sys(r, r.hostOnly ? 'Playback is now host-only' : 'Everyone can control playback'); pushRoom(r); }
      break;
    case 'stat':
      m.drift = Math.round(Number(msg.drift) || 0); m.rtt = Math.round(Number(msg.rtt) || 0);
      break;
  }
}

function closePoll(r, p) {
  p.open = false;
  const win = [...p.opts].sort((a, b) => b.votes.length - a.votes.length)[0];
  sys(r, `Poll closed: "${win.label}" won with ${win.votes.length} vote(s)`);
  if (win.src && win.votes.length > 0) {
    if (!r.pb.src) loadVideo(r, win.src, win.label, 'poll');
    else if (!r.playlist.some(x => x.src === win.src)) r.playlist.push({ id: uid(), src: win.src, title: win.label, by: 'Poll', votes: [], addedAt: now() });
  }
  pushRoom(r);
}

// ---------- HTTP + WS plumbing ----------
const server = http.createServer((req, res) => {
  let p = req.url.split('?')[0];
  if (p === '/health') { res.writeHead(200); return res.end(JSON.stringify({ rooms: rooms.size })); }
  if (p === '/' || p.startsWith('/r/')) p = '/index.html'; // invite links: /r/ABCDE
  const f = path.join(PUB, path.normalize(p));
  if (!f.startsWith(PUB)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': (MIME[path.extname(f)] || 'application/octet-stream') + '; charset=utf-8' });
    res.end(d);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024, perMessageDeflate: false });
wss.on('connection', (ws) => {
  const ctx = { ws, room: null, id: null };
  ws._socket.setNoDelay(true); // no Nagle batching: matters for the 250ms budget
  ws.on('message', (d) => { try { onMessage(ctx, d.toString()); } catch (e) { console.error(e); } });
  ws.on('close', () => {
    const r = ctx.room, m = r && r.members.get(ctx.id);
    if (!m || m.ws !== ws) return; // already replaced by a newer connection
    m.online = false;
    pushRoom(r);
    m.dropTimer = setTimeout(() => removeMember(r, m.id), RECONNECT_GRACE_MS);
  });
  ws.on('error', () => {});
});

// Heartbeat: terminate half-open sockets so the client's reconnect logic kicks in.
const alive = new WeakMap();
wss.on('connection', (ws) => { alive.set(ws, true); ws.on('pong', () => alive.set(ws, true)); });
setInterval(() => {
  for (const ws of wss.clients) {
    if (alive.get(ws) === false) { ws.terminate(); continue; }
    alive.set(ws, false);
    try { ws.ping(); } catch {}
  }
}, 15000);

server.listen(PORT, () => console.log(`Watch Party running on http://localhost:${PORT}`));
