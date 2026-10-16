// Load + correctness test: N clients in one room, measures control propagation latency,
// late join, and reconnect. Usage: node bench.js [clients=12] [rounds=200] [url=ws://localhost:3000]
const WebSocket = require('ws');
const N = +process.argv[2] || 12, ROUNDS = +process.argv[3] || 200, URL = process.argv[4] || 'ws://localhost:3000';
const pct = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.ceil(p / 100 * s.length) - 1)]; };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function client(name, room, id) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(URL), c = { ws, name, got: [], waiters: [] };
    ws.on('open', () => ws.send(JSON.stringify({ t: 'join', room, name, id })));
    ws.on('message', (d) => {
      const m = JSON.parse(d);
      if (m.t === 'joined') { c.id = m.you; c.room = m.room.code; c.joined = m; resolve(c); }
      if (m.t === 'error') reject(new Error(m.error));
      if (m.t === 'pb') { c.lastPb = m; c.waiters.forEach(w => w(m)); c.waiters = []; }
    });
    ws.on('error', reject);
  });
}
const nextPb = (c) => new Promise(r => c.waiters.push(r));

(async () => {
  const host = await client('host', null);
  const others = [];
  for (let i = 1; i < N; i++) others.push(await client('user' + i, host.room));
  console.log(`room ${host.room} with ${N} clients`);

  host.ws.send(JSON.stringify({ t: 'load', src: 'http://example.com/v.mp4', title: 'test' }));
  await sleep(1800);

  // 1. propagation latency: host sends a control, every other client records arrival time
  const lat = [];
  for (let i = 0; i < ROUNDS; i++) {
    const act = ['play', 'pause', 'seek'][i % 3], t0 = process.hrtime.bigint();
    const waits = others.map(nextPb);
    host.ws.send(JSON.stringify({ t: 'ctl', act, pos: i }));
    await Promise.all(waits.map(w => w.then(() => lat.push(Number(process.hrtime.bigint() - t0) / 1e6))));
    await sleep(10);
  }
  console.log(`propagation over ${lat.length} deliveries: p50=${pct(lat, 50).toFixed(2)}ms p95=${pct(lat, 95).toFixed(2)}ms p99=${pct(lat, 99).toFixed(2)}ms max=${Math.max(...lat).toFixed(2)}ms  (limit: p95 <= 250ms)`);

  // 2. concurrent actions: everyone fires at once; all clients must end on the same seq/state
  await Promise.all([host, ...others].map((c, i) => c.ws.send(JSON.stringify({ t: 'ctl', act: i % 2 ? 'pause' : 'seek', pos: i * 10 }))));
  await sleep(300);
  const fin = [host, ...others].map(c => c.lastPb.seq + ':' + c.lastPb.pb.playing + ':' + c.lastPb.pb.pos);
  console.log('concurrent actions converge:', new Set(fin).size === 1 ? 'PASS' : 'FAIL ' + [...new Set(fin)]);

  // 3. late join mid-playback: should receive a current position, not 0
  host.ws.send(JSON.stringify({ t: 'ctl', act: 'play', pos: 100 }));
  await sleep(2000);
  const late = await client('late', host.room);
  console.log(`late join position: ${late.joined.pb.pos.toFixed(2)}s (expected ~102s)`, Math.abs(late.joined.pb.pos - 102) < 0.5 ? 'PASS' : 'FAIL');

  // 4. reconnect with the same id keeps identity and receives current state
  const old = others[0];
  old.ws.terminate(); await sleep(200);
  const back = await client('user1', host.room, old.id);
  console.log('reconnect keeps identity:', back.id === old.id ? 'PASS' : 'FAIL', '| state restored:', back.joined.pb.playing ? 'PASS' : 'FAIL');

  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
