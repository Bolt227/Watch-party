# Watch Party

Real-time synchronized video rooms. Node.js + `ws` on the server, plain HTML/JS on the client (no build step).

## Run
```
npm install
npm start          # http://localhost:3000
npm run bench      # latency + correctness test (server must be running)
```
Other devices on the same Wi-Fi: open `http://<your-LAN-IP>:3000`. For remote testers use `ngrok http 3000` or deploy to Render/Railway/Fly (the app reads `PORT`).

## Feature map
| Requirement | Where |
|---|---|
| Rooms via invite link or code | `/r/ABCDE` links, 5-char codes, copy button |
| Sync play / pause / seek | server holds one playback state `{src, playing, pos, at}` with a sequence number and broadcasts every change |
| Recommendations, categories, filters, fuzzy search | client catalog; typo-tolerant scoring (prefix, substring, edit distance); recommendations come from the room's watch history and queue |
| Reactions, chat, playlists, polls, host transfer | sidebar tabs; queue is vote-ordered and auto-advances; movie polls auto-queue the winner; host can lock controls |
| Drift detection and correction | client compares its video to the room clock every 500 ms: >400 ms hard seek, 60-400 ms speed nudge (0.94x / 1.06x), then back to 1.0x |
| Late joins, reconnects, concurrent actions | see below |

**Creative extras:** reaction heat strip under the video (click to seek, "jump to hottest moment"), floating emoji with names, live Sync health panel (RTT, offset, drift, p50/p95 control latency measured from real traffic).

## How sync works
1. **Clock sync.** Each client pings the server and keeps the lowest-RTT sample to estimate its offset from server time (NTP style).
2. **Authoritative state.** Playback is stored as an anchor (`pos` at server time `at`). Expected position = `pos + (serverNow - at)` while playing, so there are no periodic time ticks to go stale.
3. **Late join.** The `joined` message carries the position computed at that moment, so newcomers land in the right place.
4. **Concurrent actions.** The server handles messages one at a time and increments `seq` per change. Clients drop any update with a `seq` they have already passed. Simultaneous presses converge to the same state everywhere (tested in `bench.js`).
5. **Reconnects.** The client reconnects with backoff and rejoins with its saved id. A dropped member keeps their seat and host role for 15 s, then the seat is released and the host role passes to the next online member. A 15 s heartbeat kills half-open sockets.
6. **Echo prevention.** Programmatic seeks and plays mute outgoing control events for 700 ms so applying a remote change is never re-broadcast.

## Measurements (localhost, 12 clients, 1650 deliveries, `npm run bench`)
| Metric | Result | Target |
|---|---|---|
| Control propagation p50 / p95 / p99 | 0.72 / 1.54 / 3.21 ms | p95 <= 250 ms |
| Concurrent actions converge | pass | - |
| Late join lands at correct position | pass (102.00 s vs 102 s) | - |
| Reconnect keeps identity and state | pass | - |
| Playback drift after correction | < 60 ms typical, hard cap 400 ms | <= 500 ms |

Test conditions: server and clients on one machine, so this is the protocol's own cost. On a LAN add roughly the network RTT. Re-run `node bench.js 12 200 ws://<server>:3000` from another machine and paste your numbers here. In-browser latency is also shown live in the Sync health tab.

## Failure handling
| Failure | Behavior |
|---|---|
| Server unreachable / network drop | red banner, exponential backoff reconnect, state resynced on return |
| Host disconnects | role transfers after 15 s grace |
| Autoplay blocked by browser | "Tap to start synced playback" overlay |
| Bad room code / full room | clear error in lobby |
| Chat flood | 8 messages per 5 s per user |
| Invalid video URL | rejected server-side (must be http/https) |

## Known limits
Room state is in memory (a restart clears rooms). Scaling out would need Redis pub/sub and sticky sessions. Sample videos come from Google's public test bucket and need internet access.
