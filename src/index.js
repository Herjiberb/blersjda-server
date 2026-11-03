// Blersjda Madness - multiplayer relay server (Cloudflare Worker + Durable Objects).
// One Durable Object per room code. It relays messages between the host and the players
// by topic. Players/hosts connect by WebSocket, or in "HTTPS mode" by plain fetch + long-polling.
import { DurableObject } from "cloudflare:workers";

const MAX_SOCKETS_PER_ROOM = 150;
const MAX_MESSAGE_BYTES = 64 * 1024;
const MAX_TOPICS_PER_SOCKET = 8;
const MAX_QUEUE = 300;
const POLL_WAIT_MS = 25000;
const SESSION_TTL_MS = 120000;

const enc = new TextEncoder();
const b64e = (u) => { let s = ""; for (let i = 0; i < u.length; i += 8192) s += String.fromCharCode.apply(null, u.subarray(i, i + 8192)); return btoa(s); };
const b64d = (b) => { const s = atob(b), u = new Uint8Array(s.length); for (let i = 0; i < s.length; i++) u[i] = s.charCodeAt(i); return u; };
const json = (o, status = 200) => new Response(JSON.stringify(o), { status, headers: { "Content-Type": "application/json" } });

function allowedList(env) {
  return String(env.ALLOWED_ORIGINS || "").split(",").map((s) => s.trim().replace(/\/$/, "")).filter(Boolean);
}
function originAllowed(request, env) {
  const allowed = allowedList(env);
  if (allowed.includes("*")) return true;
  const origin = request.headers.get("Origin");
  return !!origin && allowed.includes(origin);
}
function withCors(res, request) {
  const origin = request.headers.get("Origin");
  if (!origin) return res;
  const h = new Headers(res.headers);
  h.set("Access-Control-Allow-Origin", origin);
  h.set("Vary", "Origin");
  return new Response(res.body, { status: res.status, headers: h });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("Blersjda server is running.", { status: 200 });
    }

    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{3,12})(\/http\/(?:open|sub|pub|poll))?$/);
    if (!m) return new Response("Not found", { status: 404 });

    const isHttp = !!m[2];

    if (request.method === "OPTIONS" && isHttp) {
      if (!originAllowed(request, env)) return new Response("Origin not allowed", { status: 403 });
      return withCors(new Response(null, { status: 204, headers: { "Access-Control-Allow-Methods": "GET,POST,OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Max-Age": "86400" } }), request);
    }

    if (!isHttp && request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected a WebSocket connection", { status: 426 });
    }
    if (!originAllowed(request, env)) {
      return new Response("Origin not allowed", { status: 403 });
    }

    const room = m[1].toUpperCase();
    const stub = env.ROOM.get(env.ROOM.idFromName(room));
    const res = await stub.fetch(request);
    return isHttp ? withCors(res, request) : res;
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // Keep-alive pings are answered without waking the object.
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    this.waiters = new Map(); // sid -> wake function (only while a poll request is in flight)
  }

  async fetch(request) {
    const url = new URL(request.url);
    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{3,12})(?:\/http\/(open|sub|pub|poll))?$/);
    if (!m) return new Response("Not found", { status: 404 });
    const room = m[1].toUpperCase();

    if (m[2]) return this.handleHttp(request, url, room, m[2]);

    if (this.ctx.getWebSockets().length >= MAX_SOCKETS_PER_ROOM) {
      return new Response("Room is full", { status: 503 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ room, topics: [] });
    return new Response(null, { status: 101, webSocket: client });
  }

  /* ---------- relay core: send to WebSocket subscribers and queue for HTTP sessions ---------- */
  async deliver(topic, body, skipWs, skipSid) {
    const t = enc.encode(topic);
    const frame = new Uint8Array(2 + t.length + body.length);
    frame[0] = t.length >> 8; frame[1] = t.length & 255; frame.set(t, 2); frame.set(body, 2 + t.length);
    for (const sock of this.ctx.getWebSockets()) {
      if (sock === skipWs) continue;
      const a = sock.deserializeAttachment();
      if (a && a.topics && a.topics.includes(topic)) {
        try { sock.send(frame); } catch { /* socket closing */ }
      }
    }
    const sessions = await this.ctx.storage.list({ prefix: "s:" });
    let b64 = null;
    for (const [key, sess] of sessions) {
      const sid = key.slice(2);
      if (sid === skipSid || !sess.topics.includes(topic)) continue;
      const q = (await this.ctx.storage.get("q:" + sid)) || [];
      q.push({ topic, b64: b64 || (b64 = b64e(body)) });
      while (q.length > MAX_QUEUE) q.shift();
      await this.ctx.storage.put("q:" + sid, q);
      const wake = this.waiters.get(sid);
      if (wake) wake();
    }
  }

  /* ---------- HTTPS mode ---------- */
  async handleHttp(request, url, room, op) {
    const prefix = `blersjda/${room}/`;
    const now = Date.now();

    if (op === "open") {
      // drop expired sessions
      const all = await this.ctx.storage.list({ prefix: "s:" });
      for (const [key, sess] of all) {
        if (now - sess.seen > SESSION_TTL_MS) await this.ctx.storage.delete([key, "q:" + key.slice(2)]);
      }
      const sid = crypto.randomUUID();
      await this.ctx.storage.put("s:" + sid, { topics: [], seen: now });
      return json({ sid });
    }

    if (op === "poll") {
      const sid = url.searchParams.get("sid") || "";
      let sess = await this.ctx.storage.get("s:" + sid);
      if (!sess) return json({ error: "nosession" }, 404);
      if (now - sess.seen > 20000) { sess.seen = now; await this.ctx.storage.put("s:" + sid, sess); }
      const old = this.waiters.get(sid); if (old) old(); // release an older poll for this session
      let wake;
      const waited = new Promise((resolve) => {
        const timer = setTimeout(() => { this.waiters.delete(sid); resolve(); }, POLL_WAIT_MS);
        wake = () => { clearTimeout(timer); if (this.waiters.get(sid) === wake) this.waiters.delete(sid); resolve(); };
      });
      this.waiters.set(sid, wake);
      let q = (await this.ctx.storage.get("q:" + sid)) || [];
      if (!q.length) { await waited; q = (await this.ctx.storage.get("q:" + sid)) || []; }
      else wake();
      if (q.length) await this.ctx.storage.delete("q:" + sid);
      return json({ msgs: q });
    }

    let o;
    try { o = await request.json(); } catch { return json({ error: "bad" }, 400); }
    const sid = String(o.sid || "");
    const sess = await this.ctx.storage.get("s:" + sid);
    if (!sess) return json({ error: "nosession" }, 404);
    const topic = String(o.topic || "");
    if (!topic || topic.length > 100 || !topic.startsWith(prefix)) return json({ error: "bad topic" }, 400);

    if (op === "sub") {
      if (!sess.topics.includes(topic) && sess.topics.length < MAX_TOPICS_PER_SOCKET) {
        sess.topics.push(topic);
        await this.ctx.storage.put("s:" + sid, sess);
      }
      return json({ ok: true });
    }

    if (op === "pub") {
      const b64 = String(o.b64 || "");
      if (b64.length > MAX_MESSAGE_BYTES * 1.4) return json({ error: "too big" }, 413);
      let body;
      try { body = b64d(b64); } catch { return json({ error: "bad" }, 400); }
      if (body.length > MAX_MESSAGE_BYTES) return json({ error: "too big" }, 413);
      await this.deliver(topic, body, null, sid);
      return json({ ok: true });
    }
    return json({ error: "not found" }, 404);
  }

  /* ---------- WebSocket mode ---------- */
  async webSocketMessage(ws, message) {
    const att = ws.deserializeAttachment() || { room: "", topics: [] };
    const prefix = `blersjda/${att.room}/`;

    // Text frames: subscribe requests.
    if (typeof message === "string") {
      let msg;
      try { msg = JSON.parse(message); } catch { return; }
      if (msg && msg.t === "s" && typeof msg.topic === "string") {
        const topic = msg.topic;
        if (topic.length > 100 || !topic.startsWith(prefix)) return;
        if (!att.topics.includes(topic) && att.topics.length < MAX_TOPICS_PER_SOCKET) {
          att.topics.push(topic);
          ws.serializeAttachment(att);
        }
      }
      return;
    }

    // Binary frames: [2-byte topic length][topic utf8][payload].
    if (message.byteLength > MAX_MESSAGE_BYTES || message.byteLength < 3) return;
    const view = new DataView(message);
    const tlen = view.getUint16(0);
    if (tlen === 0 || tlen > 100 || 2 + tlen > message.byteLength) return;
    const topic = new TextDecoder().decode(new Uint8Array(message, 2, tlen));
    if (!topic.startsWith(prefix)) return;
    await this.deliver(topic, new Uint8Array(message, 2 + tlen), ws, null);
  }

  webSocketClose(ws, code) {
    try { ws.close(code && code >= 1000 && code < 1005 ? code : 1000, "closed"); } catch {}
  }

  webSocketError(ws) {
    try { ws.close(1011, "error"); } catch {}
  }
}
