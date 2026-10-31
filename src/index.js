// Blersjda Madness - multiplayer relay server (Cloudflare Worker + Durable Objects).
// One Durable Object per room code. It relays messages between the host and the players
// by topic, replacing the public MQTT broker. It stores nothing.
import { DurableObject } from "cloudflare:workers";

const MAX_SOCKETS_PER_ROOM = 150;
const MAX_MESSAGE_BYTES = 64 * 1024;
const MAX_TOPICS_PER_SOCKET = 8;

function originAllowed(request, env) {
  const allowed = String(env.ALLOWED_ORIGINS || "")
    .split(",")
    .map((s) => s.trim().replace(/\/$/, ""))
    .filter(Boolean);
  if (allowed.includes("*")) return true;
  const origin = request.headers.get("Origin");
  return !!origin && allowed.includes(origin);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("Blersjda server is running.", { status: 200 });
    }

    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{3,12})$/);
    if (!m) return new Response("Not found", { status: 404 });

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected a WebSocket connection", { status: 426 });
    }
    if (!originAllowed(request, env)) {
      return new Response("Origin not allowed", { status: 403 });
    }

    const room = m[1].toUpperCase();
    const stub = env.ROOM.get(env.ROOM.idFromName(room));
    return stub.fetch(request);
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // Keep-alive pings are answered without waking the object.
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(request) {
    const url = new URL(request.url);
    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{3,12})$/);
    if (!m) return new Response("Not found", { status: 404 });

    if (this.ctx.getWebSockets().length >= MAX_SOCKETS_PER_ROOM) {
      return new Response("Room is full", { status: 503 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ room: m[1].toUpperCase(), topics: [] });
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, message) {
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

    // Binary frames: [2-byte topic length][topic utf8][payload]. Relay as-is.
    if (message.byteLength > MAX_MESSAGE_BYTES || message.byteLength < 3) return;
    const view = new DataView(message);
    const tlen = view.getUint16(0);
    if (tlen === 0 || tlen > 100 || 2 + tlen > message.byteLength) return;
    const topic = new TextDecoder().decode(new Uint8Array(message, 2, tlen));
    if (!topic.startsWith(prefix)) return;

    for (const sock of this.ctx.getWebSockets()) {
      if (sock === ws) continue;
      const a = sock.deserializeAttachment();
      if (a && a.topics && a.topics.includes(topic)) {
        try { sock.send(message); } catch { /* socket closing */ }
      }
    }
  }

  webSocketClose(ws, code) {
    try { ws.close(code && code >= 1000 && code < 1005 ? code : 1000, "closed"); } catch {}
  }

  webSocketError(ws) {
    try { ws.close(1011, "error"); } catch {}
  }
}
