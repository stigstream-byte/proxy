import process from "node:process";
import { decrypt } from "./tokenCrypto.js";

const startedAt = Date.now();
const endpointStats = new Map([
  ["/fetch", { inFlight: 0, total: 0, errors: 0 }],
  ["/ts-proxy", { inFlight: 0, total: 0, errors: 0 }],
  ["/m3u8-proxy", { inFlight: 0, total: 0, errors: 0 }],
]);
const state = {
  totalRequests: 0,
  totalErrors: 0,
  activeRequests: 0,
  clientConnections: 0,
  startedAt,
};

function getEntry(endpoint) {
  if (!endpointStats.has(endpoint)) {
    endpointStats.set(endpoint, { inFlight: 0, total: 0, errors: 0 });
  }
  return endpointStats.get(endpoint);
}

function normalizeEndpoint(req) {
  const path = (req.path || req.originalUrl || "/").split("?")[0];

  if (path === "/fetch") return "/fetch";
  if (path === "/ts-proxy") return "/ts-proxy";
  if (path === "/m3u8-proxy") return "/m3u8-proxy";

  if (path.startsWith("/x/")) {
    const token = path.split("/").filter(Boolean).pop();
    const payload = decrypt(token);

    if (payload?.e === "fetch") return "/fetch";
    if (payload?.e === "ts-proxy") return "/ts-proxy";
    if (payload?.e === "m3u8-proxy") return "/m3u8-proxy";
  }

  return null;
}

export function attachStatsMiddleware(app, server) {
  app.use((req, res, next) => {
    let endpoint;
    try {
      endpoint = normalizeEndpoint(req);
    } catch {
      // malformed token, etc. — don't let this crash the request pipeline
      endpoint = null;
    }
    if (!endpoint) return next();

    const entry = getEntry(endpoint);

    state.totalRequests += 1;
    state.activeRequests += 1;
    entry.total += 1;
    entry.inFlight += 1;

    let finalized = false;
    const finalize = () => {
      if (finalized) return;
      finalized = true;

      state.activeRequests = Math.max(0, state.activeRequests - 1);
      entry.inFlight = Math.max(0, entry.inFlight - 1);

      // If the socket closed without the response ever finishing,
      // treat it as a failure regardless of whatever statusCode
      // happens to be sitting on res (often still the 200 default).
      const failed = !res.writableEnded || res.statusCode >= 400;
      if (failed) {
        state.totalErrors += 1;
        entry.errors += 1;
      }
    };

    res.once("finish", finalize);
    res.once("close", finalize);

    next();
  });

  let connectionCount = 0;
  server.on("connection", (socket) => {
    connectionCount += 1;
    state.clientConnections = connectionCount;

    socket.once("close", () => {
      connectionCount = Math.max(0, connectionCount - 1);
      state.clientConnections = connectionCount;
    });
  });
}

export function getStatsSnapshot() {
  const memory = process.memoryUsage();
  const byEndpoint = {};

  for (const [endpoint, entry] of endpointStats.entries()) {
    byEndpoint[endpoint] = { ...entry };
  }

  return {
    ok: true,
    timestamp: new Date().toISOString(),
    uptimeSeconds: Math.floor((Date.now() - state.startedAt) / 1000),
    activeRequests: state.activeRequests,
    clientConnections: state.clientConnections,
    totalRequests: state.totalRequests,
    totalErrors: state.totalErrors,
    byEndpoint,
    upstreamPools: {
      http: {
        activeSockets: 0,
        freeSockets: 0,
        queuedRequests: 0,
      },
      https: {
        activeSockets: 0,
        freeSockets: 0,
        queuedRequests: 0,
      },
    },
    memory: {
      rssMB: Number((memory.rss / (1024 * 1024)).toFixed(1)),
      heapUsedMB: Number((memory.heapUsed / (1024 * 1024)).toFixed(1)),
    },
  };
}
