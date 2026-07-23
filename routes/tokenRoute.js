import express from "express";
import { decrypt } from "../lib/tokenCrypto.js";
import { parseHeaders } from "../lib/headers.js";
import { runFetch, runTsProxy, runM3u8Proxy } from "../lib/handlers.js";

const router = express.Router();

// GET /x/:token
//
// A single encrypted token stands in for /fetch, /ts-proxy, or
// /m3u8-proxy plus their url/headers query params. Tokens are minted only
// by this server (see lib/tokenCrypto.js + lib/buildProxyUrl.js) whenever
// it rewrites a playlist, so a client can never forge one for an
// arbitrary URL - only replay/decrypt ones this server already handed out.
router.get("/x/:token", async (req, res) => {
  const payload = decrypt(req.params.token);
  if (!payload) {
    return res.status(400).json({ error: "Invalid or tampered token" });
  }

  const ctx = { url: payload.u, headers: parseHeaders(payload.h) };

  const endpoint = String(payload.e || "").replace(/^\//, "");

  switch (endpoint) {
    case "fetch":
      return runFetch(req, res, ctx);
    case "ts-proxy":
      return runTsProxy(req, res, ctx);
    case "m3u8-proxy":
      return runM3u8Proxy(req, res, ctx);
    default:
      return res.status(400).json({ error: "Unknown endpoint in token" });
  }
});

export default router;
