import { createServer } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");
const PORT = process.env.PORT || 3000;

const STRIP_HEADERS = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

const server = createServer(async (req, res) => {
  // Health check برای Out Plane
  if (req.url === "/healthz" || req.url === "/health") {
    res.statusCode = 200;
    res.setHeader("Content-Type", "text/plain");
    res.end("ok");
    return;
  }

  if (!TARGET_BASE) {
    res.statusCode = 500;
    res.end("Misconfigured: TARGET_DOMAIN is not set");
    return;
  }

  try {
    const targetUrl = TARGET_BASE + req.url;

    const headers = {};
    let clientIp = null;
    for (const key of Object.keys(req.headers)) {
      const k = key.toLowerCase();
      const v = req.headers[key];
      if (STRIP_HEADERS.has(k)) continue;
      if (k === "x-real-ip") { clientIp = v; continue; }
      if (k === "x-forwarded-for") { if (!clientIp) clientIp = v; continue; }
      headers[k] = Array.isArray(v) ? v.join(", ") : v;
    }
    if (clientIp) headers["x-forwarded-for"] = clientIp;

    const method = req.method;
    const hasBody = method !== "GET" && method !== "HEAD";

    const fetchOpts = { method, headers, redirect: "manual" };
    if (hasBody) {
      fetchOpts.body = Readable.toWeb(req);
      fetchOpts.duplex = "half";
    }

    const upstream = await fetch(targetUrl, fetchOpts);

    res.statusCode = upstream.status;
    for (const [k, v] of upstream.headers) {
      if (k.toLowerCase() === "transfer-encoding") continue;
      try { res.setHeader(k, v); } catch {}
    }

    if (upstream.body) {
      await pipeline(Readable.fromWeb(upstream.body), res).catch((err) => {
        if (err.code !== "ERR_STREAM_PREMATURE_CLOSE") {
          console.error("pipeline error:", err);
        }
      });
    } else {
      res.end();
    }
  } catch (err) {
    if (err.code === "ERR_STREAM_PREMATURE_CLOSE") return;

    console.error("relay error:", err);
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway: Tunnel Failed");
    }
  }
});

server.listen(PORT, () => {
  console.log(`XHTTP relay running on port ${PORT}`);
  if (TARGET_BASE) {
    console.log(`Forwarding to: ${TARGET_BASE}`);
  } else {
    console.warn("WARNING: TARGET_DOMAIN is not set");
  }
});

// Graceful shutdown برای Out Plane
const shutdown = () => {
  console.log("Shutting down...");
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10_000);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
