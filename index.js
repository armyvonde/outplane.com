import { createServer } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");
const PORT = process.env.PORT || 3000;

if (!TARGET_BASE) {
  console.error("ERROR: TARGET_DOMAIN environment variable is not set.");
  process.exit(1);
}

// Headers that must be stripped before forwarding
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
  const targetUrl = TARGET_BASE + req.url;
  const method = req.method;

  // Build forwarded headers
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

  try {
    const fetchOpts = {
      method,
      headers,
      redirect: "manual",
    };

    // XHTTP uses POST for upstream data (client→server)
    // and GET for downstream data (server→client, chunked)
    if (method !== "GET" && method !== "HEAD") {
      fetchOpts.body = Readable.toWeb(req);
      fetchOpts.duplex = "half";
    }

    const upstream = await fetch(targetUrl, fetchOpts);

    res.statusCode = upstream.status;

    for (const [k, v] of upstream.headers) {
      const kl = k.toLowerCase();
      if (kl === "transfer-encoding") continue;
      try { res.setHeader(k, v); } catch {}
    }

    // Critical for XHTTP GET responses: flush immediately, keep connection open
    if (method === "GET" && upstream.body) {
      res.setHeader("transfer-encoding", "chunked");
      res.flushHeaders();

      await pipeline(
        Readable.fromWeb(upstream.body),
        res
      ).catch((err) => {
        if (err.code !== "ERR_STREAM_PREMATURE_CLOSE") {
          console.error("[GET pipeline error]", err.message);
        }
      });
    } else if (upstream.body) {
      await pipeline(
        Readable.fromWeb(upstream.body),
        res
      ).catch((err) => {
        if (err.code !== "ERR_STREAM_PREMATURE_CLOSE") {
          console.error("[POST pipeline error]", err.message);
        }
      });
    } else {
      res.end();
    }

  } catch (err) {
    if (err.code === "ERR_STREAM_PREMATURE_CLOSE") return;

    console.error("[relay error]", err.message);
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway");
    }
  }
});

// Disable timeouts — XHTTP GET connections are long-lived
server.keepAliveTimeout = 0;
server.headersTimeout = 0;
server.timeout = 0;

server.listen(PORT, () => {
  console.log(`XHTTP relay → ${TARGET_BASE} (port ${PORT})`);
});
