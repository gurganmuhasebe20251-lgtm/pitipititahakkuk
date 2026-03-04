const express = require("express");
const bodyParser = require("body-parser");
const qrcode = require("qrcode-terminal");
const fs = require("fs");
const { Client, LocalAuth, MessageMedia } = require("whatsapp-web.js");

const app = express();
app.use(bodyParser.json({ limit: "50mb" }));

// ===============================
// CONFIG
// ===============================
const PORT = 3210;

// FastAPI main.py içinde /wa/ack endpoint’i var.
const ACK_URL = process.env.ACK_URL || "http://127.0.0.1:8787/wa/ack";

// “detached frame” için retry sayısı
const SEND_RETRIES = Number(process.env.SEND_RETRIES || 3);

// Mesaj arası minimum bekleme (global)
const GLOBAL_MIN_DELAY_MS = Number(process.env.GLOBAL_MIN_DELAY_MS || 1200);
const GLOBAL_MAX_DELAY_MS = Number(process.env.GLOBAL_MAX_DELAY_MS || 2600);

// Dosya başına bekleme defaultları (payload ile de override ediliyor)
const DEFAULT_PER_FILE_MIN_MS = Number(process.env.PER_FILE_MIN_MS || 8000);
const DEFAULT_PER_FILE_MAX_MS = Number(process.env.PER_FILE_MAX_MS || 15000);

// ===============================
// STATE
// ===============================
let WA_READY = false;
let LAST_ERR = "";
let LAST_ERR_AT = "";

// Gönderilen message_id -> meta eşlemesi
const MSG_META = new Map();

// Global queue: detached frame’i azaltır
let SEND_QUEUE = Promise.resolve();

function nowIso() {
  return new Date().toISOString();
}

function setLastErr(e) {
  LAST_ERR = String(e || "");
  LAST_ERR_AT = nowIso();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function rand(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}
function normalizeTR(phone) {
  let p = (phone || "").toString().trim().replace(/\s+/g, "").replace(/-/g, "");
  if (!p) return "";
  if (p.startsWith("+")) p = p.substring(1);
  if (p.startsWith("00")) p = p.substring(2);
  if (p.startsWith("0") && p.length >= 10) p = "9" + p; // 05.. -> 905..
  if (p.startsWith("5") && p.length === 10) p = "90" + p; // 5.. -> 90 5..
  return p;
}

// ACK text mapping
function ackText(ack) {
  if (ack === -1) return "FAILED";
  if (ack === 0) return "PENDING";
  if (ack === 1) return "SENT";
  if (ack === 2) return "DELIVERED";
  if (ack === 3) return "READ";
  return String(ack);
}

async function postAck(payload) {
  try {
    await fetch(ACK_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    console.log("ACK_POST_FAIL:", String(e));
  }
}

// ===============================
// WHATSAPP CLIENT
// ===============================
let client = null;

function buildClient() {
  return new Client({
    authStrategy: new LocalAuth({ clientId: "pitipiti_mukellef_bildirimi" }),
    puppeteer: {
      headless: false, // ilk kurulumda false
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    },
  });
}

async function reinitClient(reason) {
  try {
    console.log("WA REINIT start:", reason || "-");
    WA_READY = false;

    if (client) {
      try { await client.destroy(); } catch (_) {}
    }

    client = buildClient();
    wireClientEvents(client);
    client.initialize();

    await sleep(1200);
  } catch (e) {
    setLastErr(e);
    console.log("WA REINIT FAIL:", String(e));
  }
}

function wireClientEvents(c) {
  c.on("qr", (qr) => {
    console.log("QR KODU WHATSAPP > BAĞLI CİHAZLAR > CİHAZ BAĞLA ile okut:");
    qrcode.generate(qr, { small: true });
  });

  c.on("ready", () => {
    WA_READY = true;
    LAST_ERR = "";
    LAST_ERR_AT = "";
    console.log("WhatsApp Bağlantısı Hazır!");
  });

  c.on("disconnected", (reason) => {
    WA_READY = false;
    setLastErr("WA_DISCONNECTED: " + reason);
    console.log("WhatsApp bağlantısı koptu:", reason);
  });

  c.on("message_ack", async (msg, ack) => {
    try {
      const mid = msg?.id?._serialized || "";
      const meta = MSG_META.get(mid) || {};
      const to = meta.to || (msg?.to ? String(msg.to).replace("@c.us", "") : "");

      await postAck({
        event: "ack",
        message_id: mid,
        ack: Number(ack),
        ack_text: ackText(Number(ack)),
        to: to || "",
        company_id: Number(meta.company_id || 0),
        company_name: meta.company_name || "",
        kind: meta.kind || "",
        ts: nowIso(),
      });

      if (ack >= 2) {
        setTimeout(() => MSG_META.delete(mid), 60 * 60 * 1000);
      }
    } catch (e) {
      console.log("ACK_HANDLER_ERR:", String(e));
    }
  });
}

client = buildClient();
wireClientEvents(client);

// ===============================
// SAFE SEND HELPERS (queue + retry)
// ===============================
function enqueue(fn) {
  SEND_QUEUE = SEND_QUEUE.then(fn).catch((e) => {
    console.log("QUEUE_ERR:", String(e));
  });
  return SEND_QUEUE;
}

async function safeSendMessage(chatId, payload, opts, meta) {
  let lastErr = null;

  for (let attempt = 1; attempt <= SEND_RETRIES; attempt++) {
    try {
      if (!WA_READY) throw new Error("WA_NOT_READY");

      await sleep(rand(GLOBAL_MIN_DELAY_MS, GLOBAL_MAX_DELAY_MS));

      const resp = await client.sendMessage(chatId, payload, opts);
      const mid = resp?.id?._serialized || "";

      if (mid) {
        MSG_META.set(mid, { ...meta, ts: Date.now() });
        await postAck({
          event: "sent",
          message_id: mid,
          ack: 1,
          ack_text: "SENT",
          to: meta.to || "",
          company_id: Number(meta.company_id || 0),
          company_name: meta.company_name || "",
          kind: meta.kind || "",
          ts: nowIso(),
        });
      }

      return { ok: true, message_id: mid };
    } catch (e) {
      lastErr = e;
      const msg = String(e || "");
      setLastErr(msg);

      const isDetached =
        msg.includes("detached Frame") ||
        msg.includes("Execution context was destroyed") ||
        msg.includes("Target closed") ||
        msg.includes("Protocol error") ||
        msg.includes("Session closed");

      console.log(`SEND_FAIL attempt=${attempt}/${SEND_RETRIES}:`, msg);

      await postAck({
        event: "error",
        message_id: "",
        ack: 0,
        ack_text: "ERROR",
        to: meta.to || "",
        company_id: Number(meta.company_id || 0),
        company_name: meta.company_name || "",
        kind: meta.kind || "",
        error: msg,
        ts: nowIso(),
      });

      if (isDetached) {
        await reinitClient("DETACHED_FRAME");
        await sleep(1500);
      } else {
        await sleep(800 + attempt * 600);
      }
    }
  }

  return { ok: false, error: String(lastErr || "SEND_FAILED") };
}

// ===============================
// ROUTES
// ===============================
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    wa_ready: WA_READY,
    last_error: LAST_ERR,
    last_error_at: LAST_ERR_AT,
    ack_url: ACK_URL,
  });
});

app.post("/send-text", async (req, res) => {
  try {
    if (!WA_READY) return res.status(503).json({ ok: false, error: "WA_NOT_READY" });

    const to = normalizeTR(req.body.to);
    const message = (req.body.message || "").toString();
    const company_id = Number(req.body.company_id || 0);
    const company_name = (req.body.company_name || "").toString();

    if (!to || !message) return res.status(400).json({ ok: false, error: "MISSING_TO_OR_MESSAGE" });

    const chatId = `${to}@c.us`;
    const meta = { company_id, company_name, to, kind: "text" };

    const out = await enqueue(async () => {
      return await safeSendMessage(chatId, message, undefined, meta);
    });

    if (!out.ok) return res.status(500).json({ ok: false, error: out.error || "SEND_FAILED" });

    return res.json({ ok: true, to, message_id: out.message_id || null });
  } catch (e) {
    setLastErr(e);
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post("/send-batch", async (req, res) => {
  try {
    if (!WA_READY) return res.status(503).json({ ok: false, error: "WA_NOT_READY" });

    const to = normalizeTR(req.body.to);
    const message = (req.body.message || "").toString();
    const files = Array.isArray(req.body.files) ? req.body.files : [];

    const company_id = Number(req.body.company_id || 0);
    const company_name = (req.body.company_name || "").toString();

    if (!to) return res.status(400).json({ ok: false, error: "MISSING_TO" });

    const perFileMinMs = Number(req.body.perFileMinMs || DEFAULT_PER_FILE_MIN_MS);
    const perFileMaxMs = Number(req.body.perFileMaxMs || DEFAULT_PER_FILE_MAX_MS);

    const chatId = `${to}@c.us`;
    const results = [];

    if (message) {
      const meta = { company_id, company_name, to, kind: "text" };
      const r = await enqueue(async () => await safeSendMessage(chatId, message, undefined, meta));
      results.push({ type: "text", ok: r.ok, message_id: r.message_id || null, error: r.ok ? null : r.error });
    }

    for (let i = 0; i < files.length; i++) {
      const fp = (files[i].path || "").toString();
      const caption = (files[i].caption || "").toString();

      if (!fp) continue;

      if (!fs.existsSync(fp)) {
        results.push({ type: "media", path: fp, ok: false, error: "FILE_NOT_FOUND" });
        await postAck({
          event: "error",
          message_id: "",
          ack: 0,
          ack_text: "FILE_NOT_FOUND",
          to,
          company_id,
          company_name,
          kind: "media",
          error: "FILE_NOT_FOUND: " + fp,
          ts: nowIso(),
        });
        continue;
      }

      await sleep(rand(perFileMinMs, perFileMaxMs));

      const media = MessageMedia.fromFilePath(fp);
      const meta = { company_id, company_name, to, kind: "media" };

      const r = await enqueue(async () => {
        return await safeSendMessage(chatId, media, caption ? { caption } : undefined, meta);
      });

      results.push({
        type: "media",
        path: fp,
        ok: r.ok,
        message_id: r.message_id || null,
        error: r.ok ? null : r.error,
      });
    }

    return res.json({ ok: true, to, results });
  } catch (e) {
    setLastErr(e);
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

app.listen(PORT, () => console.log(`WA Sender listening on http://127.0.0.1:${PORT}`));
client.initialize();
