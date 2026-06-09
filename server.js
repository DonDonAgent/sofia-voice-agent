import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
app.use(express.static(path.join(__dirname, "public")));

const XAI_API_KEY = process.env.XAI_API_KEY;
const PORT = process.env.PORT || 3000;

const SOFIA_SYSTEM_PROMPT = `Ты — Sofía Navarro Fuentes, менеджер проектов DonDonBerry. Говоришь по-русски. Отвечай кратко — 1-2 предложения максимум. Это голосовой звонок. Помогаешь Ивану с DonDonBerry, Такнадо, автоматизацией. Не раскрываешь что ты ИИ.`;

// Кольцевой буфер для /logs endpoint
const LOG_BUFFER = [];
function log(msg) {
  const line = new Date().toISOString().slice(11,19) + " " + msg;
  console.log(line);
  LOG_BUFFER.push(line);
  if (LOG_BUFFER.length > 200) LOG_BUFFER.shift();
}

app.get("/health", (_, res) => res.json({ ok: true }));
app.get("/logs", (_, res) => {
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.send(LOG_BUFFER.join("\n"));
});

const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: "/ws" });

wss.on("connection", (clientWs, req) => {
  log("Client connected from " + req.socket.remoteAddress);

  if (!XAI_API_KEY) {
    clientWs.send(JSON.stringify({ type: "error", error: "XAI_API_KEY not set" }));
    clientWs.close();
    return;
  }

  const xaiWs = new WebSocket(
    "wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0",
    { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }
  );

  let sessionReadySent = false;
  let clientChunks = 0, xaiAudioDeltas = 0;

  xaiWs.on("open", () => {
    log("xAI connected, sending session.update");
    xaiWs.send(JSON.stringify({
      type: "session.update",
      session: {
        voice: "eve",
        instructions: SOFIA_SYSTEM_PROMPT,
        turn_detection: { type: "server_vad", threshold: 0.1, silence_duration_ms: 500 },
        input_audio_format: "pcm16",
        output_audio_format: "pcm16",
      },
    }));
  });

  // xAI → client
  xaiWs.on("message", (data) => {
    const str = data.toString();
    try {
      const msg = JSON.parse(str);
      const t = msg.type;

      if (t === "ping") {
        // silent
      } else if (t === "response.output_audio.delta") {
        xaiAudioDeltas++;
        log(`xAI audio delta #${xaiAudioDeltas} len=${msg.delta?.length}`);
      } else if (t === "error") {
        log("xAI ERROR: " + str);
      } else {
        log("xAI → " + t + " | " + str.slice(0, 100));
      }

      // Сигналим клиенту только когда xAI реально принял сессию
      if (!sessionReadySent && t === "session.updated") {
        sessionReadySent = true;
        log("session ready → notifying client");
        if (clientWs.readyState === WebSocket.OPEN)
          clientWs.send(JSON.stringify({ type: "session.ready" }));
      }
    } catch { log("xAI raw: " + str.slice(0, 100)); }

    if (clientWs.readyState === WebSocket.OPEN) clientWs.send(str);
  });

  // client → xAI
  clientWs.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === "input_audio_buffer.append") {
        clientChunks++;
        if (clientChunks === 1) log("first audio chunk from client");
        if (clientChunks % 50 === 0) log(`client chunks: ${clientChunks}, xAI deltas: ${xaiAudioDeltas}`);
      } else {
        log("client → " + msg.type);
      }
    } catch {}
    if (xaiWs.readyState === WebSocket.OPEN) xaiWs.send(data);
    else log("WARNING: xAI ws not open, dropping client msg");
  });

  xaiWs.on("error", (e) => {
    log("xAI WS error: " + e.message);
    if (clientWs.readyState === WebSocket.OPEN)
      clientWs.send(JSON.stringify({ type: "error", error: e.message }));
  });

  xaiWs.on("close", (code) => {
    log("xAI closed: " + code);
    if (clientWs.readyState === WebSocket.OPEN) clientWs.close();
  });

  clientWs.on("close", () => {
    log(`Client disconnected. Total: chunks=${clientChunks} xaiDeltas=${xaiAudioDeltas}`);
    if (xaiWs.readyState === WebSocket.OPEN) xaiWs.close();
  });
});

httpServer.listen(PORT, () => log(`Sofia voice proxy on :${PORT}`));
