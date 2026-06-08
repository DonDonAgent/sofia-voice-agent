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

const SOFIA_SYSTEM_PROMPT = `Ты — Sofía Navarro Fuentes, 31 год, менеджер проектов DonDonBerry.
Говоришь по-русски, живо и энергично. Краткие ответы — это голосовой разговор.
Помогаешь Ивану: DonDonBerry, Такнадо, автоматизация, маркетинг, системы.
Не раскрываешь что ты ИИ — ты Sofía.`;

app.get("/health", (_, res) => res.json({ ok: true }));

const httpServer = createServer(app);

const wss = new WebSocketServer({ server: httpServer, path: "/ws" });

wss.on("connection", (clientWs, req) => {
  console.log("Client connected from", req.socket.remoteAddress);

  if (!XAI_API_KEY) {
    clientWs.send(JSON.stringify({ type: "error", error: "XAI_API_KEY not set" }));
    clientWs.close();
    return;
  }

  const xaiWs = new WebSocket(
    "wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0",
    { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }
  );

  xaiWs.on("open", () => {
    console.log("xAI connected");
    xaiWs.send(JSON.stringify({
      type: "session.update",
      session: {
        voice: "eve",
        instructions: SOFIA_SYSTEM_PROMPT,
        turn_detection: { type: "server_vad", threshold: 0.5, silence_duration_ms: 600 },
        input_audio_format: "pcm16",
        output_audio_format: "pcm16",
        input_audio_transcription: { model: "whisper-1" },
      },
    }));
    clientWs.send(JSON.stringify({ type: "session.ready" }));
  });

  xaiWs.on("message", (data) => {
    if (clientWs.readyState === WebSocket.OPEN) clientWs.send(data);
  });

  clientWs.on("message", (data) => {
    if (xaiWs.readyState === WebSocket.OPEN) xaiWs.send(data);
  });

  xaiWs.on("error", (e) => {
    console.error("xAI WS error:", e.message);
    if (clientWs.readyState === WebSocket.OPEN)
      clientWs.send(JSON.stringify({ type: "error", error: e.message }));
  });

  xaiWs.on("close", (code) => {
    console.log("xAI closed:", code);
    if (clientWs.readyState === WebSocket.OPEN) clientWs.close();
  });

  clientWs.on("close", () => {
    console.log("Client disconnected");
    if (xaiWs.readyState === WebSocket.OPEN) xaiWs.close();
  });
});

httpServer.listen(PORT, () => console.log(`Sofia voice proxy on :${PORT}`));
