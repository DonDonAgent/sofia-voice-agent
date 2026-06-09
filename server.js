import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
app.use(express.static(path.join(__dirname, "public")));

const XAI_API_KEY = process.env.XAI_API_KEY;
const KB_URL = process.env.KB_URL;
const PORT = process.env.PORT || 3000;

const SOFIA_SYSTEM_PROMPT = `Ты — Sofía Navarro Fuentes, менеджер проектов DonDonBerry. Говоришь по-русски, живо и кратко — 1-2 предложения. Это голосовой звонок с Иваном.

ВАЖНО: Когда нужны данные — СРАЗУ говори "Сейчас проверю..." и вызывай инструмент. Не придумывай — ищи реальные данные.

Помогаешь Ивану с DonDonBerry, Такнадо, автоматизацией, маркетингом. Ты знаешь все его проекты. Не раскрываешь что ты ИИ.`;

const TOOLS = [
  { type: "function", name: "get_todos", description: "Получить список задач Ивана", parameters: { type: "object", properties: {}, required: [] } },
  { type: "function", name: "get_context", description: "Получить контекст последней сессии", parameters: { type: "object", properties: {}, required: [] } },
  { type: "function", name: "search_memory", description: "Поиск по памяти Ивана", parameters: { type: "object", properties: { query: { type: "string", description: "Поисковый запрос" } }, required: ["query"] } },
  { type: "function", name: "get_project", description: "Получить детали проекта", parameters: { type: "object", properties: { name: { type: "string", description: "Название проекта" } }, required: ["name"] } }
];

const LOG_BUFFER = [];
function log(msg) {
  const line = new Date().toISOString().slice(11,19) + " " + msg;
  console.log(line);
  LOG_BUFFER.push(line);
  if (LOG_BUFFER.length > 200) LOG_BUFFER.shift();
}

async function callKB(functionName, args) {
  if (!KB_URL) return `KB not configured. Function: ${functionName}`;
  try {
    const resp = await fetch(`${KB_URL}/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ function_name: functionName, args }),
      signal: AbortSignal.timeout(5000)
    });
    const data = await resp.json();
    return data.result || data.error || "No data";
  } catch (e) {
    log(`KB call error: ${e.message}`);
    return `Error: ${e.message}`;
  }
}

app.get("/health", (_, res) => res.json({ ok: true, kb: !!KB_URL }));
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
  let xaiAudioDeltas = 0, audioReadyCount = 0;

  xaiWs.on("open", () => {
    log("xAI connected, sending session.update");
    xaiWs.send(JSON.stringify({
      type: "session.update",
      session: {
        voice: "eve",
        instructions: SOFIA_SYSTEM_PROMPT,
        turn_detection: { type: "server_vad", threshold: 0.05, silence_duration_ms: 500 },
        input_audio_format: "pcm16",
        output_audio_format: "pcm16",
        input_audio_transcription: { model: "grok-3" },
        tools: TOOLS,
        tool_choice: "auto",
      },
    }));
  });

  xaiWs.on("message", async (data) => {
    const str = data.toString();
    let msg;
    try { msg = JSON.parse(str); } catch { log("xAI raw: " + str.slice(0, 100)); return; }

    const t = msg.type;

    if (t === "ping") {
      // silent
    } else if (t === "response.output_audio.delta") {
      xaiAudioDeltas++;
      if (xaiAudioDeltas <= 3 || xaiAudioDeltas % 10 === 0)
        log(`xAI audio delta #${xaiAudioDeltas}`);
    } else if (t === "error") {
      log("xAI ERROR: " + str);
    } else if (t === "conversation.item.input_audio_transcription.completed") {
      log(`TRANSCRIPT: \"${msg.transcript}\"`);
    } else if (t === "response.output_audio_transcript.done") {
      log(`SOFIA SAID: \"${msg.transcript}\"`);
    } else if (t === "response.function_call_arguments.done") {
      const fnName = msg.name;
      const callId = msg.call_id;
      let args = {};
      try { args = JSON.parse(msg.arguments || "{}"); } catch {}
      log(`tool call: ${fnName}(${JSON.stringify(args)})`);
      const result = await callKB(fnName, args);
      log(`tool result: ${result.slice(0, 120)}`);
      if (xaiWs.readyState === WebSocket.OPEN) {
        xaiWs.send(JSON.stringify({ type: "conversation.item.create", item: { type: "function_call_output", call_id: callId, output: result } }));
        xaiWs.send(JSON.stringify({ type: "response.create", response: { modalities: ["audio", "text"] } }));
      }
    } else {
      log("xAI → " + t + " | " + str.slice(0, 80));
    }

    if (!sessionReadySent && t === "session.updated") {
      sessionReadySent = true;
      log("session ready → response.create (greeting)");
      xaiWs.send(JSON.stringify({ type: "response.create", response: { modalities: ["audio", "text"] } }));
      if (clientWs.readyState === WebSocket.OPEN)
        clientWs.send(JSON.stringify({ type: "session.ready" }));
    }

    if (clientWs.readyState === WebSocket.OPEN) clientWs.send(str);
  });

  clientWs.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.type === "audio.ready") {
        audioReadyCount++;
        const audioB64 = msg.audio;
        const kbLen = audioB64 ? Math.round(audioB64.length / 1024) : 0;
        log(`audio.ready #${audioReadyCount} ${kbLen}KB → conversation.item.create`);
        if (xaiWs.readyState === WebSocket.OPEN && audioB64) {
          xaiWs.send(JSON.stringify({
            type: "conversation.item.create",
            item: {
              type: "message",
              role: "user",
              content: [{ type: "input_audio", audio: audioB64 }]
            }
          }));
          xaiWs.send(JSON.stringify({ type: "response.create", response: { modalities: ["audio", "text"] } }));
        }
        return;
      } else {
        log("client → " + msg.type);
      }
    } catch {}
    if (xaiWs.readyState === WebSocket.OPEN) xaiWs.send(data);
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
    log(`Client disconnected. audio.ready=${audioReadyCount} xaiDeltas=${xaiAudioDeltas}`);
    if (xaiWs.readyState === WebSocket.OPEN) xaiWs.close();
  });
});

httpServer.listen(PORT, () => log(`Sofia voice proxy :${PORT} | KB: ${KB_URL || "not set"}`));
