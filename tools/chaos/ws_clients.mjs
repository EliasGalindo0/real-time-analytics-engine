#!/usr/bin/env node
import { setTimeout as sleep } from "node:timers/promises";

const args = parseArgs(process.argv.slice(2));
const url = args["url"] ?? "ws://localhost:8080/ws";
const clients = int(args["clients"] ?? "200");
const slowRatio = float(args["slow-ratio"] ?? "0.3");
const durationSec = int(args["duration-sec"] ?? "30");

if (typeof WebSocket === "undefined") {
  console.error(
    "WebSocket is not available in this Node runtime. If you hit this, tell me and I'll switch the script to a bundled client."
  );
  process.exit(2);
}

let opened = 0,
  closed = 0,
  msgs = 0,
  errors = 0;

const sockets = [];
for (let i = 0; i < clients; i++) {
  const slow = Math.random() < slowRatio;
  const ws = new WebSocket(url);
  sockets.push(ws);

  ws.onopen = () => {
    opened++;
    // Keep connection alive; optionally don't read messages to simulate slow consumers.
    if (!slow) {
      ws.onmessage = () => {
        msgs++;
      };
    }
  };
  ws.onerror = () => {
    errors++;
  };
  ws.onclose = () => {
    closed++;
  };
}

await sleep(durationSec * 1000);
for (const ws of sockets) {
  try {
    ws.close();
  } catch {}
}

await sleep(500);
console.log(
  JSON.stringify(
    { url, clients, slowRatio, durationSec, opened, closed, msgs, errors },
    null,
    2
  )
);

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "true";
    out[k] = v;
  }
  return out;
}
function int(s) {
  s = normalizeValue(s);
  const n = Number.parseInt(s, 10);
  if (!Number.isFinite(n) || n < 0) throw new Error(`invalid int: ${s}`);
  return n;
}
function float(s) {
  s = normalizeValue(s);
  const n = Number.parseFloat(s);
  if (!Number.isFinite(n) || n < 0) throw new Error(`invalid float: ${s}`);
  return n;
}

function normalizeValue(s) {
  if (typeof s === "string" && s.includes("=")) {
    return s.split("=").pop();
  }
  return s;
}

