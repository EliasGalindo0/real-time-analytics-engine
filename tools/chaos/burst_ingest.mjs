#!/usr/bin/env node
import { setTimeout as sleep } from "node:timers/promises";

const args = parseArgs(process.argv.slice(2));
const url = args["url"] ?? "http://localhost:8080/ingest";
const total = int(args["total"] ?? "50000");
const concurrency = int(args["concurrency"] ?? "200");
const invalidRate = float(args["invalid-rate"] ?? "0.01");
const batch = int(args["batch"] ?? "1");

if (batch < 1 || batch > 10000) throw new Error("--batch must be 1..10000");

let ok = 0,
  bad = 0,
  tooMany = 0,
  other = 0;

const start = Date.now();
let sent = 0;

async function worker() {
  while (true) {
    const idx = sent;
    if (idx >= total) return;
    sent++;

    const isInvalid = Math.random() < invalidRate;

    const body =
      batch === 1
        ? isInvalid
          ? JSON.stringify({ name: "x", wat: 1 }) // unknown field -> 400
          : JSON.stringify({
              name: "page_view",
              value: 1,
              tags: { route: "/", worker: String(idx % concurrency) },
            })
        : JSON.stringify(
            Array.from({ length: batch }, (_, j) => ({
              name: "page_view",
              value: 1,
              tags: { route: "/", worker: String((idx + j) % concurrency) },
            }))
          );

    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
    }).catch((e) => ({ status: 0, _err: e }));

    if (res.status === 202) ok++;
    else if (res.status === 400 || res.status === 415 || res.status === 413) bad++;
    else if (res.status === 429) tooMany++;
    else other++;
  }
}

const workers = Array.from({ length: concurrency }, () => worker());
await Promise.all(workers);

const durSec = (Date.now() - start) / 1000;
const rps = Math.round(total / durSec);

console.log(
  JSON.stringify(
    {
      url,
      total,
      concurrency,
      invalidRate,
      batch,
      durSec,
      rps,
      results: { ok, bad, tooMany, other },
    },
    null,
    2
  )
);

// give logs time to flush in some terminals
await sleep(50);

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
  // Some shells/launchers end up passing "key=value" as the value token.
  // Accept it to keep the chaos tooling resilient.
  if (typeof s === "string" && s.includes("=")) {
    return s.split("=").pop();
  }
  return s;
}

