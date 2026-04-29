#!/usr/bin/env node
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const args = parseArgs(process.argv.slice(2));
const loops = int(args["loops"] ?? "5");
const sleepSec = int(args["sleep-sec"] ?? "2");

for (let i = 0; i < loops; i++) {
  console.log(`restart loop ${i + 1}/${loops}`);
  await run("docker", ["compose", "restart", "engine"]);
  await sleep(sleepSec * 1000);
}

function run(cmd, argv) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, argv, { stdio: "inherit" });
    p.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} exited with ${code}`));
    });
  });
}

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

function normalizeValue(s) {
  if (typeof s === "string" && s.includes("=")) {
    return s.split("=").pop();
  }
  return s;
}

