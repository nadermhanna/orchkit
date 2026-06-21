#!/usr/bin/env node
// The orchkit CLI — the agent-side adapter. A run ends by invoking
// `orchkit verdict approve|reject`. A thin wrapper over the compiled
// emitVerdict; it reads ORCHKIT_RUN_ID from its environment (planted by the
// spawn preset) and delivers the signal through the file queue at
// ORCHKIT_SIGNALS_DIR (default .orchkit/signals).

import { emitVerdict } from "../dist/cli/index.js";

const [verb, verdict] = process.argv.slice(2);

function die(message) {
  console.error(`orchkit: ${message}`);
  process.exit(1);
}

if (verb !== "verdict") {
  die(`unknown verb "${verb ?? ""}" — usage: orchkit verdict approve|reject`);
}

try {
  const { runId } = await emitVerdict(verdict);
  console.log(`orchkit: verdict "${verdict}" recorded for run ${runId}`);
} catch (e) {
  die(e instanceof Error ? e.message : String(e));
}
