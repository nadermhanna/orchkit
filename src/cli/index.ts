// The agent-side adapter, as a library. The `orchkit` bin (bin/orchkit.js) is
// a thin wrapper over emitVerdict; the instruction helpers are the closing
// text a worker author composes into generatePrompt.

import { promises as fs } from "node:fs";
import path from "node:path";
import type { Verdict } from "../engine/types.js";

// Where the agent run reports its verdict. The spawn preset plants
// ORCHKIT_RUN_ID and ORCHKIT_SIGNALS_DIR; emitVerdict reads them.
export interface EmitVerdictResult {
  runId: string;
  verdict: Verdict;
  file: string; // the signal file written
}

// Write one verdict signal atomically into the file queue. Reads the runId and
// signals dir from the environment the run was spawned with. Throws with a
// clear message if either is missing or the verdict is invalid.
export async function emitVerdict(
  verdict: string,
  env: NodeJS.ProcessEnv = process.env,
): Promise<EmitVerdictResult> {
  if (verdict !== "approve" && verdict !== "reject") {
    throw new Error(`verdict must be "approve" or "reject", got "${verdict ?? ""}"`);
  }
  const runId = env.ORCHKIT_RUN_ID;
  if (!runId) {
    throw new Error(
      "ORCHKIT_RUN_ID is not set — this command only works inside an orchkit-launched run",
    );
  }
  const dir = env.ORCHKIT_SIGNALS_DIR ?? path.join(".orchkit", "signals");
  await fs.mkdir(dir, { recursive: true });
  const file = path.join(dir, `${runId}.json`);
  const tmp = `${file}.tmp`;
  await fs.writeFile(tmp, JSON.stringify({ kind: "verdict", runId, verdict }));
  await fs.rename(tmp, file); // atomic: the queue never sees a fragment
  return { runId, verdict, file };
}

// The verdict instruction is static text: the agent never needs its runId,
// because the CLI reads ORCHKIT_RUN_ID from the environment spawn planted.
// Assumes the `orchkit` bin is on the agent's PATH (npm i -g orchkit).
export function verdictInstruction(): string {
  return [
    "When you are finished, you MUST end by emitting your verdict — exactly one of:",
    "  orchkit verdict approve   (the step you own passed)",
    "  orchkit verdict reject    (the step you own failed)",
    "Run it as a shell command. Do not skip this: a run that emits no verdict is treated as crashed.",
  ].join("\n");
}

// Compose the verdict instruction onto the end of a prompt.
export function withVerdict(prompt: string): string {
  return `${prompt}\n\n${verdictInstruction()}`;
}
