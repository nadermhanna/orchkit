// Harness preset: a ready-made spawn for Claude Code. Launches a detached
// headless `claude -p` run, plants ORCHKIT_RUN_ID and ORCHKIT_SIGNALS_DIR in
// its environment, and returns a {pid} descriptor. Liveness is checked
// separately via claudeCodeReprobe(descriptor), which works from the pid alone
// — so a restarted orchestrator can still tell whether the detached run lives.

import { spawn as spawnProcess } from "node:child_process";
import { openSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import type { JSONValue } from "../engine/types.js";

// The descriptor this preset's spawn emits and its reprobe consumes — the
// private contract between the two. Just the OS pid of the detached run. The
// index signature keeps it assignable to JSONValue (the engine persists it
// verbatim and never reads it).
type ProcessDescriptor = { pid: number } & { [key: string]: JSONValue };

const isProcessDescriptor = (d: JSONValue): d is ProcessDescriptor =>
  typeof d === "object" && d !== null && !Array.isArray(d) && typeof d.pid === "number";

// Liveness from the pid alone — no closure over the child, so it answers the
// same across restarts. `kill(pid, 0)` sends no signal; it just probes: it
// throws ESRCH when the process is gone, EPERM when it lives but we can't
// signal it (alive). Both the in-flight probe and boot recovery call this.
export async function claudeCodeReprobe(descriptor: JSONValue): Promise<boolean> {
  if (!isProcessDescriptor(descriptor)) return false; // unrecognized — treat as gone
  try {
    process.kill(descriptor.pid, 0);
    return true;
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "EPERM") return true; // alive, not ours to signal
    return false; // ESRCH and anything else — gone
  }
}

export interface ClaudeCodeSpawnOptions {
  signalsDir: string; // where the orchkit CLI will drop the verdict
  logsDir: string; // per-run stdout/stderr, <logsDir>/<runId>.log
  cwd?: string;
  env?: Record<string, string>; // extra env planted in the run (e.g. an API key)
}

export function claudeCodeSpawn(opts: ClaudeCodeSpawnOptions) {
  return async (prompt: string, ctx: { runId: string }) => {
    await fs.mkdir(opts.logsDir, { recursive: true });
    const logFd = openSync(path.join(opts.logsDir, `${ctx.runId}.log`), "a");
    // --dangerously-skip-permissions: the run is unattended, so the agent
    // gets full tool autonomy in its worktree (edit, git, subagents, MCP).
    const child = spawnProcess(
      "claude",
      ["-p", prompt, "--dangerously-skip-permissions"],
      {
        cwd: opts.cwd,
        detached: true,
        env: {
          ...process.env,
          ...opts.env,
          ORCHKIT_RUN_ID: ctx.runId,
          ORCHKIT_SIGNALS_DIR: path.resolve(opts.signalsDir),
        },
        stdio: ["ignore", logFd, logFd],
      },
    );
    child.unref();
    // The detached child outlives this process; liveness is the pid's, read
    // later by claudeCodeReprobe. A spawn that never got a pid (e.g. `claude`
    // not on PATH) reports -1 — reprobe will see it as gone immediately.
    const descriptor: ProcessDescriptor = { pid: child.pid ?? -1 };
    return { descriptor };
  };
}
