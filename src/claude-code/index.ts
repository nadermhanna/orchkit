// Harness preset: a ready-made spawn for Claude Code. Launches a detached
// headless `claude -p` run, plants ORCHKIT_RUN_ID and ORCHKIT_SIGNALS_DIR in
// its environment, and returns the local liveness probe — the child's exit is
// observable directly.

import { spawn as spawnProcess } from "node:child_process";
import { openSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";

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
    let exited = false;
    child.on("exit", () => {
      exited = true;
    });
    child.on("error", () => {
      exited = true; // e.g. `claude` not on PATH — the run never lived
    });
    return {
      isAlive: async () => !exited,
    };
  };
}
