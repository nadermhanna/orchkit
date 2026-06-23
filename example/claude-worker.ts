// The real demo: a worker that spawns an autonomous Claude Code agent on a
// task, then waits for the agent to emit its own verdict via the `orchkit`
// CLI. This is the headline use case — an unattended `claude -p` run in a
// worktree, closing the loop itself.
//
// Requires the `claude` CLI on PATH and `npm i -g orchkit` so the agent can
// run `orchkit verdict approve|reject`.

import { defineWorker } from "../src/engine/index.js";
import { claudeCodeSpawn, claudeCodeReprobe } from "../src/claude-code/index.js";
import { withVerdict } from "../src/cli/index.js";

export interface ClaudeWorkerOptions {
  name?: string;
  cwd: string; // the worktree the agent works in
  signalsDir: string; // where the agent's `orchkit verdict` drops its signal
  logsDir: string; // per-run agent stdout/stderr
  // Decide the next unit of work and the task text for it. Return null when
  // there's nothing to do.
  nextTask: () => Promise<{ itemId: string; task: string } | null>;
}

// A worker that watches `nextTask`, launches a Claude agent on each task with
// the verdict instruction appended, and lets the agent report the outcome.
export function claudeWorker(opts: ClaudeWorkerOptions) {
  return defineWorker<readonly [], { task: string }>({
    name: opts.name ?? "claude",
    integrations: [],

    ingest: async () => {
      const next = await opts.nextTask();
      if (!next) return { launch: false };
      return { launch: true, itemId: next.itemId, args: { task: next.task } };
    },

    // Compose the verdict instruction onto the task so the agent knows to close
    // the loop. withVerdict assumes `orchkit` is on the agent's PATH.
    generatePrompt: ({ task }) => withVerdict(task),

    spawn: claudeCodeSpawn({
      cwd: opts.cwd,
      signalsDir: opts.signalsDir,
      logsDir: opts.logsDir,
    }),

    // Liveness off the {pid} descriptor — survives an orchestrator restart, so
    // a reboot re-adopts a still-running agent instead of spawning a twin.
    reprobe: claudeCodeReprobe,

    onStart: async () => {},
    onApprove: async () => {},
    onReject: async () => {},
  });
}
