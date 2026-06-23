// The smallest possible worker: run a shell command, approve if it exits 0,
// reject otherwise. No external systems, no Claude, no integrations — just the
// Worker contract, end to end, so you can read how the engine drives a worker.
//
// Unlike a real agent run, the work IS the spawn here: the command runs to
// completion inside spawn, and we emit the verdict ourselves rather than
// asking an agent to. The engine's probe still observes liveness the same way.

import { spawn } from "node:child_process";
import { defineWorker } from "../src/engine/index.js";
import type { SignalQueue } from "../src/engine/index.js";

export interface ShellWorkerOptions {
  name?: string;
  // Decide the next unit of work. Return null when there's nothing to do.
  nextCommand: () => Promise<{ itemId: string; command: string } | null>;
  // The queue the engine reads — the worker emits its own verdict into it,
  // since there's no agent to run `orchkit verdict`.
  signals: SignalQueue;
}

// A worker that watches `nextCommand`, runs whatever it returns, and reports
// approve/reject based on the exit code.
export function shellWorker(opts: ShellWorkerOptions) {
  return defineWorker<readonly [], { command: string }>({
    name: opts.name ?? "shell",
    integrations: [],

    ingest: async () => {
      const next = await opts.nextCommand();
      if (!next) return { launch: false };
      return { launch: true, itemId: next.itemId, args: { command: next.command } };
    },

    generatePrompt: ({ command }) => command,

    spawn: async (command, { runId }) => {
      const child = spawn(command, { shell: true, detached: true, stdio: "ignore" });
      child.unref();
      child.on("exit", (code) => {
        void opts.signals.enqueue({
          kind: "verdict",
          runId,
          verdict: code === 0 ? "approve" : "reject",
        });
      });
      child.on("error", () => {
        void opts.signals.enqueue({ kind: "verdict", runId, verdict: "reject" });
      });
      return { descriptor: { pid: child.pid ?? -1 } };
    },

    // Liveness from the pid alone, so it holds across a restart. kill(pid, 0)
    // probes without signalling: it throws when the process is gone.
    reprobe: async (descriptor) => {
      const pid = (descriptor as { pid?: number }).pid;
      if (typeof pid !== "number" || pid < 0) return false;
      try {
        process.kill(pid, 0);
        return true;
      } catch (e) {
        return (e as NodeJS.ErrnoException).code === "EPERM";
      }
    },

    onStart: async () => {},
    onApprove: async () => {},
    onReject: async () => {},
  });
}
