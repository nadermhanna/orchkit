// The signal queue. The path a verdict travels from a finished agent run back
// to the engine. One interface, no service/driver split: the verbs have no
// logic between intent and bytes.

import { promises as fs } from "node:fs";
import path from "node:path";
import type { Signal } from "./types.js";

export interface SignalQueue {
  enqueue(signal: Signal): Promise<void>; // transport side: the CLI, an HTTP route
  peek(): Promise<Signal[]>; // tick side: read pending without removing
  ack(runId: string): Promise<void>; // tick side: remove — only after processing
}

// v1 driver: files. enqueue atomically writes <dir>/<runId>.json; peek scans
// the directory; ack deletes the file. Durability for free: a verdict emitted
// while the orchestrator is down is a file on disk at boot.
export function fileSignalQueue(dir: string): SignalQueue {
  return {
    async enqueue(signal) {
      await fs.mkdir(dir, { recursive: true });
      const file = path.join(dir, `${signal.runId}.json`);
      const tmp = `${file}.tmp`;
      await fs.writeFile(tmp, JSON.stringify(signal));
      await fs.rename(tmp, file); // atomic swap
    },
    async peek() {
      let names: string[];
      try {
        names = await fs.readdir(dir);
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code === "ENOENT") return [];
        throw e;
      }
      const signals: Signal[] = [];
      for (const name of names) {
        if (!name.endsWith(".json")) continue;
        try {
          const raw = await fs.readFile(path.join(dir, name), "utf8");
          signals.push(JSON.parse(raw) as Signal);
        } catch {
          // a half-read or vanished file is just not peeked this tick
        }
      }
      return signals;
    },
    async ack(runId) {
      await fs.rm(path.join(dir, `${runId}.json`), { force: true });
    },
  };
}

// In-memory driver backs tests.
export function memorySignalQueue(): SignalQueue {
  const pending = new Map<string, Signal>();
  return {
    async enqueue(signal) {
      pending.set(signal.runId, structuredClone(signal));
    },
    async peek() {
      return [...pending.values()].map((s) => structuredClone(s));
    },
    async ack(runId) {
      pending.delete(runId);
    },
  };
}
