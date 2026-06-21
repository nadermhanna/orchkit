// Persistence around the Store type. Two layers: a StoreService of lifecycle
// verbs the orchestrator calls, over a StoreDriver that is the swappable
// persistence seam.

import { promises as fs } from "node:fs";
import path from "node:path";
import type { Clock, Run, RunOutcome, Store, Verdict, WorkerState } from "./types.js";

// Service — the lifecycle verbs the orchestrator calls. Not CRUD: each verb
// names one engine moment and writes the whole WorkerState for it.
export interface StoreService {
  load(): Promise<Store>; // boot read; empty store if none
  recordLaunch(name: string, run: Run): Promise<void>;
  recordCompletion(name: string, runId: string, verdict: Verdict): Promise<void>;
  recordOrphaned(name: string, runId: string): Promise<void>;
}

// Driver — two methods over one aggregate, the worker. The driver IS the
// worker-repository.
export interface StoreDriver {
  getAll(): Promise<Store>; // reassemble the world on boot
  upsert(name: string, ws: WorkerState): Promise<void>; // persist one worker, atomically
}

const blankWorker = (): WorkerState => ({
  liveness: "dead",
  itemId: null,
  runs: [],
});

export function createStoreService(driver: StoreDriver, clock: Clock): StoreService {
  // read the worker (or a blank one), compute the next state, upsert it whole.
  async function mutateWorker(
    name: string,
    transition: (current: WorkerState) => WorkerState,
  ): Promise<void> {
    const store = await driver.getAll();
    const current = store[name] ?? blankWorker();
    await driver.upsert(name, transition(current));
  }

  // completion and orphaning are the same transition — close the open run,
  // free the station — differing only in the outcome written.
  function endRun(name: string, runId: string, outcome: Exclude<RunOutcome, null>) {
    return mutateWorker(name, (w) => ({
      ...w,
      liveness: "dead",
      itemId: null, // station freed; the run holds the history
      runs: w.runs.map((r) =>
        r.runId === runId && r.endedAt === null
          ? { ...r, endedAt: clock.now(), outcome }
          : r,
      ),
    }));
  }

  return {
    load: () => driver.getAll(),

    recordLaunch: (name, run) =>
      mutateWorker(name, (w) => ({
        ...w,
        liveness: "alive",
        itemId: run.itemId,
        runs: [...w.runs, run], // append-only
      })),

    recordCompletion: (name, runId, verdict) => endRun(name, runId, verdict),

    recordOrphaned: (name, runId) => endRun(name, runId, "orphaned"),
  };
}

// JSON-file driver: read-modify-write the whole file, made durable by
// temp-then-rename — the real file is only ever the complete old or complete
// new store, never a fragment.
export function jsonFileDriver(filePath: string): StoreDriver {
  const readAll = async (): Promise<Store> => {
    try {
      return JSON.parse(await fs.readFile(filePath, "utf8")) as Store;
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === "ENOENT") return {}; // first boot
      throw e;
    }
  };
  return {
    getAll: readAll,
    async upsert(name, ws) {
      const store = await readAll();
      store[name] = ws;
      await fs.mkdir(path.dirname(filePath), { recursive: true });
      const tmp = `${filePath}.tmp`;
      await fs.writeFile(tmp, JSON.stringify(store, null, 2));
      await fs.rename(tmp, filePath); // atomic swap
    },
  };
}

// In-memory driver backs tests; structuredClone enforces the copy-in/copy-out
// boundary that serialization gives the file driver for free.
export function memoryDriver(seed: Store = {}): StoreDriver {
  const state = structuredClone(seed);
  return {
    async getAll() {
      return structuredClone(state);
    },
    async upsert(name, ws) {
      state[name] = structuredClone(ws);
    },
  };
}
