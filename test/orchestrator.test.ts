import { describe, expect, it, vi } from "vitest";
import { createOrchestrator } from "../src/engine/orchestrator.js";
import { createStoreService, memoryDriver } from "../src/engine/store.js";
import { memorySignalQueue } from "../src/engine/signals.js";
import type { StoreService } from "../src/engine/store.js";
import type {
  AnyWorker,
  IngestionResult,
  Integration,
  Store,
} from "../src/engine/types.js";

const clock = { now: () => 42 };

interface Harness {
  worker: AnyWorker;
  alive: { value: boolean | (() => boolean) };
  spawned: string[]; // prompts, in launch order
  hooks: string[]; // hook firing order
  decisions: IngestionResult<{ ticket: string }>[]; // consumed FIFO; default no-launch
  hookStores: Store[]; // store snapshot each verdict hook saw
}

function makeWorker(name = "worker"): Harness {
  const worker: Harness = {
    alive: { value: true },
    spawned: [],
    hooks: [],
    decisions: [],
    hookStores: [],
    worker: undefined as unknown as AnyWorker,
  };
  const noopIntegration: Integration = { name: "noop", helpers: {} };
  worker.worker = {
    name,
    integrations: [noopIntegration],
    ingest: async () => worker.decisions.shift() ?? { launch: false },
    generatePrompt: (args: { ticket: string }) => `/work ${args.ticket}`,
    spawn: async (prompt: string) => {
      worker.spawned.push(prompt);
      return { descriptor: { worker: name } };
    },
    // Liveness is read off worker.alive (boolean or thunk), keyed by worker name in
    // the descriptor — same control the old isAlive closure gave the tests,
    // now routed the way the engine reprobes for real.
    reprobe: async () =>
      typeof worker.alive.value === "function" ? worker.alive.value() : worker.alive.value,
    onStart: async () => {
      worker.hooks.push("onStart");
    },
    onApprove: async (_int, store) => {
      worker.hooks.push("onApprove");
      worker.hookStores.push(store as Store);
    },
    onReject: async (_int, store) => {
      worker.hooks.push("onReject");
      worker.hookStores.push(store as Store);
    },
  } as AnyWorker;
  return worker;
}

function build(harnesses: Harness[], seed: Store = {}, storeOverride?: StoreService) {
  const store = storeOverride ?? createStoreService(memoryDriver(seed), clock);
  const signals = memorySignalQueue();
  let n = 0;
  const orchestrator = createOrchestrator(
    {
      workers: harnesses.map((harness) => harness.worker),
      store,
      signals,
      clock,
      mintRunId: () => `run-${++n}`,
      log: () => {},
    },
    { tickMs: 60_000 },
  );
  return { orchestrator, store, signals };
}

const launch = (ticket: string): IngestionResult<{ ticket: string }> => ({
  launch: true,
  args: { ticket },
  itemId: ticket,
});

describe("launch", () => {
  it("ingest -> generatePrompt -> spawn -> recordLaunch -> onStart", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"));
    const { orchestrator, store } = build([worker]);

    await orchestrator.tick();

    expect(worker.spawned).toEqual(["/work SQU-9"]);
    expect(worker.hooks).toEqual(["onStart"]);
    const ws = (await store.load())["worker"]!;
    expect(ws).toMatchObject({ liveness: "alive", itemId: "SQU-9" });
    expect(ws.runs[0]).toMatchObject({ runId: "run-1", itemId: "SQU-9", outcome: null });
  });

  it("a busy station is never re-ingested (registry gates ingest)", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"), launch("SQU-10"));
    const { orchestrator } = build([worker]);

    await orchestrator.tick();
    await orchestrator.tick(); // agent still alive — must not double-spawn

    expect(worker.spawned).toHaveLength(1);
  });

  it("a failed spawn records nothing and the item is re-claimed next tick", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"), launch("SQU-9"));
    let first = true;
    const originalSpawn = worker.worker.spawn;
    worker.worker.spawn = async (prompt, ctx) => {
      if (first) {
        first = false;
        throw new Error("boom");
      }
      return originalSpawn(prompt, ctx);
    };
    const { orchestrator, store } = build([worker]);

    await orchestrator.tick();
    expect((await store.load())["worker"]).toBeUndefined();

    await orchestrator.tick();
    expect(worker.spawned).toEqual(["/work SQU-9"]);
  });

  it("when recordLaunch throws, the station reads busy and the write retries next tick", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"), launch("TWIN"));
    const real = createStoreService(memoryDriver(), clock);
    let failures = 1;
    const flaky: StoreService = {
      ...real,
      recordLaunch: async (name, run) => {
        if (failures-- > 0) throw new Error("disk full");
        return real.recordLaunch(name, run);
      },
    };
    const { orchestrator, store } = build([worker], {}, flaky);

    await orchestrator.tick(); // spawn ok, record fails
    expect(worker.spawned).toHaveLength(1);
    expect(worker.hooks).toEqual([]); // onStart waits for the record

    await orchestrator.tick(); // retry lands; no twin spawned
    expect(worker.spawned).toHaveLength(1);
    expect(worker.hooks).toEqual(["onStart"]);
    expect((await store.load())["worker"]!.runs).toHaveLength(1);
  });
});

describe("completion", () => {
  it("fires the verdict hook against the in-flight store, records, acks", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"));
    const { orchestrator, store, signals } = build([worker]);

    await orchestrator.tick();
    await signals.enqueue({ kind: "verdict", runId: "run-1", verdict: "approve" });
    await orchestrator.tick();

    expect(worker.hooks).toEqual(["onStart", "onApprove"]);
    // the hook saw the run still in flight
    expect(worker.hookStores[0]!["worker"]!.itemId).toBe("SQU-9");
    const ws = (await store.load())["worker"]!;
    expect(ws).toMatchObject({ liveness: "dead", itemId: null });
    expect(ws.runs[0]!.outcome).toBe("approve");
    expect(await signals.peek()).toEqual([]); // acked
  });

  it("a reject verdict fires onReject", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"));
    const { orchestrator, signals } = build([worker]);

    await orchestrator.tick();
    await signals.enqueue({ kind: "verdict", runId: "run-1", verdict: "reject" });
    await orchestrator.tick();

    expect(worker.hooks).toEqual(["onStart", "onReject"]);
  });

  it("an unknown runId is dropped (acked) and changes nothing", async () => {
    const worker = makeWorker();
    const { orchestrator, store, signals } = build([worker]);

    await signals.enqueue({ kind: "verdict", runId: "ghost", verdict: "approve" });
    await orchestrator.tick();

    expect(await signals.peek()).toEqual([]);
    expect(worker.hooks).toEqual([]);
    expect(await store.load()).toEqual({});
  });

  it("a throwing hook changes nothing in the record", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"));
    worker.worker.onApprove = async () => {
      throw new Error("linear is down");
    };
    const { orchestrator, store, signals } = build([worker]);

    await orchestrator.tick();
    await signals.enqueue({ kind: "verdict", runId: "run-1", verdict: "approve" });
    await orchestrator.tick();

    const ws = (await store.load())["worker"]!;
    expect(ws.runs[0]!.outcome).toBe("approve"); // recorded anyway
    expect(await signals.peek()).toEqual([]); // acked anyway
  });
});

describe("orphaning", () => {
  it("a dead run with no verdict is orphaned and the station freed", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"));
    const { orchestrator, store } = build([worker]);

    await orchestrator.tick();
    worker.alive.value = false;
    await orchestrator.tick();

    const ws = (await store.load())["worker"]!;
    expect(ws.runs[0]!.outcome).toBe("orphaned");
    expect(ws).toMatchObject({ liveness: "dead", itemId: null });
    expect(worker.hooks).toEqual(["onStart"]); // no verdict hook for an orphan
  });

  it("the verdict wins the race: a dead run whose signal landed completes normally", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"));
    const { orchestrator, store, signals } = build([worker]);

    await orchestrator.tick();
    worker.alive.value = false;
    // signal lands after this tick's resolve step would have read the queue:
    // simulate by enqueueing now and letting the probe's final peek find it
    await signals.enqueue({ kind: "verdict", runId: "run-1", verdict: "approve" });
    await orchestrator.tick();

    expect((await store.load())["worker"]!.runs[0]!.outcome).toBe("approve");
    expect(worker.hooks).toContain("onApprove");
  });

  it("a throwing probe means unknown — the run keeps waiting", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"));
    worker.alive.value = () => {
      throw new Error("probe flake");
    };
    const { orchestrator, store } = build([worker]);

    await orchestrator.tick();
    await orchestrator.tick();

    expect((await store.load())["worker"]!.runs[0]!.outcome).toBeNull(); // still open
  });
});

describe("error containment", () => {
  it("one worker's throwing ingest does not stop another's launch in the same tick", async () => {
    const bad = makeWorker("bad");
    bad.worker.ingest = async () => {
      throw new Error("ingest boom");
    };
    const good = makeWorker("good");
    good.decisions.push(launch("SQU-1"));
    const { orchestrator } = build([bad, good]);

    await orchestrator.tick();

    expect(good.spawned).toHaveLength(1);
  });
});

describe("boot", () => {
  it("drains recovered verdicts, re-adopts live runs, orphans only the dead", async () => {
    const worker = makeWorker();
    const seed: Store = {
      worker: {
        liveness: "alive",
        itemId: "SQU-9",
        runs: [
          { itemId: "SQU-8", runId: "done-run", startedAt: 1, endedAt: 2, outcome: "approve", descriptor: { worker: "worker" } },
          { itemId: "SQU-9", runId: "open-with-verdict", startedAt: 3, endedAt: null, outcome: null, descriptor: { worker: "worker" } },
        ],
      },
      // live and dead seed IDENTICAL store rows: one open run with a descriptor.
      // The only thing that differs is what their reprobe says at boot (set on
      // the workers below) — that, not the store, decides re-adopt vs. orphan.
      live: {
        liveness: "alive",
        itemId: "SQU-6",
        runs: [{ itemId: "SQU-6", runId: "open-alive", startedAt: 3, endedAt: null, outcome: null, descriptor: { worker: "live" } }],
      },
      dead: {
        liveness: "alive",
        itemId: "SQU-7",
        runs: [{ itemId: "SQU-7", runId: "open-no-verdict", startedAt: 3, endedAt: null, outcome: null, descriptor: { worker: "dead" } }],
      },
    };
    const liveWorker = makeWorker("live"); // reprobe -> alive (default): run is re-adopted
    const deadWorker = makeWorker("dead");
    deadWorker.alive.value = false; // reprobe -> dead: run is orphaned
    const { orchestrator, store, signals } = build([worker, liveWorker, deadWorker], seed);
    await signals.enqueue({ kind: "verdict", runId: "open-with-verdict", verdict: "approve" });

    await orchestrator.start();
    await orchestrator.stop();

    const after = await store.load();
    expect(after["worker"]!.runs.find((r) => r.runId === "open-with-verdict")!.outcome).toBe("approve");
    expect(worker.hooks).toContain("onApprove"); // recovered verdict completes normally
    expect(after["live"]!.runs[0]!.outcome).toBeNull(); // still alive — re-adopted, left open
    expect(after["dead"]!.runs[0]!.outcome).toBe("orphaned"); // dead — orphaned
    expect(await signals.peek()).toEqual([]);
  });

  it("a re-adopted run gates ingest and is not re-spawned", async () => {
    const worker = makeWorker();
    const seed: Store = {
      worker: {
        liveness: "alive",
        itemId: "SQU-9",
        runs: [{ itemId: "SQU-9", runId: "open-alive", startedAt: 3, endedAt: null, outcome: null, descriptor: { worker: "worker" } }],
      },
    };
    // ingest would launch if the station read idle — it must not, because the
    // live run was re-adopted. This is the duplicate-spawn-on-restart guard.
    worker.decisions.push(launch("SQU-9"));
    const { orchestrator, store } = build([worker], seed);

    await orchestrator.start();
    await orchestrator.stop();

    expect(worker.spawned).toEqual([]); // no twin onto the worktree
    expect((await store.load())["worker"]!.runs).toHaveLength(1);
  });

  it("a run whose reprobe is unknown at boot is not orphaned", async () => {
    const worker = makeWorker();
    // reprobe throws -> "unknown". Unknown is not dead: the run stays open to
    // be re-probed, never blind-orphaned on a flaky probe at boot.
    worker.alive.value = () => {
      throw new Error("probe flake");
    };
    const seed: Store = {
      worker: {
        liveness: "alive",
        itemId: "SQU-9",
        runs: [{ itemId: "SQU-9", runId: "open-unknown", startedAt: 3, endedAt: null, outcome: null, descriptor: { worker: "worker" } }],
      },
    };
    const { orchestrator, store } = build([worker], seed);

    await orchestrator.start();
    await orchestrator.stop();

    expect((await store.load())["worker"]!.runs[0]!.outcome).toBeNull(); // left open
  });
});

describe("loop safety lives in ingest, not the engine", () => {
  it("the engine relaunches as long as ingest says launch — the cap is the worker's", async () => {
    const worker = makeWorker();
    worker.decisions.push(launch("SQU-9"), launch("SQU-9"), launch("SQU-9"));
    worker.alive.value = false; // every run dies instantly
    const { orchestrator, store } = build([worker]);

    await orchestrator.tick(); // launch 1
    await orchestrator.tick(); // orphan 1, launch 2
    await orchestrator.tick(); // orphan 2, launch 3

    const ws = (await store.load())["worker"]!;
    expect(worker.spawned).toHaveLength(3);
    expect(ws.runs.filter((r) => r.outcome === "orphaned")).toHaveLength(2);
  });
});
