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

function makeWorker(name = "w"): Harness {
  const h: Harness = {
    alive: { value: true },
    spawned: [],
    hooks: [],
    decisions: [],
    hookStores: [],
    worker: undefined as unknown as AnyWorker,
  };
  const noopIntegration: Integration = { name: "noop", helpers: {} };
  h.worker = {
    name,
    integrations: [noopIntegration],
    ingest: async () => h.decisions.shift() ?? { launch: false },
    generatePrompt: (args: { ticket: string }) => `/work ${args.ticket}`,
    spawn: async (prompt: string) => {
      h.spawned.push(prompt);
      return {
        isAlive: async () =>
          typeof h.alive.value === "function" ? h.alive.value() : h.alive.value,
      };
    },
    onStart: async () => {
      h.hooks.push("onStart");
    },
    onApprove: async (_int, store) => {
      h.hooks.push("onApprove");
      h.hookStores.push(store as Store);
    },
    onReject: async (_int, store) => {
      h.hooks.push("onReject");
      h.hookStores.push(store as Store);
    },
  } as AnyWorker;
  return h;
}

function build(harnesses: Harness[], seed: Store = {}, storeOverride?: StoreService) {
  const store = storeOverride ?? createStoreService(memoryDriver(seed), clock);
  const signals = memorySignalQueue();
  let n = 0;
  const orchestrator = createOrchestrator(
    {
      workers: harnesses.map((h) => h.worker),
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
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"));
    const { orchestrator, store } = build([h]);

    await orchestrator.tick();

    expect(h.spawned).toEqual(["/work SQU-9"]);
    expect(h.hooks).toEqual(["onStart"]);
    const ws = (await store.load())["w"]!;
    expect(ws).toMatchObject({ liveness: "alive", itemId: "SQU-9" });
    expect(ws.runs[0]).toMatchObject({ runId: "run-1", itemId: "SQU-9", outcome: null });
  });

  it("a busy station is never re-ingested (registry gates ingest)", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"), launch("SQU-10"));
    const { orchestrator } = build([h]);

    await orchestrator.tick();
    await orchestrator.tick(); // agent still alive — must not double-spawn

    expect(h.spawned).toHaveLength(1);
  });

  it("a failed spawn records nothing and the item is re-claimed next tick", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"), launch("SQU-9"));
    let first = true;
    const originalSpawn = h.worker.spawn;
    h.worker.spawn = async (prompt, ctx) => {
      if (first) {
        first = false;
        throw new Error("boom");
      }
      return originalSpawn(prompt, ctx);
    };
    const { orchestrator, store } = build([h]);

    await orchestrator.tick();
    expect((await store.load())["w"]).toBeUndefined();

    await orchestrator.tick();
    expect(h.spawned).toEqual(["/work SQU-9"]);
  });

  it("when recordLaunch throws, the station reads busy and the write retries next tick", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"), launch("TWIN"));
    const real = createStoreService(memoryDriver(), clock);
    let failures = 1;
    const flaky: StoreService = {
      ...real,
      recordLaunch: async (name, run) => {
        if (failures-- > 0) throw new Error("disk full");
        return real.recordLaunch(name, run);
      },
    };
    const { orchestrator, store } = build([h], {}, flaky);

    await orchestrator.tick(); // spawn ok, record fails
    expect(h.spawned).toHaveLength(1);
    expect(h.hooks).toEqual([]); // onStart waits for the record

    await orchestrator.tick(); // retry lands; no twin spawned
    expect(h.spawned).toHaveLength(1);
    expect(h.hooks).toEqual(["onStart"]);
    expect((await store.load())["w"]!.runs).toHaveLength(1);
  });
});

describe("completion", () => {
  it("fires the verdict hook against the in-flight store, records, acks", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"));
    const { orchestrator, store, signals } = build([h]);

    await orchestrator.tick();
    await signals.enqueue({ kind: "verdict", runId: "run-1", verdict: "approve" });
    await orchestrator.tick();

    expect(h.hooks).toEqual(["onStart", "onApprove"]);
    // the hook saw the run still in flight
    expect(h.hookStores[0]!["w"]!.itemId).toBe("SQU-9");
    const ws = (await store.load())["w"]!;
    expect(ws).toMatchObject({ liveness: "dead", itemId: null });
    expect(ws.runs[0]!.outcome).toBe("approve");
    expect(await signals.peek()).toEqual([]); // acked
  });

  it("a reject verdict fires onReject", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"));
    const { orchestrator, signals } = build([h]);

    await orchestrator.tick();
    await signals.enqueue({ kind: "verdict", runId: "run-1", verdict: "reject" });
    await orchestrator.tick();

    expect(h.hooks).toEqual(["onStart", "onReject"]);
  });

  it("an unknown runId is dropped (acked) and changes nothing", async () => {
    const h = makeWorker();
    const { orchestrator, store, signals } = build([h]);

    await signals.enqueue({ kind: "verdict", runId: "ghost", verdict: "approve" });
    await orchestrator.tick();

    expect(await signals.peek()).toEqual([]);
    expect(h.hooks).toEqual([]);
    expect(await store.load()).toEqual({});
  });

  it("a throwing hook changes nothing in the record", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"));
    h.worker.onApprove = async () => {
      throw new Error("linear is down");
    };
    const { orchestrator, store, signals } = build([h]);

    await orchestrator.tick();
    await signals.enqueue({ kind: "verdict", runId: "run-1", verdict: "approve" });
    await orchestrator.tick();

    const ws = (await store.load())["w"]!;
    expect(ws.runs[0]!.outcome).toBe("approve"); // recorded anyway
    expect(await signals.peek()).toEqual([]); // acked anyway
  });
});

describe("orphaning", () => {
  it("a dead run with no verdict is orphaned and the station freed", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"));
    const { orchestrator, store } = build([h]);

    await orchestrator.tick();
    h.alive.value = false;
    await orchestrator.tick();

    const ws = (await store.load())["w"]!;
    expect(ws.runs[0]!.outcome).toBe("orphaned");
    expect(ws).toMatchObject({ liveness: "dead", itemId: null });
    expect(h.hooks).toEqual(["onStart"]); // no verdict hook for an orphan
  });

  it("the verdict wins the race: a dead run whose signal landed completes normally", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"));
    const { orchestrator, store, signals } = build([h]);

    await orchestrator.tick();
    h.alive.value = false;
    // signal lands after this tick's resolve step would have read the queue:
    // simulate by enqueueing now and letting the probe's final peek find it
    await signals.enqueue({ kind: "verdict", runId: "run-1", verdict: "approve" });
    await orchestrator.tick();

    expect((await store.load())["w"]!.runs[0]!.outcome).toBe("approve");
    expect(h.hooks).toContain("onApprove");
  });

  it("a throwing probe means unknown — the run keeps waiting", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"));
    h.alive.value = () => {
      throw new Error("probe flake");
    };
    const { orchestrator, store } = build([h]);

    await orchestrator.tick();
    await orchestrator.tick();

    expect((await store.load())["w"]!.runs[0]!.outcome).toBeNull(); // still open
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
  it("drains recovered verdicts (hooks fire), then orphans the rest", async () => {
    const h = makeWorker();
    const seed: Store = {
      w: {
        liveness: "alive",
        itemId: "SQU-9",
        runs: [
          { itemId: "SQU-8", runId: "done-run", startedAt: 1, endedAt: 2, outcome: "approve" },
          { itemId: "SQU-9", runId: "open-with-verdict", startedAt: 3, endedAt: null, outcome: null },
        ],
      },
      x: {
        liveness: "alive",
        itemId: "SQU-7",
        runs: [{ itemId: "SQU-7", runId: "open-no-verdict", startedAt: 3, endedAt: null, outcome: null }],
      },
    };
    const x = makeWorker("x");
    const { orchestrator, store, signals } = build([h, x], seed);
    await signals.enqueue({ kind: "verdict", runId: "open-with-verdict", verdict: "approve" });

    await orchestrator.start();
    await orchestrator.stop();

    const after = await store.load();
    expect(after["w"]!.runs.find((r) => r.runId === "open-with-verdict")!.outcome).toBe("approve");
    expect(h.hooks).toContain("onApprove"); // recovered verdict completes normally
    expect(after["x"]!.runs[0]!.outcome).toBe("orphaned"); // probe didn't survive restart
    expect(await signals.peek()).toEqual([]);
  });
});

describe("loop safety lives in ingest, not the engine", () => {
  it("the engine relaunches as long as ingest says launch — the cap is the worker's", async () => {
    const h = makeWorker();
    h.decisions.push(launch("SQU-9"), launch("SQU-9"), launch("SQU-9"));
    h.alive.value = false; // every run dies instantly
    const { orchestrator, store } = build([h]);

    await orchestrator.tick(); // launch 1
    await orchestrator.tick(); // orphan 1, launch 2
    await orchestrator.tick(); // orphan 2, launch 3

    const ws = (await store.load())["w"]!;
    expect(h.spawned).toHaveLength(3);
    expect(ws.runs.filter((r) => r.outcome === "orphaned")).toHaveLength(2);
  });
});
