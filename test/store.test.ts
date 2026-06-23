import { describe, expect, it } from "vitest";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createStoreService, jsonFileDriver, memoryDriver } from "../src/engine/store.js";
import type { Run } from "../src/engine/types.js";

const fixedClock = { now: () => 1_000 };

const run = (overrides: Partial<Run> = {}): Run => ({
  itemId: "SQU-1",
  runId: "r1",
  startedAt: 500,
  endedAt: null,
  outcome: null,
  descriptor: { pid: 123 },
  ...overrides,
});

describe("store service", () => {
  it("loads an empty store on first boot", async () => {
    const store = createStoreService(memoryDriver(), fixedClock);
    expect(await store.load()).toEqual({});
  });

  it("recordLaunch appends the run and marks the station alive", async () => {
    const store = createStoreService(memoryDriver(), fixedClock);
    await store.recordLaunch("w", run());
    const state = (await store.load())["w"]!;
    expect(state.liveness).toBe("alive");
    expect(state.itemId).toBe("SQU-1");
    expect(state.runs).toHaveLength(1);
    expect(state.runs[0]!.outcome).toBeNull();
  });

  it("recordCompletion closes only the matching open run and frees the station", async () => {
    const store = createStoreService(memoryDriver(), fixedClock);
    await store.recordLaunch("w", run({ runId: "old", endedAt: null }));
    await store.recordCompletion("w", "old", "reject");
    await store.recordLaunch("w", run({ runId: "new" }));
    await store.recordCompletion("w", "new", "approve");

    const state = (await store.load())["w"]!;
    expect(state.liveness).toBe("dead");
    expect(state.itemId).toBeNull();
    expect(state.runs.map((r) => r.outcome)).toEqual(["reject", "approve"]);
    expect(state.runs.every((r) => r.endedAt === 1_000)).toBe(true);
  });

  it("recordOrphaned closes the run as orphaned", async () => {
    const store = createStoreService(memoryDriver(), fixedClock);
    await store.recordLaunch("w", run());
    await store.recordOrphaned("w", "r1");
    const state = (await store.load())["w"]!;
    expect(state.runs[0]!.outcome).toBe("orphaned");
    expect(state.itemId).toBeNull();
  });

  it("a closed run is never re-closed by a duplicate verb", async () => {
    const store = createStoreService(memoryDriver(), fixedClock);
    await store.recordLaunch("w", run());
    await store.recordCompletion("w", "r1", "approve");
    await store.recordOrphaned("w", "r1"); // late duplicate
    const state = (await store.load())["w"]!;
    expect(state.runs[0]!.outcome).toBe("approve");
  });
});

describe("json file driver", () => {
  it("persists atomically and survives reload", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "orchkit-store-"));
    const file = path.join(dir, "store.json");
    const store = createStoreService(jsonFileDriver(file), fixedClock);
    await store.recordLaunch("w", run());

    const reread = createStoreService(jsonFileDriver(file), fixedClock);
    expect((await reread.load())["w"]!.itemId).toBe("SQU-1");
    // the real file is complete JSON, never a fragment
    const raw = await readFile(file, "utf8");
    expect(JSON.parse(raw)["w"].runs).toHaveLength(1);
  });
});
