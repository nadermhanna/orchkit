import { describe, expect, it } from "vitest";
import { mkdtemp, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileSignalQueue, memorySignalQueue } from "../src/engine/signals.js";
import type { Signal } from "../src/engine/types.js";

const signal = (runId: string): Signal => ({ kind: "verdict", runId, verdict: "approve" });

for (const [label, make] of [
  ["memory", () => memorySignalQueue()],
  [
    "file",
    async () => fileSignalQueue(await mkdtemp(path.join(tmpdir(), "orchkit-sig-"))),
  ],
] as const) {
  describe(`${label} signal queue`, () => {
    it("peek is non-destructive; ack removes", async () => {
      const q = await make();
      await q.enqueue(signal("r1"));
      await q.enqueue(signal("r2"));
      expect((await q.peek()).map((s) => s.runId).sort()).toEqual(["r1", "r2"]);
      expect(await q.peek()).toHaveLength(2); // still there
      await q.ack("r1");
      expect((await q.peek()).map((s) => s.runId)).toEqual(["r2"]);
    });

    it("peek on an empty/never-written queue returns []", async () => {
      const q = await make();
      expect(await q.peek()).toEqual([]);
    });

    it("ack of an unknown runId is a no-op", async () => {
      const q = await make();
      await expect(q.ack("ghost")).resolves.toBeUndefined();
    });
  });
}

describe("file signal queue durability", () => {
  it("a signal enqueued by one instance is visible to a fresh one (survives restart)", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "orchkit-sig-"));
    await fileSignalQueue(dir).enqueue(signal("r1"));
    const rebooted = fileSignalQueue(dir);
    expect((await rebooted.peek())[0]!.runId).toBe("r1");
    // no tmp fragments left behind
    expect((await readdir(dir)).every((f) => !f.endsWith(".tmp"))).toBe(true);
  });
});
