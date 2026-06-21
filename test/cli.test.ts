import { describe, expect, it } from "vitest";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { emitVerdict, verdictInstruction, withVerdict } from "../src/cli/index.js";

describe("emitVerdict", () => {
  it("writes an atomic verdict signal into ORCHKIT_SIGNALS_DIR", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "orchkit-cli-"));
    const env = { ORCHKIT_RUN_ID: "run-1", ORCHKIT_SIGNALS_DIR: dir };

    const result = await emitVerdict("approve", env as NodeJS.ProcessEnv);

    expect(result).toMatchObject({ runId: "run-1", verdict: "approve" });
    const written = JSON.parse(await readFile(path.join(dir, "run-1.json"), "utf8"));
    expect(written).toEqual({ kind: "verdict", runId: "run-1", verdict: "approve" });
  });

  it("rejects an invalid verdict", async () => {
    const env = { ORCHKIT_RUN_ID: "run-1", ORCHKIT_SIGNALS_DIR: "/tmp" };
    await expect(emitVerdict("maybe", env as NodeJS.ProcessEnv)).rejects.toThrow(
      /must be "approve" or "reject"/,
    );
  });

  it("throws a clear error when ORCHKIT_RUN_ID is missing", async () => {
    await expect(emitVerdict("approve", {} as NodeJS.ProcessEnv)).rejects.toThrow(
      /ORCHKIT_RUN_ID is not set/,
    );
  });
});

describe("verdict instruction", () => {
  it("names both verdicts and the bin command", () => {
    const text = verdictInstruction();
    expect(text).toContain("orchkit verdict approve");
    expect(text).toContain("orchkit verdict reject");
  });

  it("withVerdict appends the instruction to a prompt", () => {
    const composed = withVerdict("do the thing");
    expect(composed.startsWith("do the thing")).toBe(true);
    expect(composed).toContain("orchkit verdict approve");
  });
});
