// The orchestrator. The single long-lived process: control flow only, no
// model, no domain knowledge. Tick-driven, single actor.

import { randomUUID } from "node:crypto";
import type { StoreService } from "./store.js";
import type { SignalQueue } from "./signals.js";
import type {
  AnyWorker,
  Clock,
  IntegrationBag,
  Run,
  Signal,
  Verdict,
} from "./types.js";

export interface EngineDeps {
  workers: AnyWorker[]; // the in-process plugins
  store: StoreService;
  signals: SignalQueue; // the orchestrator touches peek/ack only
  clock: Clock;
  mintRunId?: () => string; // deterministic ids in tests; defaults to a UUID
  log?: (message: string) => void;
}

export interface Orchestrator {
  start(): Promise<void>; // boot sequence, then begin ticking on the interval
  stop(): Promise<void>; // stop ticking; nothing to drain — boot is the recovery path
  tick(): Promise<void>; // one full pass: resolve -> probe -> ingest/launch
}

interface RegistryEntry {
  workerName: string;
  recorded: boolean; // recordLaunch landed; false during the double-spawn gap
  run: Run; // carries the descriptor reprobe needs — no closure held here
}

const projectBag = (worker: AnyWorker): IntegrationBag<any> =>
  Object.fromEntries(worker.integrations.map((i) => [i.name, i.helpers]));

export function createOrchestrator(
  deps: EngineDeps,
  opts?: { tickMs?: number },
): Orchestrator {
  const { workers, store, signals, clock } = deps;
  const mintRunId = deps.mintRunId ?? (() => randomUUID());
  const log = deps.log ?? ((m: string) => console.log(`[orchkit] ${m}`));
  const tickMs = opts?.tickMs ?? 5_000;

  // runId -> entry, populated at launch and rebuilt at boot. A worker with an
  // entry is busy regardless of what the store says — the ingest gate.
  const registry = new Map<string, RegistryEntry>();
  const workerByName = new Map(workers.map((w) => [w.name, w]));

  let ticking = false;
  let interval: NodeJS.Timeout | null = null;

  async function fireHook(
    entry: RegistryEntry,
    hookName: "onStart" | "onApprove" | "onReject",
  ): Promise<void> {
    const worker = workerByName.get(entry.workerName);
    if (!worker) return; // a recovered run for a worker no longer configured
    try {
      const snapshot = await store.load();
      await worker[hookName](projectBag(worker), snapshot, worker.name);
    } catch (e) {
      log(`hook ${hookName} for ${entry.workerName}/${entry.run.runId} threw: ${e}`);
    }
  }

  // Fire the verdict's hook, record the completion, ack, drop the entry — in
  // that order. The hook fires before the record because it reads the
  // in-flight store; ack is last so a crash leaves the signal for boot.
  async function resolveCompletion(entry: RegistryEntry, verdict: Verdict): Promise<void> {
    await fireHook(entry, verdict === "approve" ? "onApprove" : "onReject");
    await store.recordCompletion(entry.workerName, entry.run.runId, verdict);
    await signals.ack(entry.run.runId);
    registry.delete(entry.run.runId);
    log(`run ${entry.run.runId} (${entry.workerName}) completed: ${verdict}`);
  }

  async function resolveSignals(pending: Signal[]): Promise<void> {
    for (const signal of pending) {
      try {
        const entry = registry.get(signal.runId);
        if (!entry) {
          // already completed, already orphaned, a duplicate, or stale from a
          // previous engine life — drop and log; this rule makes intake idempotent.
          log(`unknown runId ${signal.runId} — dropping signal`);
          await signals.ack(signal.runId);
          continue;
        }
        // a verdict that arrived during the recordLaunch gap waits in the queue
        if (!entry.recorded) continue;
        await resolveCompletion(entry, signal.verdict);
      } catch (e) {
        log(`resolving signal for ${signal.runId} failed: ${e}`);
      }
    }
  }

  // Liveness via the worker's reprobe, keyed off the durable descriptor. Three
  // states, kept distinct so the caller never conflates them: only a confirmed
  // "dead" is orphaned. "unknown" — reprobe threw, or the worker is no longer
  // configured — leaves the run open to re-probe next tick rather than orphan
  // blind.
  async function getLiveness(
    entry: RegistryEntry,
  ): Promise<"alive" | "dead" | "unknown"> {
    const worker = workerByName.get(entry.workerName);
    if (!worker) return "unknown";
    try {
      return (await worker.reprobe(entry.run.descriptor)) ? "alive" : "dead";
    } catch {
      return "unknown";
    }
  }

  async function probeAndOrphan(): Promise<void> {
    for (const entry of [...registry.values()]) {
      try {
        // an unrecorded launch retries the write each tick until it lands
        if (!entry.recorded) {
          await store.recordLaunch(entry.workerName, entry.run);
          entry.recorded = true;
          await fireHook(entry, "onStart");
          continue;
        }
        if ((await getLiveness(entry)) !== "dead") continue; // alive or unknown: keep waiting
        // dead: peek once more — the verdict wins the race
        const lastLook = await signals.peek();
        const won = lastLook.find((s) => s.runId === entry.run.runId);
        if (won) {
          await resolveCompletion(entry, won.verdict);
          continue;
        }
        await store.recordOrphaned(entry.workerName, entry.run.runId);
        registry.delete(entry.run.runId);
        log(`run ${entry.run.runId} (${entry.workerName}) orphaned`);
      } catch (e) {
        log(`probing ${entry.run.runId} failed: ${e}`);
      }
    }
  }

  async function ingestAndLaunch(): Promise<void> {
    const busy = new Set([...registry.values()].map((e) => e.workerName));
    for (const worker of workers) {
      if (busy.has(worker.name)) continue; // the registry gates ingest
      try {
        const snapshot = await store.load();
        const decision = await worker.ingest(projectBag(worker), snapshot, worker.name);
        if (!decision.launch) continue;
        const runId = mintRunId();
        const prompt = worker.generatePrompt(decision.args);
        const handle = await worker.spawn(prompt, { runId });
        // entry lands synchronously on spawn success, before the durable
        // write — the double-spawn gate.
        const run: Run = {
          itemId: decision.itemId,
          runId,
          startedAt: clock.now(),
          endedAt: null,
          outcome: null,
          descriptor: handle.descriptor,
        };
        const entry: RegistryEntry = {
          workerName: worker.name,
          recorded: false,
          run,
        };
        registry.set(runId, entry);
        log(`launched ${worker.name} on ${decision.itemId} (run ${runId})`);
        try {
          await store.recordLaunch(worker.name, run);
          entry.recorded = true;
        } catch (e) {
          // station reads busy via the registry; the write retries each tick
          log(`recordLaunch for ${runId} failed (will retry): ${e}`);
          continue;
        }
        await fireHook(entry, "onStart");
      } catch (e) {
        log(`worker ${worker.name} failed this tick: ${e}`);
      }
    }
  }

  async function tick(): Promise<void> {
    if (ticking) return; // non-reentrant: preserves the single actor
    ticking = true;
    try {
      await resolveSignals(await signals.peek());
      await probeAndOrphan();
      await ingestAndLaunch();
    } catch (e) {
      log(`tick failed: ${e}`); // the loop never dies
    } finally {
      ticking = false;
    }
  }

  async function start(): Promise<void> {
    // Boot: load, rebuild the registry from open runs, drain the queue so
    // verdicts that landed while the engine was down complete normally, then
    // reprobe each remaining open run off its persisted descriptor. A run still
    // alive is re-adopted — kept in the registry, reprobed on later ticks like
    // any live run — so a restart never spawns a twin onto a worktree a prior
    // agent is still editing. Only a run reprobe calls dead is orphaned.
    const snapshot = await store.load();
    for (const [workerName, ws] of Object.entries(snapshot)) {
      for (const run of ws.runs) {
        if (run.endedAt === null) {
          registry.set(run.runId, { workerName, recorded: true, run });
        }
      }
    }
    await resolveSignals(await signals.peek());
    for (const entry of [...registry.values()]) {
      if ((await getLiveness(entry)) !== "dead") {
        log(`boot: run ${entry.run.runId} (${entry.workerName}) re-adopted (not confirmed dead)`);
        continue; // re-adopted: stays registered, busy, reprobed each tick
      }
      await store.recordOrphaned(entry.workerName, entry.run.runId);
      registry.delete(entry.run.runId);
      log(`boot: run ${entry.run.runId} (${entry.workerName}) orphaned`);
    }
    interval = setInterval(() => void tick(), tickMs);
    void tick();
  }

  async function stop(): Promise<void> {
    if (interval) clearInterval(interval);
    interval = null;
  }

  return { start, stop, tick };
}
