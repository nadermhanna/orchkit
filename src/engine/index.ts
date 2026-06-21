// The orchkit core — the root export. Everything a worker author needs to
// wire and run the engine. Batteries (the claude-code spawn preset, the
// verdict CLI helpers) live in the `orchkit/claude-code` and `orchkit/cli`
// subpaths and are not pulled in by importing the core.

export { createOrchestrator } from "./orchestrator.js";
export type { EngineDeps, Orchestrator } from "./orchestrator.js";

export {
  createStoreService,
  jsonFileDriver,
  memoryDriver,
} from "./store.js";
export type { StoreService, StoreDriver } from "./store.js";

export { fileSignalQueue, memorySignalQueue } from "./signals.js";
export type { SignalQueue } from "./signals.js";

export { defineWorker } from "./types.js";
export type {
  AnyIntegration,
  AnyWorker,
  Clock,
  Helper,
  HelperMap,
  Hook,
  Ingest,
  IngestionResult,
  Integration,
  IntegrationBag,
  Liveness,
  ReadonlyStore,
  Run,
  RunOutcome,
  Signal,
  Store,
  Verdict,
  Worker,
  WorkerState,
} from "./types.js";
