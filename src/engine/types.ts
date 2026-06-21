// The engine's data structures. These are the contract between the
// orchestrator, the store, the signal queue, and worker authors.

// ---------------------------------------------------------------------------
// Integrations & helpers
// ---------------------------------------------------------------------------

// An action against one external system: typed args in, typed output. Config
// (tokens, base urls, clients) is the integration's own concern — the engine
// never sees it.
export type Helper<In, Out> = (args: In) => Promise<Out>;

// A map of helper-name -> helper. A concrete integration pins real In/Out per
// helper; this is just the shape "some bag of helpers".
export type HelperMap = Record<string, Helper<any, any>>;

// Wraps one external system (gh, a tracker, claude). Carries its name and a
// bag of helpers.
export interface Integration<
  Name extends string = string,
  H extends HelperMap = HelperMap,
> {
  name: Name;
  helpers: H;
}

// Any integration with its specifics erased.
export type AnyIntegration = Integration<string, HelperMap>;

// Turn a worker's integration tuple into the keyed bag a hook receives:
// { [integrationName]: { [helperName]: (args) => Promise<Out> } }.
export type IntegrationBag<T extends readonly AnyIntegration[]> = {
  [I in T[number] as I["name"]]: I["helpers"];
};

// ---------------------------------------------------------------------------
// The state store (engine-owned run history)
// ---------------------------------------------------------------------------

// The agent's judgment of the step it owns, emitted by the run itself.
export type Verdict = "approve" | "reject";

// How a run ended (or hasn't). The verdicts are agent-emitted; `orphaned` is
// engine-assigned. `null` = still running.
export type RunOutcome = Verdict | "orphaned" | null;

// One launched agent run. The engine is the only writer of these.
export interface Run {
  itemId: string; // immutable label: which unit of work this run was for
  runId: string; // opaque, engine-minted label for this run — NOT an OS pid
  startedAt: number; // ms epoch
  endedAt: number | null; // null while running; an orphan's is detection time
  outcome: RunOutcome;
}

// Per-station running status. Orphanhood is a run outcome, not a liveness.
export type Liveness = "alive" | "dead";

// Per-station state, keyed by worker name in the store.
export interface WorkerState {
  liveness: Liveness;
  itemId: string | null; // the item at this station right now (null when idle)
  runs: Run[]; // append-only history
}

// The whole store: worker name -> that station's state.
export type Store = Record<string, WorkerState>;
export type ReadonlyStore = Readonly<Record<string, Readonly<WorkerState>>>;

// ---------------------------------------------------------------------------
// Signals (run -> engine)
// ---------------------------------------------------------------------------

// What a run emits out to the engine, delivered through the signal queue. The
// verdict is the only v1 signal.
export type Signal = { kind: "verdict"; runId: string; verdict: Verdict };

// ---------------------------------------------------------------------------
// Hooks & ingestion
// ---------------------------------------------------------------------------

// A hook performs a domain consequence at a point in a run's lifecycle. It
// acts on the world; it does not report back.
export type Hook<T extends readonly AnyIntegration[]> = (
  integrations: IntegrationBag<T>,
  store: ReadonlyStore,
  workerName: string,
) => Promise<void>;

// ingest's answer: launch-or-not, plus on launch the args and the itemId.
export type IngestionResult<Args> =
  | { launch: true; args: Args; itemId: string }
  | { launch: false };

// Called whenever the worker is idle: "is there a next unit of work, and if
// so, what do I launch the agent with?"
export type Ingest<T extends readonly AnyIntegration[], Args> = (
  integrations: IntegrationBag<T>,
  store: ReadonlyStore,
  workerName: string,
) => Promise<IngestionResult<Args>>;

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

// Static config template for one agent type. Carries no per-ticket data;
// per-item state lives in the store under the worker's name.
export interface Worker<
  T extends readonly AnyIntegration[] = readonly AnyIntegration[],
  Args = void,
> {
  name: string;
  generatePrompt: (args: Args) => string; // assembles the launch string, pure over args
  // Launch an agent on the prompt. The engine mints the runId; spawn starts
  // the run and makes that id reachable by it (e.g. ORCHKIT_RUN_ID).
  spawn: (
    prompt: string,
    ctx: { runId: string },
  ) => Promise<{
    isAlive: () => Promise<boolean>;
  }>;
  integrations: T;
  ingest: Ingest<T, Args>;
  onStart: Hook<T>;
  onApprove: Hook<T>;
  onReject: Hook<T>;
}

// Any worker with its generics erased — what EngineDeps carries.
export type AnyWorker = Worker<readonly AnyIntegration[], any>;

// Convenience on-ramp: identity at runtime, inference at author time.
export function defineWorker<T extends readonly AnyIntegration[], Args = void>(
  worker: Worker<T, Args>,
): Worker<T, Args> {
  return worker;
}

// One source of time, injectable so tests can freeze it.
export interface Clock {
  now(): number;
}
