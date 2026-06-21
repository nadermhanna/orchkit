# orchkit

A tick-driven engine for orchestrating parallel, autonomous agent runs. Each
run renders a verdict — **approve** or **reject** — and work moves from one
station to the next.

orchkit is the engine, not a product. It launches agents, tracks their runs,
survives restarts, and collects each run's verdict. *What* an agent does, and
*what happens* on approve or reject, is yours to write. A reference Claude Code
worker and a zero-dependency shell worker live in [`example/`](./example) so you
can read the whole contract end to end and copy from it.

```sh
npm install orchkit
```

Requires Node ≥ 20. ESM only.

## The model

Five pieces, each knowing only the one below it:

- **Orchestrator** — one long-lived process. Control flow only: no model, no
  domain knowledge. On each tick it resolves incoming verdicts, probes running
  agents (orphaning the dead), then asks each idle worker if there's work to
  launch.
- **Store** — append-only run history per worker. A `StoreService` of lifecycle
  verbs over a swappable `StoreDriver` (JSON-file and in-memory ship).
- **Signal queue** — the path a verdict travels from a finished run back to the
  engine. File-backed (durable across restarts) and in-memory drivers ship.
- **Worker** — your plugin. `ingest` decides what to claim, `generatePrompt`
  assembles the launch string, `spawn` starts the agent, and `onStart` /
  `onApprove` / `onReject` hooks perform consequences.
- **Integrations** — thin wrappers over external systems (a tracker, GitHub,
  Slack), exposed to your hooks as a typed bag. You write these.

A run that emits no verdict and whose process dies is **orphaned** — the engine
frees the station so the next item can be claimed. Restart-safe: open runs are
rebuilt at boot, verdicts that landed while the engine was down complete
normally, and anything still open is orphaned.

## Quick start: the shell worker

The smallest worker — run a command, approve on exit 0 — with no Claude and no
network. It demonstrates the full `Worker` contract.

```ts
import {
  createOrchestrator,
  createStoreService,
  jsonFileDriver,
  fileSignalQueue,
} from "orchkit";
// Reference worker — copy example/shell-worker.ts into your project.
import { shellWorker } from "./shell-worker.js";

const signals = fileSignalQueue(".orchkit/signals");
const store = createStoreService(jsonFileDriver(".orchkit/store.json"), {
  now: () => Date.now(),
});

const tasks = [{ itemId: "build", command: "npm run build" }];
const worker = shellWorker({
  signals,
  nextCommand: async () => tasks.shift() ?? null,
});

const orchestrator = createOrchestrator({
  workers: [worker],
  store,
  signals,
  clock: { now: () => Date.now() },
});

await orchestrator.start();
```

## The real thing: a Claude Code worker

`claudeCodeSpawn` launches a detached headless `claude -p` run in a worktree,
plants `ORCHKIT_RUN_ID` and `ORCHKIT_SIGNALS_DIR` in its environment, and
returns a liveness probe. The agent closes the loop by running the `orchkit`
CLI itself, so install it globally on the host the agents run on:

```sh
npm install -g orchkit
```

```ts
import {
  createOrchestrator,
  createStoreService,
  jsonFileDriver,
  fileSignalQueue,
} from "orchkit";
// Reference worker — copy example/claude-worker.ts into your project.
import { claudeWorker } from "./claude-worker.js";

const signals = fileSignalQueue(".orchkit/signals");
const store = createStoreService(jsonFileDriver(".orchkit/store.json"), {
  now: () => Date.now(),
});

const worker = claudeWorker({
  cwd: "/path/to/worktree",
  signalsDir: ".orchkit/signals",
  logsDir: ".orchkit/logs",
  nextTask: async () => ({
    itemId: "lint-fix",
    task: "Fix the lint error in src/app.ts. Run the linter to confirm.",
  }),
});

const orchestrator = createOrchestrator({
  workers: [worker],
  store,
  signals,
  clock: { now: () => Date.now() },
});

await orchestrator.start();
```

`claudeWorker` composes the verdict instruction onto your task with
`withVerdict`, so the agent is told to finish by running
`orchkit verdict approve` or `orchkit verdict reject`. The instruction assumes
`orchkit` is on the agent's PATH — that's what the global install is for.

## Writing your own worker

A worker is a static config template. Use `defineWorker` for type inference,
and compose the batteries rather than reimplementing them.

```ts
import { defineWorker } from "orchkit";
import { claudeCodeSpawn } from "orchkit/claude-code";
import { withVerdict } from "orchkit/cli";

const myWorker = defineWorker({
  name: "implement",
  integrations: [myTracker], // your typed integrations; [] if none
  ingest: async (integrations, store, workerName) => {
    const next = await integrations.tracker.nextTicket();
    if (!next) return { launch: false };
    return { launch: true, itemId: next.id, args: next };
  },
  generatePrompt: (ticket) => withVerdict(`Implement ${ticket.id}: ${ticket.title}`),
  spawn: claudeCodeSpawn({
    cwd: "/path/to/worktree",
    signalsDir: ".orchkit/signals",
    logsDir: ".orchkit/logs",
  }),
  onStart: async (integrations) => {
    /* e.g. move the ticket to In Progress */
  },
  onApprove: async (integrations) => {
    /* e.g. open a PR, move the ticket to Done */
  },
  onReject: async (integrations) => {
    /* e.g. comment the findings, move the ticket back */
  },
});
```

### Loop safety lives in your `ingest`, not the engine

The engine relaunches a worker as long as its `ingest` says `launch`. If every
run fails and `ingest` keeps handing back the same item, the engine will keep
launching it. A retry cap, a backoff, or a "skip after N rejects" rule belongs
in your `ingest` — read the run history off the `store` snapshot it's given.

## Package layout

| Import | What |
| --- | --- |
| `orchkit` | The core: `createOrchestrator`, the store + signal services and their drivers, `defineWorker`, and all types. |
| `orchkit/claude-code` | The `claudeCodeSpawn` battery. |
| `orchkit/cli` | `emitVerdict`, `verdictInstruction`, `withVerdict`. |

The `shellWorker` and `claudeWorker` reference workers live in
[`example/`](./example) — read and copy them, they are not a package export.

Importing the core pulls in none of the batteries.

## Runtime state

With the file-backed drivers, state lives under `.orchkit/` (git-ignore it):

- `.orchkit/store.json` — run history, written atomically.
- `.orchkit/signals/` — the file signal queue; one `<runId>.json` per pending verdict.
- `.orchkit/logs/<runId>.log` — each spawned agent's stdout/stderr (with the claude-code battery).

## License

MIT © Nader Hanna
