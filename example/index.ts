// Reference workers. These are teaching examples, not production workers —
// read them to learn the Worker contract, then write your own.

export { shellWorker } from "./shell-worker.js";
export type { ShellWorkerOptions } from "./shell-worker.js";

export { claudeWorker } from "./claude-worker.js";
export type { ClaudeWorkerOptions } from "./claude-worker.js";
