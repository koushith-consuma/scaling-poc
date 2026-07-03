/**
 * Creates a timeout that aborts a run if it exceeds the configured duration.
 * Returns an AbortController whose signal aborts after timeoutMs.
 */
export function createRunTimeout(timeoutMs: number): { controller: AbortController; clear: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort('timeout'), timeoutMs);
  // Prevent the timer from keeping the process alive if the run finishes early.
  if (typeof timer === 'object' && 'unref' in timer) {
    timer.unref();
  }
  const clear = () => clearTimeout(timer);
  return { controller, clear };
}
