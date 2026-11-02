import type { OrchestrationV2ConnectionRecovery, RunId } from "@t3tools/contracts";

export type ConnectionRecoveryNotice =
  | { readonly kind: "waiting" | "resumed"; readonly title: string }
  | { readonly kind: "unavailable"; readonly title: string; readonly detail: string };

/** A marker for an earlier run says nothing about the thread once newer work has started. */
export function connectionRecoveryNotice(thread: {
  readonly connectionRecovery?: OrchestrationV2ConnectionRecovery | null;
  readonly latestRun: { readonly runId: RunId } | null;
}): ConnectionRecoveryNotice | null {
  const recovery = thread.connectionRecovery;
  if (recovery == null) return null;
  switch (recovery.status) {
    case "resumed":
      return { kind: "resumed", title: "Task resumed" };
    case "resuming":
      return { kind: "waiting", title: "Connection lost. Waiting to resume…" };
    case "waiting":
      return recovery.runId === thread.latestRun?.runId
        ? { kind: "waiting", title: "Connection lost. Waiting to resume…" }
        : null;
    case "unavailable":
      return recovery.runId !== thread.latestRun?.runId
        ? null
        : {
            kind: "unavailable",
            title: "Connection lost",
            detail:
              recovery.reason === "gave_up"
                ? "Could not resume automatically. Send a message to continue."
                : "Automatic resume isn't available for this provider or connection. Send a message to continue.",
          };
  }
}
