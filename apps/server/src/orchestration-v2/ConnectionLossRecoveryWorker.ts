import {
  isConnectionLossFailure,
  latestExecutedRun,
  latestRootProviderFailure,
} from "@t3tools/shared/orchestrationV2ThreadError";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  MessageId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConnectionRecovery,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  type ProviderInstanceId,
  type ServerSettings as ServerSettingsValue,
  type ThreadId,
} from "@t3tools/contracts";
import type { ProviderConnectionCheck } from "@t3tools/provider-core/server/driver";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as DueHint from "../scheduling/DueHint.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { forkParked } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";
import { isNativeMaintenanceCommand } from "./CommandPolicy.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

// Turning the setting on must not wake old failures, and a thread left waiting overnight is the
// longest gap after which continuing unprompted is still what the user would expect.
const ARM_WINDOW_MS = 10 * 60 * 1_000;
const MAX_WAIT_MS = 24 * 60 * 60 * 1_000;
const RESUMED_NOTICE_MS = 5_000;
// Reachability cannot see a custom route that T3 has no record of, so this bounds its failures.
const MAX_ATTEMPTS = 3;
const MIN_CHECK_DELAY_MS = 5_000;
const MAX_CHECK_DELAY_MS = 60_000;

const CONTINUE_TEXT = "Continue where you left off.";

// Runs and turn items T3 writes itself say nothing about whether the provider is back.
const NON_PROGRESS_ITEM_TYPES: ReadonlySet<OrchestrationV2TurnItem["type"]> = new Set([
  "user_message",
  "notification",
  "error",
  "run_interrupt_request",
  "run_interrupt_result",
  "system_notice",
  "checkpoint",
]);

export interface ConnectionRecoveryThreadState {
  readonly thread: Pick<
    OrchestrationV2AppThread,
    "archivedAt" | "settledOverride" | "snoozedUntil" | "connectionRecovery"
  >;
  readonly run: OrchestrationV2Run | null;
  readonly failure: OrchestrationV2ProviderFailure | null;
  readonly enabled: boolean;
  /** The run's provider instance can check its connection. */
  readonly supported: boolean;
  /** Stop, an open question, or work that is not this thread's own to continue. */
  readonly declined: boolean;
  readonly progressed: boolean;
}

type ConnectionRecoveryStep =
  | { readonly type: "none" }
  | { readonly type: "resume"; readonly recovery: OrchestrationV2ConnectionRecovery }
  | { readonly type: "set"; readonly next: OrchestrationV2ConnectionRecovery | null };

const NONE: ConnectionRecoveryStep = { type: "none" };

const isLive = (run: OrchestrationV2Run) =>
  run.status === "preparing" || run.status === "starting" || run.status === "running";

export function connectionRecoveryStep(
  state: ConnectionRecoveryThreadState,
  nowMs: number,
): ConnectionRecoveryStep {
  const { thread, run, failure } = state;
  const recovery = thread.connectionRecovery ?? null;
  const since = DateTime.formatIso(DateTime.makeUnsafe(nowMs));
  const set = (next: OrchestrationV2ConnectionRecovery | null): ConnectionRecoveryStep => ({
    type: "set",
    next,
  });
  const clear = recovery === null ? NONE : set(null);
  if (run === null || thread.archivedAt !== null || thread.settledOverride === "settled")
    return clear;

  const interrupted =
    run.status === "failed" &&
    failure !== null &&
    (state.supported ? isConnectionLossFailure(failure) : failure.class === "transport_error");
  const continuesRecovery =
    recovery !== null && run.connectionLossContinuationOfRunId === recovery.runId;

  if (!interrupted) {
    if (recovery?.status === "resuming" && continuesRecovery) {
      if (isLive(run))
        return state.progressed ? set({ ...recovery, status: "resumed", since }) : NONE;
      return run.status === "completed" || run.status === "waiting"
        ? set({ ...recovery, status: "resumed", since })
        : clear;
    }
    if (recovery?.status === "resumed")
      return nowMs - Date.parse(recovery.since) < RESUMED_NOTICE_MS ? NONE : clear;
    return clear;
  }

  if (!state.enabled || state.declined)
    return recovery?.runId === run.id && recovery.status === "unavailable" ? NONE : clear;

  const wait = (attempt: number) =>
    set(
      attempt > MAX_ATTEMPTS
        ? { runId: run.id, status: "unavailable", attempt, since, reason: "gave_up" }
        : { runId: run.id, status: "waiting", attempt, since },
    );

  if (recovery?.runId === run.id) {
    switch (recovery.status) {
      case "unavailable":
        return NONE;
      case "resumed":
        return clear;
      // The continuation was cancelled before it started, so its source is latest again.
      case "resuming":
        return wait(recovery.attempt + 1);
      case "waiting":
        if (nowMs - Date.parse(recovery.since) >= MAX_WAIT_MS)
          return set({ ...recovery, status: "unavailable", since, reason: "gave_up" });
        return thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > nowMs
          ? NONE
          : { type: "resume", recovery };
    }
  }

  if (
    !continuesRecovery &&
    nowMs - DateTime.toEpochMillis(run.completedAt ?? run.requestedAt) > ARM_WINDOW_MS
  )
    return clear;
  if (!state.supported)
    return set({ runId: run.id, status: "unavailable", attempt: 1, since, reason: "unsupported" });
  return wait(continuesRecovery && recovery.status === "resuming" ? recovery.attempt + 1 : 1);
}

// Events that can change a thread's step; streaming items are read while a continuation runs.
const affectsConnectionRecovery = (event: OrchestrationV2DomainEvent) =>
  event.type.startsWith("thread.") ||
  event.type === "run.created" ||
  event.type === "run.updated" ||
  event.type === "runtime-request.updated" ||
  (event.type === "turn-item.updated" && event.payload.type === "error");

const makeSweep = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const settings = yield* ServerSettings.ServerSettingsService;
  const instances = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const due = DueHint.make();
  yield* forkParked(
    Stream.runForEach(orchestrator.streamDomainEvents, (event) =>
      affectsConnectionRecovery(event) ? due.invalidate : Effect.void,
    ),
  );
  yield* forkParked(Stream.runForEach(settings.streamChanges, () => due.invalidate));
  let checkDelayMs = MIN_CHECK_DELAY_MS;
  let nextCheckAtMs = 0;

  const readState = Effect.fn("ConnectionLossRecoveryWorker.readState")(function* (
    threadId: ThreadId,
    preferences: ServerSettingsValue,
  ) {
    const { thread, runs, runtimeRequests } = yield* projections.getThreadRecords(threadId, [
      "runs",
      "runtimeRequests",
    ]);
    const run = latestExecutedRun(runs);
    const items =
      run === null
        ? []
        : (yield* projections.getThreadRecords(threadId, ["turnItems"], {
            turnItemRunIds: [run.id],
            turnItemTypes: ["error", "run_interrupt_request"],
          })).turnItems;
    const sourceMessage =
      run === null
        ? undefined
        : (yield* projections.getThreadRecords(threadId, ["messages"], {
            messageIds: [run.userMessageId],
          })).messages[0];
    const progressed =
      run !== null &&
      isLive(run) &&
      thread.connectionRecovery?.status === "resuming" &&
      (yield* projections.getThreadRecords(threadId, ["turnItems"], {
        turnItemRunIds: [run.id],
      })).turnItems.some((item) => !NON_PROGRESS_ITEM_TYPES.has(item.type));
    const instance =
      run === null ? undefined : yield* instances.getInstance(run.providerInstanceId);
    return {
      thread,
      run,
      failure: latestRootProviderFailure(run, items),
      enabled: resolveProjectSettings(preferences, thread.projectId).settings
        .resumeThreadsAfterConnectionLoss,
      supported: instance?.checkConnection !== undefined,
      declined:
        items.some((item) => item.type === "run_interrupt_request") ||
        runtimeRequests.some((request) => request.status === "pending") ||
        run?.delegatedTaskId !== undefined ||
        thread.lineage.relationshipToParent === "subagent" ||
        (sourceMessage !== undefined && isNativeMaintenanceCommand(sourceMessage)),
      progressed,
    } satisfies ConnectionRecoveryThreadState;
  });

  const update = (
    threadId: ThreadId,
    expected: OrchestrationV2ConnectionRecovery | null,
    next: OrchestrationV2ConnectionRecovery | null,
    nowMs: number,
  ) =>
    orchestrator.dispatch({
      type: "thread.connection-recovery.update",
      commandId: CommandId.make(
        `connection-recovery:${threadId}:${nowMs}:${next?.status ?? "cleared"}`,
      ),
      threadId,
      expected,
      connectionRecovery: next,
    });

  const resume = (threadId: ThreadId, run: OrchestrationV2Run, identity: string) =>
    threads.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`command:connection-resume:${identity}`),
      threadId,
      messageId: MessageId.make(`message:connection-resume:${identity}`),
      // Codex resumes promptlessly; this text only reaches a provider alongside pending context.
      text: CONTINUE_TEXT,
      notification: {
        source: { kind: "system" },
        outcome: "updated",
        summary: "Connection restored, resuming this turn",
        detail: CONTINUE_TEXT,
      },
      attachments: [],
      modelSelection: run.modelSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "agent",
      creationSource: "server",
      connectionLossContinuationOfRunId: run.id,
    });

  return Effect.fn("ConnectionLossRecoveryWorker.sweep")(function* () {
    const now = yield* DateTime.now;
    const nowMs = DateTime.toEpochMillis(now);
    if (yield* due.isIdle(nowMs)) return;
    const readVersion = yield* due.version;
    const preferences = yield* settings.getSettings;
    const threadIds = yield* projections.getConnectionRecoveryThreadIds(
      DateTime.subtract(now, { milliseconds: ARM_WINDOW_MS }),
    );
    let recheck = false;
    let wakeAtMs = Number.POSITIVE_INFINITY;
    const ready: Array<{
      readonly threadId: ThreadId;
      readonly run: OrchestrationV2Run;
      readonly recovery: OrchestrationV2ConnectionRecovery;
    }> = [];
    for (const threadId of threadIds) {
      yield* Effect.gen(function* () {
        const state = yield* readState(threadId, preferences);
        const recovery = state.thread.connectionRecovery ?? null;
        const step = connectionRecoveryStep(state, nowMs);
        if (step.type === "set") {
          yield* update(threadId, recovery, step.next, nowMs);
          recheck = true;
        } else if (step.type === "resume" && state.run !== null) {
          ready.push({ threadId, run: state.run, recovery: step.recovery });
        } else if (recovery?.status === "resuming") {
          recheck = true;
        } else if (recovery?.status === "resumed") {
          wakeAtMs = Math.min(wakeAtMs, Date.parse(recovery.since) + RESUMED_NOTICE_MS);
        } else if (recovery?.status === "waiting" && state.thread.snoozedUntil != null) {
          wakeAtMs = Math.min(wakeAtMs, DateTime.toEpochMillis(state.thread.snoozedUntil));
        }
      }).pipe(
        Effect.catchCause((cause) => {
          recheck = true;
          return Effect.logWarning("orchestration-v2.connection-recovery.step-failed", {
            threadId,
            cause,
          });
        }),
      );
    }
    if (ready.length > 0 && nowMs >= nextCheckAtMs) {
      // One connection serves every waiting thread on an instance, so check it once per sweep.
      const checks = new Map<ProviderInstanceId, ProviderConnectionCheck>();
      for (const { threadId, run, recovery } of ready) {
        yield* Effect.gen(function* () {
          let check = checks.get(run.providerInstanceId);
          if (check === undefined) {
            const instance = yield* instances.getInstance(run.providerInstanceId);
            check = yield* instance?.checkConnection ?? Effect.succeed("unverifiable" as const);
            checks.set(run.providerInstanceId, check);
          }
          if (check === "unverifiable") {
            yield* update(
              threadId,
              recovery,
              {
                ...recovery,
                status: "unavailable",
                since: DateTime.formatIso(now),
                reason: "unsupported",
              },
              nowMs,
            );
          } else if (check === "reachable") {
            yield* resume(threadId, run, `${run.id}:${recovery.since}`).pipe(
              // Disabled instances and threads above a policy ceiling cannot resume however long
              // this waits; the existing error stays and manual continuation still works.
              Effect.catchTags({
                ThreadManagementProviderInstanceDisabledError: () =>
                  update(
                    threadId,
                    recovery,
                    {
                      ...recovery,
                      status: "unavailable",
                      since: DateTime.formatIso(now),
                      reason: "gave_up",
                    },
                    nowMs,
                  ),
                OrchestratorRuntimeModeAboveCeilingError: () =>
                  update(
                    threadId,
                    recovery,
                    {
                      ...recovery,
                      status: "unavailable",
                      since: DateTime.formatIso(now),
                      reason: "gave_up",
                    },
                    nowMs,
                  ),
              }),
            );
            recheck = true;
          }
        }).pipe(
          Effect.catchCause((cause) => {
            recheck = true;
            return Effect.logWarning("orchestration-v2.connection-recovery.resume-failed", {
              threadId,
              cause,
            });
          }),
        );
      }
      if ([...checks.values()].includes("unreachable")) {
        nextCheckAtMs = nowMs + checkDelayMs;
        checkDelayMs = Math.min(checkDelayMs * 2, MAX_CHECK_DELAY_MS);
      } else {
        nextCheckAtMs = 0;
        checkDelayMs = MIN_CHECK_DELAY_MS;
      }
    }
    if (ready.length > 0 && nextCheckAtMs > nowMs) wakeAtMs = Math.min(wakeAtMs, nextCheckAtMs);
    // A sweep that acted, or failed to, re-checks on the next tick.
    if (recheck) return;
    yield* due.record(readVersion, wakeAtMs, nowMs);
  });
});

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const sweep = yield* makeSweep;
    const scheduler = yield* Scheduler.Scheduler;
    yield* scheduler.register("connection-loss-recovery", sweep());
  }),
);
