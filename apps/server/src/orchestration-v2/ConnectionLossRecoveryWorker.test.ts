import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Command,
  type OrchestrationV2ConnectionRecovery,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ConnectionLossRecoveryWorker from "./ConnectionLossRecoveryWorker.ts";
import {
  connectionRecoveryStep,
  type ConnectionRecoveryThreadState,
} from "./ConnectionLossRecoveryWorker.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import {
  continuationFailedBeforePrompt,
  isRestartContinuationSource,
} from "./RestartBackgroundNote.ts";
import { restartContinuationRun } from "./RestartContinuation.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

const MINUTE_MS = 60 * 1_000;
const HOUR_MS = 60 * MINUTE_MS;
const nowMs = Date.parse("2026-11-02T12:00:00.000Z");
const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));

const SOURCE = RunId.make("run:source");
const CONTINUATION = RunId.make("run:continuation");

const run = (overrides: Partial<OrchestrationV2Run>) =>
  ({
    id: SOURCE,
    status: "failed",
    requestedAt: DateTime.makeUnsafe(nowMs - MINUTE_MS),
    completedAt: DateTime.makeUnsafe(nowMs - 1_000),
    providerInstanceId: ProviderInstanceId.make("codex"),
    ...overrides,
  }) as OrchestrationV2Run;

const continuation = (status: OrchestrationV2Run["status"]) =>
  run({
    id: CONTINUATION,
    status,
    connectionLossContinuationOfRunId: SOURCE,
    completedAt: status === "running" ? null : DateTime.makeUnsafe(nowMs - 500),
  });

const connectionLost: OrchestrationV2ProviderFailure = {
  class: "transport_error",
  message: "stream disconnected",
  code: "responseStreamDisconnected",
  retryable: null,
};

const marker = (
  status: OrchestrationV2ConnectionRecovery["status"],
  overrides: Partial<OrchestrationV2ConnectionRecovery> = {},
): OrchestrationV2ConnectionRecovery => ({
  runId: SOURCE,
  status,
  attempt: 1,
  since: iso(nowMs - 1_000),
  ...overrides,
});

const state = (
  overrides: Partial<ConnectionRecoveryThreadState> & {
    readonly recovery?: OrchestrationV2ConnectionRecovery | null;
    readonly snoozedUntil?: DateTime.Utc | null;
  } = {},
): ConnectionRecoveryThreadState => {
  const { recovery = null, snoozedUntil = null, ...rest } = overrides;
  return {
    thread: {
      archivedAt: null,
      settledOverride: null,
      snoozedUntil,
      connectionRecovery: recovery,
    },
    run: run({}),
    failure: connectionLost,
    enabled: true,
    supported: true,
    declined: false,
    progressed: false,
    ...rest,
  };
};

const waitingThread = (recovery: OrchestrationV2ConnectionRecovery | null) =>
  ({
    id: ThreadId.make("thread:lost"),
    projectId: ProjectId.make("project:lost"),
    archivedAt: null,
    settledOverride: null,
    snoozedUntil: null,
    connectionRecovery: recovery,
    lineage: { relationshipToParent: null },
  }) as unknown as OrchestrationV2AppThread;

const stepOf = (overrides: Parameters<typeof state>[0], at = nowMs) =>
  connectionRecoveryStep(state(overrides), at);

describe("connectionRecoveryStep", () => {
  it("arms a confirmed connection failure and waits", () => {
    const step = stepOf({});
    assert.deepEqual(step, {
      type: "set",
      next: { runId: SOURCE, status: "waiting", attempt: 1, since: iso(nowMs) },
    });
  });

  it("starts no recovery while the setting is off, and cancels a pending wait when disabled", () => {
    assert.deepEqual(stepOf({ enabled: false }), { type: "none" });
    assert.deepEqual(stepOf({ enabled: false, recovery: marker("waiting") }), {
      type: "set",
      next: null,
    });
  });

  it("does not arm auth, quota or unrelated failures", () => {
    for (const failure of [
      { ...connectionLost, class: "provider_error", code: "unauthorized" },
      { ...connectionLost, class: "usage_limit", code: "usageLimitExceeded" },
      { ...connectionLost, class: "provider_error", code: "internalServerError" },
      // T3's own undelivered-answer item is a transport error with no provider code.
      { ...connectionLost, code: null },
    ] satisfies ReadonlyArray<OrchestrationV2ProviderFailure>) {
      assert.deepEqual(stepOf({ failure }), { type: "none" });
    }
  });

  it("does not arm a run that is still going, finished, or stopped", () => {
    for (const status of ["running", "completed", "interrupted", "cancelled"] as const) {
      assert.deepEqual(stepOf({ run: run({ status }), failure: null }), { type: "none" });
    }
  });

  it("does not wake failures from before the arm window", () => {
    const old = run({ completedAt: DateTime.makeUnsafe(nowMs - 10 * MINUTE_MS - 1) });
    assert.deepEqual(stepOf({ run: old }), { type: "none" });
  });

  it("lets Stop and open questions win over a pending wait", () => {
    assert.deepEqual(stepOf({ declined: true }), { type: "none" });
    assert.deepEqual(stepOf({ declined: true, recovery: marker("waiting") }), {
      type: "set",
      next: null,
    });
  });

  it("resumes a waiting thread unless it is snoozed", () => {
    const recovery = marker("waiting");
    assert.deepEqual(stepOf({ recovery }), { type: "resume", recovery });
    assert.deepEqual(stepOf({ recovery, snoozedUntil: DateTime.makeUnsafe(nowMs + MINUTE_MS) }), {
      type: "none",
    });
  });

  it("gives up after waiting a day instead of resuming unprompted much later", () => {
    const recovery = marker("waiting");
    const step = stepOf({ recovery }, nowMs - 1_000 + 24 * HOUR_MS);
    assert(step.type === "set");
    assert.equal(step.next?.status, "unavailable");
    assert.equal(step.next?.reason, "gave_up");
  });

  it("drops a marker for an earlier run when newer work started", () => {
    const newer = run({ id: RunId.make("run:newer"), status: "running", completedAt: null });
    assert.deepEqual(stepOf({ run: newer, failure: null, recovery: marker("waiting") }), {
      type: "set",
      next: null,
    });
  });

  it("clears on archive and settle", () => {
    const recovery = marker("waiting");
    const archived = state({ recovery });
    assert.deepEqual(
      connectionRecoveryStep(
        { ...archived, thread: { ...archived.thread, archivedAt: DateTime.makeUnsafe(nowMs) } },
        nowMs,
      ),
      { type: "set", next: null },
    );
    assert.deepEqual(
      connectionRecoveryStep(
        { ...archived, thread: { ...archived.thread, settledOverride: "settled" } },
        nowMs,
      ),
      { type: "set", next: null },
    );
  });

  it("reports Task resumed only after real progress, then clears it", () => {
    const recovery = marker("resuming");
    const running = continuation("running");
    assert.deepEqual(stepOf({ run: running, failure: null, recovery }), { type: "none" });
    const resumed = stepOf({ run: running, failure: null, recovery, progressed: true });
    assert(resumed.type === "set");
    assert.equal(resumed.next?.status, "resumed");
    assert.equal(resumed.next?.since, iso(nowMs));
    const notice = marker("resumed", { since: iso(nowMs) });
    assert.deepEqual(stepOf({ run: running, failure: null, recovery: notice }, nowMs + 4_999), {
      type: "none",
    });
    assert.deepEqual(stepOf({ run: running, failure: null, recovery: notice }, nowMs + 5_000), {
      type: "set",
      next: null,
    });
  });

  it("counts a continuation that finished as progress", () => {
    const step = stepOf({
      run: continuation("completed"),
      failure: null,
      recovery: marker("resuming"),
    });
    assert(step.type === "set");
    assert.equal(step.next?.status, "resumed");
  });

  it("leaves a non-connection continuation failure alone", () => {
    assert.deepEqual(
      stepOf({
        run: continuation("failed"),
        failure: { ...connectionLost, class: "provider_error", code: "internalServerError" },
        recovery: marker("resuming"),
      }),
      { type: "set", next: null },
    );
  });

  it("waits again after a continuation loses the connection, and gives up after three", () => {
    const again = stepOf({ run: continuation("failed"), recovery: marker("resuming") });
    assert.deepEqual(again, {
      type: "set",
      next: { runId: CONTINUATION, status: "waiting", attempt: 2, since: iso(nowMs) },
    });
    const exhausted = stepOf({
      run: continuation("failed"),
      recovery: marker("resuming", { attempt: 3 }),
    });
    assert(exhausted.type === "set");
    assert.equal(exhausted.next?.status, "unavailable");
    assert.equal(exhausted.next?.reason, "gave_up");
    assert.deepEqual(stepOf({ recovery: marker("unavailable", { reason: "gave_up" }) }), {
      type: "none",
    });
  });

  it("recovers when a continuation was cancelled before it started", () => {
    const step = stepOf({ recovery: marker("resuming") });
    assert.deepEqual(step, {
      type: "set",
      next: { runId: SOURCE, status: "waiting", attempt: 2, since: iso(nowMs) },
    });
  });

  it("explains instead of promising a resume for providers without a connection check", () => {
    const failure = { ...connectionLost, code: null };
    assert.deepEqual(stepOf({ supported: false, failure }), {
      type: "set",
      next: {
        runId: SOURCE,
        status: "unavailable",
        attempt: 1,
        since: iso(nowMs),
        reason: "unsupported",
      },
    });
    assert.deepEqual(stepOf({ supported: false, failure, enabled: false }), { type: "none" });
  });
});

it("leaves a failed run to this worker, so a restart cannot also continue it", () => {
  const source = run({});
  assert.isFalse(isRestartContinuationSource(source, []));
  assert.isFalse(continuationFailedBeforePrompt(source, []));
  assert.isUndefined(
    restartContinuationRun(
      {
        thread: waitingThread(marker("waiting")),
        runs: [source],
        providerThreads: [],
        providerSessions: [],
        providerTurns: [],
        runtimeRequests: [],
        turnItems: [],
      },
      DateTime.makeUnsafe(nowMs),
    ),
  );
});

const THREAD = ThreadId.make("thread:lost");
const INSTANCE = ProviderInstanceId.make("codex");

const makeSweepLayer = (input: {
  readonly ids: Effect.Effect<ReadonlyArray<ThreadId>>;
  readonly thread: () => OrchestrationV2AppThread;
  readonly check: () => Effect.Effect<"reachable" | "unreachable" | "unverifiable">;
  readonly onUpdate?: (next: OrchestrationV2ConnectionRecovery | null) => Effect.Effect<void>;
  readonly onDispatch: (command: OrchestrationV2Command) => Effect.Effect<void>;
  readonly events: Queue.Queue<OrchestrationV2DomainEvent>;
}) =>
  ConnectionLossRecoveryWorker.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Scheduler.layer,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getConnectionRecoveryThreadIds: () => input.ids,
          getThreadRecords: ((_threadId: ThreadId, fields: ReadonlyArray<string>) =>
            Effect.succeed({
              thread: input.thread(),
              ...(fields.includes("runs")
                ? { runs: [run({ userMessageId: MessageId.make("message:source") })] }
                : {}),
              ...(fields.includes("runtimeRequests") ? { runtimeRequests: [] } : {}),
              ...(fields.includes("turnItems")
                ? {
                    turnItems: [
                      {
                        id: "item:lost",
                        type: "error",
                        status: "failed",
                        runId: SOURCE,
                        ordinal: 1,
                        updatedAt: DateTime.makeUnsafe(nowMs),
                        failure: connectionLost,
                      },
                    ],
                  }
                : {}),
              ...(fields.includes("messages") ? { messages: [] } : {}),
            })) as never,
        }),
        Layer.mock(ThreadManagement.ThreadManagementService)({
          dispatch: ((command: OrchestrationV2Command) =>
            input.onDispatch(command).pipe(Effect.as({ sequence: 1 }))) as never,
        }),
        Layer.mock(Orchestrator.OrchestratorV2)({
          streamDomainEvents: Stream.fromQueue(input.events),
          dispatch: ((command: { readonly connectionRecovery?: unknown }) =>
            (
              input.onUpdate?.(
                command.connectionRecovery as OrchestrationV2ConnectionRecovery | null,
              ) ?? Effect.void
            ).pipe(Effect.as({ sequence: 1 }))) as never,
        }),
        Layer.mock(ServerSettings.ServerSettingsService)({
          getSettings: Effect.succeed({
            ...DEFAULT_SERVER_SETTINGS,
            resumeThreadsAfterConnectionLoss: true,
          }),
          streamChanges: Stream.never,
        }),
        Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
          getInstance: (() =>
            Effect.succeed({ instanceId: INSTANCE, checkConnection: input.check() })) as never,
        }),
      ),
    ),
  );

it.effect("backs off while offline and sends one quiet continuation once reachable", () =>
  Effect.gen(function* () {
    const startMs = Date.parse("2026-11-02T12:00:00.000Z");
    yield* TestClock.setTime(startMs);
    const recovery = marker("waiting", { since: iso(startMs) });
    let current: OrchestrationV2ConnectionRecovery = recovery;
    let reachable = false;
    const checks = yield* Queue.unbounded<number>();
    const dispatched = yield* Queue.unbounded<OrchestrationV2Command>();
    const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
    const layer = makeSweepLayer({
      ids: Effect.succeed([THREAD]),
      thread: () => waitingThread(current),
      check: () =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((ms) => Queue.offer(checks, ms - startMs)),
          Effect.map(() => (reachable ? "reachable" : "unreachable")),
        ),
      onDispatch: (command) =>
        Effect.gen(function* () {
          current = { ...current, status: "resuming" };
          yield* Queue.offer(dispatched, command);
        }),
      events,
    });
    yield* Effect.gen(function* () {
      assert.equal(yield* Queue.take(checks), 0);
      yield* TestClock.adjust("5 seconds");
      assert.equal(yield* Queue.take(checks), 5_000);
      // The delay doubles, so the 10s tick does not check.
      yield* TestClock.adjust("10 seconds");
      assert.equal(yield* Queue.take(checks), 15_000);
      assert.equal(yield* Queue.size(dispatched), 0);
      reachable = true;
      yield* TestClock.adjust("20 seconds");
      assert.equal(yield* Queue.take(checks), 35_000);
      const command = yield* Queue.take(dispatched);
      assert(command.type === "message.dispatch");
      assert.equal(command.connectionLossContinuationOfRunId, SOURCE);
      assert.isDefined(command.notification);
      assert.equal(command.createdBy, "agent");
      assert.deepEqual(command.attachments, []);
      yield* TestClock.adjust("30 seconds");
      assert.equal(yield* Queue.size(dispatched), 0);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("marks a route it cannot verify as unavailable instead of waiting", () =>
  Effect.gen(function* () {
    const updates = yield* Queue.unbounded<OrchestrationV2ConnectionRecovery | null>();
    const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
    yield* TestClock.setTime(nowMs);
    const layer = makeSweepLayer({
      ids: Effect.succeed([THREAD]),
      thread: () => waitingThread(marker("waiting", { since: iso(nowMs) })),
      check: () => Effect.succeed("unverifiable"),
      onUpdate: (next) => Queue.offer(updates, next).pipe(Effect.asVoid),
      onDispatch: () => Effect.die("must not resume"),
      events,
    });
    yield* Effect.gen(function* () {
      const next = yield* Queue.take(updates);
      assert.equal(next?.status, "unavailable");
      assert.equal(next?.reason, "unsupported");
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("skips SQL while idle until a relevant event arrives", () =>
  Effect.gen(function* () {
    const startMs = Date.parse("2026-11-02T12:00:00.000Z");
    yield* TestClock.setTime(startMs);
    const queries = yield* Queue.unbounded<number>();
    const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
    const layer = makeSweepLayer({
      ids: Clock.currentTimeMillis.pipe(
        Effect.flatMap((ms) => Queue.offer(queries, ms - startMs)),
        Effect.as([]),
      ),
      thread: () => waitingThread(null),
      check: () => Effect.die("no thread is waiting"),
      onDispatch: () => Effect.die("no thread is waiting"),
      events,
    });
    yield* Effect.gen(function* () {
      assert.equal(yield* Queue.take(queries), 0);
      yield* TestClock.adjust("30 seconds");
      yield* Queue.offer(events, { type: "run.updated", payload: {} } as never);
      yield* TestClock.adjust("5 seconds");
      // No tick queried in the idle 30s; the event wakes the 35s tick.
      assert.equal(yield* Queue.take(queries), 35_000);
    }).pipe(Effect.provide(layer));
  }),
);
