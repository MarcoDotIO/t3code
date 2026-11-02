import { RunId, type OrchestrationV2ConnectionRecovery } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { connectionRecoveryNotice } from "./connectionRecovery.ts";

const source = RunId.make("run:source");
const recovery = (
  status: OrchestrationV2ConnectionRecovery["status"],
  reason?: OrchestrationV2ConnectionRecovery["reason"],
): OrchestrationV2ConnectionRecovery => ({
  runId: source,
  status,
  attempt: 1,
  since: "2026-11-02T12:00:00.000Z",
  ...(reason === undefined ? {} : { reason }),
});
const onRun = (runId: RunId) => ({ latestRun: { runId } });

describe("connectionRecoveryNotice", () => {
  it("shows nothing without a marker, so healthy threads stay unchanged", () => {
    expect(connectionRecoveryNotice({ ...onRun(source), connectionRecovery: null })).toBeNull();
  });

  it("keeps waiting until the continuation shows provider progress", () => {
    const continuation = RunId.make("run:continuation");
    expect(
      connectionRecoveryNotice({ ...onRun(source), connectionRecovery: recovery("waiting") })?.kind,
    ).toBe("waiting");
    expect(
      connectionRecoveryNotice({ ...onRun(continuation), connectionRecovery: recovery("resuming") })
        ?.kind,
    ).toBe("waiting");
    expect(
      connectionRecoveryNotice({ ...onRun(continuation), connectionRecovery: recovery("resumed") })
        ?.title,
    ).toBe("Task resumed");
  });

  it("hides a marker for an earlier run once newer work started", () => {
    const newer = onRun(RunId.make("run:newer"));
    expect(connectionRecoveryNotice({ ...newer, connectionRecovery: recovery("waiting") })).toBe(
      null,
    );
    expect(
      connectionRecoveryNotice({
        ...newer,
        connectionRecovery: recovery("unavailable", "unsupported"),
      }),
    ).toBeNull();
  });

  it("explains why an interruption will not resume on its own", () => {
    const unsupported = connectionRecoveryNotice({
      ...onRun(source),
      connectionRecovery: recovery("unavailable", "unsupported"),
    });
    const gaveUp = connectionRecoveryNotice({
      ...onRun(source),
      connectionRecovery: recovery("unavailable", "gave_up"),
    });
    expect(unsupported?.kind === "unavailable" && unsupported.detail).toContain("isn't available");
    expect(gaveUp?.kind === "unavailable" && gaveUp.detail).toContain("Could not resume");
  });
});
