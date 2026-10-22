import { describe, expect, it } from "vite-plus/test";

import { hostFreezeNotice } from "./hostFreeze.ts";

const stall = {
  detectedAt: "2026-10-20T07:50:37.633Z",
  durationMs: 12_422,
  loadAverage: 219.4,
  cpuCount: 18,
};

describe("hostFreezeNotice", () => {
  it("blames the CPU only when the load it shows is above the core count", () => {
    expect(hostFreezeNotice("leftbook", stall)?.description).toBe(
      "Its CPU is overloaded (load 219 on 18 cores).",
    );
    expect(hostFreezeNotice("leftbook", { ...stall, loadAverage: 18.4 })?.description).toBe(
      "T3 Code on leftbook was too busy to answer.",
    );
  });
});
