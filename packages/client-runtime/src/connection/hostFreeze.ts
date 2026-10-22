import type { ServerEventLoopStall } from "@t3tools/contracts";

export interface HostFreezeNotice {
  readonly title: string;
  readonly description: string;
}

/**
 * Says why a host stopped answering, from the recent stall it reports on
 * connect. Web shows it as a toast, mobile in the environment's connection row.
 */
export function hostFreezeNotice(
  hostLabel: string,
  stall: ServerEventLoopStall | undefined,
): HostFreezeNotice | null {
  if (stall === undefined) return null;
  const { cpuCount } = stall;
  // Compared as shown, so a load of 8.4 on 8 cores never reads "overloaded (load 8 on 8 cores)".
  const load = stall.loadAverage === undefined ? undefined : Math.round(stall.loadAverage);
  return {
    title: `${hostLabel} froze for ${Math.round(stall.durationMs / 1000)}s`,
    description:
      load !== undefined && load > cpuCount
        ? `Its CPU is overloaded (load ${load} on ${cpuCount} cores).`
        : `T3 Code on ${hostLabel} was too busy to answer.`,
  };
}
