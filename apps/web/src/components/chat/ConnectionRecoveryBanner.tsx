import type { ConnectionRecoveryNotice } from "@t3tools/client-runtime/state/connectionRecovery";
import { CheckIcon, UnplugIcon } from "lucide-react";
import { Button } from "../ui/button";
import type { ComposerBannerStackItem } from "./ComposerBannerStack";

export function connectionRecoveryBannerItem(input: {
  readonly notice: ConnectionRecoveryNotice;
  readonly onStop: (() => void) | null;
}): ComposerBannerStackItem {
  const { notice, onStop } = input;
  if (notice.kind === "resumed") {
    return {
      id: "connection-recovery",
      variant: "success",
      icon: <CheckIcon />,
      // Success banners are not announced by the stack, and this one disappears on its own.
      title: <span role="status">{notice.title}</span>,
    };
  }
  return {
    id: "connection-recovery",
    variant: "warning",
    icon: <UnplugIcon />,
    title: notice.title,
    ...(notice.kind === "unavailable" ? { description: notice.detail } : {}),
    actions:
      notice.kind === "waiting" && onStop !== null ? (
        <Button size="xs" variant="ghost" onClick={onStop}>
          Stop
        </Button>
      ) : null,
  };
}
