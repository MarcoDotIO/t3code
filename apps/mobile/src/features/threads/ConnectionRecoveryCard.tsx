import { connectionRecoveryNotice } from "@t3tools/client-runtime/state/connectionRecovery";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { View } from "react-native";
import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";

export function ConnectionRecoveryCard({
  thread,
  onStop,
}: {
  thread: EnvironmentThreadShell;
  onStop: () => void;
}) {
  const notice = connectionRecoveryNotice(thread);
  if (notice === null) return null;
  return (
    <View
      accessibilityLiveRegion="polite"
      accessibilityRole={notice.kind === "resumed" ? "text" : "alert"}
      className="mx-3 mb-2 flex-row flex-wrap items-center justify-between gap-2 rounded-xl border border-border-subtle bg-screen p-3"
    >
      <View className="min-w-0 flex-1 gap-1">
        <Text className="text-sm text-foreground">{notice.title}</Text>
        {notice.kind === "unavailable" ? (
          <Text className="text-xs text-foreground-muted">{notice.detail}</Text>
        ) : null}
      </View>
      {notice.kind === "waiting" ? <Button label="Stop" tone="secondary" onPress={onStop} /> : null}
    </View>
  );
}
