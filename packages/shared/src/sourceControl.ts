const SCP_SSH_REMOTE_PATTERN = /^[a-zA-Z0-9._-]+@([^:/]+):/;
// Git still accepts its deprecated git+ssh and ssh+git spellings, which package manifests carry.
const SSH_URL_PATTERN = /^(?:ssh|git\+ssh|ssh\+git):\/\//i;

export function isSshRemoteUrl(remoteUrl: string): boolean {
  const trimmed = remoteUrl.trim();
  return SCP_SSH_REMOTE_PATTERN.test(trimmed) || SSH_URL_PATTERN.test(trimmed);
}
