// A "-fork.g<sha>" version marks a build made by scripts/fork/fork-update.sh. It runs beside the
// official app with its own identity and data, and updates from source instead of releases.
const FORK_VERSION_PATTERN = /^[^-+]+-fork\.g([0-9a-f]{7,40})$/;

export const FORK_APP_ID = "com.t3tools.t3code.fork";
export const FORK_USER_DATA_DIRECTORY = "t3code-fork";
export const FORK_BASE_DIRECTORY = ".t3-fork";
export const FORK_SOURCE_FILE = "fork-source.json";

export function isForkDesktopVersion(version: string): boolean {
  return FORK_VERSION_PATTERN.test(version);
}

export function forkBuildCommit(version: string): string | null {
  return FORK_VERSION_PATTERN.exec(version)?.[1] ?? null;
}
