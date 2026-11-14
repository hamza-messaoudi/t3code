#!/usr/bin/env bash
# Updates "T3 Code (Fork)" from source: merges your fork and upstream into the local branch,
# builds the macOS app, and installs it. A merge conflict aborts the merge and exits 2.
#
#   fork-update.sh check            fetch both remotes, list incoming commits
#   fork-update.sh sync             merge origin/<branch> then upstream/main, push the result
#   fork-update.sh build            build the .app from the checkout
#   fork-update.sh install APP [PID]  wait for PID to exit, replace the installed app, relaunch
#   fork-update.sh all              sync, build, install (first install from a terminal)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BRANCH="${T3_FORK_BRANCH:-custom}"
UPSTREAM_REF="upstream/main"
ORIGIN_REF="origin/$BRANCH"
APP_NAME="T3 Code (Fork)"
INSTALL_PATH="${T3_FORK_INSTALL_PATH:-/Applications/$APP_NAME.app}"
OUTPUT_DIR="$REPO/release/fork"
FORK_HOME="${T3_FORK_HOME:-$HOME/.t3-fork}"

g() { git -C "$REPO" "$@"; }
fail() { echo "ERROR $*" >&2; exit "${EXIT_CODE:-1}"; }

fetch_remotes() {
  g fetch --quiet upstream main || fail "could not fetch upstream"
  # The fork branch may not exist on origin yet.
  g fetch --quiet origin "$BRANCH" 2>/dev/null || true
}

incoming_refs() {
  g rev-parse --verify --quiet "$ORIGIN_REF" >/dev/null && echo "$ORIGIN_REF"
  echo "$UPSTREAM_REF"
}

cmd_check() {
  fetch_remotes
  # macOS ships bash 3.2, which has no mapfile.
  local refs=() ref
  while read -r ref; do refs+=("$ref"); done < <(incoming_refs)
  echo "HEAD $(g rev-parse HEAD)"
  echo "UPSTREAM $(g rev-parse "$UPSTREAM_REF")"
  echo "COUNT $(g rev-list --count --no-merges "${refs[@]}" ^HEAD)"
  g log --no-merges --format='COMMIT %h %s' -n 50 "${refs[@]}" ^HEAD
}

cmd_sync() {
  local current
  current="$(g symbolic-ref --short HEAD 2>/dev/null || echo detached)"
  [[ "$current" == "$BRANCH" ]] || EXIT_CODE=3 fail "checkout is on '$current', not '$BRANCH'"
  [[ -z "$(g status --porcelain --untracked-files=no)" ]] ||
    EXIT_CODE=3 fail "checkout has uncommitted changes; commit or stash them first"
  fetch_remotes
  local ref files
  while read -r ref; do
    g merge-base --is-ancestor "$ref" HEAD && continue
    if ! g merge --no-edit --quiet -m "Merge $ref into $BRANCH" "$ref" >/dev/null 2>&1; then
      files="$(g diff --name-only --diff-filter=U | head -20 | tr '\n' ' ')"
      g merge --abort
      EXIT_CODE=2 fail "conflict merging $ref in: ${files:-unknown files}"
    fi
  done < <(incoming_refs)
  if ! g rev-parse --verify --quiet "$ORIGIN_REF" >/dev/null ||
    ! g merge-base --is-ancestor HEAD "$ORIGIN_REF"; then
    g push --quiet origin "$BRANCH" || echo "WARN could not push $BRANCH to origin" >&2
  fi
  echo "HEAD $(g rev-parse HEAD)"
}

ensure_vp() {
  command -v vp >/dev/null && return
  # shellcheck disable=SC1091
  [[ -f "$HOME/.config/vite-plus/env" ]] && . "$HOME/.config/vite-plus/env"
  command -v vp >/dev/null || fail "vp is not installed (curl -fsSL https://vite.plus | bash)"
}

cmd_build() {
  ensure_vp
  local arch version app
  arch="$(uname -m)"
  [[ "$arch" == "x86_64" ]] && arch="x64"
  version="$(node -p "require('$REPO/apps/desktop/package.json').version")-fork.g$(g rev-parse --short=10 HEAD)"
  echo "PROGRESS 10 installing dependencies"
  (cd "$REPO" && vp i >&2)
  echo "PROGRESS 30 building $version"
  rm -rf "$OUTPUT_DIR"
  (cd "$REPO" && node scripts/build-desktop-artifact.ts --platform mac --target dir \
    --arch "$arch" --build-version "$version" --output-dir "$OUTPUT_DIR" >&2)
  app="$(find "$OUTPUT_DIR" -maxdepth 2 -name "$APP_NAME.app" -type d | head -1)"
  [[ -n "$app" ]] || fail "build finished without $APP_NAME.app in $OUTPUT_DIR"
  echo "PROGRESS 100 built"
  echo "VERSION $version"
  echo "APP $app"
}

cmd_install() {
  local app="${1:?usage: install APP [PID]}" pid="${2:-}"
  [[ -d "$app" ]] || fail "no app at $app"
  if [[ -n "$pid" ]]; then
    for _ in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  fi
  rm -rf "$INSTALL_PATH"
  ditto "$app" "$INSTALL_PATH"
  mkdir -p "$FORK_HOME"
  printf '{ "repo": "%s", "branch": "%s" }\n' "$REPO" "$BRANCH" >"$FORK_HOME/fork-source.json"
  open "$INSTALL_PATH"
  echo "INSTALLED $INSTALL_PATH"
}

cmd_all() {
  cmd_sync
  local app
  app="$(cmd_build | tee /dev/stderr | sed -n 's/^APP //p')"
  cmd_install "$app"
}

case "${1:-}" in
  check | sync | build | all) "cmd_$1" ;;
  install) shift; cmd_install "$@" ;;
  *) sed -n '2,10p' "$0" >&2; exit 64 ;;
esac
