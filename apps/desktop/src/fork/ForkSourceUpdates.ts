/**
 * ForkSourceUpdates - the update button of a fork build. Instead of downloading releases, it runs
 * scripts/fork/fork-update.sh in the checkout the app was built from: merge the fork and upstream,
 * build the app, swap it in. A merge conflict stops the update and leaves the checkout as it was.
 *
 * @module ForkSourceUpdates
 */
import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";

import type { DesktopUpdateState } from "@t3tools/contracts";
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as DesktopEnvironment from "../app/DesktopEnvironment.ts";
import * as DesktopObservability from "../app/DesktopObservability.ts";
import * as DesktopState from "../app/DesktopState.ts";
import * as ElectronApp from "../electron/ElectronApp.ts";
import * as ElectronWindow from "../electron/ElectronWindow.ts";
import * as IpcChannels from "../ipc/channels.ts";
import * as DesktopUpdates from "../updates/DesktopUpdates.ts";
import { createInitialDesktopUpdateState } from "../updates/updateMachine.ts";
import { FORK_SOURCE_FILE, forkBuildCommit, isForkDesktopVersion } from "./forkBuild.ts";

const decodeForkSource = Schema.decodeUnknownOption(
  fromLenientJson(Schema.Struct({ repo: Schema.String, branch: Schema.String })),
);

const CHECK_INTERVAL = Duration.hours(4);
const EXIT_CONFLICT = 2;

const { logInfo, logWarning } = DesktopObservability.makeComponentLogger("fork-updater");

interface ScriptResult {
  readonly code: number;
  readonly lines: ReadonlyArray<string>;
  readonly error: string | null;
}

/** Fields of the script's `KEY value` stdout lines; repeated keys keep every value. */
function field(lines: ReadonlyArray<string>, key: string): ReadonlyArray<string> {
  const prefix = `${key} `;
  return lines.filter((line) => line.startsWith(prefix)).map((line) => line.slice(prefix.length));
}

const make = Effect.gen(function* () {
  const environment = yield* DesktopEnvironment.DesktopEnvironment;
  const electronWindow = yield* ElectronWindow.ElectronWindow;
  const electronApp = yield* ElectronApp.ElectronApp;
  const desktopState = yield* DesktopState.DesktopState;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = environment.path;

  const source = yield* fileSystem
    .readFileString(path.join(environment.baseDir, FORK_SOURCE_FILE))
    .pipe(Effect.option, Effect.map(Option.flatMap(decodeForkSource)));
  const scriptPath = Option.map(source, ({ repo }) =>
    path.join(repo, "scripts/fork/fork-update.sh"),
  );
  const disabledMessage = Option.isNone(source)
    ? `No ${FORK_SOURCE_FILE} in ${environment.baseDir}. Install once with scripts/fork/fork-update.sh all.`
    : null;

  const stateRef = yield* Ref.make<DesktopUpdateState>({
    ...createInitialDesktopUpdateState(environment.appVersion, environment.runtimeInfo, "latest"),
    ...(disabledMessage === null
      ? { enabled: true, status: "idle" as const }
      : { message: disabledMessage }),
  });
  const changes = yield* PubSub.sliding<DesktopUpdateState>(16);
  const activeRef = yield* Ref.make<"check" | "download" | "install" | null>(null);
  const builtAppRef = yield* Ref.make<string | null>(null);

  const emitState = Ref.get(stateRef).pipe(
    Effect.flatMap((state) => electronWindow.sendAll(IpcChannels.UPDATE_STATE_CHANNEL, state)),
  );
  const update = (f: (state: DesktopUpdateState) => DesktopUpdateState) =>
    Ref.updateAndGet(stateRef, f).pipe(
      Effect.tap((state) => PubSub.publish(changes, state)),
      Effect.tap(() => emitState),
    );

  const runScript = (args: ReadonlyArray<string>, onLine?: (line: string) => void) =>
    Effect.callback<ScriptResult>((resume) => {
      if (Option.isNone(scriptPath) || Option.isNone(source)) {
        resume(Effect.succeed({ code: -1, lines: [], error: disabledMessage }));
        return;
      }
      const lines: string[] = [];
      let error: string | null = null;
      const child = NodeChildProcess.spawn("/bin/bash", [scriptPath.value, ...args], {
        cwd: source.value.repo,
        env: { ...process.env, T3_FORK_BRANCH: source.value.branch, T3_FORK_HOME: environment.baseDir },
        stdio: ["ignore", "pipe", "pipe"],
      });
      NodeReadline.createInterface({ input: child.stdout }).on("line", (line) => {
        lines.push(line);
        onLine?.(line);
      });
      NodeReadline.createInterface({ input: child.stderr }).on("line", (line) => {
        if (line.startsWith("ERROR ")) error = line.slice("ERROR ".length);
      });
      child.on("error", (cause) => resume(Effect.succeed({ code: -1, lines, error: cause.message })));
      child.on("close", (code) => resume(Effect.succeed({ code: code ?? -1, lines, error })));
      return Effect.sync(() => child.kill());
    });

  /** Claims the updater for one action; `null` when another action holds it. */
  const claim = <A>(action: "check" | "download" | "install", effect: Effect.Effect<A>) =>
    Ref.modify(activeRef, (active) => (active === null ? [true, action] : [false, active])).pipe(
      Effect.flatMap((claimed) =>
        claimed
          ? effect.pipe(Effect.map(Option.some), Effect.ensuring(Ref.set(activeRef, null)))
          : Effect.succeedNone,
      ),
    );

  const check = Effect.fn("ForkSourceUpdates.check")(function* (reason: string) {
    if (disabledMessage !== null) return false;
    yield* Effect.annotateCurrentSpan({ reason });
    const ran = yield* claim(
      "check",
      Effect.gen(function* () {
        const previous = yield* update((state) => ({
          ...state,
          status: state.downloadedVersion === null ? "checking" : state.status,
          checkedAt: new Date().toISOString(),
          message: null,
          errorContext: null,
        }));
        const result = yield* runScript(["check"]);
        if (result.code !== 0) {
          yield* logWarning("fork update check failed", { error: result.error });
          yield* update((state) => ({
            ...state,
            status: "error",
            message: `Could not check for updates: ${result.error ?? "unknown error"}`,
            errorContext: "check",
            canRetry: true,
          }));
          return;
        }
        if (previous.downloadedVersion !== null) {
          yield* update((state) => ({ ...state, status: "downloaded" }));
          return;
        }
        const head = field(result.lines, "HEAD")[0] ?? "";
        const upstream = field(result.lines, "UPSTREAM")[0] ?? "";
        const count = Number(field(result.lines, "COUNT")[0] ?? "0");
        const builtCommit = forkBuildCommit(environment.appVersion);
        // A checkout that moved past the running build is also worth rebuilding.
        const checkoutAhead = builtCommit === null || !head.startsWith(builtCommit);
        if (count === 0 && !checkoutAhead) {
          yield* update((state) => ({
            ...state,
            status: "up-to-date",
            availableVersion: null,
            releaseNotes: [],
          }));
          return;
        }
        const availableVersion =
          count > 0 ? `upstream@${upstream.slice(0, 7)}` : `local@${head.slice(0, 7)}`;
        const items = field(result.lines, "COMMIT").slice(0, 20);
        yield* update((state) => ({
          ...state,
          status: "available",
          availableVersion,
          releaseNotes: [
            {
              version: availableVersion,
              items: items.length > 0 ? items : ["Rebuild from your local checkout"],
              totalItems: Math.max(count, items.length, 1),
            },
          ],
          omittedReleaseCount: 0,
          canRetry: false,
        }));
      }),
    );
    return Option.isSome(ran);
  });

  const failDownload = (message: string) =>
    update((state) => ({
      ...state,
      status: "error",
      downloadPercent: null,
      message,
      errorContext: "download",
      canRetry: true,
    }));

  const download = claim(
    "download",
    Effect.gen(function* () {
      yield* update((state) => ({
        ...state,
        status: "downloading",
        downloadPercent: 0,
        message: null,
        errorContext: null,
      }));
      const sync = yield* runScript(["sync"]);
      if (sync.code === EXIT_CONFLICT) {
        const repo = Option.match(source, { onNone: () => "", onSome: ({ repo }) => ` in ${repo}` });
        yield* logWarning("fork update stopped on a merge conflict", { error: sync.error });
        yield* failDownload(
          `Update stopped: ${sync.error ?? "merge conflict"}. The merge was aborted; resolve it${repo}, commit, then retry.`,
        );
        return false;
      }
      if (sync.code !== 0) {
        yield* failDownload(`Update stopped: ${sync.error ?? "git sync failed"}`);
        return false;
      }
      yield* update((state) => ({ ...state, downloadPercent: 5 }));
      const context = yield* Effect.context<never>();
      const build = yield* runScript(["build"], (line) => {
        const percent = Number(/^PROGRESS (\d+)/.exec(line)?.[1]);
        if (Number.isFinite(percent)) {
          void Effect.runPromiseWith(context)(
            update((state) => ({ ...state, downloadPercent: percent })),
          );
        }
      });
      const app = field(build.lines, "APP")[0];
      const version = field(build.lines, "VERSION")[0];
      if (build.code !== 0 || app === undefined || version === undefined) {
        yield* failDownload(`Build failed: ${build.error ?? "see the fork-updater log"}`);
        return false;
      }
      yield* Ref.set(builtAppRef, app);
      yield* logInfo("fork update built", { version, app });
      yield* update((state) => ({
        ...state,
        status: "downloaded",
        downloadPercent: 100,
        downloadedVersion: version,
      }));
      return true;
    }),
  );

  const install = (expectedVersion?: string) =>
    claim(
      "install",
      Effect.gen(function* () {
        const state = yield* Ref.get(stateRef);
        const app = yield* Ref.get(builtAppRef);
        if (
          app === null ||
          Option.isNone(scriptPath) ||
          (expectedVersion !== undefined && expectedVersion !== state.downloadedVersion)
        ) {
          return false;
        }
        // The installer outlives this process: it waits for the quit, swaps the app, relaunches.
        NodeChildProcess.spawn("/bin/bash", [scriptPath.value, "install", app, String(process.pid)], {
          detached: true,
          stdio: "ignore",
          env: process.env,
        }).unref();
        yield* electronApp.quit;
        return true;
      }),
    );

  const actionResult = (ran: Option.Option<boolean>) =>
    Ref.get(stateRef).pipe(
      Effect.map((state) => ({
        accepted: Option.isSome(ran),
        completed: Option.getOrElse(ran, () => false),
        state,
      })),
    );

  return DesktopUpdates.DesktopUpdates.of({
    getState: Ref.get(stateRef),
    isActionActive: Ref.get(activeRef).pipe(Effect.map((active) => active !== null)),
    isInstallActive: Ref.get(activeRef).pipe(Effect.map((active) => active === "install")),
    subscribe: Effect.gen(function* () {
      const subscription = yield* PubSub.subscribe(changes);
      return { latest: yield* Ref.get(stateRef), changes: Stream.fromSubscription(subscription) };
    }),
    emitState,
    disabledReason: Effect.succeed(Option.fromNullishOr(disabledMessage)),
    configure: Effect.gen(function* () {
      yield* Ref.set(desktopState.canInstallUpdates, disabledMessage === null);
      yield* emitState;
      if (disabledMessage !== null) return;
      yield* check("startup").pipe(
        Effect.repeat(Schedule.spaced(CHECK_INTERVAL)),
        Effect.forkScoped,
      );
    }),
    setChannel: () => Ref.get(stateRef),
    check: (reason) =>
      check(reason).pipe(
        Effect.flatMap((checked) =>
          Ref.get(stateRef).pipe(Effect.map((state) => ({ checked, state }))),
        ),
      ),
    download: download.pipe(Effect.flatMap(actionResult)),
    install: install().pipe(Effect.flatMap(actionResult)),
    installPrepared: (expectedVersion) =>
      install(expectedVersion).pipe(
        Effect.flatMap(actionResult),
        Effect.map((result) => ({ ...result, failed: false })),
      ),
  });
});

/** Fork builds update from source; every other build keeps the release updater. */
export const layer = Layer.unwrap(
  DesktopEnvironment.DesktopEnvironment.pipe(
    Effect.map((environment) =>
      isForkDesktopVersion(environment.appVersion)
        ? Layer.effect(DesktopUpdates.DesktopUpdates, make)
        : DesktopUpdates.layer,
    ),
  ),
);
