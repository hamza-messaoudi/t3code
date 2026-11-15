/**
 * ForkSourceUpdates - the update button of a fork build. Instead of downloading releases, it runs
 * scripts/fork/fork-update.sh in the checkout the app was built from: merge the fork and upstream,
 * build the app, swap it in. A merge conflict stops the update and leaves the checkout as it was.
 *
 * @module ForkSourceUpdates
 */
import type { DesktopUpdateState } from "@t3tools/contracts";
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import * as DateTime from "effect/DateTime";
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
import { ChildProcess, ChildProcessSpawner } from "effect/process";

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
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
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

  const scriptEnv = (repoBranch: string) => ({
    T3_FORK_BRANCH: repoBranch,
    T3_FORK_HOME: environment.baseDir,
  });

  const runScript = Effect.fn("ForkSourceUpdates.runScript")(function* (
    args: ReadonlyArray<string>,
    onLine?: (line: string) => Effect.Effect<void>,
  ) {
    if (Option.isNone(scriptPath) || Option.isNone(source)) {
      return { code: -1, lines: [], error: disabledMessage } satisfies ScriptResult;
    }
    const command = ChildProcess.make("/bin/bash", [scriptPath.value, ...args], {
      cwd: source.value.repo,
      env: scriptEnv(source.value.branch),
      extendEnv: true,
      stdout: "pipe",
      stderr: "pipe",
    });
    const lines: string[] = [];
    let error: string | null = null;
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* spawner.spawn(command);
        const toLines = (stream: typeof handle.stdout) =>
          stream.pipe(Stream.decodeText(), Stream.splitLines);
        const [, , exitCode] = yield* Effect.all(
          [
            Stream.runForEach(toLines(handle.stdout), (line) => {
              lines.push(line);
              return onLine?.(line) ?? Effect.void;
            }),
            Stream.runForEach(toLines(handle.stderr), (line) =>
              Effect.sync(() => {
                if (line.startsWith("ERROR ")) error = line.slice("ERROR ".length);
              }),
            ),
            handle.exitCode,
          ],
          { concurrency: "unbounded" },
        );
        return { code: Number(exitCode), lines, error } satisfies ScriptResult;
      }),
    ).pipe(
      Effect.catch((cause) =>
        Effect.succeed({ code: -1, lines, error: error ?? cause.message } satisfies ScriptResult),
      ),
    );
  });

  /** Claims the updater for one action; `null` when another action holds it. */
  const claim = <A>(action: "check" | "download" | "install", effect: Effect.Effect<A>) =>
    Ref.modify(activeRef, (active) => (active === null ? [true, action] : [false, active])).pipe(
      Effect.flatMap((claimed) =>
        claimed
          ? effect.pipe(Effect.asSome, Effect.ensuring(Ref.set(activeRef, null)))
          : Effect.succeedNone,
      ),
    );

  const check = Effect.fn("ForkSourceUpdates.check")(function* (reason: string) {
    if (disabledMessage !== null) return false;
    yield* Effect.annotateCurrentSpan({ reason });
    const ran = yield* claim(
      "check",
      Effect.gen(function* () {
        const checkedAt = DateTime.formatIso(yield* DateTime.now);
        const previous = yield* update((state) => ({
          ...state,
          status: state.downloadedVersion === null ? "checking" : state.status,
          checkedAt,
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
        const repo = Option.match(source, {
          onNone: () => "",
          onSome: ({ repo }) => ` in ${repo}`,
        });
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
      const build = yield* runScript(["build"], (line) => {
        const percent = Number(/^PROGRESS (\d+)/.exec(line)?.[1]);
        return Number.isFinite(percent)
          ? update((state) => ({ ...state, downloadPercent: percent })).pipe(Effect.asVoid)
          : Effect.void;
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
        // The installer must outlive this app: nohup + & lets it wait for the quit, swap, relaunch.
        const installer = ChildProcess.make(
          "/bin/bash",
          [
            "-c",
            'nohup /bin/bash "$0" install "$1" "$2" >"$3" 2>&1 &',
            scriptPath.value,
            app,
            String(process.pid),
            path.join(environment.logDir, "fork-install.log"),
          ],
          { stdout: "ignore", stderr: "ignore" },
        );
        const started = yield* Effect.scoped(
          spawner.spawn(installer).pipe(Effect.flatMap((handle) => handle.exitCode)),
        ).pipe(Effect.option);
        if (Option.isNone(started)) return false;
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
    setChannel: Effect.fn("ForkSourceUpdates.setChannel")(function* () {
      return yield* Ref.get(stateRef);
    }),
    check: Effect.fn("ForkSourceUpdates.checkResult")(function* (reason: string) {
      const checked = yield* check(reason);
      return { checked, state: yield* Ref.get(stateRef) };
    }),
    download: download.pipe(Effect.flatMap(actionResult)),
    install: install().pipe(Effect.flatMap(actionResult)),
    installPrepared: Effect.fn("ForkSourceUpdates.installPrepared")(function* (
      expectedVersion: string,
    ) {
      return { ...(yield* actionResult(yield* install(expectedVersion))), failed: false };
    }),
  });
});

/** Fork builds update from source; every other build keeps the release updater. */
export const layer = Layer.effect(
  DesktopUpdates.DesktopUpdates,
  Effect.gen(function* () {
    const environment = yield* DesktopEnvironment.DesktopEnvironment;
    return isForkDesktopVersion(environment.appVersion) ? yield* make : yield* DesktopUpdates.make;
  }),
);
