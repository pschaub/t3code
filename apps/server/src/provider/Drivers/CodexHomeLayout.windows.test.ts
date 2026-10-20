import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import { materializeCodexShadowHome } from "./CodexHomeLayout.ts";

const makeHarness = Effect.fn("CodexHomeLayout.windows.makeHarness")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-windows-codex-home-" });
  const shared = path.join(root, "shared");
  const shadow = path.join(root, "shadow");
  yield* fs.makeDirectory(shared);
  yield* fs.makeDirectory(shadow);
  yield* fs.writeFileString(path.join(shared, "config.toml"), "shared config");
  yield* fs.writeFileString(path.join(shared, "AGENTS.md"), "shared instructions");
  yield* fs.writeFileString(path.join(shared, "auth.json"), "shared credentials");
  yield* fs.writeFileString(path.join(shadow, "auth.json"), "account credentials");
  const refusingFs: FileSystem.FileSystem = {
    ...fs,
    symlink: (target) =>
      Effect.fail(
        PlatformError.systemError({
          _tag: "Unknown",
          module: "FileSystem",
          method: "symlink",
          pathOrDescriptor: target,
          cause: Object.assign(new Error("symlink privilege not held"), { code: "EPERM" }),
        }),
      ),
  };
  const materialize = (platform: NodeJS.Platform = "win32") =>
    materializeCodexShadowHome({
      mode: "authOverlay",
      sharedHomePath: shared,
      effectiveHomePath: shadow,
      continuationKey: "test",
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, refusingFs),
      Effect.provideService(HostProcess.Platform, platform),
    );
  return { fs, path, shared, shadow, materialize };
});

it.effect(
  "sets up an account without symlink privilege, sharing directories and snapshotting configuration",
  () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      yield* h.materialize();
      expect(yield* h.fs.readFileString(h.path.join(h.shadow, "AGENTS.md"))).toBe(
        "shared instructions",
      );
      expect(yield* h.fs.readFileString(h.path.join(h.shadow, "config.toml"))).toBe(
        "shared config",
      );
      expect(yield* h.fs.readFileString(h.path.join(h.shadow, "auth.json"))).toBe(
        "account credentials",
      );
      yield* h.fs.writeFileString(
        h.path.join(h.shadow, "sessions", "test.jsonl"),
        "shared session",
      );
      expect(yield* h.fs.readFileString(h.path.join(h.shared, "sessions", "test.jsonl"))).toBe(
        "shared session",
      );
      expect(yield* h.fs.exists(h.path.join(h.shadow, ".t3-local-files"))).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "preserves account files across refreshes, new transient files, and shared configuration changes",
  () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      yield* h.materialize();
      yield* h.fs.writeFileString(h.path.join(h.shared, "config.toml"), "changed main config");
      yield* h.fs.writeFileString(h.path.join(h.shadow, "AGENTS.md"), "account instructions");
      yield* h.fs.writeFileString(h.path.join(h.shadow, "state_5.sqlite"), "account database");
      yield* h.fs.writeFileString(h.path.join(h.shared, "state_5.sqlite"), "main database");
      yield* h.fs.writeFileString(
        h.path.join(h.shared, "..codex-global-state.json.bak.tmp-test"),
        "transient",
      );
      yield* h.materialize();
      yield* h.materialize();
      expect(yield* h.fs.readFileString(h.path.join(h.shadow, "config.toml"))).toBe(
        "shared config",
      );
      expect(yield* h.fs.readFileString(h.path.join(h.shadow, "AGENTS.md"))).toBe(
        "account instructions",
      );
      expect(yield* h.fs.readFileString(h.path.join(h.shadow, "state_5.sqlite"))).toBe(
        "account database",
      );
      expect(yield* h.fs.readFileString(h.path.join(h.shared, "state_5.sqlite"))).toBe(
        "main database",
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("keeps existing account configuration and rejects a real directory conflict", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    yield* h.fs.writeFileString(h.path.join(h.shadow, "config.toml"), "existing account config");
    yield* h.materialize();
    expect(yield* h.fs.readFileString(h.path.join(h.shadow, "config.toml"))).toBe(
      "existing account config",
    );
    yield* h.fs.remove(h.path.join(h.shadow, "sessions"));
    yield* h.fs.makeDirectory(h.path.join(h.shadow, "sessions"));
    yield* h.fs.writeFileString(h.path.join(h.shadow, "sessions", "keep.txt"), "user data");
    expect((yield* Effect.flip(h.materialize()))._tag).toBe("CodexShadowHomeEntryConflictError");
    expect(yield* h.fs.readFileString(h.path.join(h.shadow, "sessions", "keep.txt"))).toBe(
      "user data",
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("keeps symlink failures visible outside Windows", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    expect((yield* Effect.flip(h.materialize("linux")))._tag).toBe(
      "CodexShadowHomeFileSystemError",
    );
    expect(yield* h.fs.exists(h.path.join(h.shadow, ".t3-local-files"))).toBe(false);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
