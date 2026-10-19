import { assert, it } from "@effect/vitest";
import {
  type ModelSelection,
  type OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  type RuntimeMode,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ProcessRunner from "../processRunner.ts";

import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as GitHubCredentials from "@t3tools/source-control-github/server/GitHubCredentials";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";

const projectId = ProjectId.make("project:runtime-policy");
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.5",
} satisfies ModelSelection;

function makeThread(input: {
  readonly now: DateTime.Utc;
  readonly worktreePath: string | null;
  readonly runtimeMode?: RuntimeMode;
}): OrchestrationV2AppThread {
  const threadId = ThreadId.make("thread:runtime-policy");
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId,
    title: "Runtime policy",
    providerInstanceId,
    modelSelection,
    runtimeMode: input.runtimeMode ?? "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: input.worktreePath,
    activeProviderThreadId: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: threadId,
    },
    forkedFrom: null,
    createdAt: input.now,
    updatedAt: input.now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

const grokInstanceId = ProviderInstanceId.make("grok");
const supportedRuntimeModesByInstance = new Map<ProviderInstanceId, ReadonlyArray<RuntimeMode>>([
  [grokInstanceId, ["approval-required", "auto", "full-access"]],
]);
const providerInstanceFor = (instanceId: ProviderInstanceId) =>
  ({
    snapshot: {
      getSnapshot: Effect.succeed({
        supportedRuntimeModes: supportedRuntimeModesByInstance.get(instanceId),
      } as ServerProvider),
    },
  }) as ProviderInstance;

const layerTest = RuntimePolicy.layerFromProjectStore.pipe(
  Layer.provide(
    Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
      getInstance: (instanceId) => Effect.succeed(providerInstanceFor(instanceId)),
      listInstances: Effect.succeed([]),
      listUnavailable: Effect.succeed([]),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(
    Layer.mock(ProjectStore.ProjectStoreV2)({
      get: () =>
        Effect.succeed(
          Option.some({
            projectId,
            title: "Project",
            workspaceRoot: "/project-root",
            defaultModelSelection: null,
            defaultThreadEnvMode: null,
            autoPull: false,
            faviconPath: null,
            projectIcon: null,
            scripts: [],
            createdAt: "2026-06-21T00:00:00.000Z",
            updatedAt: "2026-06-21T00:00:00.000Z",
            deletedAt: null,
          }),
        ),
    }),
  ),
);

it.layer(layerTest)("RuntimePolicyV2", (it) => {
  it.effect("uses the project root for local-checkout threads", () =>
    Effect.gen(function* () {
      const policy = yield* RuntimePolicy.RuntimePolicyV2;
      const now = yield* DateTime.now;
      const resolved = yield* policy.resolve({
        thread: makeThread({ now, worktreePath: null }),
        modelSelection,
      });
      assert.equal(resolved.cwd, "/project-root");
    }),
  );

  it.effect("prefers a provisioned worktree over the project root", () =>
    Effect.gen(function* () {
      const policy = yield* RuntimePolicy.RuntimePolicyV2;
      const now = yield* DateTime.now;
      const resolved = yield* policy.resolve({
        thread: makeThread({ now, worktreePath: "/project-worktree" }),
        modelSelection,
      });
      assert.equal(resolved.cwd, "/project-worktree");
    }),
  );

  it.effect("runs a mode the provider does not offer in Supervised", () =>
    Effect.gen(function* () {
      const policy = yield* RuntimePolicy.RuntimePolicyV2;
      const now = yield* DateTime.now;
      const modeFor = (instanceId: ProviderInstanceId, runtimeMode: RuntimeMode) =>
        policy
          .resolve({
            thread: makeThread({ now, worktreePath: null, runtimeMode }),
            modelSelection: { instanceId, model: "test-model" },
          })
          .pipe(Effect.map((resolved) => resolved.runtimeMode));

      assert.equal(yield* modeFor(grokInstanceId, "auto-accept-edits"), "approval-required");
      assert.equal(yield* modeFor(grokInstanceId, "auto"), "auto");
      assert.equal(yield* modeFor(grokInstanceId, "full-access"), "full-access");
      assert.equal(yield* modeFor(providerInstanceId, "auto-accept-edits"), "auto-accept-edits");
    }),
  );
});

const tokenReads: Array<string | null | undefined> = [];
const repositoryReads: Array<string> = [];

const layerWithAccounts = layerTest.pipe(
  Layer.provide(
    Layer.mergeAll(
      Layer.effect(
        RepositoryIdentityResolver.RepositoryIdentityResolver,
        RepositoryIdentityResolver.make(),
      ).pipe(
        Layer.provide(
          Layer.mock(ProcessRunner.ProcessRunner)({
            run: ({ args }) =>
              Effect.sync(() => {
                const cwd = args[1] ?? "";
                if (args.includes("rev-parse")) repositoryReads.push(cwd);
                const host =
                  cwd === "/enterprise-worktree" ? "github.acme-corp.example" : "github.com";
                const owner = cwd === "/project-root" ? "someone" : "acme";
                return {
                  stdout: args.includes("rev-parse")
                    ? `${cwd}\n`
                    : args.includes("remote")
                      ? `origin\thttps://${host}/${owner}/web.git (fetch)\n`
                      : "",
                  stderr: "",
                  code: ChildProcessSpawner.ExitCode(0),
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                };
              }),
          }),
        ),
      ),
      Layer.mock(GitHubCredentials.GitHubCredentials)({
        accountFor: (_host, repository) =>
          Effect.succeed(repository === "acme/web" ? "work" : undefined),
        get: (host, repository) =>
          Effect.sync(() => {
            tokenReads.push(repository);
            return {
              host,
              token: Redacted.make("work-token"),
              source: "gh" as const,
              fingerprint: "fingerprint",
              account: "work",
            };
          }),
      }),
    ),
  ),
);

it.layer(layerWithAccounts)("RuntimePolicyV2 with GitHub accounts", (it) => {
  it.effect("resolves repository credentials only when opening a provider session", () =>
    Effect.gen(function* () {
      const policy = yield* RuntimePolicy.RuntimePolicyV2;
      const now = yield* DateTime.now;
      const input = {
        thread: makeThread({ now, worktreePath: "/project-worktree" }),
        modelSelection,
      };
      const dispatch = yield* policy.resolve(input);
      assert.equal(dispatch.cwd, "/project-worktree");
      assert.deepEqual(repositoryReads, []);
      assert.deepEqual(tokenReads, []);
      assert.isUndefined(dispatch.environment);

      const company = yield* policy.resolve({
        ...input,
        includeEnvironment: true,
      });
      assert.deepEqual(ProviderAdapter.runtimePolicyEnvironment(company), {
        GH_TOKEN: "work-token",
      });
      assert.notInclude(JSON.stringify(company), "work-token");
      assert.deepEqual(repositoryReads, ["/project-worktree"]);

      const personal = yield* policy.resolve({
        thread: makeThread({ now, worktreePath: null }),
        modelSelection,
        includeEnvironment: true,
      });
      assert.isUndefined(personal.environment);
      assert.deepEqual(tokenReads, ["acme/web"]);

      // gh would send an Enterprise Server token to any host a command names, so none is handed
      // out there, even where a rule picks an account.
      const enterprise = yield* policy.resolve({
        thread: makeThread({ now, worktreePath: "/enterprise-worktree" }),
        modelSelection,
        includeEnvironment: true,
      });
      assert.isUndefined(enterprise.environment);
      assert.deepEqual(tokenReads, ["acme/web"]);
    }).pipe(Effect.provideService(HostProcess.Environment, {})),
  );
});

it.effect("opens agent sessions with the account selected by gh default or GH_REPO", () =>
  Effect.gen(function* () {
    const accounts = new Map([
      ["work/app", "work"],
      ["personal/notes", "personal"],
      ["research/dataset", "research"],
    ]);
    const processLayer = Layer.mock(ProcessRunner.ProcessRunner)({
      run: ({ args }) =>
        Effect.succeed({
          stdout: args.includes("rev-parse")
            ? "/project-worktree\n"
            : args.includes("--get-regexp")
              ? "remote.origin.gh-resolved base\n"
              : "origin\thttps://github.com/personal/notes.git (fetch)\nupstream\thttps://github.com/work/app.git (fetch)\n",
          stderr: "",
          code: ChildProcessSpawner.ExitCode(0),
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        }),
    });
    const dependencies = Layer.mergeAll(
      Layer.effect(
        RepositoryIdentityResolver.RepositoryIdentityResolver,
        RepositoryIdentityResolver.make(),
      ).pipe(Layer.provide(processLayer)),
      Layer.mock(GitHubCredentials.GitHubCredentials)({
        accountFor: (_host, repository) => Effect.succeed(accounts.get(repository ?? "")),
        get: (host, repository) => {
          const account = accounts.get(repository ?? "");
          return account === undefined
            ? Effect.fail(new GitHubCredentials.GitHubNotSignedInError({ host }))
            : Effect.succeed({
                host,
                account,
                token: Redacted.make(`${account}-token`),
                source: "gh" as const,
                fingerprint: account,
              });
        },
      }),
    );
    const environments = [];
    for (const environment of [{}, { GH_REPO: "research/dataset" }]) {
      const resolved = yield* Effect.gen(function* () {
        const policy = yield* RuntimePolicy.RuntimePolicyV2;
        const now = yield* DateTime.now;
        return yield* policy.resolve({
          thread: makeThread({ now, worktreePath: "/project-worktree" }),
          modelSelection,
          includeEnvironment: true,
        });
      }).pipe(
        Effect.provide(layerTest.pipe(Layer.provide(dependencies))),
        Effect.provideService(HostProcess.Environment, environment),
      );
      environments.push(ProviderAdapter.runtimePolicyEnvironment(resolved));
    }
    assert.deepEqual(environments, [
      { GH_TOKEN: "personal-token" },
      { GH_TOKEN: "research-token" },
    ]);
  }),
);
