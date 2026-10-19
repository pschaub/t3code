import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProjectId,
  RunAttemptId,
  RunId,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import { pendingBackgroundTurnItems } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { resolveSelfInvocation } from "@t3tools/shared/nodeRuntime";
import * as Exit from "effect/Exit";
import * as DateTime from "effect/DateTime";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import {
  makeProviderReplayGate,
  type ProviderReplayGate,
} from "@t3tools/provider-testing/replayGate";
import type { ProviderContinuationRequest } from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ServerSettings from "../../serverSettings.ts";
import type {
  AcpRegistryAvailableCommands,
  AcpRegistryLiveConfiguration,
} from "@t3tools/provider-acp-registry/testing";
import * as AcpRegistrySupport from "@t3tools/provider-acp-registry/server/AcpRegistrySupport";
import * as AcpRegistryRuntimeCoordinator from "@t3tools/provider-acp-registry/server/AcpRegistryRuntimeCoordinator";
import { ACP_SESSION_MODE_OPTION_ID } from "@t3tools/provider-acp/server/sessionConfig";
import * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  decodeAcpReplayTranscript,
  makeAcpReplayCompletenessAssertion,
  makeAcpReplayRuntime,
} from "./AcpAdapterV2.testkit.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { BUILT_IN_PROVIDER_ADAPTER_DRIVER_KINDS_V2 } from "../builtInProviderAdapterDrivers.ts";
import { AcpRegistryAdapterV2Driver } from "@t3tools/provider-acp-registry/server";
import {
  ACP_REGISTRY_PROVIDER,
  makeAcpRegistryAdapterV2,
  acpRegistryPromptFailure,
} from "@t3tools/provider-acp-registry/testing";

const registryUrl = "https://registry.test/registry.json";
const decodeAcpRegistryAdapterSettings = Schema.decodeUnknownEffect(
  AcpRegistryAdapterV2Driver.configSchema,
);

const layerHost = TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer));

const layerRegistry = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) =>
    Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        Response.json({
          version: "1.0.0",
          agents: [
            {
              id: "fixture-agent",
              name: "Fixture Agent",
              version: "1.0.0",
              description: "ACP V2 adapter fixture",
              distribution: {
                binary: {
                  "darwin-aarch64": {
                    archive: "https://registry.test/unused",
                    cmd: "fixture-agent",
                    args: [],
                  },
                  "linux-x86_64": {
                    archive: "https://registry.test/unused",
                    cmd: "fixture-agent",
                    args: [],
                  },
                },
              },
            },
          ],
        }),
      ),
    ),
  ),
);

const layerInjectedRuntimeCatalog = Layer.mock(AcpRegistrySupport.AcpRegistryCatalog)({
  resolve: () => Effect.die("the runtime is injected"),
});

const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  layerHost,
  layerRegistry,
  ServerSettings.layerTest(),
);

const CLAUDE_ROOT_SESSION = "claude-session";

/** Replay frames of Claude Agent (claude-agent-acp 0.90.0) on ACP v2. */
const claudeFrames = {
  open: [
    { type: "expect_outbound", frame: { kind: "request", method: "initialize", params: "<any>" } },
    {
      type: "emit_inbound",
      frame: {
        kind: "response",
        method: "initialize",
        result: {
          protocolVersion: 2,
          info: { name: "@agentclientprotocol/claude-agent-acp", version: "0.90.0" },
          capabilities: { session: {} },
          authMethods: [],
        },
      },
    },
    { type: "expect_outbound", frame: { kind: "request", method: "session/new", params: "<any>" } },
    {
      type: "emit_inbound",
      frame: {
        kind: "response",
        method: "session/new",
        result: { sessionId: CLAUDE_ROOT_SESSION },
      },
    },
  ],
  update: (sessionId: string, sessionUpdate: Record<string, unknown>) => ({
    type: "emit_inbound",
    frame: {
      kind: "notification",
      method: "session/update",
      params: { sessionId, update: sessionUpdate },
    },
  }),
  childState: (parent: string, child: string, state: string) =>
    claudeFrames.update(parent, {
      sessionUpdate: "subagent_update",
      sessionId: child,
      state: { state },
    }),
  /** A prompt Claude takes in and works on. */
  prompt: (ordinal: number) => [
    {
      type: "expect_outbound",
      frame: { kind: "request", method: "session/prompt", params: "<any>" },
    },
    claudeFrames.update(CLAUDE_ROOT_SESSION, {
      sessionUpdate: "user_message",
      messageId: `user-${ordinal}`,
      content: [{ type: "text", text: "test prompt" }],
    }),
    {
      type: "emit_inbound",
      frame: {
        kind: "response",
        method: "session/prompt",
        result: { messageId: `user-${ordinal}` },
      },
    },
    claudeFrames.update(CLAUDE_ROOT_SESSION, { sessionUpdate: "state_update", state: "running" }),
  ],
  idle: (stopReason: string) =>
    claudeFrames.update(CLAUDE_ROOT_SESSION, {
      sessionUpdate: "state_update",
      state: "idle",
      stopReason,
    }),
  closeRequest: {
    type: "expect_outbound",
    frame: {
      kind: "request",
      method: "session/close",
      params: { sessionId: CLAUDE_ROOT_SESSION },
    },
  },
  closed: {
    type: "emit_inbound",
    frame: { kind: "response", method: "session/close", result: {} },
  },
};

const openClaudeReplaySession = Effect.fn("openClaudeReplaySession")(function* (
  entries: ReadonlyArray<unknown>,
  replayGate?: ProviderReplayGate,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const replayDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-claude-lifecycle-" });
  const transcript = yield* decodeAcpReplayTranscript(
    {
      provider: ACP_REGISTRY_PROVIDER,
      protocol: "acp.ndjson-jsonrpc",
      version: "1",
      scenario: "claude-lifecycle",
      entries: entries as never,
    },
    ACP_REGISTRY_PROVIDER,
  );
  const offers: Array<ProviderContinuationRequest> = [];
  const offered = yield* Deferred.make<void>();
  const continuationRequests = {
    offer: (request: ProviderContinuationRequest) =>
      Effect.sync(() => {
        offers.push(request);
      }).pipe(Effect.andThen(Deferred.succeed(offered, undefined)), Effect.asVoid),
  };
  const instanceId = ProviderInstanceId.make("claude-lifecycle");
  let onTermination: NonNullable<
    AcpSessionRuntime.AcpSessionRuntimeOptions["onTermination"]
  > = () => Effect.die("Runtime has not started");
  const replayRuntime = makeAcpReplayRuntime({
    transcript,
    statusPath: path.join(replayDir, "status.json"),
    scriptPath: yield* path.fromFileUrl(
      new URL("../../../scripts/acp-replay-agent.ts", import.meta.url),
    ),
    childProcessSpawner,
    fileSystem,
    ...(replayGate === undefined ? {} : { replayGate }),
  });
  const adapterOptions = {
    selfInvocation: yield* resolveSelfInvocation(),
    instanceId,
    settings: yield* decodeAcpRegistryAdapterSettings({ agentId: "claude-acp" }),
    environment: {},
    continuationRequests,
    makeRuntime: (runtimeInput: Parameters<typeof replayRuntime>[0]) => {
      onTermination = runtimeInput.onTermination;
      return replayRuntime(runtimeInput);
    },
  };
  const adapter = yield* makeAcpRegistryAdapterV2(adapterOptions).pipe(
    Effect.provide(layerInjectedRuntimeCatalog),
  );
  const threadId = ThreadId.make("thread-claude-lifecycle");
  const modelSelection = { instanceId, model: "default" } as const;
  const runtimeMode = "approval-required";
  const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
    runtimeMode,
    interactionMode: "default",
    cwd: replayDir,
  });
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("session-claude-lifecycle"),
    modelSelection,
    runtimePolicy,
  });
  yield* Effect.addFinalizer(() => Effect.sync(() => replayGate?.releaseAll()));
  assert.isDefined(
    runtime.hasPendingBackgroundWork,
    "Claude ACP v2 must report background work after foreground idle",
  );
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(events, event)),
    Effect.forkScoped,
  );
  const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
  const now = yield* DateTime.now;
  const turnInput = (
    ordinal: number,
    options: { readonly continuation?: boolean } = {},
  ): ProviderAdapter.ProviderAdapterV2TurnInput => ({
    appThread: {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId: ProjectId.make(`project-claude-lifecycle`),
      title: "Claude Agent",
      providerInstanceId: instanceId,
      modelSelection,
      runtimeMode,
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThread.id,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    threadId,
    runId: RunId.make(`run-claude-lifecycle-${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt-claude-lifecycle-${ordinal}`),
    rootNodeId: NodeId.make(`node-claude-lifecycle-${ordinal}`),
    providerThread,
    message: {
      ...(options.continuation === true
        ? { createdBy: "agent" as const, creationSource: "provider" as const }
        : { createdBy: "user" as const, creationSource: "web" as const }),
      messageId: MessageId.make(`message-claude-lifecycle-${ordinal}`),
      text: "test prompt",
      attachments: [],
    },
    modelSelection,
    runtimePolicy,
  });
  const until = (predicate: (event: ProviderAdapter.ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      const seen: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
      while (true) {
        const event = yield* Queue.take(events);
        seen.push(event);
        if (predicate(event)) return seen;
      }
    });
  const throughMarker = (title: string) =>
    until(
      (event) =>
        event.type === "provider_thread.updated" &&
        event.providerThread.nativeMetadata?.title === title,
    );
  const terminal = until((event) => event.type === "turn.terminal").pipe(
    Effect.map((seen) => {
      const event = seen.at(-1)!;
      if (event.type !== "turn.terminal") throw new Error("Missing terminal");
      return event;
    }),
  );
  return {
    runtime,
    providerThread,
    turnInput,
    until,
    throughMarker,
    terminal,
    offers,
    offered,
    terminate: () =>
      onTermination(
        new EffectAcpErrors.AcpTransportError({
          detail: "Injected fixture process exit",
          cause: "test",
        }),
      ),
  };
});

const claudeMarker = (title: string) =>
  claudeFrames.update(CLAUDE_ROOT_SESSION, {
    sessionUpdate: "session_info_update",
    title,
  });
const claudeChild = [
  claudeFrames.update(CLAUDE_ROOT_SESSION, {
    sessionUpdate: "tool_call_update",
    toolCallId: "agent-1",
    name: "Agent",
    title: "Review",
    kind: "think",
    status: "in_progress",
    rawInput: { prompt: "Review the code", description: "Review", run_in_background: true },
  }),
  claudeFrames.childState(CLAUDE_ROOT_SESSION, "child-1", "running"),
  claudeFrames.update("child-1", {
    sessionUpdate: "session_message",
    messageId: "agent-1",
    senderSessionId: CLAUDE_ROOT_SESSION,
    recipientSessionId: "child-1",
    content: [{ type: "text", text: "Review the code" }],
  }),
  claudeFrames.update(CLAUDE_ROOT_SESSION, {
    sessionUpdate: "tool_call_update",
    toolCallId: "agent-1",
    status: "completed",
    _meta: { claudeCode: { toolResponse: { isAsync: true } } },
  }),
];

describe("AcpRegistryAdapterV2", () => {
  it("preserves and sanitizes structured ACP errors without exposing arbitrary defects", () => {
    const limit = new EffectAcpErrors.AcpRequestError({
      code: -31001,
      errorMessage: "Rate limit exceeded for mistral (model: mistral-vibe-cli-latest).",
    });
    assert.deepEqual(acpRegistryPromptFailure("mistral-vibe", limit), {
      class: "usage_limit",
      message: limit.errorMessage,
      code: "-31001",
      retryable: null,
    });
    assert.equal(acpRegistryPromptFailure("other-agent", limit).class, "provider_error");
    assert.equal(
      acpRegistryPromptFailure("mistral-vibe", new Error("private defect")).message,
      "Provider turn failed.",
    );
    const rejected = acpRegistryPromptFailure(
      "any-agent",
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Request rejected. api_key=private-key https://example.test/?token=secret",
      }),
    );
    assert.include(rejected.message, "Request rejected.");
    assert.notInclude(rejected.message, "private-key");
    assert.notInclude(rejected.message, "token=secret");
  });
  it("is registered as a generic provider driver with schema defaults", () => {
    assert.isTrue(BUILT_IN_PROVIDER_ADAPTER_DRIVER_KINDS_V2.has(ACP_REGISTRY_PROVIDER));
    assert.equal(AcpRegistryAdapterV2Driver.driverKind, ACP_REGISTRY_PROVIDER);
    assert.deepEqual(AcpRegistryAdapterV2Driver.defaultConfig(), {
      source: "registry",
      commandArgs: [],
      enabled: true,
      agentId: "",
      commandPath: "",
      authMethodId: "",
      distribution: "auto",
      customModels: [],
    });
  });

  describe("the agent's own mode picker", () => {
    type Frame = Record<string, unknown>;
    const outbound = (method: string, params: unknown = "<any>"): Frame => ({
      type: "expect_outbound",
      frame: { kind: "request", method, params },
    });
    const answer = (method: string, result: unknown): Frame => ({
      type: "emit_inbound",
      frame: { kind: "response", method, result },
    });
    const permissionModeOption = (currentValue: string) => ({
      id: "permission-mode",
      name: "Permission mode",
      category: "mode",
      type: "select",
      currentValue,
      options: ["ask", "auto"].map((value) => ({ value, name: value })),
    });

    const openWithStoredModePick = Effect.fn("openWithStoredModePick")(function* (input: {
      readonly setup: unknown;
      readonly modeFrames: ReadonlyArray<Frame>;
      readonly storedModePick: string;
    }) {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const replayDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-acp-registry-mode-pick-",
      });
      const statusPath = path.join(replayDir, "status.json");
      const transcript = yield* decodeAcpReplayTranscript(
        {
          provider: ACP_REGISTRY_PROVIDER,
          protocol: "acp.ndjson-jsonrpc",
          version: "1",
          scenario: "stored-mode-pick",
          entries: [
            outbound("initialize"),
            answer("initialize", {
              protocolVersion: 1,
              agentCapabilities: { loadSession: false },
              authMethods: [{ id: "test", name: "Test" }],
            }),
            outbound("session/new"),
            answer("session/new", { sessionId: "agent-session", ...(input.setup as object) }),
            ...input.modeFrames,
          ] as never,
        },
        ACP_REGISTRY_PROVIDER,
      );
      const instanceId = ProviderInstanceId.make("acp-registry-mode-pick");
      const adapter = yield* makeAcpRegistryAdapterV2({
        selfInvocation: yield* resolveSelfInvocation(),
        instanceId,
        settings: yield* decodeAcpRegistryAdapterSettings({
          agentId: "fixture-agent",
          authMethodId: "test",
        }),
        environment: {},
        makeRuntime: makeAcpReplayRuntime({
          transcript,
          statusPath,
          scriptPath: yield* path.fromFileUrl(
            new URL("../../../scripts/acp-replay-agent.ts", import.meta.url),
          ),
          childProcessSpawner,
          fileSystem,
        }),
      }).pipe(Effect.provide(layerInjectedRuntimeCatalog));
      yield* adapter
        .openSession({
          threadId: ThreadId.make("thread-acp-registry-mode-pick"),
          providerSessionId: ProviderSessionId.make("provider-session-mode-pick"),
          modelSelection: {
            instanceId,
            model: "default",
            options: [{ id: ACP_SESSION_MODE_OPTION_ID, value: input.storedModePick }],
          },
          runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
            runtimeMode: "approval-required",
            interactionMode: "default",
            cwd: replayDir,
          }),
        })
        .pipe(Effect.scoped);
      yield* makeAcpReplayCompletenessAssertion(fileSystem, statusPath, transcript);
    });

    it.effect("switches an agent that only advertises modes with session/set_mode", () =>
      openWithStoredModePick({
        setup: {
          modes: {
            currentModeId: "default",
            availableModes: ["default", "autoEdit"].map((id) => ({ id, name: id })),
          },
        },
        modeFrames: [
          outbound("session/set_mode", { sessionId: "agent-session", modeId: "autoEdit" }),
          answer("session/set_mode", {}),
        ],
        storedModePick: "autoEdit",
      }).pipe(Effect.provide(layerTest), Effect.scoped),
    );

    it.effect("switches a mode config option under its own id", () =>
      openWithStoredModePick({
        setup: { configOptions: [permissionModeOption("ask")] },
        modeFrames: [
          outbound("session/set_config_option", {
            sessionId: "agent-session",
            configId: "permission-mode",
            value: "auto",
          }),
          answer("session/set_config_option", { configOptions: [permissionModeOption("auto")] }),
        ],
        storedModePick: "auto",
      }).pipe(Effect.provide(layerTest), Effect.scoped),
    );
  });

  it.effect("offers client terminals to Devin only and client fs to no registry agent", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const advertisedCapabilities = Effect.fn("advertisedCapabilities")(function* (
        agentId: string,
      ) {
        let clientCapabilities: unknown;
        const instanceId = ProviderInstanceId.make(`acp-registry-capabilities-${agentId}`);
        const adapter = yield* makeAcpRegistryAdapterV2({
          selfInvocation: yield* resolveSelfInvocation(),
          instanceId,
          settings: yield* decodeAcpRegistryAdapterSettings({ agentId, authMethodId: "test" }),
          environment: {},
          makeRuntime: (input) =>
            Effect.gen(function* () {
              clientCapabilities = input.clientCapabilities;
              const { processEnvironment: _processEnvironment, ...runtimeInput } = input;
              const context = yield* Layer.build(
                AcpSessionRuntime.layer({
                  ...runtimeInput,
                  spawn: {
                    command: process.execPath,
                    args: [mockAgentPath],
                    cwd: input.cwd,
                    env: { T3_ACP_SESSION_LIFECYCLE: "1" },
                  },
                  authMethodId: "test",
                }).pipe(
                  Layer.provide(
                    Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
                  ),
                ),
              );
              return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
                Effect.provide(context),
              );
            }),
        }).pipe(Effect.provide(layerInjectedRuntimeCatalog));
        const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
        });
        yield* adapter.openSession({
          threadId: ThreadId.make(`thread-acp-registry-capabilities-${agentId}`),
          providerSessionId: ProviderSessionId.make(`provider-session-capabilities-${agentId}`),
          modelSelection: { instanceId, model: "default" },
          runtimePolicy,
        });
        return clientCapabilities;
      });

      assert.deepInclude(yield* advertisedCapabilities("devin"), {
        fs: { readTextFile: false, writeTextFile: false },
        terminal: true,
      });
      assert.deepInclude(yield* advertisedCapabilities("gemini"), {
        fs: { readTextFile: false, writeTextFile: false },
        terminal: false,
      });
    }).pipe(Effect.provide(layerTest), Effect.scoped),
  );

  it.effect("launches Claude Agent on ACP v2 unless its instance environment decides", () =>
    Effect.gen(function* () {
      const launchSwitch = Effect.fn("launchSwitch")(function* (
        config: Record<string, unknown>,
        environment: NodeJS.ProcessEnv = {},
      ) {
        let launched: NodeJS.ProcessEnv | undefined;
        const instanceId = ProviderInstanceId.make("acp-registry-launch-environment");
        const adapter = yield* makeAcpRegistryAdapterV2({
          selfInvocation: yield* resolveSelfInvocation(),
          instanceId,
          settings: yield* decodeAcpRegistryAdapterSettings(config),
          environment,
        }).pipe(
          Effect.provide(
            Layer.mock(AcpRegistrySupport.AcpRegistryCatalog)({
              resolve: (_settings, _cwd, launchEnvironment) => {
                launched = launchEnvironment;
                return Effect.fail(
                  new AcpRegistrySupport.AcpRegistryError({
                    reason: "runner_unavailable",
                    detail: "Recorded the launch environment.",
                  }),
                );
              },
            }),
          ),
        );
        yield* adapter
          .openSession({
            threadId: ThreadId.make("thread-acp-registry-launch-environment"),
            providerSessionId: ProviderSessionId.make("provider-session-launch-environment"),
            modelSelection: { instanceId, model: "default" },
            runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: process.cwd(),
            }),
          })
          .pipe(Effect.flip);
        return launched?.CLAUDE_AGENT_ACP_EXPERIMENTAL_V2;
      });

      assert.equal(yield* launchSwitch({ agentId: "claude-acp" }), "1");
      assert.equal(
        yield* launchSwitch(
          { source: "local", commandPath: "/opt/bin/claude-agent-acp" },
          { PATH: "/opt/bin" },
        ),
        "1",
      );
      assert.equal(
        yield* launchSwitch({ agentId: "claude-acp" }, { CLAUDE_AGENT_ACP_EXPERIMENTAL_V2: "0" }),
        "0",
      );
      assert.isUndefined(yield* launchSwitch({ agentId: "gemini" }));
    }).pipe(Effect.provide(layerTest), Effect.scoped),
  );

  it.effect("settles Claude's accepted foreground reply while retaining child output", () =>
    Effect.gen(function* () {
      const gate = makeProviderReplayGate(["accepted", "child-output", "wake"]);
      const { update, idle, prompt } = claudeFrames;
      const session = yield* openClaudeReplaySession(
        [
          ...claudeFrames.open,
          prompt(1)[0],
          idle("end_turn"),
          claudeMarker("before acceptance"),
          { ...prompt(1)[1], label: "accepted" },
          ...prompt(1).slice(2),
          update(CLAUDE_ROOT_SESSION, {
            sessionUpdate: "tool_call_update",
            toolCallId: "agent-1",
            name: "Agent",
            title: "Review",
            kind: "think",
            status: "pending",
            rawInput: { description: "Review" },
          }),
          ...claudeChild,
          update(CLAUDE_ROOT_SESSION, {
            sessionUpdate: "agent_message_chunk",
            messageId: "reply",
            content: { type: "text", text: "The review is running." },
          }),
          idle("end_turn"),
          {
            ...update("child-1", {
              sessionUpdate: "agent_message_chunk",
              messageId: "child-reply",
              content: { type: "text", text: "Found the regression." },
            }),
            label: "child-output",
          },
          claudeFrames.childState(CLAUDE_ROOT_SESSION, "child-1", "idle"),
          claudeMarker("child output"),
          {
            ...update(CLAUDE_ROOT_SESSION, {
              sessionUpdate: "user_message",
              messageId: "wake-user",
              content: [{ type: "text", text: "Continue the review." }],
            }),
            label: "wake",
          },
          idle("end_turn"),
          claudeMarker("continuation idle"),
          claudeFrames.closeRequest,
          claudeFrames.closed,
        ],
        gate,
      );
      yield* session.runtime.startTurn(session.turnInput(1));
      const beforeAcceptance = yield* session.throughMarker("before acceptance");
      assert.isFalse(beforeAcceptance.some((event) => event.type === "turn.terminal"));
      gate.release("accepted");
      const foreground = yield* session.until((event) => event.type === "turn.terminal");
      const foregroundTerminal = foreground.at(-1)!;
      if (foregroundTerminal.type !== "turn.terminal")
        throw new Error("Missing foreground terminal");
      assert.equal(foregroundTerminal.status, "completed");
      assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);
      gate.release("child-output");
      const childOutput = yield* session.throughMarker("child output");
      const message = childOutput.find(
        (event) =>
          event.type === "message.updated" && event.message.text === "Found the regression.",
      );
      assert.isDefined(message, "child output survives foreground completion");
      if (message?.type === "message.updated") {
        assert.notEqual(message.message.threadId, session.turnInput(1).threadId);
      }
      assert.isFalse(childOutput.some((event) => event.type === "turn.terminal"));
      assert.isTrue(
        childOutput.some(
          (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
        ),
      );
      const rootItems = new Map(
        [...foreground, ...childOutput].flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.threadId === session.turnInput(1).threadId
            ? [[event.turnItem.id, event.turnItem] as const]
            : [],
        ),
      );
      const generic = [...rootItems.values()].filter((item) => item.type === "dynamic_tool");
      assert.lengthOf(generic, 1, "the early partial tool was projected");
      assert.isTrue(
        generic.every((item) => item.status === "completed"),
        "promotion settles the provisional generic tool",
      );
      assert.isFalse(
        [...rootItems.values()].some((item) => item.type === "user_message"),
        "the submitted root prompt is already owned by the app",
      );
      assert.deepEqual(pendingBackgroundTurnItems({ turnItems: [...rootItems.values()] }), []);
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
      gate.release("wake");
      yield* Deferred.await(session.offered);
      yield* session.throughMarker("continuation idle");
      const offer = session.offers[0]!;
      yield* offer.dispatchIfCurrent!(Effect.void);
      yield* session.runtime.startTurn(session.turnInput(2, { continuation: true }));
      const continuation = yield* session.until((event) => event.type === "turn.terminal");
      const terminal = continuation.at(-1)!;
      if (terminal.type !== "turn.terminal") throw new Error("Missing continuation terminal");
      assert.equal(
        terminal.status,
        "completed",
        "buffered acceptance preserves the latest live idle",
      );
      assert.isFalse(
        continuation.some(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "user_message",
        ),
        "autonomous root echoes only signal acceptance",
      );
    }).pipe(Effect.provide(layerTest), Effect.scoped),
  );

  it.effect.each([
    { kind: "approval", stopReason: "end_turn", status: "completed" },
    { kind: "question", stopReason: "error", status: "failed" },
  ] as const)("continues Claude's background $kind and preserves $status idle", (testCase) =>
    Effect.gen(function* () {
      const gate = makeProviderReplayGate(["wake"]);
      const method =
        testCase.kind === "approval" ? "session/request_permission" : "elicitation/create";
      const params =
        testCase.kind === "approval"
          ? {
              sessionId: "child-1",
              title: "Run tests?",
              subject: {
                type: "tool_call",
                toolCall: {
                  toolCallId: "test-command",
                  name: "Bash",
                  title: "Run tests",
                  kind: "execute",
                  status: "pending",
                  rawInput: { command: "vp test run" },
                },
              },
              options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }],
            }
          : {
              sessionId: "child-1",
              mode: "form",
              message: "Which file?",
              requestedSchema: {
                type: "object",
                properties: { file: { type: "string", title: "File", enum: ["a.ts", "b.ts"] } },
                required: ["file"],
              },
            };
      const session = yield* openClaudeReplaySession(
        [
          ...claudeFrames.open,
          ...claudeFrames.prompt(1),
          ...claudeChild,
          claudeFrames.idle("end_turn"),
          {
            ...claudeFrames.update(CLAUDE_ROOT_SESSION, {
              sessionUpdate: "state_update",
              state: "running",
            }),
            label: "wake",
          },
          claudeFrames.update(CLAUDE_ROOT_SESSION, {
            sessionUpdate: "user_message",
            messageId: "wake-user",
            content: [{ type: "text", text: "Continue the review." }],
          }),
          { type: "emit_inbound", frame: { kind: "request", method, params } },
          {
            type: "expect_outbound",
            frame: {
              kind: "response",
              method,
              result:
                testCase.kind === "approval"
                  ? { outcome: { outcome: "selected", optionId: "allow" } }
                  : { action: "accept", content: { file: "a.ts" } },
            },
          },
          claudeFrames.update(CLAUDE_ROOT_SESSION, {
            sessionUpdate: "state_update",
            state: "idle",
            stopReason: testCase.stopReason,
            ...(testCase.status === "failed"
              ? { error: { code: -32603, message: "API Error: 529 overloaded" } }
              : {}),
          }),
          claudeFrames.closeRequest,
          claudeFrames.closed,
        ],
        gate,
      );
      yield* session.runtime.startTurn(session.turnInput(1));
      assert.equal((yield* session.terminal).status, "completed");
      assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);
      gate.release("wake");
      yield* Deferred.await(session.offered);
      const offer = session.offers[0]!;
      assert.isDefined(offer.dispatchIfCurrent);
      yield* offer.dispatchIfCurrent!(Effect.void);
      yield* session.runtime.startTurn(session.turnInput(2, { continuation: true }));
      const seen = yield* session.until(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      assert.isFalse(
        seen.some(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "user_message",
        ),
      );
      const request = seen.at(-1)!;
      if (request.type !== "runtime_request.updated") throw new Error("Missing request");
      assert.isTrue(
        seen.some(
          (event) =>
            event.type === "node.updated" &&
            event.node.id === request.runtimeRequest.nodeId &&
            event.node.runId === session.turnInput(2).runId,
        ),
      );
      yield* session.runtime.respondToRuntimeRequest({
        requestId: request.runtimeRequest.id,
        ...(testCase.kind === "approval"
          ? { decision: "accept" as const }
          : { answers: { file: "a.ts" } }),
      });
      const terminal = yield* session.terminal;
      assert.equal(terminal.status, testCase.status);
      if (testCase.status === "failed") {
        assert.include(terminal.failure?.message ?? "", "529 overloaded");
      }
    }).pipe(Effect.provide(layerTest), Effect.scoped),
  );

  it.effect.each(["error", "end_turn"])(
    "hands live Claude child tools to the next run after %s and keeps nested output in the child",
    (stopReason) =>
      Effect.gen(function* () {
        const { update, childState, idle, prompt } = claudeFrames;
        const session = yield* openClaudeReplaySession([
          ...claudeFrames.open,
          ...prompt(1),
          ...claudeChild,
          update("child-1", {
            sessionUpdate: "tool_call_update",
            toolCallId: "child-tool",
            name: "Bash",
            title: "Read package",
            kind: "execute",
            status: "in_progress",
            rawInput: { command: "cat package.json" },
          }),
          idle(stopReason),
          ...prompt(2),
          childState(CLAUDE_ROOT_SESSION, "child-1", "running"),
          update("child-1", {
            sessionUpdate: "session_message",
            messageId: "resume-child",
            senderSessionId: CLAUDE_ROOT_SESSION,
            recipientSessionId: "child-1",
            content: [{ type: "text", text: "Continue the review." }],
          }),
          claudeMarker("resumed"),
          childState("child-1", "grandchild", "running"),
          update("grandchild", {
            sessionUpdate: "tool_call_update",
            toolCallId: "nested-tool",
            name: "Bash",
            title: "Read nested package",
            kind: "execute",
            status: "in_progress",
            rawInput: { command: "cat nested/package.json" },
          }),
          update("grandchild", {
            sessionUpdate: "agent_message_chunk",
            messageId: "nested-reply",
            content: { type: "text", text: "Nested output" },
          }),
          claudeMarker("nested"),
          idle("end_turn"),
          claudeFrames.closeRequest,
          claudeFrames.closed,
        ]);
        yield* session.runtime.startTurn(session.turnInput(1));
        const settled = yield* session.terminal;
        assert.equal(settled.status, stopReason === "error" ? "failed" : "completed");
        if (settled.status === "failed") assert.isTrue(settled.childTurnsContinue);
        yield* session.runtime.startTurn(session.turnInput(2));
        const resumed = yield* session.throughMarker("resumed");
        const row = resumed.find((event) => event.type === "subagent.updated");
        assert.isDefined(row);
        if (row?.type !== "subagent.updated") throw new Error("Missing resumed child");
        assert.equal(row.subagent.runId, session.turnInput(2).runId);
        assert.lengthOf(
          resumed.filter(
            (event) =>
              event.type === "message.updated" &&
              event.message.threadId === row.subagent.childThreadId &&
              event.message.role === "user" &&
              event.message.text === "Continue the review.",
          ),
          1,
          "child resume prompts remain visible once",
        );
        const openTools = resumed.filter(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.status === "running",
        );
        assert.lengthOf(openTools, 1, "handoff announces each open child tool once");
        const nested = yield* session.throughMarker("nested");
        const messages = nested.filter(
          (event) => event.type === "message.updated" && event.message.text === "Nested output",
        );
        assert.lengthOf(messages, 1);
        if (messages[0]?.type === "message.updated") {
          assert.equal(messages[0].message.threadId, row.subagent.childThreadId);
        }
        const nestedTool = nested.find(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
        );
        assert.isDefined(nestedTool);
        if (nestedTool?.type === "turn_item.updated") {
          assert.equal(nestedTool.turnItem.threadId, row.subagent.childThreadId);
        }
        assert.equal((yield* session.terminal).status, "completed");
        assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.provide(layerTest), Effect.scoped),
  );

  it.effect("fails active Claude work and releases its children when the process exits", () =>
    Effect.gen(function* () {
      const session = yield* openClaudeReplaySession([
        ...claudeFrames.open,
        ...claudeFrames.prompt(1),
        ...claudeChild,
        claudeMarker("child ready"),
      ]);
      yield* session.runtime.startTurn(session.turnInput(1));
      yield* session.throughMarker("child ready");
      yield* session.terminate();
      const terminal = yield* session.terminal;
      assert.equal(terminal.status, "failed");
      assert.equal(terminal.failure?.class, "transport_error");
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
      const settled = yield* openClaudeReplaySession([
        ...claudeFrames.open,
        ...claudeFrames.prompt(1),
        ...claudeChild,
        claudeFrames.childState("child-1", "grandchild", "running"),
        claudeFrames.update("grandchild", {
          sessionUpdate: "tool_call_update",
          toolCallId: "nested-tool",
          name: "Bash",
          kind: "execute",
          status: "in_progress",
          rawInput: { command: "cat package.json" },
        }),
        claudeFrames.childState(CLAUDE_ROOT_SESSION, "child-1", "idle"),
        claudeFrames.idle("end_turn"),
      ]);
      yield* settled.runtime.startTurn(settled.turnInput(1));
      const foreground = yield* settled.until((event) => event.type === "turn.terminal");
      const completed = foreground.at(-1)!;
      if (completed.type !== "turn.terminal") throw new Error("Missing settled root terminal");
      assert.equal(completed.status, "completed");
      assert.isTrue(yield* settled.runtime.hasPendingBackgroundWork!);
      yield* settled.terminate();
      assert.isFalse(
        yield* settled.runtime.hasPendingBackgroundWork!,
        "exit releases visible owners held open by nested work",
      );
      const exited = yield* settled.until(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "subagent" &&
          event.turnItem.status === "failed",
      );
      assert.isTrue(
        exited.some(
          (event) => event.type === "subagent.updated" && event.subagent.status === "failed",
        ),
      );
      const items = new Map(
        [...foreground, ...exited].flatMap((event) =>
          event.type === "turn_item.updated" ? [[event.turnItem.id, event.turnItem] as const] : [],
        ),
      );
      assert.deepEqual(pendingBackgroundTurnItems({ turnItems: [...items.values()] }), []);
      assert.isTrue(
        exited.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.status === "interrupted",
        ),
      );
    }).pipe(Effect.provide(layerTest), Effect.scoped),
  );

  it.effect("keeps Claude background work after a refused Stop and closes it on retry", () =>
    Effect.gen(function* () {
      const session = yield* openClaudeReplaySession([
        ...claudeFrames.open,
        ...claudeFrames.prompt(1),
        ...claudeChild,
        claudeFrames.idle("end_turn"),
        claudeFrames.closeRequest,
        {
          type: "emit_inbound",
          frame: {
            kind: "response",
            method: "session/close",
            error: { code: -32603, message: "Close refused" },
          },
        },
        claudeFrames.closeRequest,
        claudeFrames.closed,
      ]);
      yield* session.runtime.startTurn(session.turnInput(1));
      const ended = yield* session.terminal;
      const stop = {
        providerThread: session.providerThread,
        providerTurnId: ended.providerTurnId,
        requestRuntimeRestart: true,
      };
      const refused = yield* session.runtime.interruptTurn(stop).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(refused), "a refused close must not report a successful Stop");
      assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);
      yield* session.runtime.interruptTurn(stop);
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.provide(layerTest), Effect.scoped),
  );

  it.effect("opens a real ACP child process resolved from registry configuration", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const host = yield* ProviderHost.ProviderHost;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const resolver = yield* AcpRegistrySupport.makeAcpRegistryCatalog({
        cacheDir: host.paths.providerStatusCacheDir,
        toolsDir: host.paths.baseDir + "/tools",
        registryUrl,
      });
      const settings = yield* decodeAcpRegistryAdapterSettings({
        agentId: "fixture-agent",
        commandPath: process.execPath,
        authMethodId: "test",
      });
      let startupActive = false;
      let startupCount = 0;
      const instanceId = ProviderInstanceId.make("acp-registry-fixture");
      const commandsPublished = yield* Deferred.make<{
        readonly instanceId: ProviderInstanceId;
        readonly commands: AcpRegistryAvailableCommands;
      }>();
      const configurationPublished = yield* Deferred.make<AcpRegistryLiveConfiguration>();
      const adapter = yield* makeAcpRegistryAdapterV2({
        selfInvocation: yield* resolveSelfInvocation(),
        instanceId,
        settings,
        environment: {
          T3_ACP_SESSION_LIFECYCLE: "1",
          T3_ACP_COMMAND_ADVERTISEMENT_DELAY_MS: "750",
        },
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator)({
              withForegroundStartup: (agentId, effect) =>
                Effect.acquireUseRelease(
                  Effect.sync(() => {
                    assert.equal(agentId, "fixture-agent");
                    startupActive = true;
                    startupCount += 1;
                  }),
                  () => effect,
                  () =>
                    Effect.sync(() => {
                      startupActive = false;
                    }),
                ),
              runBackgroundProbe: (_agentId, effect) => effect.pipe(Effect.map(Option.some)),
              withSessionMutation: (effect) => effect,
              clearAvailableCommands: () => Effect.void,
              publishAvailableCommands: (publishedInstanceId, commands) =>
                Deferred.succeed(commandsPublished, {
                  instanceId: publishedInstanceId,
                  commands,
                }).pipe(Effect.asVoid),
              getAvailableCommands: () => Effect.succeed(Option.none()),
              watchAvailableCommands: () => Effect.never,
              clearLiveConfiguration: () => Effect.void,
              publishLiveConfiguration: (_publishedInstanceId, configuration) =>
                Deferred.succeed(configurationPublished, configuration).pipe(Effect.asVoid),
              getLiveConfiguration: () => Effect.succeed(Option.none()),
              watchLiveConfiguration: () => Effect.never,
              requestUrlAuthentication: () => Effect.succeed(false),
              acceptUrlAuthentication: () => Effect.succeed(false),
              getUrlAuthAction: () => Effect.succeed(Option.none()),
              watchUrlAuthAction: () => Effect.never,
            }),
            Layer.mock(AcpRegistrySupport.AcpRegistryCatalog)({
              resolve: (configuredSettings, cwd, environment) =>
                Effect.sync(() => assert.isTrue(startupActive)).pipe(
                  Effect.andThen(resolver.resolve(configuredSettings, cwd, environment)),
                  Effect.map((resolved) => ({
                    ...resolved,
                    spawn: {
                      ...resolved.spawn,
                      args: [mockAgentPath],
                    },
                  })),
                ),
            }),
          ),
        ),
      );
      const threadId = ThreadId.make("thread-acp-registry-fixture");
      const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const modelSelection = { instanceId, model: "default" } as const;
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-acp-registry-fixture"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });

      assert.equal(runtime.providerSession.driver, "acpRegistry");
      assert.equal(startupCount, 1);
      assert.isFalse(startupActive);
      assert.equal(providerThread.nativeThreadRef?.nativeId, "mock-session-1");
      assert.equal(providerThread.nativeMetadata?.itemIdentityVersion, 2);
      assert.isTrue(runtime.providerSession.capabilities.threads.canReadThreadSnapshot);
      assert.isTrue(runtime.providerSession.capabilities.threads.canForkThread);
      assert.deepEqual(yield* Deferred.await(commandsPublished), {
        instanceId,
        commands: {
          slashCommands: [
            {
              name: "review",
              description: "Review the current changes",
              input: { hint: "focus" },
            },
          ],
          skills: [
            {
              name: "workspace-skill",
              description: "Run the workspace skill",
              path: "acp://skill/workspace-skill",
              scope: "agent",
              enabled: true,
            },
          ],
        },
      });
      const configuration = yield* Deferred.await(configurationPublished);
      assert.equal(configuration.currentModelId, "default");
      assert.deepInclude(configuration.models[0], {
        id: "default",
        name: "Auto",
        description: null,
      });
    }).pipe(Effect.provide(layerTest), Effect.scoped),
  );
});
