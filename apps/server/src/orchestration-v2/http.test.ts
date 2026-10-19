import {
  AuthSessionId,
  type AuthEnvironmentScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  EnvironmentId,
  EventId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  OrchestrationV2AppThread,
  OrchestrationV2ShellSnapshot,
  OrchestrationV2ShellSnapshotJson,
  type ThreadPullRequestLink,
  OrchestrationV2TurnItem,
  OrchestrationV2ThreadTranscript,
  PROVIDER_SEND_TURN_MAX_FILE_BYTES,
  ThreadId,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import { threadTranscriptHeader } from "@t3tools/shared/threadTranscript";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import { ProjectEnrichmentService } from "../project/ProjectEnrichmentService.ts";
import * as OrchestrationHttp from "./http.ts";
import { EventSinkV2 } from "./EventSink.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

class OrchestrationTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.orchestration,
) {}
const decodeTranscript = Schema.decodeUnknownEffect(
  Schema.toCodecJson(OrchestrationV2ThreadTranscript),
);

const decodeShellSnapshot = Schema.decodeUnknownEffect(
  Schema.toCodecJson(OrchestrationV2ShellSnapshot),
);

const decodeShellSnapshotWire = Schema.decodeUnknownEffect(
  Schema.toEncoded(OrchestrationV2ShellSnapshotJson),
);

const now = DateTime.makeUnsafe("2026-09-29T00:00:00.000Z");
const environmentId = EnvironmentId.make("source-بيئة");
const output = "Full tool output. ".repeat(4_000);
const thread = Schema.decodeUnknownSync(OrchestrationV2AppThread)({
  id: "source-thread",
  projectId: "source-project",
  title: "Source 界\nconversation",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "gpt-5.4" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { rootThreadId: "source-thread", parentThreadId: null, relationshipToParent: null },
  forkedFrom: null,
  createdBy: "user",
  creationSource: "web",
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
});
const items = Schema.decodeUnknownSync(Schema.Array(OrchestrationV2TurnItem))(
  Array.from({ length: 200 }, (_, position) => ({
    id: `item-${position}`,
    threadId: "source-thread",
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: position,
    type: "command_execution",
    status: "completed",
    title: "Read notes",
    input: "cat notes.md",
    output: position === 0 ? output : "Done",
    startedAt: now,
    completedAt: now,
    updatedAt: now,
  })),
);

const makeHttpHarness = Effect.fnUntraced(function* (allowed = () => true) {
  const runtime = ProviderReplayHarness.layerWithRegistry(
    { name: "orchestration-http" },
    ProviderAdapterRegistry.layerFromAdapters([]),
    { runEffectWorker: false },
  );
  const services = yield* Layer.build(
    Layer.mergeAll(
      runtime,
      ThreadManagementService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(runtime, CommandReceiptStore.layer, ServerSettings.layerTest({})),
        ),
      ),
      OrchestrationEventStore.layer,
      ProjectStore.layer,
    ).pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
  );
  const eventSink = Context.get(services, EventSinkV2);
  const routes = HttpApiBuilder.layer(OrchestrationTestApi).pipe(
    Layer.provide(OrchestrationHttp.layer),
    Layer.provide(
      Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(environmentId),
      }),
    ),
    Layer.provide(
      Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
        Effect.suspend(() =>
          effect.pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, {
              sessionId: AuthSessionId.make("test-session"),
              subject: "test",
              method: "bearer-access-token",
              scopes: new Set<AuthEnvironmentScope>(allowed() ? ["orchestration:read"] : []),
            }),
          ),
        ),
      ),
    ),
    Layer.provide(Layer.succeedContext(services)),
    Layer.provide(Layer.mock(ProjectEnrichmentService)({})),
    Layer.provide(HttpPlatform.layer),
    Layer.provide(Etag.layerWeak),
    Layer.provide(NodeServices.layer),
  );
  const { handler, dispose } = HttpRouter.toWebHandler(routes, { disableLogger: true });
  yield* Effect.addFinalizer(() => Effect.promise(dispose));
  const request = (path: string) =>
    Effect.promise(() =>
      handler(
        new Request(`http://localhost${path}`, {
          headers: { [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT },
        }),
      ),
    );
  return { services, eventSink, request };
});

it.effect("exports full history with size, scope, and missing-thread checks", () =>
  Effect.gen(function* () {
    let allowed = true;
    const { services, eventSink, request } = yield* makeHttpHarness(() => allowed);
    yield* eventSink.write({
      events: [
        {
          id: EventId.make("create-source"),
          type: "thread.created",
          threadId: thread.id,
          occurredAt: now,
          payload: thread,
        },
      ],
    });
    const read = (threadId: ThreadId) =>
      request(`/api/orchestration/threads/${threadId}/transcript`);

    const emptyResponse = yield* read(thread.id);
    expect(emptyResponse.status).toBe(200);
    expect(
      (yield* decodeTranscript(yield* Effect.promise(() => emptyResponse.json()))).items,
    ).toEqual([]);
    yield* eventSink.write({
      events: items.map((item) => ({
        id: EventId.make(`create-${item.id}`),
        type: "turn-item.updated" as const,
        threadId: thread.id,
        occurredAt: now,
        payload: item,
      })),
    });

    const response = yield* read(thread.id);
    expect(response.status).toBe(200);
    const transcript = yield* decodeTranscript(yield* Effect.promise(() => response.json()));
    expect(transcript.items).toHaveLength(200);
    expect(transcript.items[0]?.item).toMatchObject({ output });
    expect(transcript.items[0]?.visibility).toBe("local");
    expect(transcript.items[199]?.position).toBe(199);

    const emptyOutputRows = transcript.items.map((row, position) =>
      position === 0 ? { ...row, item: { ...row.item, output: "" } } : row,
    );
    const header = threadTranscriptHeader(environmentId, transcript);
    const baseSize =
      Buffer.byteLength(header, "utf8") +
      emptyOutputRows.reduce(
        (size, row) => size + Buffer.byteLength(`${JSON.stringify(row)}\n`, "utf8"),
        0,
      );
    const boundaryOutput = "界" + "x".repeat(PROVIDER_SEND_TURN_MAX_FILE_BYTES - baseSize - 3);
    for (const [suffix, expectedStatus] of [
      ["", 200],
      ["x", 400],
    ] as const) {
      yield* eventSink.write({
        events: [
          {
            id: EventId.make(`boundary-${expectedStatus}`),
            type: "turn-item.updated",
            threadId: thread.id,
            occurredAt: now,
            payload: {
              ...items.find((item) => item.type === "command_execution")!,
              output: boundaryOutput + suffix,
            },
          },
        ],
      });
      const response = yield* read(thread.id);
      expect(response.status).toBe(expectedStatus);
      if (expectedStatus === 400) {
        const error = yield* Context.get(services, ThreadManagementService.ThreadManagementService)
          .getThreadTranscript(thread.id)
          .pipe(
            Effect.provideService(ServerEnvironment.ServerEnvironmentIdentity, {
              getEnvironmentId: Effect.succeed(environmentId),
            }),
            Effect.flip,
          );
        expect(error).toBeInstanceOf(ThreadManagementService.ThreadTranscriptTooLargeError);
        expect(yield* Effect.promise(() => response.json())).toMatchObject({
          _tag: "EnvironmentRequestInvalidError",
          reason: "thread_transcript_too_large",
        });
      }
    }

    expect((yield* read(ThreadId.make("missing"))).status).toBe(404);
    allowed = false;
    expect((yield* read(thread.id)).status).toBe(403);
  }),
);

it.effect("deduplicates equal pull request lists only in opted-in HTTP shell snapshots", () =>
  Effect.gen(function* () {
    const { eventSink, request } = yield* makeHttpHarness();
    const link = (number: number): ThreadPullRequestLink => ({
      host: "github.com",
      repository: "pingdotgg/t3code",
      number,
      url: `https://github.com/pingdotgg/t3code/pull/${number}`,
      source: "agent",
      linkedAt: "2026-09-29T00:00:00.000Z",
      snapshot: null,
      stack: null,
    });
    const lists = [[link(1), link(2)], [link(1), link(2)], [link(1)]];
    yield* eventSink.write({
      events: lists.map((pullRequests, index) => {
        const id = ThreadId.make(`shell-${index}`);
        return {
          id: EventId.make(`create-${id}`),
          type: "thread.created" as const,
          threadId: id,
          occurredAt: now,
          payload: { ...thread, id, pullRequests },
        };
      }),
    });

    const defaultResponse = yield* request("/api/orchestration/shell");
    expect(defaultResponse.status).toBe(200);
    const json = yield* Effect.promise(() => defaultResponse.json());
    const full = yield* decodeShellSnapshotWire(json);
    const decoded = yield* decodeShellSnapshot(json);
    expect(decoded.threads).toHaveLength(3);
    for (const [index, links] of lists.entries()) {
      expect(decoded.threads.find((row) => row.id === `shell-${index}`)?.pullRequests).toEqual(
        links,
      );
    }
    expect(full.threads.every((row) => !("pullRequestReference" in row))).toBe(true);

    const response = yield* request("/api/orchestration/shell?deduplicatePullRequests=true");
    expect(response.status).toBe(200);
    const compact = yield* Effect.promise(() => response.json());
    const duplicateIndices = decoded.threads.flatMap((row, index) =>
      row.id === "shell-0" || row.id === "shell-1" ? [index] : [],
    );
    const [first, duplicate] = duplicateIndices;
    expect(first).toBeDefined();
    expect(duplicate).toBeDefined();
    expect(compact).toEqual({
      ...full,
      threads: full.threads.map((row, index) => {
        if (index !== duplicate) return row;
        const { pullRequests: _links, ...rest } = row;
        return { ...rest, pullRequestReference: first };
      }),
    });
    const disabled = yield* request("/api/orchestration/shell?deduplicatePullRequests=false");
    expect(disabled.status).toBe(200);
    expect(yield* Effect.promise(() => disabled.json())).toEqual(full);
  }),
);
