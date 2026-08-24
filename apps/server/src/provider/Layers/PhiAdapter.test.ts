import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type PhiSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import { makePhiAdapter } from "./PhiAdapter.ts";
import { makeFakePhiSpawner } from "./PhiRpcTestFixtures.ts";

const settings = (overrides: Partial<PhiSettings> = {}): PhiSettings => ({
  enabled: true,
  binaryPath: "",
  homePath: "",
  ...overrides,
});
const threadId = (value: string) => ThreadId.make(value);
const provider = ProviderDriverKind.make("phi");
const instanceId = ProviderInstanceId.make("phi");
const serverConfigTestLayer = ServerConfig.layerTest(process.cwd(), process.cwd()).pipe(
  Layer.provide(NodeServices.layer),
);

const provideRuntime = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
) =>
  effect.pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provide(serverConfigTestLayer),
  );

it.effect("spawns Phi RPC with cwd, config environment, title, and selected model", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const adapter = yield* provideRuntime(
        makePhiAdapter(settings({ homePath: "/config/phi" }), {
          instanceId,
          environment: { PATH: "/tools" },
        }),
        fake.service,
      );
      const session = yield* adapter.startSession({
        provider,
        providerInstanceId: instanceId,
        threadId: threadId("thread-start"),
        runtimeMode: "full-access",
        cwd: "/workspace",
        title: "Fix transport",
        modelSelection: {
          instanceId,
          model: "example/model-one",
        },
      });
      const child = fake.children[0];
      NodeAssert.ok(child);

      NodeAssert.equal(child.command.command, "phi");
      NodeAssert.deepEqual(child.command.args, [
        "--mode",
        "rpc",
        "--name",
        "Fix transport",
        "--model",
        "example/model-one",
      ]);
      NodeAssert.equal(child.command.options.cwd, "/workspace");
      NodeAssert.equal(child.command.options.env?.PI_CODING_AGENT_DIR, "/config/phi");
      NodeAssert.equal(child.command.options.env?.PHI_CODING_AGENT_DIR, "/config/phi");
      NodeAssert.equal(session.status, "ready");
      NodeAssert.equal(session.model, "example/model-one");
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "session-1",
        sessionPath: "/tmp/phi-session-1.jsonl",
      });
      NodeAssert.deepEqual(
        child.commands.slice(0, 2).map((command) => command.type),
        ["new_session", "get_state"],
      );
    }),
  ),
);

it.effect("streams assistant text deltas and completes the turn on agent_settled", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const adapter = yield* provideRuntime(
        makePhiAdapter(settings(), { instanceId }),
        fake.service,
      );
      const id = threadId("thread-stream");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === id),
        Stream.take(9),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        provider,
        threadId: id,
        runtimeMode: "approval-required",
      });
      const turn = yield* adapter.sendTurn({
        threadId: id,
        input: "Give a short answer",
      });
      const child = fake.children[0];
      NodeAssert.ok(child);
      yield* child.emitBytes([
        '{"type":"message_update","assistantMessageEvent":{"type":"text_start","contentIndex":0}}\n',
        '{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"Hel',
        'lo"}}\r\n{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":" world"}}\n',
        '{"type":"message_update","assistantMessageEvent":{"type":"text_end","contentIndex":0,"content":"Hello world"}}\n{"type":"agent_settled"}\n',
      ]);

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      const deltas = events.filter((event) => event.type === "content.delta");
      NodeAssert.deepEqual(
        deltas.map((event) => (event.type === "content.delta" ? event.payload.delta : "")),
        ["Hello", " world"],
      );
      NodeAssert.equal(events.at(-1)?.type, "turn.completed");
      const completed = events.at(-1);
      if (completed?.type === "turn.completed") {
        NodeAssert.equal(completed.turnId, turn.turnId);
        NodeAssert.equal(completed.payload.state, "completed");
      }
      const snapshot = yield* adapter.readThread(id);
      NodeAssert.deepEqual(snapshot.turns[0]?.items, [
        { type: "assistant_message", text: "Hello world" },
      ]);
      const sessions = yield* adapter.listSessions();
      NodeAssert.equal(sessions[0]?.status, "ready");
      NodeAssert.equal(sessions[0]?.activeTurnId, undefined);
    }),
  ),
);

it.effect("maps interruptTurn to abort and emits turn.aborted", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const adapter = yield* provideRuntime(
        makePhiAdapter(settings(), { instanceId }),
        fake.service,
      );
      const id = threadId("thread-abort");
      yield* adapter.startSession({ provider, threadId: id, runtimeMode: "full-access" });
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.threadId === id &&
            (event.type === "turn.started" || event.type === "turn.aborted"),
        ),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      const turn = yield* adapter.sendTurn({ threadId: id, input: "Keep working" });
      yield* adapter.interruptTurn(id, turn.turnId);

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["turn.started", "turn.aborted"],
      );
      NodeAssert.equal(fake.children[0]?.commands.at(-1)?.type, "abort");
    }),
  ),
);

it.effect("rejects concurrent turns and model changes without sending another prompt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const adapter = yield* provideRuntime(
        makePhiAdapter(settings(), { instanceId }),
        fake.service,
      );
      const id = threadId("thread-turn-guard");
      yield* adapter.startSession({
        provider,
        threadId: id,
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "example/model-one" },
      });
      const active = yield* adapter.sendTurn({ threadId: id, input: "First prompt" });
      const concurrentError = yield* adapter
        .sendTurn({ threadId: id, input: "Second prompt" })
        .pipe(Effect.flip);
      NodeAssert.equal(concurrentError._tag, "ProviderAdapterRequestError");

      yield* adapter.interruptTurn(id, active.turnId);
      const modelError = yield* adapter
        .sendTurn({
          threadId: id,
          input: "Use another model",
          modelSelection: { instanceId, model: "example/model-two" },
        })
        .pipe(Effect.flip);
      NodeAssert.equal(modelError._tag, "ProviderAdapterValidationError");
      NodeAssert.equal(
        fake.children[0]?.commands.filter((command) => command.type === "prompt").length,
        1,
      );
    }),
  ),
);

it.effect("turns a child crash into redacted canonical failure events", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const adapter = yield* provideRuntime(
        makePhiAdapter(settings(), {
          instanceId,
          environment: { PRIVATE_TOKEN: "environment-secret" },
        }),
        fake.service,
      );
      const id = threadId("thread-crash");
      yield* adapter.startSession({ provider, threadId: id, runtimeMode: "full-access" });
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.threadId === id &&
            (event.type === "turn.completed" ||
              event.type === "runtime.error" ||
              event.type === "session.exited"),
        ),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.sendTurn({ threadId: id, input: "Do not echo this prompt" });
      const child = fake.children[0];
      NodeAssert.ok(child);
      yield* child.emitStderr("stderr-secret environment-secret Do not echo this prompt");
      yield* child.crash(23);

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["turn.completed", "runtime.error", "session.exited"],
      );
      const text = events
        .flatMap((event) => [
          event.type === "runtime.error" ? event.payload.message : "",
          event.type === "session.exited" ? (event.payload.reason ?? "") : "",
        ])
        .join(" ");
      NodeAssert.equal(text.includes("23"), true);
      NodeAssert.equal(text.includes("stderr-secret"), false);
      NodeAssert.equal(text.includes("environment-secret"), false);
      NodeAssert.equal(text.includes("Do not echo this prompt"), false);
      NodeAssert.equal(yield* adapter.hasSession(id), false);
    }),
  ),
);

it.effect("stops sessions, cleans up children, and reports presence accurately", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const adapter = yield* provideRuntime(
        makePhiAdapter(settings(), { instanceId }),
        fake.service,
      );
      const first = threadId("thread-stop-one");
      const second = threadId("thread-stop-two");
      yield* adapter.startSession({ provider, threadId: first, runtimeMode: "full-access" });
      yield* adapter.startSession({ provider, threadId: second, runtimeMode: "full-access" });
      NodeAssert.equal(yield* adapter.hasSession(first), true);
      NodeAssert.equal((yield* adapter.listSessions()).length, 2);

      yield* adapter.stopSession(first);
      NodeAssert.equal(fake.children[0]?.killed(), true);
      NodeAssert.equal(yield* adapter.hasSession(first), false);
      yield* adapter.stopAll();
      NodeAssert.equal(fake.children[1]?.killed(), true);
      NodeAssert.deepEqual(yield* adapter.listSessions(), []);
    }),
  ),
);

it.effect("serializes concurrent starts for one thread and keeps only the replacement", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const adapter = yield* provideRuntime(
        makePhiAdapter(settings(), { instanceId }),
        fake.service,
      );
      const id = threadId("thread-concurrent-start");
      yield* Effect.all(
        [
          adapter.startSession({ provider, threadId: id, runtimeMode: "full-access" }),
          adapter.startSession({ provider, threadId: id, runtimeMode: "full-access" }),
        ],
        { concurrency: "unbounded" },
      );

      NodeAssert.equal(fake.children.length, 2);
      NodeAssert.equal(fake.children[0]?.killed(), true);
      NodeAssert.equal(fake.children[1]?.killed(), false);
      const sessions = yield* adapter.listSessions();
      NodeAssert.equal(sessions.length, 1);
      NodeAssert.equal(sessions[0]?.threadId, id);
    }),
  ),
);

it.effect("switches to a persisted Phi session and rejects unsupported rollback", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const adapter = yield* provideRuntime(
        makePhiAdapter(settings(), { instanceId }),
        fake.service,
      );
      const id = threadId("thread-resume");
      yield* adapter.startSession({
        provider,
        threadId: id,
        runtimeMode: "full-access",
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "persisted-id",
          sessionPath: "/sessions/persisted.jsonl",
        },
      });
      const child = fake.children[0];
      NodeAssert.ok(child);
      NodeAssert.deepEqual(child.commands[0], {
        type: "switch_session",
        sessionPath: "/sessions/persisted.jsonl",
        id: child.commands[0]?.id,
      });

      const error = yield* adapter.rollbackThread(id, 1).pipe(Effect.flip);
      NodeAssert.equal(error._tag, "ProviderAdapterRequestError");
      if (error._tag === "ProviderAdapterRequestError") {
        NodeAssert.equal(error.detail.includes("not supported"), true);
      }
    }),
  ),
);

it.effect("can be built through a layer with the same scoped dependencies as the driver", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePhiSpawner();
    const runtimeLayer = Layer.mergeAll(
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, fake.service),
      serverConfigTestLayer,
    );
    const session = yield* Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* makePhiAdapter(settings(), { instanceId });
        return yield* adapter.startSession({
          provider,
          threadId: threadId("thread-layer"),
          runtimeMode: "full-access",
        });
      }),
    ).pipe(Effect.provide(runtimeLayer));
    NodeAssert.equal(session.provider, "phi");
  }),
);
