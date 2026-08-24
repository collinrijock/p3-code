import * as NodeAssert from "node:assert/strict";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { makePhiRpcTransport } from "./PhiRpcTransport.ts";
import { makeFakePhiSpawner } from "./PhiRpcTestFixtures.ts";

const encoder = new TextEncoder();

function assertCarriesNoSecret(value: unknown, secret: string): void {
  const seen = new WeakSet<object>();
  const visit = (current: unknown): void => {
    if (typeof current === "string") {
      NodeAssert.equal(current.includes(secret), false);
      return;
    }
    if (typeof current !== "object" || current === null || seen.has(current)) return;
    seen.add(current);
    visit((current as { readonly message?: unknown }).message);
    visit((current as { readonly cause?: unknown }).cause);
    for (const nested of Object.values(current)) visit(nested);
  };
  visit(value);
}

it.effect("decodes fragmented and batched JSONL without splitting Unicode separators", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const transport = yield* makePhiRpcTransport({
        binaries: ["phi"],
        cwd: "/workspace",
        environment: { PATH: "/bin" },
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.service));
      const child = fake.children[0];
      NodeAssert.ok(child);
      const collected = yield* transport.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );

      const framed = encoder.encode(
        [
          '{"type":"first","text":"left\u2028middle\u2029right"}\r\n',
          "\n",
          '{"type":"second"}\n{"type":"third"}\n',
        ].join(""),
      );
      const separatorOffset = framed.indexOf(0xe2);
      yield* child.emitRawBytes([
        framed.slice(0, 7),
        framed.slice(7, separatorOffset + 1),
        framed.slice(separatorOffset + 1, separatorOffset + 2),
        framed.slice(separatorOffset + 2, framed.length - 9),
        framed.slice(framed.length - 9),
      ]);

      const events = Array.from(yield* Fiber.join(collected).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["first", "second", "third"],
      );
      NodeAssert.equal(events[0]?.text, "left\u2028middle\u2029right");
    }),
  ),
);

it.effect("correlates command responses while keeping events on the event stream", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const transport = yield* makePhiRpcTransport({
        binaries: ["phi"],
        cwd: "/workspace",
        environment: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.service));
      const eventFiber = yield* transport.events.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      const child = fake.children[0];
      NodeAssert.ok(child);
      yield* child.emitJson([{ type: "agent_start" }]);
      const response = yield* transport.request({ type: "get_state" });
      const events = Array.from(yield* Fiber.join(eventFiber).pipe(Effect.timeout("1 second")));

      NodeAssert.equal(response.success, true);
      NodeAssert.equal(response.command, "get_state");
      NodeAssert.equal(events[0]?.type, "agent_start");
      NodeAssert.equal(child.commands[0]?.type, "get_state");
    }),
  ),
);

it.effect("falls back from a missing phi binary to pi", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const attempts: string[] = [];
      const fallbackSpawner = ChildProcessSpawner.make((command) => {
        const binary = command._tag === "StandardCommand" ? command.command : "";
        attempts.push(binary);
        return binary === "phi"
          ? Effect.fail(
              PlatformError.systemError({
                _tag: "NotFound",
                module: "ChildProcess",
                method: "spawn",
              }),
            )
          : fake.service.spawn(command);
      });
      const transport = yield* makePhiRpcTransport({
        binaries: ["phi", "pi"],
        cwd: "/workspace",
        environment: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fallbackSpawner));

      NodeAssert.equal(transport.selectedBinary, "pi");
      NodeAssert.deepEqual(attempts, ["phi", "pi"]);
      NodeAssert.equal(fake.children[0]?.command.command, "pi");
    }),
  ),
);

it.effect("fails malformed frames with a typed error that omits frame contents", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const transport = yield* makePhiRpcTransport({
        binaries: ["phi"],
        cwd: "/workspace",
        environment: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.service));
      const failureFiber = yield* Stream.runHead(transport.events).pipe(
        Effect.flip,
        Effect.forkChild,
      );
      const child = fake.children[0];
      NodeAssert.ok(child);
      yield* child.emitBytes(['{"type":"message_update","secret":"do-not-leak"\n']);
      const failure = yield* Fiber.join(failureFiber).pipe(Effect.timeout("1 second"));

      NodeAssert.equal(failure._tag, "PhiRpcMalformedFrameError");
      NodeAssert.equal(failure.frameNumber, 1);
      assertCarriesNoSecret(failure, "do-not-leak");
    }),
  ),
);

it.effect("redacts stderr when the child process crashes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const transport = yield* makePhiRpcTransport({
        binaries: ["phi"],
        cwd: "/workspace",
        environment: { PRIVATE_TOKEN: "environment-secret" },
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.service));
      const failureFiber = yield* Stream.runHead(transport.events).pipe(
        Effect.flip,
        Effect.forkChild,
      );
      const child = fake.children[0];
      NodeAssert.ok(child);
      yield* child.emitStderr("stderr-secret environment-secret");
      yield* child.crash(17);
      const failure = yield* Fiber.join(failureFiber).pipe(Effect.timeout("1 second"));

      NodeAssert.equal(failure._tag, "PhiRpcTransportError");
      if (failure._tag === "PhiRpcTransportError") {
        NodeAssert.equal(failure.operation, "exit");
        NodeAssert.equal(failure.detail.includes("17"), true);
      }
      assertCarriesNoSecret(failure, "stderr-secret");
      assertCarriesNoSecret(failure, "environment-secret");
    }),
  ),
);

it.effect("closes the owned child process exactly once", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeFakePhiSpawner();
      const transport = yield* makePhiRpcTransport({
        binaries: ["phi"],
        cwd: "/workspace",
        environment: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.service));
      const child = fake.children[0];
      NodeAssert.ok(child);
      yield* transport.close;
      yield* transport.close;
      NodeAssert.equal(child.killed(), true);
    }),
  ),
);
