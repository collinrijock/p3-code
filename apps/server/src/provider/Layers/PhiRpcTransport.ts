import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { isCommandMissingCause } from "../providerSnapshot.ts";

export class PhiRpcMalformedFrameError extends Schema.TaggedErrorClass<PhiRpcMalformedFrameError>()(
  "PhiRpcMalformedFrameError",
  {
    frameNumber: Schema.Number,
  },
) {
  override get message(): string {
    return `Phi RPC emitted malformed JSONL frame ${this.frameNumber}.`;
  }
}

export class PhiRpcTransportError extends Schema.TaggedErrorClass<PhiRpcTransportError>()(
  "PhiRpcTransportError",
  {
    operation: Schema.String,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `Phi RPC transport failed during ${this.operation}: ${this.detail}`;
  }
}

export type PhiRpcFailure = PhiRpcMalformedFrameError | PhiRpcTransportError;

export interface PhiRpcResponse {
  readonly id?: string;
  readonly type: "response";
  readonly command: string;
  readonly success: boolean;
  readonly data?: unknown;
  readonly error?: string;
}

export type PhiRpcEvent = Readonly<Record<string, unknown>>;

type Inbound =
  | { readonly _tag: "event"; readonly event: PhiRpcEvent }
  | { readonly _tag: "failure"; readonly error: PhiRpcFailure };

export interface PhiRpcTransport {
  readonly selectedBinary: string;
  readonly request: (
    command: Readonly<Record<string, unknown>> & { readonly type: string },
  ) => Effect.Effect<PhiRpcResponse, PhiRpcFailure>;
  readonly events: Stream.Stream<PhiRpcEvent, PhiRpcFailure>;
  readonly close: Effect.Effect<void>;
}

export interface PhiRpcTransportOptions {
  readonly binaries: ReadonlyArray<string>;
  readonly cwd: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly launchArgs?: ReadonlyArray<string>;
}

const encoder = new TextEncoder();

function asResponse(value: PhiRpcEvent): PhiRpcResponse | undefined {
  if (
    value.type !== "response" ||
    typeof value.command !== "string" ||
    typeof value.success !== "boolean"
  ) {
    return undefined;
  }
  return {
    type: "response",
    command: value.command,
    success: value.success,
    ...(typeof value.id === "string" ? { id: value.id } : {}),
    ...(value.data !== undefined ? { data: value.data } : {}),
    ...(typeof value.error === "string" ? { error: value.error } : {}),
  };
}

export const makePhiRpcTransport = Effect.fn("makePhiRpcTransport")(function* (
  options: PhiRpcTransportOptions,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  let selectedBinary: string | undefined;
  let handle: ChildProcessSpawner.ChildProcessHandle | undefined;

  for (const binary of options.binaries) {
    const attempted = yield* Effect.result(
      Effect.gen(function* () {
        const spawnCommand = yield* resolveSpawnCommand(
          binary,
          ["--mode", "rpc", ...(options.launchArgs ?? [])],
          { cwd: options.cwd, env: options.environment },
        );
        return yield* spawner.spawn(
          ChildProcess.make(spawnCommand.command, spawnCommand.args, {
            cwd: options.cwd,
            env: options.environment,
            shell: spawnCommand.shell,
            stdin: { stream: "pipe", endOnDone: false },
          }),
        );
      }),
    );
    if (Result.isSuccess(attempted)) {
      selectedBinary = binary;
      handle = attempted.success;
      break;
    }
    if (isCommandMissingCause(attempted.failure)) {
      continue;
    }
    return yield* new PhiRpcTransportError({
      operation: "spawn",
      detail: "Unable to start the configured Phi RPC process.",
    });
  }

  if (!selectedBinary || !handle) {
    return yield* new PhiRpcTransportError({
      operation: "spawn",
      detail: "Neither the Phi nor Pi RPC binary could be started.",
    });
  }

  const child = handle;
  const inbound = yield* Queue.unbounded<Inbound>();
  const pending = new Map<string, Deferred.Deferred<PhiRpcResponse, PhiRpcFailure>>();
  const terminal = yield* Ref.make<PhiRpcFailure | undefined>(undefined);
  const closing = yield* Ref.make(false);
  const writeLock = yield* Semaphore.make(1);
  let requestSequence = 0;

  const failTerminal = Effect.fn("PhiRpcTransport.failTerminal")(function* (error: PhiRpcFailure) {
    const won = yield* Ref.modify(terminal, (current) =>
      current === undefined ? [true, error] : [false, current],
    );
    if (!won) return;
    for (const deferred of pending.values()) {
      yield* Deferred.fail(deferred, error);
    }
    pending.clear();
    yield* Queue.offer(inbound, { _tag: "failure", error });
  });

  let frameNumber = 0;
  let textBuffer = "";
  const decoder = new TextDecoder();

  const processLine = Effect.fn("PhiRpcTransport.processLine")(function* (rawLine: string) {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (line.trim().length === 0) return;
    frameNumber += 1;

    let decoded: unknown;
    try {
      decoded = JSON.parse(line);
    } catch {
      return yield* new PhiRpcMalformedFrameError({ frameNumber });
    }
    if (!Predicate.isRecord(decoded)) {
      return yield* new PhiRpcMalformedFrameError({ frameNumber });
    }

    const response = asResponse(decoded);
    if (response?.id) {
      const deferred = pending.get(response.id);
      if (deferred) {
        pending.delete(response.id);
        yield* Deferred.succeed(deferred, response);
        return;
      }
    }
    if (!response) {
      yield* Queue.offer(inbound, { _tag: "event", event: decoded });
    }
  });

  const processText = Effect.fn("PhiRpcTransport.processText")(function* (text: string) {
    textBuffer += text;
    while (true) {
      const newlineIndex = textBuffer.indexOf("\n");
      if (newlineIndex < 0) return;
      const line = textBuffer.slice(0, newlineIndex);
      textBuffer = textBuffer.slice(newlineIndex + 1);
      yield* processLine(line);
    }
  });

  yield* Effect.gen(function* () {
    yield* child.stdout.pipe(
      Stream.runForEach((chunk) => processText(decoder.decode(chunk, { stream: true }))),
    );
    yield* processText(decoder.decode());
    if (textBuffer.length > 0) {
      const trailing = textBuffer;
      textBuffer = "";
      yield* processLine(trailing);
    }
    if (!(yield* Ref.get(closing))) {
      yield* failTerminal(
        new PhiRpcTransportError({
          operation: "stdout",
          detail: "The Phi RPC output stream closed unexpectedly.",
        }),
      );
    }
  }).pipe(
    Effect.catch((error) =>
      failTerminal(
        PhiRpcMalformedFrameError.is(error)
          ? error
          : new PhiRpcTransportError({
              operation: "stdout",
              detail: "Failed to read the Phi RPC output stream.",
            }),
      ),
    ),
    Effect.forkScoped,
  );

  yield* child.stderr.pipe(
    Stream.runDrain,
    Effect.catch(() =>
      failTerminal(
        new PhiRpcTransportError({
          operation: "stderr",
          detail: "Failed to drain the Phi RPC diagnostic stream.",
        }),
      ),
    ),
    Effect.forkScoped,
  );

  yield* child.exitCode.pipe(
    Effect.flatMap((code) =>
      Ref.get(closing).pipe(
        Effect.flatMap((isClosing) =>
          isClosing
            ? Effect.void
            : failTerminal(
                new PhiRpcTransportError({
                  operation: "exit",
                  detail: `The Phi RPC process exited unexpectedly (code ${Number(code)}).`,
                }),
              ),
        ),
      ),
    ),
    Effect.catch(() =>
      failTerminal(
        new PhiRpcTransportError({
          operation: "exit",
          detail: "Unable to read the Phi RPC process exit status.",
        }),
      ),
    ),
    Effect.forkScoped,
  );

  yield* Effect.addFinalizer(() => Ref.set(closing, true));

  const request: PhiRpcTransport["request"] = Effect.fn("PhiRpcTransport.request")(
    function* (command) {
      const terminalError = yield* Ref.get(terminal);
      if (terminalError) return yield* terminalError;

      requestSequence += 1;
      const id = `t3-phi-${requestSequence}`;
      const deferred = yield* Deferred.make<PhiRpcResponse, PhiRpcFailure>();
      pending.set(id, deferred);
      const frame = encoder.encode(`${JSON.stringify({ ...command, id })}\n`);

      const written = yield* writeLock.withPermit(
        Stream.run(Stream.make(frame), child.stdin).pipe(
          Effect.as(true),
          Effect.catch(() => Effect.succeed(false)),
        ),
      );
      if (!written) {
        pending.delete(id);
        const error = new PhiRpcTransportError({
          operation: "stdin",
          detail: "Failed to write a Phi RPC command.",
        });
        yield* failTerminal(error);
        return yield* error;
      }
      return yield* Deferred.await(deferred);
    },
  );

  const close = Effect.gen(function* () {
    if (yield* Ref.getAndSet(closing, true)) return;
    yield* child.kill().pipe(Effect.ignore);
  });

  const events = Stream.fromQueue(inbound).pipe(
    Stream.mapEffect((item) =>
      item._tag === "event" ? Effect.succeed(item.event) : Effect.fail(item.error),
    ),
  );

  return {
    selectedBinary,
    request,
    events,
    close,
  } satisfies PhiRpcTransport;
});
