import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

const encoder = new TextEncoder();
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

export type FakePhiCommand = Readonly<Record<string, unknown>>;

export interface FakePhiChild {
  readonly command: ChildProcess.StandardCommand;
  readonly commands: Array<FakePhiCommand>;
  readonly killed: () => boolean;
  readonly emitRawBytes: (chunks: ReadonlyArray<Uint8Array>) => Effect.Effect<void>;
  readonly emitBytes: (chunks: ReadonlyArray<string>) => Effect.Effect<void>;
  readonly emitJson: (
    values: ReadonlyArray<Readonly<Record<string, unknown>>>,
    separator?: string,
  ) => Effect.Effect<void>;
  readonly emitStderr: (value: string) => Effect.Effect<void>;
  readonly crash: (code: number) => Effect.Effect<void>;
}

export interface FakePhiSpawner {
  readonly service: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly children: Array<FakePhiChild>;
}

export const makeFakePhiSpawner = Effect.fn("makeFakePhiSpawner")(function* () {
  const children = yield* Effect.sync(() => new Array<FakePhiChild>());
  let pid = 100;

  const service = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (command._tag !== "StandardCommand") {
        return yield* Effect.die(new Error("Phi tests only support standard commands."));
      }
      const stdout = yield* Queue.unbounded<Uint8Array>();
      const stderr = yield* Queue.unbounded<Uint8Array>();
      const exitCode = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
      const commands: Array<FakePhiCommand> = [];
      const sessionNumber = children.length + 1;
      const commandDecoder = new TextDecoder();
      let inputBuffer = "";
      let wasKilled = false;
      let hasExited = false;

      const emitRawBytes = (chunks: ReadonlyArray<Uint8Array>) =>
        Effect.forEach(chunks, (chunk) => Queue.offer(stdout, chunk), {
          discard: true,
        });
      const emitBytes = (chunks: ReadonlyArray<string>) =>
        emitRawBytes(chunks.map((chunk) => encoder.encode(chunk)));
      const emitJson = (
        values: ReadonlyArray<Readonly<Record<string, unknown>>>,
        separator = "\n",
      ) => emitBytes([`${values.map((value) => encodeJson(value)).join(separator)}\n`]);
      const emitStderr = (value: string) =>
        Queue.offer(stderr, encoder.encode(value)).pipe(Effect.asVoid);
      const finish = (code: number) =>
        Effect.gen(function* () {
          if (hasExited) return;
          hasExited = true;
          yield* Deferred.succeed(exitCode, ChildProcessSpawner.ExitCode(code));
        });

      const stdin = Sink.forEach((chunk: Uint8Array) =>
        Effect.gen(function* () {
          inputBuffer += commandDecoder.decode(chunk, { stream: true });
          while (true) {
            const newlineIndex = inputBuffer.indexOf("\n");
            if (newlineIndex < 0) return;
            const line = inputBuffer.slice(0, newlineIndex).replace(/\r$/, "");
            inputBuffer = inputBuffer.slice(newlineIndex + 1);
            if (!line.trim()) continue;
            const decoded = decodeJson(line);
            if (!Predicate.isObject(decoded)) continue;
            const rpcCommand = decoded as FakePhiCommand;
            commands.push(rpcCommand);
            const id = typeof rpcCommand.id === "string" ? rpcCommand.id : undefined;
            const type = typeof rpcCommand.type === "string" ? rpcCommand.type : "unknown";
            const response =
              type === "get_state"
                ? {
                    id,
                    type: "response",
                    command: type,
                    success: true,
                    data: {
                      sessionId: `session-${sessionNumber}`,
                      sessionFile: `/tmp/phi-session-${sessionNumber}.jsonl`,
                    },
                  }
                : {
                    id,
                    type: "response",
                    command: type,
                    success: true,
                    ...(type === "new_session" || type === "switch_session"
                      ? { data: { cancelled: false } }
                      : {}),
                  };
            yield* emitJson([response]);
          }
        }),
      );

      pid += 1;
      const handle = ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(pid),
        exitCode: Deferred.await(exitCode),
        isRunning: Effect.sync(() => !hasExited),
        kill: () =>
          Effect.gen(function* () {
            wasKilled = true;
            yield* finish(0);
          }),
        unref: Effect.succeed(Effect.void),
        stdin,
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.fromQueue(stderr),
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
      const child: FakePhiChild = {
        command,
        commands,
        killed: () => wasKilled,
        emitRawBytes,
        emitBytes,
        emitJson,
        emitStderr,
        crash: finish,
      };
      children.push(child);
      yield* Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore));
      return handle;
    }),
  );

  return { service, children } satisfies FakePhiSpawner;
});
