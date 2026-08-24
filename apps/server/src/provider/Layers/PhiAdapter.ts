import {
  EventId,
  type PhiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  type ProviderAdapterError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type {
  ProviderAdapterShape,
  ProviderThreadTurnSnapshot,
} from "../Services/ProviderAdapter.ts";
import { phiBinaryCandidates, withPhiHome } from "./PhiProvider.ts";
import {
  makePhiRpcTransport,
  PhiRpcMalformedFrameError,
  type PhiRpcFailure,
  type PhiRpcResponse,
  type PhiRpcTransport,
} from "./PhiRpcTransport.ts";

const PROVIDER = ProviderDriverKind.make("phi");
const PHI_RESUME_VERSION = 1 as const;
const UNSUPPORTED_INTERACTION_DETAIL =
  "Phi tool approvals and interactive user input are not supported yet.";
const UNSUPPORTED_ROLLBACK_DETAIL =
  "Phi durable history and rollback are not supported by this adapter slice.";

interface PhiResumeCursor {
  readonly schemaVersion: typeof PHI_RESUME_VERSION;
  readonly sessionPath: string;
  readonly sessionId?: string;
}

interface PhiTextItem {
  readonly itemId: RuntimeItemId;
  text: string;
  completed: boolean;
}

interface PhiSessionContext {
  session: ProviderSession;
  readonly transport: PhiRpcTransport;
  readonly scope: Scope.Closeable;
  readonly turns: Array<ProviderThreadTurnSnapshot>;
  readonly textItems: Map<number, PhiTextItem>;
  activeTurnId: TurnId | undefined;
  stopped: boolean;
}

export interface PhiAdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
}

function parseResumeCursor(raw: unknown): PhiResumeCursor | undefined {
  if (!Predicate.isObject(raw) || raw.schemaVersion !== PHI_RESUME_VERSION) return undefined;
  if (typeof raw.sessionPath !== "string" || raw.sessionPath.trim().length === 0) return undefined;
  return {
    schemaVersion: PHI_RESUME_VERSION,
    sessionPath: raw.sessionPath.trim(),
    ...(typeof raw.sessionId === "string" && raw.sessionId.trim().length > 0
      ? { sessionId: raw.sessionId.trim() }
      : {}),
  };
}

function parseStateIdentity(data: unknown): {
  readonly sessionId?: string;
  readonly sessionPath?: string;
} {
  if (!Predicate.isObject(data)) return {};
  return {
    ...(typeof data.sessionId === "string" && data.sessionId.trim().length > 0
      ? { sessionId: data.sessionId.trim() }
      : {}),
    ...(typeof data.sessionFile === "string" && data.sessionFile.trim().length > 0
      ? { sessionPath: data.sessionFile.trim() }
      : {}),
  };
}

function safeTransportDetail(error: PhiRpcFailure): string {
  return error._tag === "PhiRpcMalformedFrameError"
    ? "Phi RPC emitted a malformed protocol frame."
    : error.detail;
}

function toRequestError(method: string, error: PhiRpcFailure): ProviderAdapterRequestError {
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: safeTransportDetail(error),
  });
}

function requireSuccessfulResponse(
  response: PhiRpcResponse,
  method: string,
): Effect.Effect<PhiRpcResponse, ProviderAdapterRequestError> {
  return response.success
    ? Effect.succeed(response)
    : Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method,
          detail: `Phi RPC rejected the '${method}' request.`,
        }),
      );
}

export function makePhiAdapter(phiSettings: PhiSettings, options?: PhiAdapterOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("phi");
    const serverConfig = yield* ServerConfig;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, PhiSessionContext>();
    const threadLocks = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    let identifierSequence = 0;

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const nextIdentifier = (prefix: string) => {
      identifierSequence += 1;
      return `${prefix}-${identifierSequence}`;
    };
    const makeEventStamp = () =>
      Effect.all({
        eventId: Effect.sync(() => EventId.make(nextIdentifier("phi-event"))),
        createdAt: nowIso,
      });
    const emit = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEvents, event).pipe(Effect.asVoid);

    const getThreadLock = (threadId: ThreadId) =>
      SynchronizedRef.modifyEffect(threadLocks, (current) => {
        const existing = Option.fromNullishOr(current.get(threadId));
        return Option.match(existing, {
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
        });
      });
    const withThreadLock = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadLock(threadId), (semaphore) => semaphore.withPermit(effect));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<PhiSessionContext, ProviderAdapterSessionNotFoundError> => {
      const context = sessions.get(threadId);
      return context && !context.stopped
        ? Effect.succeed(context)
        : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
    };

    const updateSession = Effect.fn("PhiAdapter.updateSession")(function* (
      context: PhiSessionContext,
      patch: Partial<ProviderSession>,
      clearActiveTurn = false,
    ) {
      const next: ProviderSession = {
        ...context.session,
        ...patch,
        updatedAt: yield* nowIso,
      };
      if (clearActiveTurn) {
        const { activeTurnId: _activeTurnId, ...withoutActiveTurn } = next;
        context.session = withoutActiveTurn;
      } else {
        context.session = next;
      }
    });

    const completeTextItems = Effect.fn("PhiAdapter.completeTextItems")(function* (
      context: PhiSessionContext,
      turnId: TurnId,
    ) {
      for (const item of context.textItems.values()) {
        if (item.completed) continue;
        item.completed = true;
        yield* emit({
          type: "item.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          turnId,
          itemId: item.itemId,
          payload: {
            itemType: "assistant_message",
            status: "completed",
            title: "Assistant message",
            ...(item.text.length > 0 ? { detail: item.text } : {}),
          },
        });
      }
    });

    const ensureTextItem = Effect.fn("PhiAdapter.ensureTextItem")(function* (
      context: PhiSessionContext,
      turnId: TurnId,
      contentIndex: number,
    ) {
      const existing = context.textItems.get(contentIndex);
      if (existing) return existing;
      const created: PhiTextItem = {
        itemId: RuntimeItemId.make(`${turnId}:assistant:${contentIndex}`),
        text: "",
        completed: false,
      };
      context.textItems.set(contentIndex, created);
      yield* emit({
        type: "item.started",
        ...(yield* makeEventStamp()),
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        turnId,
        itemId: created.itemId,
        payload: {
          itemType: "assistant_message",
          status: "inProgress",
          title: "Assistant message",
        },
      });
      return created;
    });

    const handleEvent = Effect.fn("PhiAdapter.handleEvent")(function* (
      context: PhiSessionContext,
      event: Readonly<Record<string, unknown>>,
    ) {
      if (context.stopped) return;
      const turnId = context.activeTurnId;
      if (event.type === "message_update" && turnId) {
        const assistantEvent = event.assistantMessageEvent;
        if (!Predicate.isObject(assistantEvent)) return;
        const contentIndex =
          typeof assistantEvent.contentIndex === "number" &&
          Number.isInteger(assistantEvent.contentIndex)
            ? assistantEvent.contentIndex
            : 0;
        if (assistantEvent.type === "text_start") {
          yield* ensureTextItem(context, turnId, contentIndex);
          return;
        }
        if (assistantEvent.type === "text_delta" && typeof assistantEvent.delta === "string") {
          const item = yield* ensureTextItem(context, turnId, contentIndex);
          item.text += assistantEvent.delta;
          if (assistantEvent.delta.length > 0) {
            yield* emit({
              type: "content.delta",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: context.session.threadId,
              turnId,
              itemId: item.itemId,
              payload: {
                streamKind: "assistant_text",
                delta: assistantEvent.delta,
                contentIndex,
              },
            });
          }
          return;
        }
        if (assistantEvent.type === "text_end") {
          const item = yield* ensureTextItem(context, turnId, contentIndex);
          if (
            typeof assistantEvent.content === "string" &&
            assistantEvent.content.length >= item.text.length
          ) {
            item.text = assistantEvent.content;
          }
          if (!item.completed) {
            item.completed = true;
            yield* emit({
              type: "item.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: context.session.threadId,
              turnId,
              itemId: item.itemId,
              payload: {
                itemType: "assistant_message",
                status: "completed",
                title: "Assistant message",
                ...(item.text.length > 0 ? { detail: item.text } : {}),
              },
            });
          }
          return;
        }
      }

      if (event.type === "agent_settled" && turnId) {
        yield* completeTextItems(context, turnId);
        const items = [...context.textItems.values()].map((item) => ({
          type: "assistant_message",
          text: item.text,
        }));
        context.turns.push({ id: turnId, items });
        context.textItems.clear();
        context.activeTurnId = undefined;
        yield* updateSession(context, { status: "ready" }, true);
        yield* emit({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          turnId,
          payload: { state: "completed" },
        });
      }
    });

    const handleTransportFailure = Effect.fn("PhiAdapter.handleTransportFailure")(function* (
      context: PhiSessionContext,
      failure: PhiRpcFailure,
    ) {
      if (context.stopped) return;
      context.stopped = true;
      sessions.delete(context.session.threadId);
      const turnId = context.activeTurnId;
      const message = safeTransportDetail(failure);
      yield* updateSession(context, { status: "error", lastError: message }, true);
      if (turnId) {
        yield* emit({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          turnId,
          payload: { state: "failed", errorMessage: message },
        });
      }
      yield* emit({
        type: "runtime.error",
        ...(yield* makeEventStamp()),
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        ...(turnId ? { turnId } : {}),
        payload: { message, class: "transport_error" },
      });
      yield* emit({
        type: "session.exited",
        ...(yield* makeEventStamp()),
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        payload: { reason: message, recoverable: false, exitKind: "error" },
      });
      yield* Scope.close(context.scope, Exit.void).pipe(Effect.ignore);
    });

    const stopContext = Effect.fn("PhiAdapter.stopContext")(function* (
      context: PhiSessionContext,
      emitExit: boolean,
    ) {
      if (context.stopped) return;
      context.stopped = true;
      sessions.delete(context.session.threadId);
      yield* context.transport.close;
      yield* Scope.close(context.scope, Exit.void).pipe(Effect.ignore);
      yield* updateSession(context, { status: "closed" }, true);
      if (emitExit) {
        yield* emit({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          payload: {
            reason: "Session stopped.",
            recoverable: false,
            exitKind: "graceful",
          },
        });
      }
    });

    const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          const cwd = input.cwd?.trim() || serverConfig.cwd;
          if (!cwd) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }
          if (
            input.modelSelection !== undefined &&
            input.modelSelection.instanceId !== boundInstanceId
          ) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Phi model selection is bound to instance '${input.modelSelection.instanceId}', expected '${boundInstanceId}'.`,
            });
          }

          const existing = sessions.get(input.threadId);
          if (existing) yield* stopContext(existing, false);

          const sessionScope = yield* Scope.make("sequential");
          const launchArgs = [
            ...(input.title ? ["--name", input.title] : []),
            ...(input.modelSelection ? ["--model", input.modelSelection.model] : []),
          ];
          const transport = yield* makePhiRpcTransport({
            binaries: phiBinaryCandidates(phiSettings),
            cwd,
            environment: withPhiHome(options?.environment ?? process.env, phiSettings.homePath),
            launchArgs,
          }).pipe(
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: safeTransportDetail(cause),
                }),
            ),
            Effect.onError(() => Scope.close(sessionScope, Exit.void).pipe(Effect.ignore)),
          );

          const resume = parseResumeCursor(input.resumeCursor);
          const initializeCommand = resume
            ? ({ type: "switch_session", sessionPath: resume.sessionPath } as const)
            : ({ type: "new_session" } as const);
          const initializeMethod = resume ? "switch_session" : "new_session";
          const initialized = yield* transport.request(initializeCommand).pipe(
            Effect.mapError((error) => toRequestError(initializeMethod, error)),
            Effect.flatMap((response) => requireSuccessfulResponse(response, initializeMethod)),
            Effect.onError(() => Scope.close(sessionScope, Exit.void).pipe(Effect.ignore)),
          );
          if (Predicate.isObject(initialized.data) && initialized.data.cancelled === true) {
            yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: initializeMethod,
              detail: `Phi RPC cancelled the '${initializeMethod}' request.`,
            });
          }

          const state = yield* transport.request({ type: "get_state" }).pipe(
            Effect.mapError((error) => toRequestError("get_state", error)),
            Effect.flatMap((response) => requireSuccessfulResponse(response, "get_state")),
            Effect.onError(() => Scope.close(sessionScope, Exit.void).pipe(Effect.ignore)),
          );
          const identity = parseStateIdentity(state.data);
          const resumeCursor =
            identity.sessionPath !== undefined
              ? {
                  schemaVersion: PHI_RESUME_VERSION,
                  sessionPath: identity.sessionPath,
                  ...(identity.sessionId ? { sessionId: identity.sessionId } : {}),
                }
              : resume;
          const createdAt = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(input.modelSelection ? { model: input.modelSelection.model } : {}),
            threadId: input.threadId,
            ...(resumeCursor ? { resumeCursor } : {}),
            createdAt,
            updatedAt: createdAt,
          };
          const context: PhiSessionContext = {
            session,
            transport,
            scope: sessionScope,
            turns: [],
            textItems: new Map(),
            activeTurnId: undefined,
            stopped: false,
          };
          sessions.set(input.threadId, context);
          yield* transport.events.pipe(
            Stream.runForEach((event) => handleEvent(context, event)),
            Effect.catch((failure) => handleTransportFailure(context, failure)),
            Effect.forkIn(sessionScope),
          );

          yield* emit({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: input.threadId,
            payload: {
              message: "Phi RPC session started",
              ...(resumeCursor ? { resume: resumeCursor } : {}),
            },
          });
          yield* emit({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: input.threadId,
            payload: { state: "ready", reason: "Phi RPC session ready" },
          });
          yield* emit({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: input.threadId,
            payload: {
              ...(identity.sessionId ? { providerThreadId: identity.sessionId } : {}),
            },
          });
          return session;
        }),
      );

    const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = Effect.fn(
      "PhiAdapter.sendTurn",
    )(function* (input) {
      const context = yield* requireSession(input.threadId);
      if (context.activeTurnId) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "prompt",
          detail: "Phi already has an active turn for this session.",
        });
      }
      if (input.attachments && input.attachments.length > 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Phi attachments are not supported by this adapter slice.",
        });
      }
      const prompt = input.input?.trim();
      if (!prompt) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Phi turns require non-empty text input.",
        });
      }
      if (
        input.modelSelection !== undefined &&
        (input.modelSelection.instanceId !== boundInstanceId ||
          input.modelSelection.model !== context.session.model)
      ) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Phi model changes require a new session.",
        });
      }

      const turnId = TurnId.make(nextIdentifier("phi-turn"));
      context.activeTurnId = turnId;
      context.textItems.clear();
      yield* updateSession(context, { status: "running", activeTurnId: turnId });
      yield* emit({
        type: "turn.started",
        ...(yield* makeEventStamp()),
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: input.threadId,
        turnId,
        payload: { ...(context.session.model ? { model: context.session.model } : {}) },
      });

      yield* context.transport.request({ type: "prompt", message: prompt }).pipe(
        Effect.mapError((error) => toRequestError("prompt", error)),
        Effect.flatMap((response) => requireSuccessfulResponse(response, "prompt")),
        Effect.tapError((error) =>
          Effect.gen(function* () {
            context.activeTurnId = undefined;
            yield* updateSession(context, { status: "ready", lastError: error.detail }, true);
            yield* emit({
              type: "turn.aborted",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: input.threadId,
              turnId,
              payload: { reason: error.detail },
            });
          }),
        ),
      );
      return {
        threadId: input.threadId,
        turnId,
        ...(context.session.resumeCursor !== undefined
          ? { resumeCursor: context.session.resumeCursor }
          : {}),
      };
    });

    const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = Effect.fn(
      "PhiAdapter.interruptTurn",
    )(function* (threadId, requestedTurnId) {
      const context = yield* requireSession(threadId);
      yield* context.transport.request({ type: "abort" }).pipe(
        Effect.mapError((error) => toRequestError("abort", error)),
        Effect.flatMap((response) => requireSuccessfulResponse(response, "abort")),
      );
      const turnId = requestedTurnId ?? context.activeTurnId;
      context.activeTurnId = undefined;
      context.textItems.clear();
      yield* updateSession(context, { status: "ready" }, true);
      if (turnId) {
        yield* emit({
          type: "turn.aborted",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId,
          turnId,
          payload: { reason: "Interrupted by user." },
        });
      }
    });

    const unsupportedInteraction = (method: string) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method,
          detail: UNSUPPORTED_INTERACTION_DETAIL,
        }),
      );

    const stopSession: ProviderAdapterShape<ProviderAdapterError>["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const context = yield* requireSession(threadId);
          yield* stopContext(context, true);
        }),
      );
    const listSessions = () =>
      Effect.sync(() => [...sessions.values()].map((context) => ({ ...context.session })));
    const hasSession = (threadId: ThreadId) =>
      Effect.sync(() => {
        const context = sessions.get(threadId);
        return context !== undefined && !context.stopped;
      });
    const readThread: ProviderAdapterShape<ProviderAdapterError>["readThread"] = Effect.fn(
      "PhiAdapter.readThread",
    )(function* (threadId) {
      const context = yield* requireSession(threadId);
      return { threadId, turns: [...context.turns] };
    });
    const rollbackThread: ProviderAdapterShape<ProviderAdapterError>["rollbackThread"] = (
      threadId,
    ) =>
      requireSession(threadId).pipe(
        Effect.flatMap(() =>
          Effect.fail(
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "rollbackThread",
              detail: UNSUPPORTED_ROLLBACK_DETAIL,
            }),
          ),
        ),
      );
    const stopAll = () =>
      Effect.forEach([...sessions.values()], (context) => stopContext(context, false), {
        concurrency: "unbounded",
        discard: true,
      });

    yield* Effect.addFinalizer(() =>
      stopAll().pipe(Effect.ignore, Effect.andThen(PubSub.shutdown(runtimeEvents))),
    );

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "unsupported" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest: () => unsupportedInteraction("respondToRequest"),
      respondToUserInput: () => unsupportedInteraction("respondToUserInput"),
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEvents),
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
}
