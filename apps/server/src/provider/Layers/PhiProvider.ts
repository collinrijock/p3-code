import {
  type ModelCapabilities,
  type PhiSettings,
  type ServerProviderAuth,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  buildServerProvider,
  collectStreamAsString,
  isCommandMissingCause,
  parseGenericCliVersion,
  type CommandResult,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PHI_PRESENTATION = {
  displayName: "Phi",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: true,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});
const PROBE_TIMEOUT_MS = 8_000;
const RPC_PROBE_ARGS = ["--mode", "rpc", "--no-session"] as const;
const RPC_MODEL_COMMAND = `${JSON.stringify({ type: "get_available_models" })}\n`;

export interface PhiProbeCommandInput {
  readonly binary: string;
  readonly args: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  readonly stdin?: string;
}

export type PhiProbeCommandOutcome =
  | { readonly _tag: "completed"; readonly result: CommandResult }
  | { readonly _tag: "missing" }
  | { readonly _tag: "failed" }
  | { readonly _tag: "timedOut" };

export type PhiProbeCommandRunner<R = never> = (
  input: PhiProbeCommandInput,
) => Effect.Effect<PhiProbeCommandOutcome, never, R>;

export interface PhiCliProbe {
  readonly installed: boolean;
  readonly selectedBinary: string | null;
  readonly version: string | null;
  readonly status: "ready" | "warning" | "error";
  readonly auth: ServerProviderAuth;
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly message?: string;
}

export function phiBinaryCandidates(settings: PhiSettings): ReadonlyArray<string> {
  const configured = settings.binaryPath.trim();
  return configured ? [configured] : ["phi", "pi"];
}

export function withPhiHome(environment: NodeJS.ProcessEnv, homePath: string): NodeJS.ProcessEnv {
  if (!homePath) return environment;
  return {
    ...environment,
    PI_CODING_AGENT_DIR: homePath,
    PHI_CODING_AGENT_DIR: homePath,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

interface ParsedPhiRpcModels {
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly auth: ServerProviderAuth;
}

export function parsePhiAvailableModelsResponse(stdout: string): ParsedPhiRpcModels | undefined {
  for (const rawLine of stdout.split("\n")) {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (!line) continue;

    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      continue;
    }
    if (
      !isRecord(value) ||
      value.type !== "response" ||
      value.command !== "get_available_models" ||
      value.success !== true ||
      !isRecord(value.data) ||
      !Array.isArray(value.data.models)
    ) {
      continue;
    }

    const seen = new Set<string>();
    const authenticationStates: Array<boolean | undefined> = [];
    const models = value.data.models.flatMap((candidate): ReadonlyArray<ServerProviderModel> => {
      if (!isRecord(candidate)) return [];
      const provider = typeof candidate.provider === "string" ? candidate.provider.trim() : "";
      const id = typeof candidate.id === "string" ? candidate.id.trim() : "";
      if (!provider || !id) return [];

      const slug = `${provider}/${id}`;
      if (seen.has(slug)) return [];
      seen.add(slug);
      authenticationStates.push(
        typeof candidate.authenticated === "boolean" ? candidate.authenticated : undefined,
      );

      const name = typeof candidate.name === "string" ? candidate.name.trim() : "";
      return [
        {
          slug,
          name: name || slug,
          isCustom: false,
          capabilities: EMPTY_CAPABILITIES,
        },
      ];
    });

    const auth: ServerProviderAuth = authenticationStates.some((state) => state === true)
      ? { status: "authenticated" }
      : authenticationStates.length > 0 && authenticationStates.every((state) => state === false)
        ? { status: "unauthenticated" }
        : { status: "unknown" };
    return { models, auth };
  }
  return undefined;
}

function fallbackMessage(selectedBinary: string): string | undefined {
  return selectedBinary === "pi"
    ? "Using the fallback `pi` binary because `phi` was not found."
    : undefined;
}

export const probePhiCli = <R>(
  settings: PhiSettings,
  environment: NodeJS.ProcessEnv,
  run: PhiProbeCommandRunner<R>,
): Effect.Effect<PhiCliProbe, never, R> =>
  Effect.gen(function* () {
    const probeEnvironment = withPhiHome(environment, settings.homePath);
    let selectedBinary: string | undefined;
    let versionOutcome: PhiProbeCommandOutcome | undefined;

    for (const binary of phiBinaryCandidates(settings)) {
      const outcome = yield* run({
        binary,
        args: ["--version"],
        environment: probeEnvironment,
      });
      if (outcome._tag === "missing") continue;
      selectedBinary = binary;
      versionOutcome = outcome;
      break;
    }

    if (!selectedBinary || !versionOutcome) {
      return {
        installed: false,
        selectedBinary: null,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        models: [],
        message:
          settings.binaryPath.length > 0
            ? `Phi CLI command \`${settings.binaryPath}\` was not found.`
            : "Phi CLI was not found. Install `phi` or `pi`, or configure a binary path.",
      };
    }

    if (versionOutcome._tag === "timedOut") {
      return {
        installed: true,
        selectedBinary,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        models: [],
        message: `Phi CLI timed out while running \`${selectedBinary} --version\`.`,
      };
    }
    if (versionOutcome._tag !== "completed") {
      return {
        installed: true,
        selectedBinary,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        models: [],
        message: "Failed to execute the Phi CLI version probe.",
      };
    }

    const versionResult = versionOutcome.result;
    const version = parseGenericCliVersion(`${versionResult.stdout}\n${versionResult.stderr}`);
    if (versionResult.code !== 0) {
      return {
        installed: true,
        selectedBinary,
        version,
        status: "error",
        auth: { status: "unknown" },
        models: [],
        message: "Phi CLI is installed but its version command failed.",
      };
    }

    const rpcOutcome = yield* run({
      binary: selectedBinary,
      args: RPC_PROBE_ARGS,
      environment: probeEnvironment,
      stdin: RPC_MODEL_COMMAND,
    });
    const fallback = fallbackMessage(selectedBinary);
    if (rpcOutcome._tag !== "completed" || rpcOutcome.result.code !== 0) {
      return {
        installed: true,
        selectedBinary,
        version,
        status: "warning",
        auth: { status: "unknown" },
        models: [],
        message: [fallback, "Phi RPC model and authentication probe failed."]
          .filter(Boolean)
          .join(" "),
      };
    }

    const parsed = parsePhiAvailableModelsResponse(rpcOutcome.result.stdout);
    if (!parsed) {
      return {
        installed: true,
        selectedBinary,
        version,
        status: "warning",
        auth: { status: "unknown" },
        models: [],
        message: [fallback, "Phi RPC returned no usable model response."].filter(Boolean).join(" "),
      };
    }

    const authMessage =
      parsed.auth.status === "unauthenticated"
        ? "Phi has no authenticated models. Authenticate a model provider in Phi and try again."
        : parsed.auth.status === "unknown"
          ? "Phi RPC did not report model authentication status."
          : undefined;
    return {
      installed: true,
      selectedBinary,
      version,
      status:
        parsed.auth.status === "authenticated"
          ? "ready"
          : parsed.auth.status === "unauthenticated"
            ? "error"
            : "warning",
      auth: parsed.auth,
      models: parsed.models,
      ...([fallback, authMessage].filter(Boolean).length > 0
        ? { message: [fallback, authMessage].filter(Boolean).join(" ") }
        : {}),
    };
  });

const runPhiProbeCommand: PhiProbeCommandRunner<ChildProcessSpawner.ChildProcessSpawner> = (
  input,
) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const spawnCommand = yield* resolveSpawnCommand(input.binary, input.args, {
      env: input.environment,
    });
    const child = yield* spawner.spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: input.environment,
        shell: spawnCommand.shell,
      }),
    );
    const writeStdin =
      input.stdin === undefined
        ? Effect.void
        : Stream.run(Stream.encodeText(Stream.make(input.stdin)), child.stdin);
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        collectStreamAsString(child.stdout),
        collectStreamAsString(child.stderr),
        child.exitCode.pipe(Effect.map(Number)),
        writeStdin,
      ],
      { concurrency: "unbounded" },
    );
    return { stdout, stderr, code: exitCode } satisfies CommandResult;
  }).pipe(
    Effect.scoped,
    Effect.timeoutOption(PROBE_TIMEOUT_MS),
    Effect.result,
    Effect.map((result): PhiProbeCommandOutcome => {
      if (Result.isFailure(result)) {
        return isCommandMissingCause(result.failure) ? { _tag: "missing" } : { _tag: "failed" };
      }
      if (Option.isNone(result.success)) return { _tag: "timedOut" };
      return { _tag: "completed", result: result.success.value };
    }),
  );

export function buildInitialPhiProviderSnapshot(
  settings: PhiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    return buildServerProvider({
      presentation: PHI_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: [],
      probe: settings.enabled
        ? {
            installed: true,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Checking Phi CLI availability...",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Phi is disabled in T3 Code settings.",
          },
    });
  });
}

export const checkPhiProviderStatus = Effect.fn("checkPhiProviderStatus")(function* (
  settings: PhiSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  if (!settings.enabled) {
    return yield* buildInitialPhiProviderSnapshot(settings);
  }

  const probe = yield* probePhiCli(settings, environment, runPhiProbeCommand);
  return buildServerProvider({
    presentation: PHI_PRESENTATION,
    enabled: true,
    checkedAt,
    models: probe.models,
    probe: {
      installed: probe.installed,
      version: probe.version,
      status: probe.status,
      auth: probe.auth,
      ...(probe.message ? { message: probe.message } : {}),
    },
  });
});
