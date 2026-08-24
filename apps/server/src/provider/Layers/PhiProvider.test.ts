import { describe, expect, it } from "@effect/vitest";
import type { PhiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  parsePhiAvailableModelsResponse,
  probePhiCli,
  type PhiProbeCommandInput,
  type PhiProbeCommandOutcome,
  type PhiProbeCommandRunner,
} from "./PhiProvider.ts";

const settings = (overrides: Partial<PhiSettings> = {}): PhiSettings => ({
  enabled: true,
  binaryPath: "",
  homePath: "",
  ...overrides,
});

const completed = (stdout: string, code = 0, stderr = ""): PhiProbeCommandOutcome => ({
  _tag: "completed",
  result: { stdout, stderr, code },
});

const rpcModels = (models: ReadonlyArray<Record<string, unknown>>): PhiProbeCommandOutcome =>
  completed(
    `${JSON.stringify({
      type: "response",
      command: "get_available_models",
      success: true,
      data: { models },
    })}\n`,
  );

describe("probePhiCli", () => {
  it.effect("reports a missing CLI after trying phi then pi", () => {
    const calls: PhiProbeCommandInput[] = [];
    const run: PhiProbeCommandRunner = (input) => {
      calls.push(input);
      return Effect.succeed({ _tag: "missing" });
    };

    return Effect.gen(function* () {
      const result = yield* probePhiCli(settings(), {}, run);

      expect(result).toMatchObject({
        installed: false,
        selectedBinary: null,
        auth: { status: "unknown" },
        models: [],
      });
      expect(calls.map((call) => [call.binary, call.args])).toEqual([
        ["phi", ["--version"]],
        ["pi", ["--version"]],
      ]);
    });
  });

  it.effect("prefers phi and discovers authenticated models through RPC", () => {
    const calls: PhiProbeCommandInput[] = [];
    const run: PhiProbeCommandRunner = (input) => {
      calls.push(input);
      if (input.args[0] === "--version") {
        return Effect.succeed(completed("phi 1.2.3\n"));
      }
      return Effect.succeed(
        rpcModels([
          {
            provider: "example",
            id: "model-one",
            name: "Model One",
            authenticated: true,
            apiKey: "must-not-escape",
          },
        ]),
      );
    };

    return Effect.gen(function* () {
      const result = yield* probePhiCli(settings(), {}, run);

      expect(result).toMatchObject({
        installed: true,
        selectedBinary: "phi",
        version: "1.2.3",
        status: "ready",
        auth: { status: "authenticated" },
      });
      expect(result.models.map((model) => [model.slug, model.name])).toEqual([
        ["example/model-one", "Model One"],
      ]);
      expect(JSON.stringify(result)).not.toContain("must-not-escape");
      expect(calls.map((call) => call.binary)).toEqual(["phi", "phi"]);
      expect(calls[1]?.args).toEqual(["--mode", "rpc", "--no-session"]);
      expect(calls[1]?.stdin).toBe('{"type":"get_available_models"}\n');
    });
  });

  it.effect("falls back to pi and reports unauthenticated models", () => {
    const calls: PhiProbeCommandInput[] = [];
    const run: PhiProbeCommandRunner = (input) => {
      calls.push(input);
      if (input.binary === "phi") return Effect.succeed({ _tag: "missing" });
      if (input.args[0] === "--version") return Effect.succeed(completed("pi 0.73.1"));
      return Effect.succeed(
        rpcModels([
          {
            provider: "example",
            id: "model-two",
            name: "Model Two",
            authenticated: false,
          },
        ]),
      );
    };

    return Effect.gen(function* () {
      const result = yield* probePhiCli(settings(), {}, run);

      expect(result).toMatchObject({
        installed: true,
        selectedBinary: "pi",
        version: "0.73.1",
        status: "error",
        auth: { status: "unauthenticated" },
      });
      expect(result.message).toContain("fallback `pi`");
      expect(calls.map((call) => call.binary)).toEqual(["phi", "pi", "pi"]);
    });
  });

  it.effect("uses only a configured binary and passes the custom config path safely", () => {
    const calls: PhiProbeCommandInput[] = [];
    const run: PhiProbeCommandRunner = (input) => {
      calls.push(input);
      return Effect.succeed({ _tag: "missing" });
    };

    return Effect.gen(function* () {
      yield* probePhiCli(
        settings({ binaryPath: "/opt/phi/bin/phi", homePath: "/tmp/phi-agent" }),
        { PATH: "/usr/bin" },
        run,
      );

      expect(calls).toHaveLength(1);
      expect(calls[0]).toMatchObject({
        binary: "/opt/phi/bin/phi",
        environment: {
          PATH: "/usr/bin",
          PI_CODING_AGENT_DIR: "/tmp/phi-agent",
          PHI_CODING_AGENT_DIR: "/tmp/phi-agent",
        },
      });
    });
  });
});

describe("parsePhiAvailableModelsResponse", () => {
  it("ignores unrelated JSONL events and deduplicates provider/model ids", () => {
    const response = [
      JSON.stringify({ type: "agent_start" }),
      JSON.stringify({
        type: "response",
        command: "get_available_models",
        success: true,
        data: {
          models: [
            { provider: "acme", id: "one", name: "One", authenticated: true },
            { provider: "acme", id: "one", name: "Duplicate", authenticated: true },
          ],
        },
      }),
      "",
    ].join("\n");

    expect(parsePhiAvailableModelsResponse(response)).toMatchObject({
      auth: { status: "authenticated" },
      models: [{ slug: "acme/one", name: "One" }],
    });
  });
});
