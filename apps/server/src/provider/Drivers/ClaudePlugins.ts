/**
 * ClaudePlugins — best-effort discovery of enabled Claude Code plugin installs.
 *
 * The Agent SDK does not automatically load plugins installed through Claude
 * Code's marketplace. Live sessions must pass each enabled install explicitly
 * through the SDK's `plugins` option.
 *
 * @module provider/Drivers/ClaudePlugins
 */
import type { ClaudeSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { expandHomePath } from "../../pathExpansion.ts";
import { resolveClaudeConfigDirPath } from "./ClaudeSkills.ts";

const decodeJson = Schema.decodeUnknownExit(Schema.fromJsonString(Schema.Unknown));

type JsonRecord = Readonly<Record<string, unknown>>;

interface PluginInstall {
  readonly installPath: string;
  readonly updatedAt: number;
  readonly sourceIndex: number;
}

function asRecord(value: unknown): JsonRecord | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as JsonRecord)
    : undefined;
}

function parseJson(contents: string | undefined): unknown {
  if (contents === undefined) {
    return undefined;
  }
  const decoded = decodeJson(contents);
  return Exit.isSuccess(decoded) ? decoded.value : undefined;
}

function enabledPluginIds(settings: unknown): ReadonlyArray<string> {
  const enabledPlugins = asRecord(asRecord(settings)?.enabledPlugins);
  if (!enabledPlugins) {
    return [];
  }
  return Object.entries(enabledPlugins)
    .filter(([pluginId, enabled]) => pluginId.trim().length > 0 && enabled === true)
    .map(([pluginId]) => pluginId);
}

function installTimestamp(entry: JsonRecord): number {
  for (const key of ["lastUpdated", "installedAt"] as const) {
    const value = entry[key];
    if (typeof value !== "string") {
      continue;
    }
    const timestamp = Date.parse(value);
    if (Number.isFinite(timestamp)) {
      return timestamp;
    }
  }
  return Number.NEGATIVE_INFINITY;
}

function pluginInstalls(registry: unknown, pluginId: string): ReadonlyArray<PluginInstall> {
  const installs = asRecord(asRecord(registry)?.plugins)?.[pluginId];
  if (!Array.isArray(installs)) {
    return [];
  }

  return installs
    .flatMap((value, sourceIndex) => {
      const entry = asRecord(value);
      const installPath = entry?.installPath;
      if (typeof installPath !== "string" || installPath.trim().length === 0) {
        return [];
      }
      return [
        {
          installPath: installPath.trim(),
          updatedAt: installTimestamp(entry),
          sourceIndex,
        },
      ];
    })
    .toSorted(
      (left, right) => right.updatedAt - left.updatedAt || right.sourceIndex - left.sourceIndex,
    );
}

/**
 * Resolve the latest valid local install for each enabled user plugin.
 *
 * Missing files, malformed JSON, malformed entries, and stale cache paths are
 * ignored. Discovery therefore cannot prevent a Claude session from starting.
 */
export const discoverEnabledClaudePluginPaths = Effect.fn("discoverEnabledClaudePluginPaths")(
  function* (
    config: Pick<ClaudeSettings, "homePath">,
    cwd?: string,
    environment?: NodeJS.ProcessEnv,
  ): Effect.fn.Return<ReadonlyArray<string>, never, FileSystem.FileSystem | Path.Path> {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const configDirPath = yield* resolveClaudeConfigDirPath(
      config,
      environment ?? process.env,
      cwd,
    );

    const readJson = (filePath: string) =>
      fileSystem.readFileString(filePath).pipe(
        Effect.map(parseJson),
        Effect.orElseSucceed(() => undefined),
      );

    const [settings, registry] = yield* Effect.all([
      readJson(path.join(configDirPath, "settings.json")),
      readJson(path.join(configDirPath, "plugins", "installed_plugins.json")),
    ]);

    const selectedPaths: Array<string> = [];
    const seenPaths = new Set<string>();
    for (const pluginId of enabledPluginIds(settings)) {
      for (const install of pluginInstalls(registry, pluginId)) {
        const installPath = path.resolve(expandHomePath(install.installPath));
        if (seenPaths.has(installPath)) {
          break;
        }
        const info = yield* fileSystem
          .stat(installPath)
          .pipe(Effect.orElseSucceed(() => undefined));
        if (info?.type !== "Directory") {
          continue;
        }
        seenPaths.add(installPath);
        selectedPaths.push(installPath);
        break;
      }
    }

    return selectedPaths;
  },
);
