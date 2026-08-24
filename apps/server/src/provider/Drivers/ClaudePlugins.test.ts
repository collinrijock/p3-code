import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { discoverEnabledClaudePluginPaths } from "./ClaudePlugins.ts";

const writeJson = Effect.fn(function* (filePath: string, value: unknown) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fileSystem.makeDirectory(path.dirname(filePath), { recursive: true });
  yield* fileSystem.writeFileString(filePath, JSON.stringify(value));
});

const makePluginDirectory = Effect.fn(function* (pluginPath: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  yield* fileSystem.makeDirectory(pluginPath, { recursive: true });
});

it.layer(NodeServices.layer)("discoverEnabledClaudePluginPaths", (it) => {
  it.effect("selects the latest valid install for each enabled plugin and deduplicates paths", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-claude-plugins-",
      });
      const configDir = path.join(tempDir, "claude-home");
      const oldInstall = path.join(tempDir, "cache", "review", "1.0.0");
      const latestValidInstall = path.join(tempDir, "cache", "review", "2.0.0");
      const staleInstall = path.join(tempDir, "cache", "review", "3.0.0");
      const fileInsteadOfDirectory = path.join(tempDir, "cache", "not-a-directory");
      yield* makePluginDirectory(oldInstall);
      yield* makePluginDirectory(latestValidInstall);
      yield* fileSystem.makeDirectory(path.dirname(fileInsteadOfDirectory), { recursive: true });
      yield* fileSystem.writeFileString(fileInsteadOfDirectory, "not a plugin directory");

      yield* writeJson(path.join(configDir, "settings.json"), {
        enabledPlugins: {
          "review@marketplace": true,
          "disabled@marketplace": false,
          "missing@marketplace": true,
          "malformed@marketplace": true,
          "duplicate@marketplace": true,
        },
      });
      yield* writeJson(path.join(configDir, "plugins", "installed_plugins.json"), {
        version: 2,
        plugins: {
          "review@marketplace": [
            {
              installPath: oldInstall,
              lastUpdated: "2026-01-01T00:00:00.000Z",
            },
            {
              installPath: latestValidInstall,
              lastUpdated: "2026-02-01T00:00:00.000Z",
            },
            {
              installPath: staleInstall,
              lastUpdated: "2026-03-01T00:00:00.000Z",
            },
          ],
          "disabled@marketplace": [
            {
              installPath: oldInstall,
              lastUpdated: "2026-04-01T00:00:00.000Z",
            },
          ],
          "missing@marketplace": [
            {
              installPath: path.join(tempDir, "missing-plugin"),
              lastUpdated: "2026-04-01T00:00:00.000Z",
            },
          ],
          "malformed@marketplace": [
            null,
            { installPath: 42 },
            {
              installPath: fileInsteadOfDirectory,
              lastUpdated: "not-a-date",
            },
          ],
          "duplicate@marketplace": [
            {
              installPath: latestValidInstall,
              installedAt: "2026-02-01T00:00:00.000Z",
            },
          ],
        },
      });

      const pluginPaths = yield* discoverEnabledClaudePluginPaths({ homePath: configDir });

      assert.deepEqual(pluginPaths, [latestValidInstall]);
    }).pipe(Effect.scoped),
  );

  it.effect("uses CLAUDE_CONFIG_DIR when homePath is empty", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-claude-plugins-env-",
      });
      const workspace = path.join(tempDir, "workspace");
      const relativeConfigDir = "relative-claude-config";
      const configDir = path.join(workspace, relativeConfigDir);
      const pluginDir = path.join(tempDir, "cache", "env-plugin");
      yield* makePluginDirectory(pluginDir);
      yield* writeJson(path.join(configDir, "settings.json"), {
        enabledPlugins: { "env@marketplace": true },
      });
      yield* writeJson(path.join(configDir, "plugins", "installed_plugins.json"), {
        version: 2,
        plugins: {
          "env@marketplace": [{ installPath: pluginDir }],
        },
      });

      const pluginPaths = yield* discoverEnabledClaudePluginPaths({ homePath: "" }, workspace, {
        CLAUDE_CONFIG_DIR: relativeConfigDir,
      });

      assert.deepEqual(pluginPaths, [pluginDir]);
    }).pipe(Effect.scoped),
  );

  it.effect("prefers homePath over CLAUDE_CONFIG_DIR", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-claude-plugins-home-",
      });
      const explicitConfigDir = path.join(tempDir, "explicit-config");
      const environmentConfigDir = path.join(tempDir, "environment-config");
      const explicitPluginDir = path.join(tempDir, "cache", "explicit-plugin");
      const environmentPluginDir = path.join(tempDir, "cache", "environment-plugin");
      yield* makePluginDirectory(explicitPluginDir);
      yield* makePluginDirectory(environmentPluginDir);

      for (const [configDir, pluginId, pluginDir] of [
        [explicitConfigDir, "explicit@marketplace", explicitPluginDir],
        [environmentConfigDir, "environment@marketplace", environmentPluginDir],
      ] as const) {
        yield* writeJson(path.join(configDir, "settings.json"), {
          enabledPlugins: { [pluginId]: true },
        });
        yield* writeJson(path.join(configDir, "plugins", "installed_plugins.json"), {
          version: 2,
          plugins: { [pluginId]: [{ installPath: pluginDir }] },
        });
      }

      const pluginPaths = yield* discoverEnabledClaudePluginPaths(
        { homePath: explicitConfigDir },
        undefined,
        { CLAUDE_CONFIG_DIR: environmentConfigDir },
      );

      assert.deepEqual(pluginPaths, [explicitPluginDir]);
    }).pipe(Effect.scoped),
  );

  it.effect("returns no paths when cache data is missing or malformed", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-claude-plugins-malformed-",
      });
      const missingConfigDir = path.join(tempDir, "missing");
      const malformedConfigDir = path.join(tempDir, "malformed");

      const missing = yield* discoverEnabledClaudePluginPaths({ homePath: missingConfigDir });

      yield* fileSystem.makeDirectory(path.join(malformedConfigDir, "plugins"), {
        recursive: true,
      });
      yield* fileSystem.writeFileString(path.join(malformedConfigDir, "settings.json"), "{");
      yield* fileSystem.writeFileString(
        path.join(malformedConfigDir, "plugins", "installed_plugins.json"),
        "[]",
      );
      const malformed = yield* discoverEnabledClaudePluginPaths({
        homePath: malformedConfigDir,
      });

      assert.deepEqual(missing, []);
      assert.deepEqual(malformed, []);
    }).pipe(Effect.scoped),
  );
});
