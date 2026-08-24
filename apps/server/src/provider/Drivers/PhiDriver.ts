import {
  PhiSettings,
  ProviderDriverKind,
  TextGenerationError,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import type * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { ProviderAdapterRequestError, ProviderDriverError } from "../Errors.ts";
import { buildInitialPhiProviderSnapshot, checkPhiProviderStatus } from "../Layers/PhiProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const DRIVER_KIND = ProviderDriverKind.make("phi");
const decodePhiSettings = Schema.decodeSync(PhiSettings);
const maintenanceCapabilities = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});
const RUNTIME_UNAVAILABLE_DETAIL =
  "Phi session runtime is not available yet; this driver currently supports provider discovery only.";

export type PhiDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | ServerSettingsService;

const unavailableAdapterOperation = (method: string) =>
  Effect.fail(
    new ProviderAdapterRequestError({
      provider: DRIVER_KIND,
      method,
      detail: RUNTIME_UNAVAILABLE_DETAIL,
    }),
  );

function makeUnavailablePhiAdapter(): ProviderAdapterShape<ProviderAdapterRequestError> {
  return {
    provider: DRIVER_KIND,
    capabilities: { sessionModelSwitch: "unsupported" },
    startSession: () => unavailableAdapterOperation("startSession"),
    sendTurn: () => unavailableAdapterOperation("sendTurn"),
    interruptTurn: () => unavailableAdapterOperation("interruptTurn"),
    respondToRequest: () => unavailableAdapterOperation("respondToRequest"),
    respondToUserInput: () => unavailableAdapterOperation("respondToUserInput"),
    stopSession: () => unavailableAdapterOperation("stopSession"),
    listSessions: () => Effect.succeed([]),
    hasSession: () => Effect.succeed(false),
    readThread: () => unavailableAdapterOperation("readThread"),
    rollbackThread: () => unavailableAdapterOperation("rollbackThread"),
    stopAll: () => Effect.void,
    streamEvents: Stream.empty,
  };
}

function makeUnavailablePhiTextGeneration(): TextGeneration.TextGeneration["Service"] {
  const unavailable = (
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle",
  ) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: RUNTIME_UNAVAILABLE_DETAIL,
      }),
    );
  return {
    generateCommitMessage: () => unavailable("generateCommitMessage"),
    generatePrContent: () => unavailable("generatePrContent"),
    generateBranchName: () => unavailable("generateBranchName"),
    generateThreadTitle: () => unavailable("generateThreadTitle"),
  };
}

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export const PhiDriver: ProviderDriver<PhiSettings, PhiDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Phi",
    supportsMultipleInstances: true,
  },
  configSchema: PhiSettings,
  defaultConfig: (): PhiSettings => decodePhiSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies PhiSettings;
      const checkProvider = checkPhiProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<PhiSettings>>({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialPhiProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Phi snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter: makeUnavailablePhiAdapter(),
        textGeneration: makeUnavailablePhiTextGeneration(),
      } satisfies ProviderInstance;
    }),
};
