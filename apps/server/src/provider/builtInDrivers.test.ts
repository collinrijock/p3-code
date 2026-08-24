import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind } from "@t3tools/contracts";

import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";

describe("BUILT_IN_DRIVERS", () => {
  it("registers the Phi provider driver", () => {
    const phi = BUILT_IN_DRIVERS.find(
      (driver) => driver.driverKind === ProviderDriverKind.make("phi"),
    );

    expect(phi?.metadata).toMatchObject({
      displayName: "Phi",
      supportsMultipleInstances: true,
    });
    expect(phi?.defaultConfig()).toEqual({
      enabled: false,
      binaryPath: "",
      homePath: "",
    });
  });
});
