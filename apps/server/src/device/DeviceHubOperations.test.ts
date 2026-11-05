import { expect, it } from "@effect/vitest";
import { LOCAL_DEVICE_HOST_ID } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { HttpClient, HttpClientResponse } from "effect/http";

import { makeDeviceHubOperations } from "./DeviceHubOperations.ts";

it.effect("lists only AVD names when the emulator mixes log lines into -list-avds", () =>
  Effect.gen(function* () {
    const operations = makeDeviceHubOperations({
      hostId: LOCAL_DEVICE_HOST_ID,
      origin: "http://device.test",
      http: HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(request, Response.json({ simulators: [], emulators: [] })),
        ),
      ),
      run: () =>
        Effect.succeed({
          code: 0,
          stderr: "",
          stdout:
            "INFO    | Storing crashdata in: /tmp/android-user/emu-crash-34.1.19.db, detection is enabled for process: 4242\nPixel_8_API_34\n",
        }),
      androidAvailable: Effect.succeed(true),
    });

    const { devices } = yield* operations.list;
    expect(devices.map((device) => device.name)).toEqual(["Pixel_8_API_34"]);
  }),
);
