import { expect, it } from "@effect/vitest";

import {
  BUILT_IN_SOURCE_CONTROL_DRIVER_LIST,
  sourceControlDefinitions,
} from "./sourceControlDefinitions.ts";

it("runs a driver for every host the server and clients read, in their order", () => {
  expect(BUILT_IN_SOURCE_CONTROL_DRIVER_LIST.map((driver) => driver.definition)).toEqual(
    sourceControlDefinitions.definitions,
  );
});

it.each(["ssh", "git+ssh", "ssh+git"])(
  "does not take the port of a %s:// remote for the web server's",
  (scheme) => {
    expect(
      sourceControlDefinitions.recognizeRemote(
        `${scheme}://git@github.example.com:2222/team/project.git`,
      ),
    ).toMatchObject({ kind: "github", baseUrl: "https://github.example.com" });
  },
);
