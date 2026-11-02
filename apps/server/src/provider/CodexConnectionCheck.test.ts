import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/http";

import { checkCodexConnection } from "./CodexConnectionCheck.ts";

const answering = (status: number, requested: Array<string> = []) =>
  HttpClient.make((request) => {
    requested.push(request.url);
    return Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status })));
  });

const check = (
  client: HttpClient.HttpClient,
  input: Partial<Parameters<typeof checkCodexConnection>[0]> = {},
) =>
  checkCodexConnection({ authType: "chatgpt", environment: {}, ...input }).pipe(
    Effect.provideService(HttpClient.HttpClient, client),
  );

describe("checkCodexConnection", () => {
  it.effect("reports reachable when the model endpoint refuses an anonymous probe", () =>
    Effect.gen(function* () {
      for (const status of [200, 401, 403, 404, 405]) {
        assert.equal(yield* check(answering(status)), "reachable");
      }
    }),
  );

  it.effect("keeps waiting through overloads, timeouts and redirects", () =>
    Effect.gen(function* () {
      for (const status of [302, 408, 429, 500, 503]) {
        assert.equal(yield* check(answering(status)), "unreachable");
      }
    }),
  );

  it.effect("keeps waiting when the request fails, as it does during a DNS or network outage", () =>
    Effect.gen(function* () {
      const refused = HttpClient.make(() => Effect.die("unused")).pipe(
        HttpClient.mapRequestEffect(() => Effect.fail(new Error("getaddrinfo ENOTFOUND") as never)),
      );
      assert.equal(yield* check(refused), "unreachable");
    }),
  );

  it.effect("gives up on a request that never answers", () =>
    Effect.gen(function* () {
      const fiber = yield* check(HttpClient.make(() => Effect.never)).pipe(Effect.forkChild);
      yield* TestClock.adjust("5 seconds");
      assert.equal(yield* Fiber.join(fiber), "unreachable");
    }),
  );

  it.effect("checks the endpoint for the signed-in account", () =>
    Effect.gen(function* () {
      const requested: Array<string> = [];
      yield* check(answering(401, requested));
      yield* check(answering(401, requested), { authType: "apiKey" });
      assert.deepEqual(requested, [
        "https://chatgpt.com/backend-api/codex/responses",
        "https://api.openai.com/v1/responses",
      ]);
    }),
  );

  it.effect("waits rather than giving up when the account is unknown during an outage", () =>
    Effect.gen(function* () {
      assert.equal(
        yield* check(
          HttpClient.make(() => Effect.die("no request")),
          { authType: undefined },
        ),
        "unreachable",
      );
    }),
  );

  it.effect("cannot verify proxies, custom base URLs or other account kinds", () =>
    Effect.gen(function* () {
      const untouched = HttpClient.make(() => Effect.die("no request"));
      for (const environment of [
        { HTTPS_PROXY: "http://proxy:3128" },
        { all_proxy: "socks5://proxy" },
        { OPENAI_BASE_URL: "https://litellm.internal" },
      ]) {
        assert.equal(yield* check(untouched, { environment }), "unverifiable");
      }
      assert.equal(yield* check(untouched, { authType: "amazonBedrock" }), "unverifiable");
    }),
  );
});
