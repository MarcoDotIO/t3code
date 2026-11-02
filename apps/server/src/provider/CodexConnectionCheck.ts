import type { ProviderConnectionCheck } from "@t3tools/provider-core/server/driver";
import * as Effect from "effect/Effect";
import { FetchHttpClient, HttpClient } from "effect/http";

const MODEL_ENDPOINT_BY_AUTH_TYPE: Readonly<Record<string, string>> = {
  chatgpt: "https://chatgpt.com/backend-api/codex/responses",
  apiKey: "https://api.openai.com/v1/responses",
};

// A direct check says nothing about a route through these, and could not reach past a proxy.
const CUSTOM_ROUTE_VARIABLE = /^(?:https?_proxy|all_proxy|openai_base_url|chatgpt_base_url)$/i;

/**
 * Checks the model endpoint Codex uses rather than the local app-server. Any answer short of an
 * overload or a timeout means the connection works: an unauthenticated probe is refused by design.
 */
export const checkCodexConnection = Effect.fn("CodexConnectionCheck.check")(
  function* (input: {
    readonly authType: string | undefined;
    readonly environment: NodeJS.ProcessEnv;
  }): Effect.fn.Return<ProviderConnectionCheck, unknown, HttpClient.HttpClient> {
    if (
      Object.entries(input.environment).some(
        ([name, value]) => value && CUSTOM_ROUTE_VARIABLE.test(name),
      )
    )
      return "unverifiable";
    // The account can be unknown only while Codex itself cannot answer, so wait and ask again.
    if (input.authType === undefined) return "unreachable";
    const endpoint = MODEL_ENDPOINT_BY_AUTH_TYPE[input.authType];
    if (endpoint === undefined) return "unverifiable";
    const http = yield* HttpClient.HttpClient;
    const response = yield* http
      .head(endpoint)
      .pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }));
    return response.status >= 200 &&
      response.status < 500 &&
      (response.status < 300 || response.status >= 400) &&
      response.status !== 408 &&
      response.status !== 429
      ? "reachable"
      : "unreachable";
  },
  Effect.timeoutOrElse({
    duration: "5 seconds",
    orElse: () => Effect.succeed<ProviderConnectionCheck>("unreachable"),
  }),
  Effect.orElseSucceed((): ProviderConnectionCheck => "unreachable"),
);
