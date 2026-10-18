// opencode-rate-limit-retry — OpenCode v2 server plugin (fork).
//
// Upstream @bdliyq/opencode-rate-limit-retry@1.0.3 targets the pre-v2 plugin
// contract: `export default async function ({ client }) { return { event } }`.
// OpenCode v2 rejects that shape at load time:
//
//   PluginModule.LoadError: Plugin must export a default definition with an id
//   and an effect or setup function. (cause: SchemaError(Expected object
//   at ["default"]))
//
// This fork migrates it to the v2 contract (`Plugin.define({ id, setup })`)
// and to the v2 event surface. The retry policy itself is unchanged:
// exponential backoff with jitter, capped attempts, same config file lookup,
// same debug log path.

import { Plugin } from "@opencode/plugin";
import { appendFileSync } from "node:fs";
import { join } from "node:path";

interface RetryConfig {
  enabled: boolean;
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
  jitterFactor: number;
  errorPatterns: string[];
}

const DEFAULT_CONFIG: RetryConfig = {
  enabled: true,
  maxRetries: 5,
  baseDelayMs: 5000,
  maxDelayMs: 120000,
  jitterFactor: 0.2,
  errorPatterns: [
    "rate increased too quickly",
    "scale requests more smoothly",
    "ensure system stability",
    "rate limit",
    "rate_limit",
    "too many requests",
    "quota exceeded",
    "usage exceeded",
  ],
};

const DEBUG_LOG_PATH = join(
  process.env.HOME || "~",
  ".config",
  "opencode",
  "rate-limit-retry-debug.log",
);

function writeDebugLog(content: string) {
  try {
    appendFileSync(DEBUG_LOG_PATH, `[${new Date().toISOString()}] ${content}\n`);
  } catch {
    // A missing/unwritable debug log must never take the plugin down.
  }
}

async function loadConfig(): Promise<RetryConfig> {
  const configPaths = [
    join(process.env.HOME || "~", ".config", "opencode", "rate-limit-retry.json"),
    join(process.cwd(), ".opencode", "rate-limit-retry.json"),
    join(process.cwd(), "rate-limit-retry.json"),
  ];
  for (const configPath of configPaths) {
    try {
      const file = Bun.file(configPath);
      if (file.size > 0) {
        return { ...DEFAULT_CONFIG, ...JSON.parse(await file.text()) };
      }
    } catch {
      // Missing or malformed config falls through to the next candidate.
    }
  }
  return DEFAULT_CONFIG;
}

function isRateLimitError(error: unknown, patterns: string[]): boolean {
  if (!error || typeof error !== "object") return false;
  const err = error as {
    type?: string;
    message?: string;
    status?: number;
    response?: { body?: string };
  };
  const haystack = [
    String(err.response?.body ?? ""),
    String(err.message ?? ""),
    String(err.type ?? ""),
    String(err.status ?? ""),
  ]
    .join(" ")
    .toLowerCase();
  return patterns.some((pattern) => haystack.includes(pattern.toLowerCase()));
}

function calculateDelay(
  attempt: number,
  baseDelayMs: number,
  maxDelayMs: number,
  jitterFactor: number,
): number {
  const exponential = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
  const jitter = exponential * jitterFactor * (Math.random() * 2 - 1);
  return Math.max(0, Math.round(exponential + jitter));
}

interface StepFailed {
  type: "session.step.failed";
  data: {
    sessionID: string;
    error: { type: string; message: string; status?: number; response?: { body?: string } };
  };
}

interface ModelSelected {
  type: "session.model.selected";
  data: { sessionID: string; model: { providerID: string; id: string } };
}

function isStepFailed(event: { type: string; data?: unknown }): event is StepFailed {
  if (event.type !== "session.step.failed") return false;
  const data = event.data as StepFailed["data"] | undefined;
  return typeof data?.sessionID === "string" && typeof data?.error === "object" && data.error !== null;
}

function isModelSelected(event: { type: string; data?: unknown }): event is ModelSelected {
  if (event.type !== "session.model.selected") return false;
  const data = event.data as ModelSelected["data"] | undefined;
  return typeof data?.sessionID === "string" && typeof data?.model?.providerID === "string";
}

export default Plugin.define({
  id: "opencode-rate-limit-retry",
  async setup(ctx) {
    const config = await loadConfig();
    if (!config.enabled) {
      writeDebugLog(`[SETUP] disabled via config, exiting`);
      return;
    }

    const retryState = new Map<
      string,
      { attempts: number; lastAttemptTime: number }
    >();
    const retryingSessions = new Set<string>();
    const sessionModels = new Map<string, { providerID: string; modelID: string }>();

    writeDebugLog(`[SETUP] listening for rate limit errors (maxRetries=${config.maxRetries})`);

    const controller = new AbortController();

    (async () => {
      try {
        for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
          if (!event || typeof event.type !== "string") continue;

          if (isModelSelected(event)) {
            const { sessionID, model } = event.data;
            sessionModels.set(sessionID, { providerID: model.providerID, modelID: model.id });
            writeDebugLog(`[TRACK] ${sessionID} => ${model.providerID}/${model.id}`);
            continue;
          }

          if (!isStepFailed(event)) continue;

          const { sessionID, error } = event.data;
          if (!isRateLimitError(error, config.errorPatterns)) {
            writeDebugLog(`[SKIP] ${sessionID} step failed but not a rate limit error`);
            continue;
          }
          writeDebugLog(`[MATCH] rate limit error in ${sessionID}: ${error.message}`);

          if (retryingSessions.has(sessionID)) {
            writeDebugLog(`[SKIP] ${sessionID} already retrying`);
            continue;
          }

          let state = retryState.get(sessionID);
          const now = Date.now();
          if (!state || now - state.lastAttemptTime > 300_000) {
            state = { attempts: 0, lastAttemptTime: now };
            retryState.set(sessionID, state);
          }

          if (state.attempts >= config.maxRetries) {
            writeDebugLog(`[SKIP] ${sessionID} exhausted ${state.attempts}/${config.maxRetries} retries`);
            retryState.delete(sessionID);
            continue;
          }

          const model = sessionModels.get(sessionID);
          if (!model) {
            writeDebugLog(`[SKIP] no model recorded for ${sessionID}`);
            continue;
          }

          state.attempts += 1;
          state.lastAttemptTime = now;
          retryingSessions.add(sessionID);

          const delay = calculateDelay(
            state.attempts - 1,
            config.baseDelayMs,
            config.maxDelayMs,
            config.jitterFactor,
          );
          writeDebugLog(
            `[RETRY] ${sessionID} attempt ${state.attempts}/${config.maxRetries} ` +
              `on ${model.providerID}/${model.modelID} after ${delay}ms`,
          );

          try {
            // Re-select the model that failed: the provider, not the session,
            // is what recovered after the backoff.
            await ctx.session.switchModel({ sessionID, model });

            await new Promise((resolve) => setTimeout(resolve, delay));

            await ctx.session.synthetic({
              sessionID,
              text:
                `Rate limit retry — attempt ${state.attempts}/${config.maxRetries} ` +
                `after ${Math.round(delay / 1000)}s backoff. Continue from where you left off.`,
              description: "rate-limit-retry",
            });

            state.attempts = 0;
            writeDebugLog(`[RETRY] ${sessionID} retry notice delivered, attempt counter reset`);
          } catch (cause) {
            writeDebugLog(`[RETRY] ${sessionID} retry failed: ${String(cause)}`);
          } finally {
            retryingSessions.delete(sessionID);
          }
        }
      } catch (cause) {
        writeDebugLog(`[STREAM] event subscription ended: ${String(cause)}`);
      }
    })();

    return () => {
      controller.abort();
      writeDebugLog(`[SHUTDOWN] plugin cleanup complete`);
    };
  },
});

// `await` is not permitted at module scope in this loader's transform, so the
// config reader resolves through an explicit async helper instead.
async function await0(path: string): Promise<string> {
  return await Bun.file(path).text();
}