import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AsyncDbWriter, DatabaseOperations } from "@better-ccflare/database";
import { UsageCollector } from "../usage-collector";
import type { StartMessage } from "../worker-messages";

const JSONL_ENV_KEY = "BETTER_CCFLARE_REQUEST_RESPONSE_JSONL_PATH";
const ORIGINAL_JSONL_PATH = process.env[JSONL_ENV_KEY];

function makeCollector(getStorePayloads: () => boolean): UsageCollector {
	const dbOps = {
		saveRequest: async () => {},
	} as unknown as DatabaseOperations;

	const asyncWriter = {
		enqueue: async (fn: () => Promise<void> | void) => {
			await fn();
		},
		canAcceptPayload: () => true,
		recordPayloadDrop: () => {},
		enqueuePayload: (
			_requestId: string,
			_bytes: number,
			task: () => Promise<void> | void,
		) => {
			void Promise.resolve(task());
			return true;
		},
		dispose: async () => {},
	} as unknown as AsyncDbWriter;

	return new UsageCollector(dbOps, asyncWriter, getStorePayloads, () => {});
}

function createStartMessage(requestId: string): StartMessage {
	return {
		type: "start",
		messageId: `msg-${requestId}`,
		requestId,
		accountId: null,
		method: "POST",
		path: "/v1/messages",
		timestamp: Date.now(),
		requestHeaders: { "content-type": "application/json" },
		requestBody: Buffer.from(
			JSON.stringify({
				messages: [{ role: "user", content: "hello" }],
			}),
		).toString("base64"),
		project: null,
		responseStatus: 200,
		responseHeaders: { "content-type": "application/json" },
		isStream: false,
		providerName: "anthropic",
		accountBillingType: null,
		accountAutoPauseOnOverageEnabled: 0,
		accountName: null,
		agentUsed: null,
		comboName: null,
		apiKeyId: null,
		apiKeyName: null,
		retryAttempt: 0,
		failoverAttempts: 0,
	};
}

describe("UsageCollector JSONL logging", () => {
	let tmpPath: string | null = null;

	afterEach(() => {
		if (ORIGINAL_JSONL_PATH === undefined) {
			delete process.env[JSONL_ENV_KEY];
		} else {
			process.env[JSONL_ENV_KEY] = ORIGINAL_JSONL_PATH;
		}
		if (tmpPath) {
			rmSync(tmpPath, { recursive: true, force: true });
			tmpPath = null;
		}
	});

	it("writes a JSONL record for request/response bodies when enabled", async () => {
		tmpPath = mkdtempSync(join(tmpdir(), "better-ccflare-jsonl-"));
		const jsonlPath = join(tmpPath, "requests.jsonl");
		process.env[JSONL_ENV_KEY] = jsonlPath;

		const collector = makeCollector(() => false);
		try {
			collector.handleStart(createStartMessage("req-jsonl"));
			await collector.handleEnd({
				type: "end",
				requestId: "req-jsonl",
				success: true,
				responseBody: Buffer.from(
					JSON.stringify({ content: [{ text: "world" }] }),
				).toString("base64"),
			});

			const lines = readFileSync(jsonlPath, "utf8").trim().split("\n");
			const entry = JSON.parse(lines[0]) as {
				request: { body: string | null };
				response: { body: string | null };
				meta: { requestId: string };
			};

			expect(lines.length).toBe(1);
			expect(entry.meta.requestId).toBe("req-jsonl");
			expect(entry.request.body).toContain('"content":"hello"');
			expect(entry.response.body).toContain('"text":"world"');
		} finally {
			collector.dispose();
		}
	});

	it("captures streaming response chunks into JSONL when payload storage is disabled", async () => {
		tmpPath = mkdtempSync(join(tmpdir(), "better-ccflare-jsonl-"));
		const jsonlPath = join(tmpPath, "stream.jsonl");
		process.env[JSONL_ENV_KEY] = jsonlPath;

		const collector = makeCollector(() => false);
		try {
			const start = createStartMessage("req-stream");
			start.isStream = true;
			start.responseHeaders = { "content-type": "text/event-stream" };

			collector.handleStart(start);
			collector.handleChunk(
				"req-stream",
				new TextEncoder().encode("data: one\n\ndata: two\n\n"),
			);
			await collector.handleEnd({
				type: "end",
				requestId: "req-stream",
				success: true,
			});

			const lines = readFileSync(jsonlPath, "utf8").trim().split("\n");
			const entry = JSON.parse(lines[0]) as {
				response: { body: string | null };
				meta: { isStream: boolean };
			};

			expect(entry.meta.isStream).toBe(true);
			expect(entry.response.body).toContain("data: one");
			expect(entry.response.body).toContain("data: two");
		} finally {
			collector.dispose();
		}
	});
});
