import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import wakeAlarmExtension from "./index.ts";

interface SentCapture {
	content: unknown;
	options: { triggerTurn?: boolean; deliverAs?: string } | undefined;
}

async function waitFor(condition: () => boolean | Promise<boolean>, label: string, timeoutMs = 10_000): Promise<void> {
	const start = Date.now();
	while (!(await condition())) {
		if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${label}`);
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
}

/**
 * The extension shell's delivery contract: an in-session wake must STEER into a
 * running turn (the agent loop drains the steering queue before every LLM
 * request), not queue as a followUp that only lands after the turn ends. Idle
 * sessions still get triggerTurn => an immediate new turn.
 */
test("session wakes steer into the running turn instead of piling up at turn end", { timeout: 30_000 }, async () => {
	// The shutdown hook would auto-spawn a daemon; tests manage their own processes.
	const prevNoSpawn = process.env.WAKE_ALARM_NO_AUTOSPAWN;
	process.env.WAKE_ALARM_NO_AUTOSPAWN = "1";
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-index-test-"));
	const statePath = path.join(dir, ".pi", "wake-alarm.state.json");
	const sessionFile = path.join(dir, "session.jsonl");
	// A fired timer whose wake is still undelivered and owned by this session:
	// session_start({flushPending:true}) must deliver it through sessionEmit.
	await fs.mkdir(path.dirname(statePath), { recursive: true });
	await fs.writeFile(statePath, `${JSON.stringify({
		version: 3,
		alarms: [{ id: "t1", name: "T1", kind: "timer", active: false, createdAt: 1, dueAt: 2, triggeredAt: 3, revision: 1 }],
		outbox: [{ eventId: "t1:1:abc", alarmId: "t1", alarmName: "T1", ownerSessionFile: sessionFile, triggeredAt: Date.now() - 1000, events: [{ kind: "timer", fingerprint: "timer:t1:1" }], message: "[Wake alarm] T1 (t1)" }],
	}, null, 2)}\n`);

	const sent: SentCapture[] = [];
	const handlers = new Map<string, (event: unknown, ctx: unknown) => Promise<void>>();
	const fakePi = {
		registerTool: () => undefined,
		registerCommand: () => undefined,
		on: (event: string, handler: (event: unknown, ctx: unknown) => Promise<void>) => handlers.set(event, handler),
		sendMessage: (message: { content: unknown }, options: SentCapture["options"]) => { sent.push({ content: message.content, options }); },
		exec: async () => ({ stdout: "", stderr: "", code: 0 }),
	};
	try {
		wakeAlarmExtension(fakePi as never);
		await handlers.get("session_start")!(undefined, {
			cwd: dir,
			hasUI: false,
			sessionManager: { getSessionFile: () => sessionFile },
		});
		await waitFor(() => sent.length === 1, "the pending wake to be delivered", 10_000);
		assert.match(String(sent[0].content), /\[Wake alarm\] T1/);
		assert.equal(sent[0].options?.deliverAs, "steer", "mid-turn wakes must steer into the running turn");
		assert.equal(sent[0].options?.triggerTurn, true, "idle sessions still get an immediate new turn");
	} finally {
		await handlers.get("session_shutdown")!(undefined, undefined).catch(() => undefined);
		if (prevNoSpawn === undefined) delete process.env.WAKE_ALARM_NO_AUTOSPAWN; else process.env.WAKE_ALARM_NO_AUTOSPAWN = prevNoSpawn;
	}
});
