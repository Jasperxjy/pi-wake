import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { WakeAlarmRuntime, type EmitFn, type ExecFn } from "./runtime.ts";
import type { AlarmState, FiredEvent, TimerAlarmState } from "./core.ts";

const SESSION_X = "C:\\sessions\\x.jsonl";

async function makeFixture(alarms: AlarmState[]): Promise<{ dir: string; statePath: string; configPath: string }> {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-runtime-test-"));
	// No config file on purpose: timer-only usage must work without wake-alarm.json.
	const configPath = path.join(dir, "wake-alarm.json");
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(statePath, `${JSON.stringify({ version: 2, alarms }, null, 2)}\n`);
	return { dir, statePath, configPath };
}

function readAlarms(statePath: string): Promise<AlarmState[]> {
	return fs.readFile(statePath, "utf8").then((text) => (JSON.parse(text) as { alarms: AlarmState[] }).alarms);
}

const noopExec: ExecFn = async () => ({ stdout: "", stderr: "", code: 0 });

function recordingEmit(calls: { alarm: AlarmState; events: FiredEvent[] }[]): EmitFn {
	return (alarm, events) => {
		calls.push({ alarm, events });
		return true;
	};
}

async function waitFor(condition: () => boolean, label: string, timeoutMs = 3000): Promise<void> {
	const start = Date.now();
	while (!condition()) {
		if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${label}`);
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
}

function dueTimer(id: string, ownerSessionFile?: string): TimerAlarmState {
	return { id, name: `Timer ${id}`, kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 50, ownerSessionFile };
}

test("concurrent sessions fire only the alarms they own and never clobber each other", async () => {
	const { statePath, configPath } = await makeFixture([dueTimer("owned-by-x", SESSION_X), dueTimer("ownerless")]);
	const xCalls: { alarm: AlarmState; events: FiredEvent[] }[] = [];
	const yCalls: { alarm: AlarmState; events: FiredEvent[] }[] = [];
	const x = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: recordingEmit(xCalls),
		execFn: noopExec,
		owns: (alarm) => (alarm.ownerSessionFile === undefined ? false : alarm.ownerSessionFile === SESSION_X),
	});
	const y = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: recordingEmit(yCalls),
		execFn: noopExec,
		owns: (alarm) => alarm.ownerSessionFile === undefined,
	});
	try {
		await x.start();
		await waitFor(() => xCalls.length === 1, "X to fire its owned timer");
		await y.start();
		await waitFor(() => yCalls.length === 1, "Y to fire the ownerless timer");
		assert.deepEqual(xCalls.map((call) => call.alarm.id), ["owned-by-x"]);
		assert.deepEqual(yCalls.map((call) => call.alarm.id), ["ownerless"]);

		// Merge-save: a new alarm created through X must survive Y's subsequent writes.
		await x.runAction({ action: "set_timer", id: "late-addition", name: "Added by X", after: "1h" }, { ownerSessionFile: SESSION_X });
		await y.runAction({ action: "pause", id: "ownerless" });
		const disk = await readAlarms(statePath);
		const byId = new Map(disk.map((alarm) => [alarm.id, alarm]));
		assert.equal(byId.size, 3);
		assert.equal((byId.get("owned-by-x") as AlarmState & { triggeredAt?: number }).triggeredAt !== undefined, true);
		assert.equal(byId.get("owned-by-x")?.pendingWake, undefined);
		assert.equal((byId.get("ownerless") as AlarmState & { triggeredAt?: number }).triggeredAt !== undefined, true);
		assert.equal(byId.get("ownerless")?.active, false);
		assert.equal(byId.get("late-addition")?.active, true);
	} finally {
		await x.stop();
		await y.stop();
	}
});

test("a session does not flush pending wakes owned by another session", async () => {
	const pending: AlarmState = {
		...dueTimer("foreign-wake", SESSION_X),
		active: false,
		triggeredAt: 1000,
		lastTriggeredAt: 1000,
		pauseReason: "timer fired",
		pendingWake: { triggeredAt: 1000, events: [{ kind: "timer", fingerprint: "timer:foreign-wake:1000" }] },
		dueAt: 1000,
		createdAt: 500,
	};
	const { statePath, configPath } = await makeFixture([pending]);
	const calls: { alarm: AlarmState; events: FiredEvent[] }[] = [];
	const other = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: recordingEmit(calls),
		execFn: noopExec,
		owns: (alarm) => alarm.ownerSessionFile === "C:\\sessions\\someone-else.jsonl",
	});
	try {
		await other.start({ flushPending: true });
		await new Promise((resolve) => setTimeout(resolve, 300));
		assert.equal(calls.length, 0);
		const disk = await readAlarms(statePath);
		assert.ok(disk[0].pendingWake, "foreign pending wake must stay in the outbox");
	} finally {
		await other.stop();
	}
});
