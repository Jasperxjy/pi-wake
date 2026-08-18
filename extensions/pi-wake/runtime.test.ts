import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { WakeAlarmRuntime, type EmitFn, type ExecFn, type StoredState } from "./runtime.ts";
import type { AlarmState, OutboxEntry, TimerAlarmState } from "./core.ts";

const SESSION_X = "C:\\sessions\\x.jsonl";

async function makeFixture(alarms: AlarmState[], outbox: OutboxEntry[] = []): Promise<{ dir: string; statePath: string; configPath: string }> {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-runtime-test-"));
	// No config file on purpose: timer-only usage must work without wake-alarm.json.
	const configPath = path.join(dir, "wake-alarm.json");
	const statePath = path.join(dir, "state.json");
	await writeState(statePath, alarms, outbox);
	return { dir, statePath, configPath };
}

async function writeState(statePath: string, alarms: AlarmState[], outbox: OutboxEntry[] = []): Promise<void> {
	await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms, outbox } as StoredState, null, 2)}\n`);
}

async function readState(statePath: string): Promise<StoredState> {
	return JSON.parse(await fs.readFile(statePath, "utf8")) as StoredState;
}

const noopExec: ExecFn = async () => ({ stdout: "", stderr: "", code: 0 });

function recordingEmit(calls: OutboxEntry[]): EmitFn {
	return (entry) => {
		calls.push(entry);
		return true;
	};
}

async function waitFor(condition: () => boolean | Promise<boolean>, label: string, timeoutMs = 3000): Promise<void> {
	const start = Date.now();
	while (!(await condition())) {
		if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${label}`);
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
}

function dueTimer(id: string, ownerSessionFile?: string): TimerAlarmState {
	return { id, name: `Timer ${id}`, kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 50, ownerSessionFile, revision: 1 };
}

/** A timer due far enough in the future that a rival runtime's mutation reliably lands before the first tick. */
function futureTimer(id: string, ownerSessionFile?: string): TimerAlarmState {
	return { id, name: `Timer ${id}`, kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 1500, ownerSessionFile, revision: 1 };
}

test("concurrent sessions fire only the alarms they own and never clobber each other", async () => {
	const { statePath, configPath } = await makeFixture([dueTimer("owned-by-x", SESSION_X), dueTimer("ownerless")]);
	const xCalls: OutboxEntry[] = [];
	const yCalls: OutboxEntry[] = [];
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
		assert.deepEqual(xCalls.map((entry) => entry.alarmId), ["owned-by-x"]);
		assert.deepEqual(yCalls.map((entry) => entry.alarmId), ["ownerless"]);

		// Merge-save: a new alarm created through X must survive Y's subsequent writes.
		await x.runAction({ action: "set_timer", id: "late-addition", name: "Added by X", after: "1h" }, { ownerSessionFile: SESSION_X });
		await y.runAction({ action: "pause", id: "ownerless" });
		const disk = await readState(statePath);
		const byId = new Map(disk.alarms.map((alarm) => [alarm.id, alarm]));
		assert.equal(byId.size, 3);
		assert.equal((byId.get("owned-by-x") as AlarmState & { triggeredAt?: number }).triggeredAt !== undefined, true);
		assert.equal((byId.get("ownerless") as AlarmState & { triggeredAt?: number }).triggeredAt !== undefined, true);
		assert.equal(byId.get("ownerless")?.active, false);
		assert.equal(byId.get("late-addition")?.active, true);
		assert.deepEqual(disk.outbox, [], "delivered wakes are removed from the outbox");
	} finally {
		await x.stop();
		await y.stop();
	}
});

test("a session does not flush outbox wakes owned by another session", async () => {
	const fired: AlarmState = {
		...dueTimer("foreign-wake", SESSION_X),
		active: false,
		triggeredAt: 1000,
		lastTriggeredAt: 1000,
		pauseReason: "timer fired",
		dueAt: 1000,
		createdAt: 500,
	};
	const entry: OutboxEntry = {
		eventId: "foreign-wake:1000:abc",
		alarmId: "foreign-wake",
		alarmName: "Timer foreign-wake",
		ownerSessionFile: SESSION_X,
		triggeredAt: 1000,
		events: [{ kind: "timer", fingerprint: "timer:foreign-wake:1000" }],
		message: "[Wake alarm] Timer foreign-wake (foreign-wake)",
	};
	const { statePath, configPath } = await makeFixture([fired], [entry]);
	const calls: OutboxEntry[] = [];
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
		const disk = await readState(statePath);
		assert.equal(disk.outbox.length, 1, "the foreign wake must stay in the outbox");
	} finally {
		await other.stop();
	}
});

test("a stale scheduler cannot fire a timer that was paused by another runtime", async () => {
	const { statePath, configPath } = await makeFixture([futureTimer("shared")]);
	const calls: OutboxEntry[] = [];
	// A loaded the timer while it was due in ~1.5s and arms its scheduler.
	const a = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: recordingEmit(calls),
		execFn: noopExec,
		claimantId: "A",
	});
	await a.start({ flushPending: false });
	// B (scheduling-disabled, so it only mutates) pauses before A's deadline hits.
	const b = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: () => true,
		execFn: noopExec,
		schedulingEnabled: false,
		claimantId: "B",
	});
	await b.start({ flushPending: false });
	try {
		await b.runAction({ action: "pause", id: "shared" });
		// Make sure the pause is durable on disk BEFORE the original deadline passes.
		await waitFor(async () => !(await readState(statePath)).alarms[0].active, "the pause to land on disk", 2_000);
		// Wait well past the original deadline.
		await new Promise((resolve) => setTimeout(resolve, 1_200));
		assert.equal(calls.length, 0, "a stale scheduler must not fire a paused timer");
		const disk = (await readState(statePath)).alarms[0];
		assert.equal(disk.active, false);
		assert.equal(disk.kind === "timer" && disk.triggeredAt, undefined, "the paused timer never fired");
		assert.ok((disk.revision ?? 0) >= 2, "both writers advanced the revision");
	} finally {
		await a.stop();
		await b.stop();
	}
});

test("a stale scheduler cannot fire a timer that was reset to a later deadline", async () => {
	const { statePath, configPath } = await makeFixture([futureTimer("shared")]);
	const calls: OutboxEntry[] = [];
	const a = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: recordingEmit(calls),
		execFn: noopExec,
		claimantId: "A",
	});
	await a.start({ flushPending: false });
	const b = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: () => true,
		execFn: noopExec,
		schedulingEnabled: false,
		claimantId: "B",
	});
	await b.start({ flushPending: false });
	try {
		await b.runAction({ action: "reset", id: "shared", after: "1h" });
		await waitFor(async () => {
			const alarm = (await readState(statePath)).alarms[0];
			return alarm.kind === "timer" && alarm.dueAt > Date.now();
		}, "the reset to land on disk", 2_000);
		await new Promise((resolve) => setTimeout(resolve, 1_200));
		assert.equal(calls.length, 0, "a stale scheduler must not fire a reset timer");
		const disk = (await readState(statePath)).alarms[0];
		assert.ok(disk.kind === "timer" && disk.dueAt > Date.now(), "the reset deadline is in the future");
		assert.equal(disk.active, true);
		assert.ok((disk.revision ?? 0) >= 2);
	} finally {
		await a.stop();
		await b.stop();
	}
});

test("resetting an alarm with an undelivered wake keeps both valid and delivers the wake", async () => {
	const { statePath, configPath } = await makeFixture([dueTimer("shared")], [{
		eventId: "shared:1000:old",
		alarmId: "shared",
		alarmName: "Timer shared",
		ownerSessionFile: SESSION_X,
		triggeredAt: 1000,
		events: [{ kind: "timer", fingerprint: "timer:shared:1000" }],
		message: "[Wake alarm] Timer shared (shared)",
	}]);
	const calls: OutboxEntry[] = [];
	const b = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: recordingEmit(calls),
		execFn: noopExec,
		schedulingEnabled: false,
	});
	try {
		await b.start({ flushPending: false });
		await b.runAction({ action: "reset", id: "shared", after: "1h" });
		const disk = await readState(statePath);
		const timer = disk.alarms[0];
		assert.equal(timer.active, true, "reset re-arms the timer");
		assert.ok(timer.kind === "timer" && timer.dueAt > Date.now(), "reset deadline is in the future");
		assert.equal(disk.outbox.length, 1, "the undelivered wake survives the reset as an independent outbox fact");
		// The merged state must restore cleanly for a fresh runtime (no cross-generation corruption).
		const fresh = new WakeAlarmRuntime({
			cwd: path.dirname(statePath),
			configPath,
			statePath,
			emit: recordingEmit(calls),
			execFn: noopExec,
			schedulingEnabled: false,
		});
		await fresh.start({ flushPending: false });
		assert.equal(fresh.alarmCount, 1);
		assert.equal(fresh.outboxCount, 1);
		await fresh.stop();
	} finally {
		await b.stop();
	}
});

test("reconcile adopts alarms created on disk after startup and drops removed ones", async () => {
	const { statePath, configPath } = await makeFixture([]);
	const runtime = new WakeAlarmRuntime({
		cwd: path.dirname(statePath),
		configPath,
		statePath,
		emit: () => true,
		execFn: noopExec,
		schedulingEnabled: false,
	});
	try {
		await runtime.start({ flushPending: false });
		assert.equal(runtime.alarmCount, 0);
		// Another session creates a timer on disk while this runtime is already running.
		await writeState(statePath, [{ ...dueTimer("later"), dueAt: Date.now() + 60_000 }]);
		await runtime.reconcileFromDisk();
		assert.equal(runtime.alarmCount, 1);
		assert.match(await runtime.runAction({ action: "list" }), /later/);
		// A subsequent local action still works against the adopted alarm.
		assert.match(await runtime.runAction({ action: "pause", id: "later" }), /Paused later/);
		// The alarm vanishes from disk (another session removed it): reconcile drops it locally.
		await writeState(statePath, []);
		await runtime.reconcileFromDisk();
		assert.equal(runtime.alarmCount, 0);
	} finally {
		await runtime.stop();
	}
});
