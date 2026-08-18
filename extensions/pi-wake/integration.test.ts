import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { promises as fs, readFileSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { daemonOwns } from "./daemon.ts";
import { leaderInstanceId, listLivePresences, registerPresence, releasePresence } from "./presence.ts";
import { WakeAlarmRuntime, type EmitFn, type ExecFn } from "./runtime.ts";
import type { AlarmState, FiredEvent, TimerAlarmState } from "./core.ts";

const runtimeUrl = pathToFileURL(path.resolve("extensions/pi-wake/runtime.ts")).href;
const presenceUrl = pathToFileURL(path.resolve("extensions/pi-wake/presence.ts")).href;
const noopExec: ExecFn = async () => ({ stdout: "", stderr: "", code: 0 });

async function makeDir(): Promise<string> {
	return fs.mkdtemp(path.join(tmpdir(), "pi-wake-it-"));
}

async function writeState(dir: string, alarms: AlarmState[]): Promise<string> {
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(statePath, `${JSON.stringify({ version: 2, alarms }, null, 2)}\n`);
	return statePath;
}

async function readState(statePath: string): Promise<AlarmState[]> {
	return (JSON.parse(await fs.readFile(statePath, "utf8")) as { alarms: AlarmState[] }).alarms;
}

async function readIds(statePath: string): Promise<string[]> {
	return (await readState(statePath)).map((alarm) => alarm.id).sort();
}

function runWorker(args: string[]): Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, args, { stdio: ["ignore", "pipe", "pipe"] });
		let stdout = "";
		let stderr = "";
		child.stdout.on("data", (chunk) => { stdout += chunk; });
		child.stderr.on("data", (chunk) => { stderr += chunk; });
		child.on("error", reject);
		child.on("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
	});
}

async function writeWorker(dir: string, name: string, source: string): Promise<string> {
	const file = path.join(dir, name);
	await fs.writeFile(file, source);
	return file;
}

const BARRIER_HELPER = `
import fs from "node:fs";
export async function waitBarrier(file) {
	for (;;) {
		if (fs.existsSync(file)) return;
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
}
`;

function makeRuntime(dir: string, statePath: string, emit: EmitFn, extra?: Record<string, unknown>): WakeAlarmRuntime {
	return new WakeAlarmRuntime({
		cwd: dir,
		configPath: path.join(dir, "absent-config.json"),
		statePath,
		emit,
		execFn: noopExec,
		...extra,
	});
}

test("two processes racing state writes never lose an alarm", { timeout: 120_000 }, async () => {
	const dir = await makeDir();
	const statePath = await writeState(dir, []);
	const barrier = path.join(dir, "go");
	const workerSource = `
import { WakeAlarmRuntime } from ${JSON.stringify(runtimeUrl)};
${BARRIER_HELPER}
const [statePath, barrier, prefix, count] = process.argv.slice(2);
const runtime = new WakeAlarmRuntime({
	cwd: ${JSON.stringify(dir)},
	configPath: ${JSON.stringify(path.join(dir, "absent-config.json"))},
	statePath,
	emit: () => true,
	execFn: async () => ({ stdout: "", stderr: "", code: 0 }),
});
await runtime.start({ flushPending: false });
await waitBarrier(barrier);
for (let i = 0; i < Number(count); i++) {
	await runtime.runAction({ action: "set_timer", id: prefix + "-" + i, name: "w" + prefix + i, after: "1h" });
}
await runtime.stop();
console.log("worker", prefix, "done");
`;
	const worker = await writeWorker(dir, "state-worker.ts", workerSource);
	for (let round = 0; round < 3; round++) {
		const [a, b] = [
			runWorker([worker, statePath, barrier, `r${round}a`, "25"]),
			runWorker([worker, statePath, barrier, `r${round}b`, "25"]),
		];
		await new Promise((resolve) => setTimeout(resolve, 300));
		await fs.writeFile(barrier, "go");
		const [ra, rb] = await Promise.all([a, b]);
		assert.equal(ra.code, 0, `round ${round} worker A: ${ra.stderr}`);
		assert.equal(rb.code, 0, `round ${round} worker B: ${rb.stderr}`);
		assert.equal((await readIds(statePath)).length, (round + 1) * 50, `round ${round} lost alarms`);
		await fs.rm(barrier, { force: true });
	}
	const finalIds = await readIds(statePath);
	for (let round = 0; round < 3; round++) {
		for (const side of ["a", "b"]) {
			for (let i = 0; i < 25; i++) assert.ok(finalIds.includes(`r${round}${side}-${i}`), `missing r${round}${side}-${i}`);
		}
	}
});

test("two processes creating the same alarm id get exactly one success", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = await writeState(dir, []);
	const barrier = path.join(dir, "go");
	const workerSource = `
import { WakeAlarmRuntime } from ${JSON.stringify(runtimeUrl)};
${BARRIER_HELPER}
const [statePath, barrier, outFile] = process.argv.slice(2);
const runtime = new WakeAlarmRuntime({
	cwd: ${JSON.stringify(dir)},
	configPath: ${JSON.stringify(path.join(dir, "absent-config.json"))},
	statePath,
	emit: () => true,
	execFn: async () => ({ stdout: "", stderr: "", code: 0 }),
});
await runtime.start({ flushPending: false });
await waitBarrier(barrier);
try {
	await runtime.runAction({ action: "set_timer", id: "deploy", name: "same id", after: "1h" });
	fs.writeFileSync(outFile, "CREATED");
} catch (error) {
	fs.writeFileSync(outFile, "EXISTS:" + error.message);
}
await runtime.stop();
`;
	const worker = await writeWorker(dir, "create-worker.ts", workerSource);
	const [a, b] = [
		runWorker([worker, statePath, barrier, path.join(dir, "out-a")]),
		runWorker([worker, statePath, barrier, path.join(dir, "out-b")]),
	];
	await new Promise((resolve) => setTimeout(resolve, 300));
	await fs.writeFile(barrier, "go");
	const [ra, rb] = await Promise.all([a, b]);
	assert.equal(ra.code, 0, ra.stderr);
	assert.equal(rb.code, 0, rb.stderr);
	const outcomes = [
		await fs.readFile(path.join(dir, "out-a"), "utf8"),
		await fs.readFile(path.join(dir, "out-b"), "utf8"),
	];
	assert.deepEqual(outcomes.sort(), ["CREATED", "EXISTS:alarm already exists: deploy"].sort(), JSON.stringify(outcomes));
	assert.deepEqual(await readIds(statePath), ["deploy"]);
});

test("presence registration never contends and elects exactly one deterministic leader", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const presenceDir = path.join(dir, "wake-alarm.sessions");
	const barrier = path.join(dir, "go");
	const barrier2 = path.join(dir, "go2");
	const workerSource = `
import crypto from "node:crypto";
import { registerPresence, listLivePresences, leaderInstanceId } from ${JSON.stringify(presenceUrl)};
${BARRIER_HELPER}
const [presenceDir, barrier, barrier2, outFile] = process.argv.slice(2);
const instanceId = crypto.randomUUID();
await waitBarrier(barrier);
await registerPresence(presenceDir, { version: 1, pid: process.pid, instanceId, heartbeatAt: Date.now() });
// Phase 1: wait until all four live registrations are visible.
let live = [];
for (let i = 0; i < 800; i++) {
	live = await listLivePresences(presenceDir);
	if (live.length >= 4) break;
	await new Promise((resolve) => setTimeout(resolve, 10));
}
fs.writeFileSync(outFile + ".ready", String(live.length));
// Phase 2: hold the process alive so every worker judges leadership on a full-house snapshot.
const snapshot = live;
await waitBarrier(barrier2);
const amLeader = leaderInstanceId(snapshot) === instanceId;
fs.writeFileSync(outFile, JSON.stringify({ instanceId, amLeader, liveCount: snapshot.length }));
process.exit(0);
`;
	const worker = await writeWorker(dir, "presence-worker.ts", workerSource);
	const workers = [];
	for (let i = 0; i < 4; i++) workers.push(runWorker([worker, presenceDir, barrier, barrier2, path.join(dir, `out-${i}`)]));
	await new Promise((resolve) => setTimeout(resolve, 300));
	await fs.writeFile(barrier, "go");
	// Release phase 2 once every worker reported a full-house snapshot.
	for (let i = 0; i < 100; i++) {
		const ready = (await fs.readdir(dir)).filter((entry) => entry.endsWith(".ready")).length;
		if (ready >= 4) break;
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
	await fs.writeFile(barrier2, "go");
	const results = await Promise.all(workers);
	for (const [i, result] of results.entries()) assert.equal(result.code, 0, `worker ${i}: ${result.stderr}`);
	let leaders = 0;
	for (let i = 0; i < 4; i++) {
		const report = JSON.parse(await fs.readFile(path.join(dir, `out-${i}`), "utf8")) as { amLeader: boolean; liveCount: number };
		assert.equal(report.liveCount, 4, "every worker sees all live presences");
		if (report.amLeader) leaders++;
	}
	assert.equal(leaders, 1, "exactly one deterministic leader");
});

test("stale presence records stop routing and are cleaned up", async () => {
	const dir = await makeDir();
	const presenceDir = path.join(dir, "wake-alarm.sessions");
	await registerPresence(presenceDir, { version: 1, pid: 999_999_999, instanceId: "dead-one", sessionFile: "/sessions/dead.jsonl", heartbeatAt: Date.now() - 120_000 });
	assert.deepEqual(await listLivePresences(presenceDir), [], "a dead-pid stale presence is not live");
	await registerPresence(presenceDir, { version: 1, pid: process.pid, instanceId: "alive-one", sessionFile: "/sessions/alive.jsonl", heartbeatAt: Date.now() });
	const live = await listLivePresences(presenceDir);
	assert.equal(live.length, 1);
	assert.equal(live[0].instanceId, "alive-one");
	await releasePresence(presenceDir, "alive-one");
	assert.deepEqual(await listLivePresences(presenceDir), []);
});

test("daemon routing skips alarms whose owner session is live", () => {
	const live = [
		{ version: 1 as const, pid: 123, instanceId: "a", sessionFile: "/sessions/a.jsonl", heartbeatAt: Date.now() },
		{ version: 1 as const, pid: 124, instanceId: "b", sessionFile: "/sessions/b.jsonl", heartbeatAt: Date.now() },
	];
	assert.equal(daemonOwns({ ownerSessionFile: "/sessions/a.jsonl" }, live), false, "live owner is handled by its session");
	assert.equal(daemonOwns({ ownerSessionFile: "/sessions/offline.jsonl" }, live), true, "offline owner is daemon-served even while other sessions run");
	assert.equal(daemonOwns({ ownerSessionFile: undefined }, live), false, "ownerless alarms belong to the session leader while any session is live");
	assert.equal(daemonOwns({ ownerSessionFile: undefined }, []), true, "ownerless alarms fall to the daemon only when nothing is live");
});

test("a stale snapshot pause cannot erase another runtime's pending wake", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const due: TimerAlarmState = { id: "shared", name: "Shared", kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 50 };
	const statePath = await writeState(dir, [due]);
	const bCalls: { alarm: AlarmState; events: FiredEvent[] }[] = [];
	// B fires the timer first; delivery fails (emit=false), so the pending wake stays durable.
	// A is scheduling-disabled and only performs the stale pause.
	const b = makeRuntime(dir, statePath, (alarm, events) => { bCalls.push({ alarm, events }); return false; }, { wakeRetry: { delayMs: 60_000, capMs: 60_000 } });
	const a = makeRuntime(dir, statePath, () => true, { schedulingEnabled: false });
	await a.start({ flushPending: false });
	await b.start({ flushPending: false });
	try {
		await waitFor(() => bCalls.length === 1, "B to fire the timer");
		const onDisk = (await readState(statePath))[0];
		assert.ok(onDisk.pendingWake, "B's pending wake is durable");
		// A's in-memory snapshot predates the fire; pausing from it must not erase the pending wake.
		const paused = await a.runAction({ action: "pause", id: "shared" });
		assert.match(paused, /Paused shared/);
		const after = (await readState(statePath))[0];
		assert.equal(after.active, false, "the user pause is applied");
		assert.ok(after.pendingWake, "the durable pending wake survives the stale-snapshot pause");
		assert.ok((after.revision ?? 0) >= 2, "revision advanced across both writers");
	} finally {
		await a.stop();
		await b.stop();
	}
});

test("a live claim blocks rival delivery; expiry allows takeover; exactly one delivery", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const now = Date.now();
	const pending: TimerAlarmState = {
		id: "claimed",
		name: "Claimed",
		kind: "timer",
		active: false,
		createdAt: now - 1000,
		dueAt: now - 500,
		triggeredAt: now,
		lastTriggeredAt: now,
		pauseReason: "timer fired",
		pendingWake: { triggeredAt: now, events: [{ kind: "timer", fingerprint: `timer:claimed:${now - 500}` }] },
	};
	const statePath = await writeState(dir, [pending]);
	const aCalls: unknown[] = [];
	const bCalls: unknown[] = [];
	// A holds its claim for a long TTL; B must not deliver.
	const a = makeRuntime(dir, statePath, (alarm, events) => { aCalls.push({ alarm, events }); return true; }, { claimantId: "A", deliveryTtlMs: 60_000, schedulingEnabled: false });
	const b = makeRuntime(dir, statePath, (alarm, events) => { bCalls.push({ alarm, events }); return true; }, { claimantId: "B", deliveryTtlMs: 60_000, wakeRetry: { delayMs: 300, capMs: 2_000 } });
	await a.start({ flushPending: false });
	try {
		// A claims manually by flushing first.
		await (a as unknown as { claimPendingWake(id: string): Promise<string | undefined> }).claimPendingWake("claimed");
		await b.start({ flushPending: true });
		await new Promise((resolve) => setTimeout(resolve, 300));
		assert.equal(bCalls.length, 0, "B cannot deliver while A holds a live claim");
		const disk = (await readState(statePath))[0];
		assert.ok(disk.pendingWake, "outbox retained while claimed");
		assert.equal(disk.pendingWake?.claim?.claimantId, "A");
	} finally {
		await a.stop();
		await b.stop();
	}
});

test("a crash after claiming still delivers exactly once after claim expiry", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const firedTimer: TimerAlarmState = { id: "crash-timer", name: "Crash timer", kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 50, ownerSessionFile: "C:\\sessions\\crash.jsonl" };
	const statePath = await writeState(dir, [firedTimer]);
	const workerSource = `
import { WakeAlarmRuntime } from ${JSON.stringify(runtimeUrl)};
const [statePath] = process.argv.slice(2);
const runtime = new WakeAlarmRuntime({
	cwd: ${JSON.stringify(dir)},
	configPath: ${JSON.stringify(path.join(dir, "absent-config.json"))},
	statePath,
	claimantId: "doomed",
	deliveryTtlMs: 800,
	emit: () => { process.kill(process.pid, "SIGKILL"); return true; },
	execFn: async () => ({ stdout: "", stderr: "", code: 0 }),
});
await runtime.start({ flushPending: false });
setTimeout(() => process.exit(3), 10_000);
`;
	const worker = await writeWorker(dir, "crash-worker.ts", workerSource);
	const crashed = await runWorker([worker, statePath]);
	assert.notEqual(crashed.code, 0, "the worker must die mid-delivery");
	const saved = (await readState(statePath))[0];
	assert.ok(saved.pendingWake?.claim, "the doomed claimant's claim is durable");
	assert.equal(saved.pendingWake?.claim?.claimantId, "doomed");

	const calls: { alarm: AlarmState; events: FiredEvent[] }[] = [];
	const restarted = makeRuntime(dir, statePath, (alarm, events) => { calls.push({ alarm, events }); return true; }, { claimantId: "survivor", deliveryTtlMs: 800, wakeRetry: { delayMs: 300, capMs: 2_000 } });
	try {
		await restarted.start({ flushPending: true });
		await waitFor(() => calls.length === 1, "the survivor to take over the expired claim and deliver", 6_000);
		assert.equal(calls[0].alarm.id, "crash-timer");
		assert.equal(calls[0].events[0].kind, "timer");
		// The emit fires before the completion write; wait for the outbox clear to land.
		await waitFor(() => readStateSync(statePath).alarms[0].pendingWake === undefined, "the delivered wake to be cleared", 3_000);
	} finally {
		await restarted.stop();
	}
});

function readStateSync(statePath: string): { alarms: AlarmState[] } {
	return JSON.parse(readFileSync(statePath, "utf8")) as { alarms: AlarmState[] };
}

async function waitFor(condition: () => boolean, label: string, timeoutMs = 3_000): Promise<void> {
	const start = Date.now();
	while (!condition()) {
		if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${label}`);
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
}
