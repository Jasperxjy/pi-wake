import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { promises as fs, readFileSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { createDaemonEmit, daemonOwns } from "./daemon.ts";
import { leaderInstanceId, listLivePresences, registerPresence, releasePresence } from "./presence.ts";
import { StateLock } from "./lock.ts";
import { WakeAlarmRuntime, type EmitFn, type ExecFn, type StoredState } from "./runtime.ts";
import { applyBaseline, createContainerAlarm, type AlarmState, type FiredEvent, type OutboxEntry, type ProbeResult, type TimerAlarmState } from "./core.ts";

const runtimeUrl = pathToFileURL(path.resolve("extensions/pi-wake/runtime.ts")).href;
const presenceUrl = pathToFileURL(path.resolve("extensions/pi-wake/presence.ts")).href;
const lockUrl = pathToFileURL(path.resolve("extensions/pi-wake/lock.ts")).href;
const noopExec: ExecFn = async () => ({ stdout: "", stderr: "", code: 0 });

async function makeDir(): Promise<string> {
	return fs.mkdtemp(path.join(tmpdir(), "pi-wake-it-"));
}

async function writeState(statePath: string, alarms: AlarmState[], outbox: OutboxEntry[] = []): Promise<void> {
	await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms, outbox } as StoredState, null, 2)}\n`);
}

async function readState(statePath: string): Promise<StoredState> {
	return JSON.parse(await fs.readFile(statePath, "utf8")) as StoredState;
}

async function readIds(statePath: string): Promise<string[]> {
	return (await readState(statePath)).alarms.map((alarm) => alarm.id).sort();
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

function cannedProbe(overrides: Record<string, unknown> = {}): { stdout: string; stderr: string; code: number } {
	return {
		stdout: JSON.stringify({
			exists: true,
			containerId: "c".repeat(64),
			running: false,
			status: "exited",
			containerStatus: "exited",
			startedAt: "2026-01-01T00:00:00.000Z",
			exitCode: 0,
			oomKilled: false,
			logMode: null,
			selectedLogPath: null,
			logFileId: null,
			logOffset: 0,
			logCursor: null,
			logReset: false,
			logBase64: "",
			tailBase64: "",
			...overrides,
		}),
		stderr: "",
		code: 0,
	};
}

test("two processes racing state writes never lose an alarm", { timeout: 120_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await writeState(statePath, []);
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
	const statePath = path.join(dir, "state.json");
	await writeState(statePath, []);
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

test("a stale snapshot pause cannot erase another runtime's undelivered outbox wake", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	const due: TimerAlarmState = { id: "shared", name: "Shared", kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 50 };
	await writeState(statePath, [due]);
	const bCalls: OutboxEntry[] = [];
	// B fires the timer first; delivery fails (emit=false), so the wake stays durable in the outbox.
	const b = makeRuntime(dir, statePath, (entry) => { bCalls.push(entry); return false; }, { wakeRetry: { delayMs: 60_000, capMs: 60_000 } });
	const a = makeRuntime(dir, statePath, () => true, { schedulingEnabled: false });
	await a.start({ flushPending: false });
	await b.start({ flushPending: false });
	try {
		await waitFor(() => bCalls.length === 1, "B to fire the timer");
		const onDisk = await readState(statePath);
		assert.equal(onDisk.outbox.length, 1, "B's wake is durable in the outbox");
		// A's in-memory snapshot predates the fire; pausing from it must not erase the wake.
		const paused = await a.runAction({ action: "pause", id: "shared" });
		assert.match(paused, /Paused shared/);
		const after = await readState(statePath);
		const alarm = after.alarms[0];
		assert.equal(alarm.active, false, "the user pause is applied");
		assert.equal(after.outbox.length, 1, "the durable wake survives the stale-snapshot pause");
		assert.ok((alarm.revision ?? 0) >= 2, "revision advanced across both writers");
	} finally {
		await a.stop();
		await b.stop();
	}
});

test("a live claim blocks rival delivery; expiry allows takeover; exactly one delivery", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const now = Date.now();
	const fired: TimerAlarmState = {
		id: "claimed",
		name: "Claimed",
		kind: "timer",
		active: false,
		createdAt: now - 1000,
		dueAt: now - 500,
		triggeredAt: now,
		lastTriggeredAt: now,
		pauseReason: "timer fired",
	};
	const entry: OutboxEntry = {
		eventId: "claimed:1000:x",
		alarmId: "claimed",
		alarmName: "Claimed",
		triggeredAt: now,
		events: [{ kind: "timer", fingerprint: `timer:claimed:${now - 500}` }],
		message: "[Wake alarm] Claimed (claimed)",
	};
	const statePath = path.join(dir, "state.json");
	await writeState(statePath, [fired], [entry]);
	const aCalls: OutboxEntry[] = [];
	const bCalls: OutboxEntry[] = [];
	// A holds its claim for a long TTL; B must not deliver.
	const a = makeRuntime(dir, statePath, (e) => { aCalls.push(e); return true; }, { claimantId: "A", deliveryTtlMs: 60_000, schedulingEnabled: false });
	const b = makeRuntime(dir, statePath, (e) => { bCalls.push(e); return true; }, { claimantId: "B", deliveryTtlMs: 60_000, wakeRetry: { delayMs: 300, capMs: 2_000 } });
	await a.start({ flushPending: false });
	try {
		// A claims the outbox entry manually.
		const token = await (a as unknown as { claimOutboxEntry(eventId: string): Promise<string | undefined> }).claimOutboxEntry("claimed:1000:x");
		assert.ok(token, "A obtains the delivery claim");
		await b.start({ flushPending: true });
		await new Promise((resolve) => setTimeout(resolve, 300));
		assert.equal(bCalls.length, 0, "B cannot deliver while A holds a live claim");
		const disk = await readState(statePath);
		assert.equal(disk.outbox.length, 1, "outbox retained while claimed");
		assert.equal(disk.outbox[0].claim?.claimantId, "A");
	} finally {
		await a.stop();
		await b.stop();
	}
});

test("a crash after claiming still delivers exactly once after claim expiry", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	const firedTimer: TimerAlarmState = { id: "crash-timer", name: "Crash timer", kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 50, ownerSessionFile: "C:\\sessions\\crash.jsonl" };
	await writeState(statePath, [firedTimer]);
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
	const saved = await readState(statePath);
	assert.equal(saved.outbox.length, 1);
	assert.ok(saved.outbox[0].claim, "the doomed claimant's claim is durable");
	assert.equal(saved.outbox[0].claim?.claimantId, "doomed");

	const calls: OutboxEntry[] = [];
	const restarted = makeRuntime(dir, statePath, (entry) => { calls.push(entry); return true; }, { claimantId: "survivor", deliveryTtlMs: 800, wakeRetry: { delayMs: 300, capMs: 2_000 } });
	try {
		await restarted.start({ flushPending: true });
		await waitFor(() => calls.length === 1, "the survivor to take over the expired claim and deliver", 6_000);
		assert.equal(calls[0].alarmId, "crash-timer");
		assert.equal(calls[0].events[0].kind, "timer");
		// The emit fires before the completion write; wait for the outbox clear to land.
		await waitFor(() => readStateSync(statePath).outbox.length === 0, "the delivered wake to be cleared", 3_000);
	} finally {
		await restarted.stop();
	}
});

test("keep policy records one outbox entry per same-kind occurrence without loss or corruption", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	// A remote section is required for the probe path; a fake identity file suffices.
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const cid = "c".repeat(64);
	const base: ProbeResult = { exists: true, containerId: cid, running: false, status: "exited", containerStatus: "exited", startedAt: "2026-01-01T00:00:00.000Z", exitCode: 0, oomKilled: false, logMode: "docker-logs", selectedLogPath: undefined, logFileId: undefined, logOffset: 0, logCursor: "2026-01-01T00:00:00Z", logBytes: new Uint8Array(), tail: "" };
	const alarm = applyBaseline(createContainerAlarm({ id: "keep", name: "Keep", container: "job", events: ["exit"], policy: "keep", now: Date.now(), statusPollMs: 1000 }), base, Date.now());
	await writeState(statePath, [alarm]);
	const calls: OutboxEntry[] = [];
	const startedAts = ["2026-01-01T00:00:01.000Z", "2026-01-01T00:00:02.000Z", "2026-01-01T00:00:02.000Z"];
	let probes = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return true; },
		execFn: async () => cannedProbe({ startedAt: startedAts[Math.min(probes++, startedAts.length - 1)] }),
	});
	try {
		await runtime.start({ flushPending: false });
		await waitFor(() => calls.length === 2, "two distinct exit occurrences to fire and deliver", 10_000);
		assert.deepEqual(calls.map((entry) => entry.events[0].kind), ["exit", "exit"]);
		assert.notEqual(calls[0].events[0].fingerprint, calls[1].events[0].fingerprint, "the two occurrences have distinct fingerprints");
		// The emit fires before the completion write; wait for both deliveries to settle on disk.
		await waitFor(() => readStateSync(statePath).outbox.length === 0, "both delivered wakes to be cleared", 3_000);
		const disk = await readState(statePath);
		assert.deepEqual(disk.outbox, [], "both wakes were delivered and removed");
		// The accumulated state (two same-kind fingerprints) restores cleanly.
		const restored = makeRuntime(dir, statePath, () => true, { schedulingEnabled: false });
		await restored.start({ flushPending: false });
		assert.equal(restored.alarmCount, 1);
		await restored.stop();
	} finally {
		await runtime.stop();
	}
});

test("dead state-lock takeover race keeps the critical section exclusive", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const lockPath = path.join(dir, "state.json.lock");
	const barrier = path.join(dir, "takeover-go");
	const logFile = path.join(dir, "critical.log");
	// Pre-place a stale lock: dead pid, old mtime, unparsable-by-token shape.
	await fs.writeFile(lockPath, `${JSON.stringify({ pid: 999_999_999, token: "dead", createdAt: Date.now() - 120_000 })}\n`);
	await fs.utimes(lockPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));
	const workerSource = `
import { StateLock } from ${JSON.stringify(lockUrl)};
${BARRIER_HELPER}
const [lockPath, barrier, logFile, id] = process.argv.slice(2);
const lock = new StateLock({ path: lockPath, hooks: { beforeTakeover: () => waitBarrier(barrier) } });
await lock.acquire();
await lock.verifyHeld();
fs.appendFileSync(logFile, "enter:" + id + "\\n");
await new Promise((resolve) => setTimeout(resolve, 120));
fs.appendFileSync(logFile, "exit:" + id + "\\n");
await lock.release();
console.log(id, "done");
`;
	const worker = await writeWorker(dir, "lock-worker.ts", workerSource);
	const [a, b] = [runWorker([worker, lockPath, barrier, logFile, "A"]), runWorker([worker, lockPath, barrier, logFile, "B"])];
	// Both contenders must observe the same stale lock and hit the takeover barrier together.
	await new Promise((resolve) => setTimeout(resolve, 300));
	await fs.writeFile(barrier, "go");
	const [ra, rb] = await Promise.all([a, b]);
	assert.equal(ra.code, 0, ra.stderr);
	assert.equal(rb.code, 0, rb.stderr);
	const lines = (await fs.readFile(logFile, "utf8")).trim().split("\n").filter(Boolean);
	assert.equal(lines.length, 4, `expected enter/exit pairs, got: ${JSON.stringify(lines)}`);
	// Strict alternation proves the two critical sections never overlapped.
	for (let i = 0; i < lines.length; i += 2) {
		const [action, id] = lines[i].split(":");
		assert.equal(action, "enter", `line ${i} must be an enter: ${JSON.stringify(lines)}`);
		assert.equal(lines[i + 1], `exit:${id}`, `enter:${id} must be closed by its own exit: ${JSON.stringify(lines)}`);
	}
	assert.deepEqual([lines[0].split(":")[1], lines[2].split(":")[1]].sort(), ["A", "B"], "both contenders entered exactly once");
});

test("daemon emit re-checks presence and refuses to spawn when the owner came back live", async () => {
	const dir = await makeDir();
	const presenceDir = path.join(dir, "wake-alarm.sessions");
	const sessionFile = path.join(dir, "owner.jsonl");
	await fs.writeFile(sessionFile, "");
	await registerPresence(presenceDir, { version: 1, pid: process.pid, instanceId: "owner-live", sessionFile, heartbeatAt: Date.now() });
	const entry: OutboxEntry = {
		eventId: "w:1:x",
		alarmId: "w",
		alarmName: "W",
		ownerSessionFile: sessionFile,
		triggeredAt: Date.now(),
		events: [{ kind: "timer", fingerprint: "timer:w:1" }],
		message: "[Wake alarm] W (w)",
	};
	const logs: string[] = [];
	let spawns = 0;
	const emit = createDaemonEmit({
		getRuntime: () => ({ runtimeConfig: { spawnOnWake: true, headlessTrust: "saved", runTimeoutMs: 1000, piCommand: "pi-stub-command", maxEvidenceChars: 1000, includeWakeEvidence: true } }) as unknown as WakeAlarmRuntime,
		presenceDir,
		dryRun: false,
		spawnDisabled: false,
		isStopping: () => false,
		log: (message) => logs.push(message),
		runPi: async () => { spawns++; return 0; },
	});
	try {
		assert.equal(await emit(entry), false, "owner is live: no spawn, wake stays in the outbox");
		assert.equal(spawns, 0);
		assert.ok(logs.some((line) => line.includes("now live")), `expected a presence re-check log line: ${JSON.stringify(logs)}`);
		// Once the owner leaves, the same daemon proceeds to spawn.
		await releasePresence(presenceDir, "owner-live");
		assert.equal(await emit(entry), true);
		assert.equal(spawns, 1);
	} finally {
		await releasePresence(presenceDir, "owner-live");
	}
	// An UNREADABLE presence registry is "unknown", never "nobody is live": fail closed.
	const badDir = path.join(dir, "registry-as-file");
	await fs.writeFile(badDir, "not a directory");
	const logs2: string[] = [];
	let spawns2 = 0;
	const emit2 = createDaemonEmit({
		getRuntime: () => ({ runtimeConfig: { spawnOnWake: true, headlessTrust: "saved", runTimeoutMs: 1000, piCommand: "pi-stub-command", maxEvidenceChars: 1000, includeWakeEvidence: true } }) as unknown as WakeAlarmRuntime,
		presenceDir: badDir,
		dryRun: false,
		spawnDisabled: false,
		isStopping: () => false,
		log: (message) => logs2.push(message),
		runPi: async () => { spawns2++; return 0; },
	});
	assert.equal(await emit2(entry), false, "an unreadable presence registry must not lead to a spawn");
	assert.equal(spawns2, 0);
	assert.ok(logs2.some((line) => line.includes("cannot verify session presence")), `expected a fail-closed log line: ${JSON.stringify(logs2)}`);
});

test("evidence is opt-in: check never leaks it, the evidence action returns historical evidence", async () => {
	const dir = await makeDir();
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ includeWakeEvidence: false }));
	const statePath = path.join(dir, "state.json");
	const base: ProbeResult = { exists: true, containerId: "abc", running: true, status: "running", containerStatus: "running", startedAt: "2026-01-01T00:00:00Z", exitCode: 0, oomKilled: false, logMode: "docker-logs", selectedLogPath: undefined, logFileId: undefined, logOffset: 0, logCursor: "2026-01-01T00:00:00Z", logBytes: new Uint8Array(), tail: "" };
	const alarm = applyBaseline(createContainerAlarm({ id: "train-run", name: "Train run", container: "job", events: ["log-error"], now: Date.now() - 60_000, statusPollMs: 60_000 }), base, Date.now() - 60_000);
	const withEvidence: AlarmState = { ...alarm, lastEvidence: "Traceback: kaboom in trainer" };
	const entry: OutboxEntry = {
		eventId: "train-run:1:x",
		alarmId: "train-run",
		alarmName: "Train run",
		triggeredAt: Date.now() - 1000,
		events: [{ kind: "log-error", fingerprint: "log-error:10:abc", evidence: "kaboom line: boom" }],
		message: "[Wake alarm] Train run (train-run)",
	};
	await writeState(statePath, [withEvidence], [entry]);
	const runtime = new WakeAlarmRuntime({ cwd: dir, configPath, statePath, emit: () => true, execFn: noopExec, schedulingEnabled: false });
	try {
		await runtime.start({ flushPending: false });
		const check = await runtime.runAction({ action: "check", id: "train-run" });
		assert.ok(!check.includes("kaboom"), "check output does not contain raw evidence text");
		const evidence = await runtime.runAction({ action: "evidence", id: "train-run" });
		assert.ok(evidence.includes("kaboom"), "the explicit evidence action returns the stored evidence");
		assert.ok(evidence.includes("boom"), "both the outbox event and the alarm-level evidence are surfaced");
	} finally {
		await runtime.stop();
	}
});

test("a deadline crossing while the probe runs fires exactly once (keep policy)", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const now = Date.now();
	const base: ProbeResult = { exists: true, containerId: "c".repeat(64), running: true, status: "running", containerStatus: "running", startedAt: "2026-01-01T00:00:00.000Z", exitCode: 0, oomKilled: false, logMode: "docker-logs", selectedLogPath: undefined, logFileId: undefined, logOffset: 0, logCursor: "2026-01-01T00:00:00Z", logBytes: new Uint8Array(), tail: "" };
	// Fast polling starts a probe at +1s; the deadline elapses at +1.5s while that probe is still in flight.
	const alarm = applyBaseline(createContainerAlarm({ id: "dl", name: "Deadline", container: "job", events: ["deadline"], policy: "keep", now, deadlineMs: 1500, statusPollMs: 1000 }), base, now);
	await writeState(statePath, [alarm]);
	const calls: OutboxEntry[] = [];
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return true; },
		execFn: async () => { await new Promise((resolve) => setTimeout(resolve, 2000)); return cannedProbe(); },
	});
	try {
		await runtime.start({ flushPending: false });
		await waitFor(() => calls.length === 1, "the deadline to fire once after the probe returns", 8_000);
		assert.equal(calls[0].events[0].kind, "deadline");
		await new Promise((resolve) => setTimeout(resolve, 900));
		assert.equal(calls.length, 1, "the deadline must not re-fire after its fingerprint was recorded");
		const disk = await readState(statePath);
		assert.equal(disk.outbox.length, 0, "delivered wake removed");
		const alarmDisk = disk.alarms[0];
		assert.ok(alarmDisk.kind === "container" && alarmDisk.eventFingerprints.deadline, "the deadline fingerprint is durably recorded");
	} finally {
		await runtime.stop();
	}
});

test("a v2 state migrates to v3 in a single process", async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	const v2Alarm = { id: "legacy", name: "Legacy", kind: "timer", active: false, createdAt: 1000, dueAt: 2000, triggeredAt: 1500, lastTriggeredAt: 1500, pauseReason: "timer fired", pendingWake: { triggeredAt: 1500, events: [{ kind: "timer", fingerprint: "timer:legacy:2000" }] } };
	await fs.writeFile(statePath, `${JSON.stringify({ version: 2, alarms: [v2Alarm] })}\n`);
	const runtime = makeRuntime(dir, statePath, () => true, { schedulingEnabled: false });
	try {
		await runtime.start({ flushPending: false });
		const disk = await readState(statePath);
		assert.equal(disk.version, 3, "the file is rewritten as version 3");
		assert.equal(disk.alarms.length, 1);
		assert.equal((disk.alarms[0] as AlarmState & { pendingWake?: unknown }).pendingWake, undefined, "the alarm no longer embeds a pendingWake");
		assert.equal(disk.outbox.length, 1, "the embedded wake becomes an outbox entry");
		assert.equal(disk.outbox[0].alarmId, "legacy");
		assert.equal(disk.outbox[0].events[0].fingerprint, "timer:legacy:2000");
		assert.equal(runtime.alarmCount, 1);
		assert.equal(runtime.outboxCount, 1);
	} finally {
		await runtime.stop();
	}
});

test("two processes racing on a v2 state produce exactly one valid v3", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	const v2Alarm = { id: "legacy", name: "Legacy", kind: "timer", active: false, createdAt: 1000, dueAt: 2000, triggeredAt: 1500, lastTriggeredAt: 1500, pauseReason: "timer fired", pendingWake: { triggeredAt: 1500, events: [{ kind: "timer", fingerprint: "timer:legacy:2000" }] } };
	await fs.writeFile(statePath, `${JSON.stringify({ version: 2, alarms: [v2Alarm] })}\n`);
	const barrier = path.join(dir, "migrate-go");
	const workerSource = `
import { WakeAlarmRuntime } from ${JSON.stringify(runtimeUrl)};
${BARRIER_HELPER}
const [statePath, barrier] = process.argv.slice(2);
const runtime = new WakeAlarmRuntime({
	cwd: ${JSON.stringify(dir)},
	configPath: ${JSON.stringify(path.join(dir, "absent-config.json"))},
	statePath,
	emit: () => false,
	execFn: async () => ({ stdout: "", stderr: "", code: 0 }),
});
await runtime.start({ flushPending: false });
await waitBarrier(barrier);
const listing = await runtime.runAction({ action: "list" });
await runtime.stop();
console.log(listing);
`;
	const worker = await writeWorker(dir, "migrate-worker.ts", workerSource);
	// Both processes start concurrently and race the migration on the same v2 file.
	const [a, b] = [runWorker([worker, statePath, barrier]), runWorker([worker, statePath, barrier])];
	await new Promise((resolve) => setTimeout(resolve, 300));
	await fs.writeFile(barrier, "go");
	const [ra, rb] = await Promise.all([a, b]);
	assert.equal(ra.code, 0, ra.stderr);
	assert.equal(rb.code, 0, rb.stderr);
	const disk = await readState(statePath);
	assert.equal(disk.version, 3);
	assert.equal(disk.alarms.length, 1);
	assert.equal(disk.outbox.length, 1);
	assert.match(`${ra.stdout}${rb.stdout}`, /Legacy/, "both workers see the migrated alarm");
});

test("remove stops future events but keeps undelivered wakes, which are still delivered", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	const owner = "C:\\sessions\\gone.jsonl";
	const entry: OutboxEntry = {
		eventId: "gone:1000:x",
		alarmId: "gone",
		alarmName: "Gone",
		ownerSessionFile: owner,
		triggeredAt: Date.now() - 50,
		events: [{ kind: "timer", fingerprint: "timer:gone:1000" }],
		message: "[Wake alarm] Gone (gone)",
	};
	await writeState(statePath, [{ id: "gone", name: "Gone", kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 60_000, ownerSessionFile: owner, revision: 1 }], [entry]);
	const a = makeRuntime(dir, statePath, () => true, { schedulingEnabled: false });
	await a.start({ flushPending: false });
	try {
		const removed = await a.runAction({ action: "remove", id: "gone" });
		assert.match(removed, /1 undelivered wake/);
		const disk = await readState(statePath);
		assert.deepEqual(disk.alarms, [], "the alarm is gone");
		assert.equal(disk.outbox.length, 1, "the undelivered wake survives removal");
		// A fresh daemon-like runtime (owner not live) must pick the ORPHAN wake up
		// through the independent outbox scheduler pass.
		const calls: OutboxEntry[] = [];
		// Nobody is live, so a daemon-like runtime owns every alarm and entry.
		const daemon = makeRuntime(dir, statePath, (e) => { calls.push(e); return true; }, { owns: () => true });
		await daemon.start({ flushPending: false });
		try {
			await waitFor(() => calls.length === 1, "the orphan wake to be delivered by the outbox scheduler", 6_000);
			assert.equal(calls[0].alarmId, "gone");
			await waitFor(() => readStateSync(statePath).outbox.length === 0, "the delivered orphan wake to be cleared", 3_000);
		} finally {
			await daemon.stop();
		}
	} finally {
		await a.stop();
	}
});

test("outbox overflow pauses the producing alarm instead of silently dropping", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" }, maxOutboxEntriesPerAlarm: 2 }));
	const cid = "c".repeat(64);
	const base: ProbeResult = { exists: true, containerId: cid, running: false, status: "exited", containerStatus: "exited", startedAt: "2026-01-01T00:00:00.000Z", exitCode: 0, oomKilled: false, logMode: "docker-logs", selectedLogPath: undefined, logFileId: undefined, logOffset: 0, logCursor: "2026-01-01T00:00:00Z", logBytes: new Uint8Array(), tail: "" };
	const alarm = applyBaseline(createContainerAlarm({ id: "cap", name: "Cap", container: "job", events: ["exit"], policy: "keep", now: Date.now(), statusPollMs: 1000 }), base, Date.now());
	await writeState(statePath, [alarm]);
	const startedAts = ["2026-01-01T00:00:01.000Z", "2026-01-01T00:00:02.000Z", "2026-01-01T00:00:03.000Z", "2026-01-01T00:00:04.000Z"];
	let probes = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => false,
		execFn: async () => cannedProbe({ startedAt: startedAts[Math.min(probes++, startedAts.length - 1)] }),
	});
	try {
		await runtime.start({ flushPending: false });
		// Two undelivered entries fill the per-alarm cap; the third occurrence must
		// pause the alarm with an explicit diagnostic rather than drop or grow forever.
		await waitFor(() => {
			const state = readStateSync(statePath);
			return state.alarms[0]?.active === false && String(state.alarms[0].pauseReason).includes("outbox overflow");
		}, "the alarm to pause on outbox overflow", 12_000);
		const disk = await readState(statePath);
		assert.equal(disk.outbox.length, 2, "exactly the configured cap of entries are retained");
		assert.ok(disk.alarms[0].pauseReason?.includes("outbox overflow"));
		// P0 gate: the overflow-causing occurrence must NOT be consumed. The persisted
		// exit fingerprint must stay at the last SUCCESSFUL fire (B), never advance to
		// the overflow-causing C — so resume re-fires the same event.
		const alarmDisk = disk.alarms[0];
		assert.ok(alarmDisk.kind === "container", "alarm is a container");
		assert.notEqual(alarmDisk.eventFingerprints["exit"], "exit:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:2026-01-01T00:00:03.000Z:0", "the overflow-causing C occurrence was not consumed (its fingerprint was not persisted)");
	} finally {
		await runtime.stop();
	}
});

test("overflow pauses without consuming; drop_wake + resume re-fires the same occurrence", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" }, maxOutboxEntriesPerAlarm: 2 }));
	const cid = "c".repeat(64);
	const base: ProbeResult = { exists: true, containerId: cid, running: false, status: "exited", containerStatus: "exited", startedAt: "2026-01-01T00:00:00.000Z", exitCode: 0, oomKilled: false, logMode: "docker-logs", selectedLogPath: undefined, logFileId: undefined, logOffset: 0, logCursor: "2026-01-01T00:00:00Z", logBytes: new Uint8Array(), tail: "" };
	const alarm = applyBaseline(createContainerAlarm({ id: "cap", name: "Cap", container: "job", events: ["exit"], policy: "keep", now: Date.now(), statusPollMs: 1000 }), base, Date.now());
	await writeState(statePath, [alarm]);
	const startedAts = ["2026-01-01T00:00:01.000Z", "2026-01-01T00:00:02.000Z", "2026-01-01T00:00:03.000Z"];
	let probes = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => false,
		execFn: async () => cannedProbe({ startedAt: startedAts[Math.min(probes++, startedAts.length - 1)] }),
	});
	try {
		await runtime.start({ flushPending: false });
		// A + B fill the per-alarm cap; the C occurrence pauses the alarm unconsumed.
		await waitFor(() => {
			const state = readStateSync(statePath);
			return state.alarms[0]?.active === false && String(state.alarms[0].pauseReason).includes("outbox overflow");
		}, "the alarm to pause on outbox overflow", 12_000);
		assert.equal((await readState(statePath)).outbox.length, 2);
		// Free one slot explicitly, then resume: the SAME occurrence (startedAt C)
		// must re-fire and produce its wake — the at-least-once gate.
		const list = await runtime.runAction({ action: "list_wakes" });
		const eventId = list.split("\n")[0].split(" | ")[0];
		assert.ok(eventId && /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/.test(eventId), `list_wakes returned an entry id: ${list}`);
		await runtime.runAction({ action: "drop_wake", eventId });
		await runtime.runAction({ action: "resume", id: "cap" });
		await waitFor(() => readStateSync(statePath).outbox.length >= 2, "the C occurrence to re-fire after resume", 12_000);
		const after = await readState(statePath);
		assert.equal(after.outbox.length, 2, "one dropped, one re-fired");
		assert.ok(after.outbox.some((entry) => entry.events[0].fingerprint.includes("2026-01-01T00:00:03.000Z")), "the re-fired wake is the overflow-causing C occurrence");
	} finally {
		await runtime.stop();
	}
});

test("outbox management: list_wakes, drop_wake, purge_wakes are explicit, never implied by remove", async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	const owner = "C:\\sessions\\gone.jsonl";
	const entryA: OutboxEntry = { eventId: "a:1:x", alarmId: "a", alarmName: "A", ownerSessionFile: owner, triggeredAt: 1000, events: [{ kind: "timer", fingerprint: "timer:a:1" }], message: "[Wake alarm] A (a)" };
	const entryB: OutboxEntry = { eventId: "b:2:y", alarmId: "b", alarmName: "B", ownerSessionFile: owner, triggeredAt: 2000, events: [{ kind: "timer", fingerprint: "timer:b:2" }], message: "[Wake alarm] B (b)" };
	await writeState(statePath, [], [entryA, entryB]);
	const runtime = makeRuntime(dir, statePath, () => true, { schedulingEnabled: false });
	try {
		await runtime.start({ flushPending: false });
		const list = await runtime.runAction({ action: "list_wakes" });
		assert.match(list, /a:1:x/);
		assert.match(list, /b:2:y/);
		assert.match(await runtime.runAction({ action: "drop_wake", eventId: "a:1:x" }), /Dropped wake a:1:x/);
		assert.equal((await readState(statePath)).outbox.length, 1);
		assert.match(await runtime.runAction({ action: "purge_wakes", id: "b" }), /Dropped 1 wake\(s\) for b/);
		assert.equal((await readState(statePath)).outbox.length, 0);
		await assert.rejects(runtime.runAction({ action: "drop_wake", eventId: "a:1:x" }), /unknown wake/);
	} finally {
		await runtime.stop();
	}
});

test("daemon adopts alarms created after startup and fires them on time (dry run)", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await writeState(statePath, []);
	const piStub = path.join(dir, "pi-stub.js");
	await fs.writeFile(piStub, "console.error('pi stub'); process.exit(0);\n");
	const child = spawn(process.execPath, [path.resolve("extensions/pi-wake/daemon.ts")], {
		cwd: dir,
		env: { ...process.env, WAKE_ALARM_CWD: dir, WAKE_ALARM_STATE_PATH: statePath, WAKE_ALARM_PI_COMMAND: piStub, WAKE_ALARM_SPAWN_DRY_RUN: "1" },
		stdio: ["ignore", "pipe", "pipe"],
		windowsHide: true,
	});
	let stdout = "";
	let stderr = "";
	child.stdout.on("data", (chunk) => { stdout += chunk; });
	child.stderr.on("data", (chunk) => { stderr += chunk; });
	try {
		// Give the daemon time to activate with an empty state, then create the alarm
		// from "another session" — the daemon must discover it on its next poll.
		await new Promise((resolve) => setTimeout(resolve, 1_000));
		const owner = "C:\\sessions\\ghost.jsonl";
		const due: TimerAlarmState = { id: "later", name: "Later", kind: "timer", active: true, createdAt: Date.now(), dueAt: Date.now() + 8_000, ownerSessionFile: owner };
		await writeState(statePath, [due]);
		const deadline = Date.now() + 45_000;
		while (Date.now() < deadline) {
			if (stdout.includes("[dry-run] would run")) break;
			if (!child.killed && child.exitCode !== null) throw new Error(`daemon exited early: ${stderr || stdout}`);
			await new Promise((resolve) => setTimeout(resolve, 200));
		}
		assert.ok(stdout.includes("[dry-run] would run"), `daemon never fired the late alarm; stdout:\n${stdout}\nstderr:\n${stderr}`);
		assert.ok(stdout.includes("--session") && stdout.includes("ghost.jsonl"), "the dry run targets the alarm's owner session");
	} finally {
		if (child.exitCode === null) child.kill();
		await new Promise((resolve) => { if (child.exitCode !== null) resolve(undefined); else child.once("exit", () => resolve(undefined)); setTimeout(resolve, 2_000); });
		if (child.exitCode === null) child.kill("SIGKILL");
	}
});

function readStateSync(statePath: string): StoredState {
	return JSON.parse(readFileSync(statePath, "utf8")) as StoredState;
}

async function waitFor(condition: () => boolean, label: string, timeoutMs = 3_000): Promise<void> {
	const start = Date.now();
	while (!condition()) {
		if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${label}`);
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
}

test("watch_container_group emits ONE summary wake when all members are terminal", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const calls: OutboxEntry[] = [];
	// All probes return the same exited container; each member fires once at its first poll.
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return true; },
		execFn: async () => cannedProbe(),
	});
	try {
		await runtime.start({ flushPending: false });
		const created = await runtime.runAction({ action: "watch_container_group", id: "interp", name: "Interp", containers: ["job-a", "job-b"], condition: "all_terminal", statusPoll: "1s" });
		assert.match(created, /interp: Interp — group active/);
		await waitFor(() => calls.length === 1, "the single group wake to fire", 15_000);
		assert.equal(calls.length, 1, "exactly one wake for the whole batch");
		assert.equal(calls[0].alarmId, "interp");
		assert.equal(calls[0].events[0].kind, "group");
		assert.match(calls[0].message, /2\/2 terminal/, calls[0].message);
		assert.match(calls[0].message, /exit 0/, calls[0].message);
		// The emit fires before the completion write; wait for the outbox clear to land.
		await waitFor(() => readStateSync(statePath).outbox.length === 0, "the delivered group wake to be cleared", 5_000);
		// Members produce no individual wakes and are paused once the group completes.
		const disk = await readState(statePath);
		assert.equal(disk.outbox.length, 0, "the delivered group wake was removed");
		const members = disk.alarms.filter((alarm) => alarm.kind === "container");
		assert.equal(members.length, 2);
		assert.ok(members.every((member) => member.active === false && member.pauseReason === "group completed"));
	} finally {
		await runtime.stop();
	}
});

test("watch_condition fires once when the remote result file satisfies the condition", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa", allowedRemoteLogRoots: ["/data/results/"] } }));
	const calls: OutboxEntry[] = [];
	let probes = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return true; },
		execFn: async () => {
			probes++;
			if (probes <= 2) return { stdout: JSON.stringify({ exists: false, size: 0, tailBase64: "" }), stderr: "", code: 0 };
			return { stdout: JSON.stringify({ exists: true, size: 128, tailBase64: Buffer.from('{"pass": true}\n').toString("base64") }), stderr: "", code: 0 };
		},
	});
	try {
		await runtime.start({ flushPending: false });
		const created = await runtime.runAction({ action: "watch_condition", id: "done", name: "Done", path: "/data/results/analysis.json", condition: "contains", value: '"pass": true', statusPoll: "1s" });
		assert.match(created, /done: Done — condition active/);
		await waitFor(() => calls.length === 1, "the condition wake to fire", 15_000);
		assert.equal(calls[0].alarmId, "done");
		assert.equal(calls[0].events[0].kind, "condition");
		const disk = await readState(statePath);
		const alarm = disk.alarms.find((candidate) => candidate.id === "done");
		assert.ok(alarm?.kind === "condition" && alarm.satisfiedAt !== undefined, "condition alarm satisfied");
	} finally {
		await runtime.stop();
	}
});

test("ack drops an alarm's undelivered wakes; remove purgePendingEvents clears them", async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	const owner = "C:\\sessions\\gone.jsonl";
	const entryA: OutboxEntry = { eventId: "a:1:x", alarmId: "a", alarmName: "A", ownerSessionFile: owner, triggeredAt: 1000, events: [{ kind: "timer", fingerprint: "timer:a:1" }], message: "[Wake alarm] A (a)" };
	const entryB: OutboxEntry = { eventId: "a:2:y", alarmId: "a", alarmName: "A", ownerSessionFile: owner, triggeredAt: 2000, events: [{ kind: "timer", fingerprint: "timer:a:2" }], message: "[Wake alarm] A (a)" };
	await writeState(statePath, [{ id: "a", name: "A", kind: "timer", active: true, createdAt: 500, dueAt: 3000, revision: 1 }], [entryA, entryB]);
	const runtime = makeRuntime(dir, statePath, () => true, { schedulingEnabled: false });
	try {
		await runtime.start({ flushPending: false });
		assert.match(await runtime.runAction({ action: "ack", id: "a" }), /dropped 2/);
		assert.equal((await readState(statePath)).outbox.length, 0);
		// remove with purgePendingEvents clears both the alarm and its remaining wakes.
		const entryC: OutboxEntry = { eventId: "a:3:z", alarmId: "a", alarmName: "A", ownerSessionFile: owner, triggeredAt: 3000, events: [{ kind: "timer", fingerprint: "timer:a:3" }], message: "[Wake alarm] A (a)" };
		await writeState(statePath, [{ id: "a", name: "A", kind: "timer", active: true, createdAt: 500, dueAt: 3000, revision: 1 }], [entryC]);
		await runtime.reconcileFromDisk();
		const removed = await runtime.runAction({ action: "remove", id: "a", purgePendingEvents: true });
		assert.match(removed, /purged 1 wake/);
		const disk = await readState(statePath);
		assert.deepEqual(disk.alarms, []);
		assert.deepEqual(disk.outbox, []);
	} finally {
		await runtime.stop();
	}
});

test("group reset rebaselines members and cannot re-fire the previous run", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const calls: OutboxEntry[] = [];
	let phase = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return true; },
		execFn: async () => {
			if (phase === 0) return cannedProbe();
			if (phase === 1) return cannedProbe({ running: true, status: "running", containerStatus: "running", startedAt: "2026-01-01T00:00:00.000Z" });
			return cannedProbe({ startedAt: "2026-01-01T00:00:09.000Z" });
		},
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "1s" });
		await waitFor(() => calls.length === 1, "first run group wake", 15_000);
		phase = 1;
		const reset = await runtime.runAction({ action: "reset", id: "g" });
		assert.match(reset, /Reset g/);
		await new Promise((resolve) => setTimeout(resolve, 2_500));
		assert.equal(calls.length, 1, "reset must not re-fire the previous run's terminal state");
		phase = 2;
		await waitFor(() => calls.length === 2, "second run group wake after reset", 15_000);
		assert.equal(calls[1].alarmId, "g");
	} finally {
		await runtime.stop();
	}
});

test("group creation is atomic: a member id collision leaves no orphan members", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const runtime = new WakeAlarmRuntime({ cwd: dir, configPath, statePath, emit: () => true, execFn: async () => cannedProbe() });
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container", id: "g-2", name: "Pre", container: "job-b", events: ["exit"], statusPoll: "60s" });
		await assert.rejects(
			runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "60s" }),
			/member alarm already exists: g-2/,
		);
		const disk = await readState(statePath);
		assert.deepEqual(disk.alarms.map((alarm) => alarm.id).sort(), ["g-2"]);
	} finally {
		await runtime.stop();
	}
});

test("removing a group removes its members in one transaction", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const runtime = new WakeAlarmRuntime({ cwd: dir, configPath, statePath, emit: () => true, execFn: async () => cannedProbe() });
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "60s" });
		const removed = await runtime.runAction({ action: "remove", id: "g" });
		assert.match(removed, /Removed g/);
		const disk = await readState(statePath);
		assert.deepEqual(disk.alarms.map((alarm) => alarm.id), [], "group and members removed together, no orphans");
	} finally {
		await runtime.stop();
	}
});

test("group completion bumps the revision of the group and every changed member", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const runtime = new WakeAlarmRuntime({ cwd: dir, configPath, statePath, emit: () => true, execFn: async () => cannedProbe() });
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "1s" });
		await waitFor(() => {
			const state = readStateSync(statePath);
			return state.alarms.some((alarm) => alarm.kind === "group" && alarm.firedAt !== undefined);
		}, "the group to complete", 15_000);
		const disk = await readState(statePath);
		const group = disk.alarms.find((alarm) => alarm.id === "g");
		assert.ok((group?.revision ?? 0) >= 2, "group revision advanced past create");
		const members = disk.alarms.filter((alarm) => alarm.kind === "container");
		assert.equal(members.length, 2);
		for (const member of members) assert.ok((member.revision ?? 0) >= 2, `member ${member.id} revision advanced past create`);
	} finally {
		await runtime.stop();
	}
});

test("a full outbox blocks the group wake until capacity is freed", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" }, maxOutboxEntries: 2 }));
	const calls: OutboxEntry[] = [];
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return false; },
		execFn: async () => cannedProbe(),
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "set_timer", id: "t1", name: "T1", after: "1s" });
		await runtime.runAction({ action: "set_timer", id: "t2", name: "T2", after: "1s" });
		await waitFor(() => readStateSync(statePath).outbox.length === 2, "the outbox to fill to its cap", 10_000);
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "1s" });
		await new Promise((resolve) => setTimeout(resolve, 3_000));
		const state1 = readStateSync(statePath);
		assert.equal(state1.outbox.length, 2, "group wake did not exceed the outbox cap");
		assert.ok(state1.alarms.some((alarm) => alarm.id === "g" && alarm.active === true), "group stayed active while the outbox was full");
		await runtime.runAction({ action: "drop_wake", eventId: state1.outbox[0].eventId });
		await waitFor(() => readStateSync(statePath).alarms.some((alarm) => alarm.id === "g" && (alarm as { firedAt?: number }).firedAt !== undefined), "the group to fire after capacity was freed", 15_000);
		assert.equal(readStateSync(statePath).outbox.length, 2, "cap respected after the group fired (1 timer + 1 group wake)");
	} finally {
		await runtime.stop();
	}
});

test("group summary includes bounded member log tails when logTailLines is set", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const calls: OutboxEntry[] = [];
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return true; },
		execFn: async () => cannedProbe({ tailBase64: Buffer.from("accuracy=0.91\nsaved checkpoint\n").toString("base64") }),
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], logTailLines: 5, statusPoll: "1s" });
		await waitFor(() => calls.length === 1, "group wake with member tails", 15_000);
		assert.match(calls[0].message, /accuracy=0.91/, "member log tail appears in the group summary");
		assert.match(calls[0].message, /saved checkpoint/, "multi-line member tail is included");
		assert.match(calls[0].message, /tail:/);
	} finally {
		await runtime.stop();
	}
});

test("logTailLines alone enables log reading in the real probe payload", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	let lastPayload: string | undefined;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => true,
		execFn: async (_file, args) => {
			const match = args[args.length - 1].match(/'([A-Za-z0-9+/=]+)'$/);
			if (match) lastPayload = Buffer.from(match[1], "base64").toString("utf8");
			return cannedProbe();
		},
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container", id: "w", name: "W", container: "job", events: ["exit"], logTailLines: 5, statusPoll: "60s" });
		assert.ok(lastPayload, "baseline probe payload captured");
		const withTail = JSON.parse(lastPayload!) as { readLogs?: boolean; tailLinesReq?: number };
		assert.equal(withTail.readLogs, true, "logTailLines alone must enable log reading in the real probe");
		assert.equal(withTail.tailLinesReq, 5);
		await runtime.runAction({ action: "watch_container", id: "w2", name: "W2", container: "job2", events: ["exit"], statusPoll: "60s" });
		const withoutTail = JSON.parse(lastPayload!) as { readLogs?: boolean };
		assert.equal(withoutTail.readLogs, false, "without logTailLines, exit-only watches stay readLogs=false");
	} finally {
		await runtime.stop();
	}
});

test("a satisfied group is frozen when the outbox is full and survives member state changes", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" }, maxOutboxEntries: 1 }));
	const calls: OutboxEntry[] = [];
	let restarting = false;
	let probes = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return false; },
		execFn: async () => {
			probes++;
			// Baselines (first two probes) see running containers; polls then see
			// abnormal exits, so the members genuinely fire their abnormal events.
			if (restarting || probes <= 2) return cannedProbe({ running: true, status: "running", containerStatus: "running", exitCode: undefined });
			return cannedProbe({ exitCode: 1 });
		},
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "set_timer", id: "t1", name: "T1", after: "1s" });
		await waitFor(() => readStateSync(statePath).outbox.length === 1, "the outbox to fill to its cap", 10_000);
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], condition: "any_abnormal", statusPoll: "1s" });
		// any_abnormal met while the outbox is full -> the group FREEZES the occurrence.
		await waitFor(() => {
			const state = readStateSync(statePath);
			const group = state.alarms.find((alarm) => alarm.id === "g");
			return group?.kind === "group" && (group as { pendingFire?: boolean }).pendingFire === true;
		}, "the group to freeze with pendingFire", 15_000);
		const frozen = readStateSync(statePath);
		assert.equal(frozen.outbox.length, 1, "no wake was appended past the cap");
		assert.ok(frozen.alarms.every((alarm) => alarm.kind !== "container" || alarm.active === false), "members are paused while the occurrence is frozen");
		// A container restart must NOT erase the frozen abnormal fingerprints: members
		// are paused, so no probe rewrites them.
		restarting = true;
		await new Promise((resolve) => setTimeout(resolve, 2_500));
		const after = readStateSync(statePath);
		const groupAfter = after.alarms.find((alarm) => alarm.id === "g");
		assert.equal(groupAfter?.kind === "group" && (groupAfter as { pendingFire?: boolean }).pendingFire, true, "the occurrence stays frozen through a restart");
		// Free a slot: the group fires with the FROZEN summary.
		await runtime.runAction({ action: "drop_wake", eventId: after.outbox[0].eventId });
		await waitFor(() => readStateSync(statePath).alarms.some((alarm) => alarm.id === "g" && alarm.kind === "group" && (alarm as { firedAt?: number }).firedAt !== undefined), "the frozen group to fire after capacity was freed", 15_000);
		const fired = readStateSync(statePath);
		assert.equal(fired.outbox.length, 1, "cap respected: the group wake took the freed slot");
		assert.ok(fired.alarms.some((alarm) => alarm.id === "g" && alarm.kind === "group" && String(alarm.summary).includes("abnormal")), "the fired summary reflects the frozen occurrence");
	} finally {
		await runtime.stop();
	}
});

test("a satisfied condition is frozen when the outbox is full and is not overwritten by a later probe", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa", allowedRemoteLogRoots: ["/data/results/"] }, maxOutboxEntries: 1 }));
	const calls: OutboxEntry[] = [];
	let fileGone = false;
	let probes = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return false; },
		execFn: async () => {
			probes++;
			if (fileGone) return { stdout: JSON.stringify({ exists: false, size: 0, tailBase64: "" }), stderr: "", code: 0 };
			return { stdout: JSON.stringify({ exists: true, size: 128, tailBase64: Buffer.from("done\n").toString("base64") }), stderr: "", code: 0 };
		},
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "set_timer", id: "t1", name: "T1", after: "1s" });
		await waitFor(() => readStateSync(statePath).outbox.length === 1, "the outbox to fill to its cap", 10_000);
		await runtime.runAction({ action: "watch_condition", id: "done", name: "Done", path: "/data/results/analysis.json", condition: "exists", statusPoll: "1s" });
		// The file is satisfied while the outbox is full: freeze lastSatisfied=true.
		await waitFor(() => {
			const state = readStateSync(statePath);
			const alarm = state.alarms.find((candidate) => candidate.id === "done");
			return alarm?.kind === "condition" && alarm.lastSatisfied === true && alarm.satisfiedAt === undefined;
		}, "the condition to freeze satisfied", 15_000);
		const probesAtFreeze = probes;
		// The file disappears: the frozen pending occurrence must NOT be overwritten.
		fileGone = true;
		await new Promise((resolve) => setTimeout(resolve, 2_500));
		const after = readStateSync(statePath);
		const alarmAfter = after.alarms.find((candidate) => candidate.id === "done");
		assert.equal(alarmAfter?.kind === "condition" && alarmAfter.lastSatisfied, true, "the frozen satisfaction survives the file disappearing");
		assert.ok(probes <= probesAtFreeze + 1, "the pending occurrence stops re-probing the external condition");
		await runtime.runAction({ action: "drop_wake", eventId: after.outbox[0].eventId });
		await waitFor(() => readStateSync(statePath).alarms.some((candidate) => candidate.id === "done" && candidate.kind === "condition" && candidate.satisfiedAt !== undefined), "the frozen condition to fire after capacity was freed", 15_000);
		assert.equal(readStateSync(statePath).outbox.length, 1, "cap respected");
	} finally {
		await runtime.stop();
	}
});

test("check acknowledges a satisfied condition even when the outbox is full", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa", allowedRemoteLogRoots: ["/data/results/"] }, maxOutboxEntries: 1 }));
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => false,
		execFn: async () => ({ stdout: JSON.stringify({ exists: true, size: 128, tailBase64: Buffer.from("done\n").toString("base64") }), stderr: "", code: 0 }),
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "set_timer", id: "t1", name: "T1", after: "1s" });
		await waitFor(() => readStateSync(statePath).outbox.length === 1, "the outbox to fill to its cap", 10_000);
		await runtime.runAction({ action: "watch_condition", id: "done", name: "Done", path: "/data/results/analysis.json", condition: "exists", statusPoll: "60s" });
		// check = acknowledge: satisfied even with a full outbox, no wake appended.
		const checked = await runtime.runAction({ action: "check", id: "done" });
		assert.match(checked, /done: Done — condition/);
		const disk = await readState(statePath);
		const alarm = disk.alarms.find((candidate) => candidate.id === "done");
		assert.equal(alarm?.kind === "condition" && alarm.satisfiedAt !== undefined, true, "check acknowledged the condition");
		assert.equal(alarm?.kind === "condition" && alarm.active, false, "acknowledged condition is done");
		assert.equal(disk.outbox.length, 1, "no wake was appended by the acknowledge");
	} finally {
		await runtime.stop();
	}
});

test("removing a group with an empty outbox reports no phantom wakes", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const runtime = new WakeAlarmRuntime({ cwd: dir, configPath, statePath, emit: () => true, execFn: async () => cannedProbe() });
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "60s" });
		const removed = await runtime.runAction({ action: "remove", id: "g" });
		assert.doesNotMatch(removed, /undelivered wake/, removed);
		assert.doesNotMatch(removed, /purged/, removed);
	} finally {
		await runtime.stop();
	}
});

test("a member replaced by a different alarm kind triggers a group integrity failure", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const runtime = new WakeAlarmRuntime({ cwd: dir, configPath, statePath, emit: () => true, execFn: async () => cannedProbe() });
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "1s" });
		// Replace member g-1 with a timer of the same id.
		await runtime.runAction({ action: "remove", id: "g-1" });
		await runtime.runAction({ action: "set_timer", id: "g-1", name: "Imposter", after: "1h" });
		await waitFor(() => {
			const state = readStateSync(statePath);
			const group = state.alarms.find((alarm) => alarm.id === "g");
			return group?.kind === "group" && String(group.pauseReason).includes("integrity failure");
		}, "the group to report an integrity failure", 15_000);
		const disk = await readState(statePath);
		const group = disk.alarms.find((alarm) => alarm.id === "g");
		assert.equal(group?.kind === "group" && group.active, false);
		assert.match(String(group?.kind === "group" ? group.pauseReason : ""), /g-1/);
	} finally {
		await runtime.stop();
	}
});

test("a frozen condition no longer depends on SSH: permanent remote failure cannot block delivery", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa", allowedRemoteLogRoots: ["/data/results/"] }, maxOutboxEntries: 1 }));
	const calls: OutboxEntry[] = [];
	let sshDown = false;
	let probes = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return false; },
		execFn: async () => {
			probes++;
			if (sshDown) throw new Error("ssh: connection refused");
			return { stdout: JSON.stringify({ exists: true, size: 128, tailBase64: Buffer.from("done\n").toString("base64") }), stderr: "", code: 0 };
		},
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "set_timer", id: "t1", name: "T1", after: "1s" });
		await waitFor(() => readStateSync(statePath).outbox.length === 1, "the outbox to fill to its cap", 10_000);
		await runtime.runAction({ action: "watch_condition", id: "done", name: "Done", path: "/data/results/analysis.json", condition: "exists", statusPoll: "1s" });
		await waitFor(() => {
			const state = readStateSync(statePath);
			const alarm = state.alarms.find((candidate) => candidate.id === "done");
			return alarm?.kind === "condition" && alarm.lastSatisfied === true && alarm.satisfiedAt === undefined;
		}, "the condition to freeze satisfied", 15_000);
		const probesAtFreeze = probes;
		// The remote host goes permanently offline: the frozen occurrence must be
		// deliverable WITHOUT any further SSH, and the frozen evidence must survive.
		sshDown = true;
		await new Promise((resolve) => setTimeout(resolve, 6_500)); // spans the 5s retry
		assert.equal(probes, probesAtFreeze, "no SSH probe after the freeze");
		const after = readStateSync(statePath);
		const alarmAfter = after.alarms.find((candidate) => candidate.id === "done");
		assert.equal(alarmAfter?.kind === "condition" && alarmAfter.lastSatisfied, true, "frozen satisfaction intact");
		assert.equal(alarmAfter?.kind === "condition" && alarmAfter.lastEvidence, "done", "frozen evidence not overwritten by SSH errors");
		await runtime.runAction({ action: "drop_wake", eventId: after.outbox[0].eventId });
		await waitFor(() => calls.some((entry) => entry.alarmId === "done"), "the frozen condition wake to be delivered without SSH", 15_000);
		assert.equal(calls.filter((entry) => entry.alarmId === "done").length, 1, "exactly one frozen wake");
		assert.equal(probes, probesAtFreeze, "delivery itself performed no SSH probe");
	} finally {
		await runtime.stop();
	}
});

test("a frozen occurrence keeps its real timestamp: Triggered at is the freeze time, not the delivery time", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa", allowedRemoteLogRoots: ["/data/results/"] }, maxOutboxEntries: 1 }));
	const calls: OutboxEntry[] = [];
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: (entry) => { calls.push(entry); return false; },
		execFn: async () => ({ stdout: JSON.stringify({ exists: true, size: 128, tailBase64: Buffer.from("done\n").toString("base64") }), stderr: "", code: 0 }),
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "set_timer", id: "t1", name: "T1", after: "1s" });
		await waitFor(() => readStateSync(statePath).outbox.length === 1, "the outbox to fill to its cap", 10_000);
		await runtime.runAction({ action: "watch_condition", id: "done", name: "Done", path: "/data/results/analysis.json", condition: "exists", statusPoll: "1s" });
		await waitFor(() => {
			const state = readStateSync(statePath);
			const alarm = state.alarms.find((candidate) => candidate.id === "done");
			return alarm?.kind === "condition" && alarm.pendingSatisfiedAt !== undefined;
		}, "the condition to freeze satisfied", 15_000);
		const frozenAt = (readStateSync(statePath).alarms.find((candidate) => candidate.id === "done") as { pendingSatisfiedAt?: number }).pendingSatisfiedAt!;
		// Free a slot well after the freeze.
		await new Promise((resolve) => setTimeout(resolve, 3_000));
		const before = readStateSync(statePath);
		await runtime.runAction({ action: "drop_wake", eventId: before.outbox[0].eventId });
		await waitFor(() => calls.length === 1, "the frozen condition wake to be delivered", 15_000);
		const deliveredAt = Date.now();
		assert.ok(Math.abs(calls[0].triggeredAt - frozenAt) < 2_000, `wake timestamp is the freeze time (frozen=${frozenAt}, triggered=${calls[0].triggeredAt})`);
		assert.ok(calls[0].triggeredAt < deliveredAt - 2_000, "the wake timestamp predates the delivery, preserving the occurrence audit time");
	} finally {
		await runtime.stop();
	}
});

test("a frozen group survives a member being removed while it awaits capacity", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" }, maxOutboxEntries: 1 }));
	let probes = 0;
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => false,
		execFn: async () => {
			probes++;
			if (probes <= 2) return cannedProbe({ running: true, status: "running", containerStatus: "running", exitCode: undefined });
			return cannedProbe({ exitCode: 1 });
		},
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "set_timer", id: "t1", name: "T1", after: "1s" });
		await waitFor(() => readStateSync(statePath).outbox.length === 1, "the outbox to fill to its cap", 10_000);
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], condition: "any_abnormal", statusPoll: "1s" });
		await waitFor(() => {
			const state = readStateSync(statePath);
			const group = state.alarms.find((alarm) => alarm.id === "g");
			return group?.kind === "group" && (group as { pendingFire?: boolean }).pendingFire === true;
		}, "the group to freeze with pendingFire", 15_000);
		// A member alarm is removed while the occurrence is frozen: the frozen fact
		// must NOT be converted into an integrity failure.
		await runtime.runAction({ action: "remove", id: "g-1" });
		await new Promise((resolve) => setTimeout(resolve, 6_000)); // spans one 5s retry
		const mid = readStateSync(statePath);
		const groupMid = mid.alarms.find((alarm) => alarm.id === "g");
		assert.equal(groupMid?.kind === "group" && (groupMid as { pendingFire?: boolean }).pendingFire, true, "the frozen occurrence survives member removal");
		assert.ok(!String(groupMid?.kind === "group" ? groupMid.pauseReason : "").includes("integrity"), "no integrity failure on a frozen group");
		// Free the slot: the FROZEN wake still fires with its frozen timestamp.
		const frozenAt = (mid.alarms.find((alarm) => alarm.id === "g") as { pendingFireAt?: number }).pendingFireAt!;
		await runtime.runAction({ action: "drop_wake", eventId: mid.outbox[0].eventId });
		await waitFor(() => readStateSync(statePath).alarms.some((alarm) => alarm.id === "g" && alarm.kind === "group" && (alarm as { firedAt?: number }).firedAt !== undefined), "the frozen group to fire after capacity was freed", 15_000);
		const fired = readStateSync(statePath);
		const groupFired = fired.alarms.find((alarm) => alarm.id === "g");
		assert.ok(groupFired?.kind === "group" && String(groupFired.summary).includes("abnormal"), "the fired summary is the frozen one (not regenerated after member removal)");
		assert.ok(groupFired?.kind === "group" && Math.abs((groupFired as { firedAt: number }).firedAt - frozenAt) < 2_000, "the fire uses the frozen occurrence timestamp");
	} finally {
		await runtime.stop();
	}
});

test("group lifecycle never mutates a same-id alarm that is no longer a group member", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => true,
		execFn: async () => cannedProbe({ running: true, status: "running", containerStatus: "running", exitCode: undefined }),
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "1s" });
		// Replace member g-1 with an unrelated timer of the same id.
		await runtime.runAction({ action: "remove", id: "g-1" });
		await runtime.runAction({ action: "set_timer", id: "g-1", name: "Imposter", after: "1h" });
		await waitFor(() => {
			const state = readStateSync(statePath);
			const group = state.alarms.find((alarm) => alarm.id === "g");
			return group?.kind === "group" && String(group.pauseReason).includes("integrity failure");
		}, "the group to report an integrity failure", 15_000);
		// The integrity handling must NOT pause the same-id replacement.
		const mid = readStateSync(statePath);
		const imposter = mid.alarms.find((alarm) => alarm.id === "g-1");
		assert.equal(imposter?.active, true, "the same-id replacement timer is not touched by the integrity failure");
		assert.equal(mid.alarms.find((alarm) => alarm.id === "g-2")?.active, false, "the still-valid member is paused");
		// remove(group) deletes the group and its VALID member only; the replacement survives.
		await runtime.runAction({ action: "remove", id: "g" });
		const after = readStateSync(statePath);
		assert.deepEqual(after.alarms.map((alarm) => alarm.id), ["g-1"], "the replacement survives group removal");
		assert.equal(after.alarms[0].active, true);
	} finally {
		await runtime.stop();
	}
});

test("condition reset clears the pending freeze state", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa", allowedRemoteLogRoots: ["/data/results/"] }, maxOutboxEntries: 1 }));
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => false,
		execFn: async () => ({ stdout: JSON.stringify({ exists: true, size: 128, tailBase64: Buffer.from("done\n").toString("base64") }), stderr: "", code: 0 }),
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "set_timer", id: "t1", name: "T1", after: "1s" });
		await waitFor(() => readStateSync(statePath).outbox.length === 1, "the outbox to fill to its cap", 10_000);
		await runtime.runAction({ action: "watch_condition", id: "done", name: "Done", path: "/data/results/analysis.json", condition: "exists", statusPoll: "1s" });
		await waitFor(() => {
			const state = readStateSync(statePath);
			const alarm = state.alarms.find((candidate) => candidate.id === "done");
			return alarm?.kind === "condition" && alarm.pendingSatisfiedAt !== undefined;
		}, "the condition to freeze", 15_000);
		await runtime.runAction({ action: "reset", id: "done" });
		const alarm = readStateSync(statePath).alarms.find((candidate) => candidate.id === "done");
		assert.equal(alarm?.kind === "condition" && alarm.pendingSatisfiedAt, undefined, "reset clears the pending freeze state");
		assert.equal(alarm?.kind === "condition" && alarm.lastSatisfied, false);
		assert.equal(alarm?.kind === "condition" && alarm.active, true);
	} finally {
		await runtime.stop();
	}
});

test("ack(group) never purges a same-id replacement's undelivered wakes", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => false,
		execFn: async () => cannedProbe({ running: true, status: "running", containerStatus: "running", exitCode: undefined }),
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "60s" });
		// Replace member g-1 with an independent timer that fires and leaves an
		// undelivered wake in the outbox.
		await runtime.runAction({ action: "remove", id: "g-1" });
		await runtime.runAction({ action: "set_timer", id: "g-1", name: "Independent", after: "1s" });
		await waitFor(() => {
			const state = readStateSync(statePath);
			return state.outbox.some((entry) => entry.alarmId === "g-1");
		}, "the independent timer's undelivered wake", 10_000);
		const before = readStateSync(statePath);
		assert.ok(before.outbox.some((entry) => entry.alarmId === "g-1"), "the replacement has a durable wake");
		// ack(group) must NOT purge that wake: the g-1 id no longer belongs to the group.
		await runtime.runAction({ action: "ack", id: "g" });
		const after = readStateSync(statePath);
		assert.ok(after.outbox.some((entry) => entry.alarmId === "g-1"), "the same-id replacement's wake survives ack(group)");
		assert.ok(after.alarms.some((alarm) => alarm.id === "g-1" && alarm.kind === "timer"), "the replacement alarm survives");
	} finally {
		await runtime.stop();
	}
});

test("ack(group) purges the group's own wakes and valid members' wakes", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(path.join(dir, "id_rsa"), "fake-key");
	const configPath = path.join(dir, "wake-alarm.json");
	await fs.writeFile(configPath, JSON.stringify({ remote: { host: "example.com", user: "u", identityFile: "id_rsa" } }));
	const runtime = new WakeAlarmRuntime({
		cwd: dir,
		configPath,
		statePath,
		emit: () => false,
		execFn: async () => cannedProbe({ running: true, status: "running", containerStatus: "running", exitCode: undefined }),
	});
	try {
		await runtime.start({ flushPending: false });
		await runtime.runAction({ action: "watch_container_group", id: "g", name: "G", containers: ["job-a", "job-b"], statusPoll: "60s" });
		// Inject durable wakes for the group itself and one valid member directly.
		const inject = async (entry: OutboxEntry): Promise<void> => {
			const disk = await readState(statePath);
			await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms: disk.alarms, outbox: [...disk.outbox, entry] }, null, 2)}
`);
			await runtime.reconcileFromDisk();
		};
		await inject({ eventId: "g:1:x", alarmId: "g", alarmName: "G", triggeredAt: 1000, events: [{ kind: "timer", fingerprint: "g:1" }], message: "[Wake alarm] G (g)" });
		await inject({ eventId: "g-2:1:y", alarmId: "g-2", alarmName: "G #2", triggeredAt: 1000, events: [{ kind: "timer", fingerprint: "g-2:1" }], message: "[Wake alarm] G #2 (g-2)" });
		await runtime.runAction({ action: "ack", id: "g" });
		const after = readStateSync(statePath);
		assert.equal(after.outbox.length, 0, "group and valid-member wakes are purged by ack(group)");
	} finally {
		await runtime.stop();
	}
});
