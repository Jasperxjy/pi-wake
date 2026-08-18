import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { createDaemonEmit } from "./daemon.ts";
import { releaseLease, tryAcquireLease } from "./lease.ts";
import { WakeAlarmRuntime, type EmitFn, type ExecFn } from "./runtime.ts";
import type { AlarmState, FiredEvent } from "./core.ts";

const runtimeUrl = pathToFileURL(path.resolve("extensions/pi-wake/runtime.ts")).href;
const leaseUrl = pathToFileURL(path.resolve("extensions/pi-wake/lease.ts")).href;
const noopExec: ExecFn = async () => ({ stdout: "", stderr: "", code: 0 });

async function makeDir(): Promise<string> {
	return fs.mkdtemp(path.join(tmpdir(), "pi-wake-it-"));
}

async function writeState(dir: string, alarms: AlarmState[]): Promise<string> {
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(statePath, `${JSON.stringify({ version: 2, alarms }, null, 2)}\n`);
	return statePath;
}

async function readIds(statePath: string): Promise<string[]> {
	const saved = JSON.parse(await fs.readFile(statePath, "utf8")) as { alarms: { id: string }[] };
	return saved.alarms.map((alarm) => alarm.id).sort();
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

test("two processes racing state writes never lose an alarm", { timeout: 120_000 }, async () => {
	const dir = await makeDir();
	const statePath = await writeState(dir, []);
	const barrier = path.join(dir, "go");
	const workerSource = `
import crypto from "node:crypto";
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
	const rounds = 3;
	for (let round = 0; round < rounds; round++) {
		const [a, b] = [
			runWorker([worker, statePath, barrier, `r${round}a`, "25"]),
			runWorker([worker, statePath, barrier, `r${round}b`, "25"]),
		];
		await new Promise((resolve) => setTimeout(resolve, 300));
		await fs.writeFile(barrier, "go");
		const [ra, rb] = await Promise.all([a, b]);
		assert.equal(ra.code, 0, `round ${round} worker A: ${ra.stderr}`);
		assert.equal(rb.code, 0, `round ${round} worker B: ${rb.stderr}`);
		const ids = await readIds(statePath);
		assert.equal(ids.length, (round + 1) * 50, `round ${round} lost alarms`);
		await fs.rm(barrier, { force: true });
	}
	const finalIds = await readIds(statePath);
	for (let round = 0; round < rounds; round++) {
		for (const side of ["a", "b"]) {
			for (let i = 0; i < 25; i++) assert.ok(finalIds.includes(`r${round}${side}-${i}`), `missing r${round}${side}-${i}`);
		}
	}
});

test("concurrent lease acquisition elects exactly one holder", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const leaseFile = path.join(dir, "wake-alarm.lock.json");
	const barrier = path.join(dir, "go");
	const workerSource = `
import crypto from "node:crypto";
import { tryAcquireLease } from ${JSON.stringify(leaseUrl)};
${BARRIER_HELPER}
const [leaseFile, barrier, outFile] = process.argv.slice(2);
await waitBarrier(barrier);
const handle = await tryAcquireLease(leaseFile, crypto.randomUUID(), undefined);
fs.writeFileSync(outFile, handle ? "HOLDER" : "NOT");
if (handle) await new Promise((resolve) => setTimeout(resolve, 500));
process.exit(0);
`;
	const worker = await writeWorker(dir, "lease-worker.ts", workerSource);
	const workers = [];
	for (let i = 0; i < 4; i++) workers.push(runWorker([worker, leaseFile, barrier, path.join(dir, `out-${i}`)]));
	await new Promise((resolve) => setTimeout(resolve, 300));
	await fs.writeFile(barrier, "go");
	const results = await Promise.all(workers);
	for (const [i, result] of results.entries()) assert.equal(result.code, 0, `worker ${i}: ${result.stderr}`);
	let holders = 0;
	for (let i = 0; i < 4; i++) {
		if ((await fs.readFile(path.join(dir, `out-${i}`), "utf8")) === "HOLDER") holders++;
	}
	assert.equal(holders, 1, "exactly one process may hold the lease");
});

test("a crash after persisting a pending wake still delivers it once on restart", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const firedTimer: AlarmState = { id: "crash-timer", name: "Crash timer", kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 50, ownerSessionFile: "C:\\sessions\\crash.jsonl" };
	const statePath = await writeState(dir, [firedTimer]);
	const workerSource = `
import { WakeAlarmRuntime } from ${JSON.stringify(runtimeUrl)};
const [statePath] = process.argv.slice(2);
const runtime = new WakeAlarmRuntime({
	cwd: ${JSON.stringify(dir)},
	configPath: ${JSON.stringify(path.join(dir, "absent-config.json"))},
	statePath,
	emit: () => { process.kill(process.pid, "SIGKILL"); return true; },
	execFn: async () => ({ stdout: "", stderr: "", code: 0 }),
});
await runtime.start({ flushPending: false });
setTimeout(() => process.exit(3), 10_000);
`;
	const worker = await writeWorker(dir, "crash-worker.ts", workerSource);
	const crashed = await runWorker([worker, statePath]);
	assert.notEqual(crashed.code, 0, "the worker must die mid-delivery");
	const saved = JSON.parse(await fs.readFile(statePath, "utf8")) as { alarms: AlarmState[] };
	assert.ok(saved.alarms[0].pendingWake, "the pending wake must be durable across the crash");

	const calls: { alarm: AlarmState; events: FiredEvent[] }[] = [];
	const emit: EmitFn = (alarm, events) => {
		calls.push({ alarm, events });
		return true;
	};
	const restarted = new WakeAlarmRuntime({ cwd: dir, configPath: path.join(dir, "absent-config.json"), statePath, emit, execFn: noopExec });
	try {
		await restarted.start({ flushPending: true });
		assert.equal(calls.length, 1, "the durable wake must be delivered exactly once");
		assert.equal(calls[0].alarm.id, "crash-timer");
		assert.equal(calls[0].events[0].kind, "timer");
		const after = JSON.parse(await fs.readFile(statePath, "utf8")) as { alarms: AlarmState[] };
		assert.equal(after.alarms[0].pendingWake, undefined, "delivered wake is cleared from the outbox");
	} finally {
		await restarted.stop();
	}
});

test("daemon emit fencing: a live session lease suppresses the resume and keeps the outbox", { timeout: 60_000 }, async () => {
	const dir = await makeDir();
	const now = Date.now();
	const fired: AlarmState = {
		id: "fenced-timer",
		name: "Fenced timer",
		kind: "timer",
		active: false,
		createdAt: now - 1000,
		dueAt: now - 500,
		triggeredAt: now,
		lastTriggeredAt: now,
		pauseReason: "timer fired",
		ownerSessionFile: "C:\\sessions\\owner.jsonl",
		pendingWake: { triggeredAt: now, events: [{ kind: "timer", fingerprint: `timer:fenced-timer:${now - 500}` }] },
	};
	const statePath = await writeState(dir, [fired]);
	const leasePath = path.join(dir, "wake-alarm.lock.json");
	const logs: string[] = [];
	let spawns = 0;
	const daemonRuntime = new WakeAlarmRuntime({
		cwd: dir,
		configPath: path.join(dir, "absent-config.json"),
		statePath,
		emit: () => false,
		execFn: noopExec,
		schedulingEnabled: false,
	});
	await daemonRuntime.start({ flushPending: false });
	const emit = createDaemonEmit({
		getRuntime: () => daemonRuntime,
		statePath,
		leasePath,
		dryRun: true,
		spawnDisabled: false,
		isStopping: () => false,
		log: (message) => logs.push(message),
		runPi: async () => { spawns++; return 0; },
	});
	try {
		// A live session holds the lease: the daemon must not spawn, and the outbox stays.
		const leaseHandle = await tryAcquireLease(leasePath, "session-instance", "C:\\sessions\\owner.jsonl");
		assert.ok(leaseHandle, "test session must hold the lease");
		const alarm = (await readState(statePath))[0];
		const delivered = await emit(alarm, alarm.pendingWake!.events, now);
		assert.equal(delivered, false);
		assert.equal(spawns, 0);
		assert.ok(logs.some((line) => line.includes("live session owns delivery")));
		assert.ok((await readState(statePath))[0].pendingWake, "outbox record must survive the fenced emit");

		// The live session flushes its own pending wake exactly once.
		const sessionCalls: { alarm: AlarmState; events: FiredEvent[] }[] = [];
		const session = new WakeAlarmRuntime({
			cwd: dir,
			configPath: path.join(dir, "absent-config.json"),
			statePath,
			emit: (alarm, events) => { sessionCalls.push({ alarm, events }); return true; },
			execFn: noopExec,
			schedulingEnabled: false,
		});
		try {
			await session.start({ flushPending: true });
			assert.equal(sessionCalls.length, 1, "exactly one logical delivery");
		} finally {
			await session.stop();
		}

		// After the session delivered, a later daemon emit sees the cleared outbox and stands down.
		await releaseLease(leaseHandle);
		const late = await emit(alarm, alarm.pendingWake!.events, now);
		assert.equal(late, true, "an already-delivered wake is acknowledged without spawning");
		assert.equal(spawns, 0, "no resume ever spawned while a session owned delivery");
	} finally {
		await daemonRuntime.stop();
	}
});

async function readState(statePath: string): Promise<AlarmState[]> {
	return (JSON.parse(await fs.readFile(statePath, "utf8")) as { alarms: AlarmState[] }).alarms;
}
