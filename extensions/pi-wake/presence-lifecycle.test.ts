import test, { type TestContext } from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import wakeAlarmExtension from "./index.ts";
import { WakeAlarmRuntime } from "./runtime.ts";

function deferred() {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => { resolve = done; });
	return { promise, resolve };
}

type Handler = (event: unknown, ctx: unknown) => Promise<void>;

test("shutdown drains a paused presence rename before withdrawing presence", { timeout: 5_000 }, async (t) => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-shutdown-"));
	const previous = process.env.WAKE_ALARM_NO_AUTOSPAWN;
	process.env.WAKE_ALARM_NO_AUTOSPAWN = "1";
	let tick: (() => void) | undefined;
	const timers: ReturnType<typeof setInterval>[] = [];
	const setIntervalOriginal = globalThis.setInterval;
	t.mock.method(globalThis, "setInterval", (callback: () => void, ms?: number) => {
		if (ms === 15_000) tick = callback;
		const timer = setIntervalOriginal(() => undefined, 60_000);
		timer.unref();
		timers.push(timer);
		return timer;
	});
	const handlers = new Map<string, Handler>();
	wakeAlarmExtension({
		registerTool: () => undefined,
		registerCommand: () => undefined,
		on: (event: string, handler: Handler) => handlers.set(event, handler),
		sendMessage: () => { throw new Error("unexpected wake in presence test"); },
		exec: async () => ({ stdout: "", stderr: "", code: 0 }),
	} as never);
	const paused = deferred();
	const resume = deferred();
	const finished = deferred();
	let shutdown: Promise<void> | undefined;
	let writing = false;
	let releasedWhileWriting = false;
	try {
		await handlers.get("session_start")!(undefined, { cwd: dir, hasUI: false, sessionManager: { getSessionFile: () => path.join(dir, "session.jsonl") } });
		const presenceDir = path.join(dir, ".pi", "wake-alarm.sessions");
		const [entry] = (await fs.readdir(presenceDir)).filter((file) => file.endsWith(".json"));
		const target = path.join(presenceDir, entry);
		const rename = fs.rename;
		const rm = fs.rm;
		t.mock.method(fs, "rename", async (from: Parameters<typeof fs.rename>[0], to: Parameters<typeof fs.rename>[1]) => {
			if (to !== target) return rename(from, to);
			writing = true;
			paused.resolve();
			await resume.promise;
			try { await rename(from, to); }
			finally { writing = false; finished.resolve(); }
		});
		t.mock.method(fs, "rm", async (file: Parameters<typeof fs.rm>[0], options?: Parameters<typeof fs.rm>[1]) => {
			if (file === target && writing) releasedWhileWriting = true;
			await rm(file, options);
		});
		// No scheduled alarms or pending operations in this fixture. Make stop's
		// microtask boundary explicit so the test does not depend on disk timing.
		t.mock.method(WakeAlarmRuntime.prototype, "stop", async () => undefined);
		assert.ok(tick);
		tick();
		await paused.promise;
		shutdown = handlers.get("session_shutdown")!(undefined, undefined);
		for (let turn = 0; turn < 12; turn++) await Promise.resolve();
		assert.equal(releasedWhileWriting, false, "release must not start while rename is suspended");
		resume.resolve();
		await shutdown;
		await finished.promise;
		assert.deepEqual(await fs.readdir(presenceDir), [], "no late write recreates presence after shutdown");
	} finally {
		resume.resolve();
		if (shutdown) await shutdown.catch(() => undefined);
		if (writing) await finished.promise;
		await handlers.get("session_shutdown")!(undefined, undefined).catch(() => undefined);
		for (const timer of timers) clearInterval(timer);
		if (previous === undefined) delete process.env.WAKE_ALARM_NO_AUTOSPAWN; else process.env.WAKE_ALARM_NO_AUTOSPAWN = previous;
		await fs.rm(dir, { recursive: true, force: true });
	}
});

async function flushMicrotasks(): Promise<void> {
	for (let turn = 0; turn < 20; turn++) await Promise.resolve();
}

async function waitFor(condition: () => boolean, label: string): Promise<void> {
	const deadline = Date.now() + 2_000;
	while (!condition()) {
		if (Date.now() > deadline) throw new Error(`timed out: ${label}`);
		await new Promise((resolve) => setTimeout(resolve, 2));
	}
}

async function fixture(t: TestContext, options: { spawnDaemon?: boolean; passive?: boolean; hasUI?: boolean; alarm?: boolean } = {}) {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-lifecycle-"));
	const previousNoSpawn = process.env.WAKE_ALARM_NO_AUTOSPAWN;
	const previousPassive = process.env.WAKE_ALARM_PASSIVE;
	process.env.WAKE_ALARM_NO_AUTOSPAWN = "1";
	if (options.passive) process.env.WAKE_ALARM_PASSIVE = "1"; else delete process.env.WAKE_ALARM_PASSIVE;
	await fs.mkdir(path.join(dir, ".pi"));
	await fs.writeFile(path.join(dir, ".pi", "wake-alarm.json"), JSON.stringify({ spawnDaemon: options.spawnDaemon ?? false }));
	await fs.writeFile(path.join(dir, ".pi", "wake-alarm.prefs.json"), JSON.stringify({ version: 1, language: "en" }));
	if (options.alarm) await fs.writeFile(path.join(dir, ".pi", "wake-alarm.state.json"), JSON.stringify({ version: 3, alarms: [{ id: "future", name: "Future", kind: "timer", active: true, createdAt: Date.now(), dueAt: Date.now() + 3_600_000, revision: 1 }], outbox: [] }));
	const ticks: Array<() => void> = [];
	const timers: ReturnType<typeof setInterval>[] = [];
	const setIntervalOriginal = globalThis.setInterval;
	t.mock.method(globalThis, "setInterval", (callback: () => void, ms?: number) => {
		if (ms === 15_000) ticks.push(callback);
		const timer = setIntervalOriginal(() => undefined, 60_000);
		timer.unref();
		timers.push(timer);
		return timer;
	});
	const handlers = new Map<string, Handler>();
	const ui: Array<{ session: string; method: string; value: unknown }> = [];
	wakeAlarmExtension({
		registerTool: () => undefined,
		registerCommand: () => undefined,
		on: (event: string, handler: Handler) => handlers.set(event, handler),
		sendMessage: () => { throw new Error("unexpected wake in lifecycle fixture"); },
		exec: async () => ({ stdout: "", stderr: "", code: 0 }),
	} as never);
	const start = (session = "one") => handlers.get("session_start")!(undefined, {
		cwd: dir, hasUI: options.hasUI ?? true,
		sessionManager: { getSessionFile: () => path.join(dir, `${session}.jsonl`) },
		ui: {
			notify: (value: string) => ui.push({ session, method: "notify", value }),
			setStatus: (_key: string, value: string | undefined) => ui.push({ session, method: "status", value }),
			setWidget: (_key: string, value: string[] | undefined) => ui.push({ session, method: "widget", value }),
		},
	});
	const stop = () => handlers.get("session_shutdown")!(undefined, undefined);
	t.after(async () => {
		await stop().catch(() => undefined);
		for (const timer of timers) clearInterval(timer);
		t.mock.restoreAll();
		syncBuiltinESMExports();
		if (previousNoSpawn === undefined) delete process.env.WAKE_ALARM_NO_AUTOSPAWN; else process.env.WAKE_ALARM_NO_AUTOSPAWN = previousNoSpawn;
		if (previousPassive === undefined) delete process.env.WAKE_ALARM_PASSIVE; else process.env.WAKE_ALARM_PASSIVE = previousPassive;
		await fs.rm(dir, { recursive: true, force: true });
	});
	return { dir, start, stop, ticks, ui, presenceDir: path.join(dir, ".pi", "wake-alarm.sessions") };
}

function captureSpawns(t: TestContext) {
	const calls: Array<{ cwd?: string }> = [];
	t.mock.method(childProcess, "spawn", (_file: string, _args: string[], options: { cwd?: string }) => {
		calls.push(options);
		return { pid: 123_456, unref: () => undefined };
	});
	syncBuiltinESMExports();
	return calls;
}

for (const scenario of [
	{ spawnDaemon: true, passive: false, disabled: false, expected: 1 },
	{ spawnDaemon: false, passive: false, disabled: false, expected: 0 },
	{ spawnDaemon: true, passive: true, disabled: false, expected: 0 },
	{ spawnDaemon: true, passive: false, disabled: true, expected: 0 },
]) {
	test(`shutdown handoff honors config=${scenario.spawnDaemon}, passive=${scenario.passive}, envDisabled=${scenario.disabled}`, { timeout: 5_000 }, async (t) => {
		const f = await fixture(t, { ...scenario, hasUI: false });
		const spawns = captureSpawns(t);
		await f.start();
		if (!scenario.disabled) delete process.env.WAKE_ALARM_NO_AUTOSPAWN;
		const first = f.stop();
		assert.equal(f.stop(), first, "shutdown is idempotent");
		await first;
		assert.equal(spawns.length, scenario.expected);
		if (spawns.length) assert.equal(spawns[0].cwd, f.dir);
		if (scenario.passive) {
			await assert.rejects(fs.access(f.presenceDir), { code: "ENOENT" });
			assert.equal(f.ticks.length, 0);
		} else assert.deepEqual(await fs.readdir(f.presenceDir), []);
	});
}

test("shutdown during config initialization drains presence without reading an unavailable getter or spawning", { timeout: 5_000 }, async (t) => {
	const f = await fixture(t, { spawnDaemon: false });
	const spawns = captureSpawns(t);
	delete process.env.WAKE_ALARM_NO_AUTOSPAWN;
	const entered = deferred();
	const resume = deferred();
	t.signal.addEventListener("abort", resume.resolve, { once: true });
	const readFile = fs.readFile;
	t.mock.method(fs, "readFile", async (...args: Parameters<typeof fs.readFile>) => {
		if (args[0] === path.join(f.dir, ".pi", "wake-alarm.json")) { entered.resolve(); await resume.promise; }
		return readFile(...args);
	});
	const starting = f.start();
	try {
		await entered.promise;
		await f.stop();
		assert.deepEqual(await fs.readdir(f.presenceDir), []);
		const updates = f.ui.length;
		resume.resolve();
		await starting;
		await flushMicrotasks();
		assert.equal(f.ticks.length, 0);
		assert.equal(spawns.length, 0);
		assert.equal(f.ui.length, updates, "late initialization cannot restore a retired UI");
	} finally { resume.resolve(); await starting; }
});

test("initialization failure closes the refresher and releases presence", async (t) => {
	const f = await fixture(t);
	await fs.writeFile(path.join(f.dir, ".pi", "wake-alarm.json"), "{invalid");
	await f.start();
	assert.deepEqual(await fs.readdir(f.presenceDir), []);
	assert.equal(f.ticks.length, 0);
	assert.ok(f.ui.some((entry) => entry.method === "notify" && String(entry.value).includes("Wake alarm disabled")));
});

for (const phase of ["daemon", "prefs"] as const) {
	test(`a widget paused while reading ${phase} never updates retired or replacement UI`, { timeout: 5_000 }, async (t) => {
		const f = await fixture(t, { alarm: true });
		await f.start();
		await waitFor(() => f.ui.some((entry) => entry.method === "widget" && entry.value !== undefined), "initial widget");
		const entered = deferred();
		const resume = deferred();
		t.signal.addEventListener("abort", resume.resolve, { once: true });
		const readFile = fs.readFile;
		const target = path.join(f.dir, ".pi", phase === "daemon" ? "wake-alarm.daemon.json" : "wake-alarm.prefs.json");
		let held = false;
		t.mock.method(fs, "readFile", async (...args: Parameters<typeof fs.readFile>) => {
			if (args[0] === target && !held) { held = true; entered.resolve(); await resume.promise; }
			return readFile(...args);
		});
		try {
			f.ticks[0]();
			await entered.promise;
			await f.stop();
			await f.start("two");
			await waitFor(() => f.ui.some((entry) => entry.session === "two" && entry.method === "widget" && entry.value !== undefined), "replacement widget");
			const oldUpdates = f.ui.filter((entry) => entry.session === "one").length;
			resume.resolve();
			await new Promise((resolve) => setImmediate(resolve));
			assert.equal(f.ui.filter((entry) => entry.session === "one").length, oldUpdates);
			const latest = f.ui.filter((entry) => entry.session === "two" && entry.method === "widget").at(-1);
			assert.notEqual(latest?.value, undefined);
		} finally { resume.resolve(); }
	});
}

test("widget refresh skips an unready runtime before any liveness I/O", { timeout: 5_000 }, async (t) => {
	const f = await fixture(t, { alarm: true });
	const entered = deferred();
	const resume = deferred();
	t.signal.addEventListener("abort", resume.resolve, { once: true });
	const readFile = fs.readFile;
	let livenessReads = 0;
	t.mock.method(fs, "readFile", async (...args: Parameters<typeof fs.readFile>) => {
		if (args[0] === path.join(f.dir, ".pi", "wake-alarm.daemon.json")) livenessReads++;
		if (args[0] === path.join(f.dir, ".pi", "wake-alarm.json")) { entered.resolve(); await resume.promise; }
		return readFile(...args);
	});
	const starting = f.start();
	try {
		await entered.promise;
		assert.equal(livenessReads, 0, "the pre-runtime presence callback must skip widget I/O");
		resume.resolve();
		await starting;
		await waitFor(() => f.ui.some((entry) => entry.method === "widget" && entry.value !== undefined), "ready widget");
		assert.equal(livenessReads, 1);
	} finally { resume.resolve(); await starting; }
});

test("an old timer callback cannot write into a new session generation", { timeout: 5_000 }, async (t) => {
	const f = await fixture(t, { hasUI: false });
	await f.start();
	const oldTick = f.ticks[0];
	await f.stop();
	await f.start("two");
	let writes = 0;
	const rename = fs.rename;
	t.mock.method(fs, "rename", async (...args: Parameters<typeof fs.rename>) => { writes++; await rename(...args); });
	oldTick();
	await flushMicrotasks();
	assert.equal(writes, 0);
	f.ticks[1]();
	await waitFor(() => writes === 1, "current-generation heartbeat");
	await f.stop();
	assert.deepEqual(await fs.readdir(f.presenceDir), []);
});

test("late automatic daemon replacement is blocked but explicit shutdown handoff remains allowed", { timeout: 5_000 }, async (t) => {
	const f = await fixture(t, { spawnDaemon: true, hasUI: false });
	const spawns = captureSpawns(t);
	await f.start();
	delete process.env.WAKE_ALARM_NO_AUTOSPAWN;
	const entered = deferred();
	const resume = deferred();
	t.signal.addEventListener("abort", resume.resolve, { once: true });
	const readFile = fs.readFile;
	let reads = 0;
	t.mock.method(fs, "readFile", async (...args: Parameters<typeof fs.readFile>) => {
		if (args[0] !== path.join(f.dir, ".pi", "wake-alarm.daemon.json")) return readFile(...args);
		if (++reads === 1) { entered.resolve(); await resume.promise; }
		return JSON.stringify({ version: 1, pid: process.pid, startedAt: 1, heartbeatAt: Date.now(), dryRun: false, pkgVersion: "0.0.1", logTail: [] });
	});
	try {
		f.ticks[0]();
		await entered.promise;
		await f.stop();
		resume.resolve();
		await new Promise((resolve) => setImmediate(resolve));
		assert.equal(spawns.length, 0, "healthy daemon satisfies handoff; retired replacement cannot spawn");
	} finally { resume.resolve(); }
});

test("presence is still released when scheduler stop fails", async (t) => {
	const f = await fixture(t, { hasUI: false });
	await f.start();
	t.mock.method(WakeAlarmRuntime.prototype, "stop", async () => { throw new Error("injected stop failure"); });
	await assert.rejects(f.stop(), /injected stop failure/);
	assert.deepEqual(await fs.readdir(f.presenceDir), []);
});

test("presence warning throttling and success reset preserve errno without leaking the registry path", { timeout: 5_000 }, async (t) => {
	const g = await fixture(t);
	await g.start();
	const rename = fs.rename;
	const rm = fs.rm;
	const readdir = fs.readdir;
	let failedCleanup: ReturnType<typeof deferred> | undefined;
	let listed: ReturnType<typeof deferred> | undefined;
	let fail = true;
	t.mock.method(fs, "rename", async (...args: Parameters<typeof fs.rename>) => {
		if (String(args[1]).startsWith(g.presenceDir) && fail) throw Object.assign(new Error(`secret ${g.dir}`), { code: "ENOENT" });
		await rename(...args);
	});
	t.mock.method(fs, "rm", async (...args: Parameters<typeof fs.rm>) => {
		await rm(...args);
		if (String(args[0]).startsWith(g.presenceDir) && String(args[0]).includes(".tmp-")) failedCleanup?.resolve();
	});
	t.mock.method(fs, "readdir", async (...args: Parameters<typeof fs.readdir>) => {
		const entries = await readdir(...args);
		if (args[0] === g.presenceDir) listed?.resolve();
		return entries;
	});
	const warnings = () => g.ui.filter((entry) => entry.method === "notify" && String(entry.value).includes("cannot refresh session presence"));
	for (let index = 1; index <= 5; index++) {
		failedCleanup = deferred();
		g.ticks[0]();
		await failedCleanup.promise;
		await flushMicrotasks();
	}
	assert.equal(warnings().length, 2);
	assert.match(String(warnings()[1].value), /rename: ENOENT; 5 consecutive/);
	assert.doesNotMatch(String(warnings()[1].value), /secret|wake-presence-lifecycle-|check.*permissions/);
	fail = false;
	listed = deferred();
	g.ticks[0]();
	await listed.promise;
	await flushMicrotasks();
	fail = true;
	failedCleanup = deferred();
	g.ticks[0]();
	await failedCleanup.promise;
	await flushMicrotasks();
	assert.equal(warnings().length, 3);
	assert.match(String(warnings()[2].value), /1 consecutive/);
});

test("the extension coalesces overlapping heartbeat ticks through its session refresher", { timeout: 5_000 }, async (t) => {
	const f = await fixture(t, { hasUI: false });
	await f.start();
	const entered = [deferred(), deferred()];
	const resume = [deferred(), deferred()];
	t.signal.addEventListener("abort", () => { for (const gate of resume) gate.resolve(); }, { once: true });
	let now = Date.now();
	t.mock.method(Date, "now", () => now);
	const writes: number[] = [];
	let running = 0;
	let peak = 0;
	const rename = fs.rename;
	t.mock.method(fs, "rename", async (...args: Parameters<typeof fs.rename>) => {
		if (!String(args[1]).startsWith(f.presenceDir)) return rename(...args);
		peak = Math.max(peak, ++running);
		const slot = writes.length;
		writes.push(JSON.parse(await fs.readFile(args[0], "utf8")).heartbeatAt);
		entered[slot]?.resolve();
		await resume[slot]?.promise;
		try { await rename(...args); } finally { running--; }
	});
	try {
		const firstBeat = ++now;
		f.ticks[0]();
		await entered[0].promise;
		now++;
		f.ticks[0]();
		const lastBeat = ++now;
		f.ticks[0]();
		resume[0].resolve();
		await entered[1].promise;
		assert.deepEqual(writes, [firstBeat, lastBeat]);
		assert.equal(peak, 1);
		const shutdown = f.stop();
		resume[1].resolve();
		await shutdown;
		assert.deepEqual(await fs.readdir(f.presenceDir), []);
	} finally { for (const gate of resume) gate.resolve(); }
});
