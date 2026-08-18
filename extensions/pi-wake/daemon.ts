#!/usr/bin/env node
/**
 * Standalone wake-alarm daemon. Watches the same project state as the in-session
 * extension and, while no live session holds the fencing lease, fires due alarms
 * by resuming the alarm's owner session in a headless Pi process:
 *
 *   pi --session <ownerSessionFile> --print "<factual wake message>"
 *
 * Project trust is respected by default (headlessTrust: "saved"); `--approve`
 * is only added when the project config explicitly sets "headlessTrust": "always".
 *
 * The spawned run gets WAKE_ALARM_PASSIVE=1, so its extension instance serves
 * tools but never schedules; this daemon stays the single active scheduler.
 * Run with: node <package>/extensions/pi-wake/daemon.ts  (Node >= 22.19).
 */
import { spawn, type ChildProcess } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildResumeArgs, type AlarmState, type FiredEvent } from "./core.ts";
import { leaseCurrentlyAlive, readLeaseFile } from "./lease.ts";
import { WakeAlarmRuntime, readStoredAlarms, wakeMessage, type EmitFn, type ExecFn } from "./runtime.ts";

const LEASE_NAME = "wake-alarm.lock.json";
const STATE_NAME = "wake-alarm.state.json";
const LEASE_POLL_MS = 5_000;
const ACTIVATION_RETRY_MS = 10_000;
const WAKE_RETRY_DELAY_MS = 60_000;
const WAKE_RETRY_MAX_ATTEMPTS = 5;
const MAX_CHILD_OUTPUT_CHARS = 2000;
const MAX_EXEC_OUTPUT_CHARS = 1024 * 1024;

const cwd = process.env.WAKE_ALARM_CWD ? path.resolve(process.env.WAKE_ALARM_CWD) : process.cwd();
const configPath = process.env.WAKE_ALARM_CONFIG_PATH ? path.resolve(process.env.WAKE_ALARM_CONFIG_PATH) : undefined;
const statePath = process.env.WAKE_ALARM_STATE_PATH ? path.resolve(process.env.WAKE_ALARM_STATE_PATH) : undefined;
const effectiveStatePath = statePath ?? path.join(cwd, ".pi", STATE_NAME);
const leasePath = path.join(cwd, ".pi", LEASE_NAME);
const dryRun = process.env.WAKE_ALARM_SPAWN_DRY_RUN === "1";
const spawnDisabled = process.env.WAKE_ALARM_SPAWN === "0";
const configuredCommand = process.env.WAKE_ALARM_PI_COMMAND;

let stopping = false;
let active: WakeAlarmRuntime | undefined;
let currentChild: ChildProcess | undefined;

function log(message: string): void {
	process.stdout.write(`[${new Date().toISOString()}] [pi-wake-daemon] ${message}\n`);
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface PiLaunch {
	file: string;
	prefix: string[];
}

let piLaunch: Promise<PiLaunch> | undefined;

/**
 * Modern Node refuses to spawn .cmd shims without a shell, so on Windows the
 * npm shim is unwrapped and the pi CLI script is run with this Node directly.
 * WAKE_ALARM_PI_COMMAND / config piCommand may point at a cli.js (run with
 * Node) or at any directly spawnable executable.
 */
export function resolvePiLaunch(configured?: string): Promise<PiLaunch> {
	piLaunch ??= (async (): Promise<PiLaunch> => {
		const command = configured ?? configuredCommand;
		if (command) {
			if (/\.js$/i.test(command)) return { file: process.execPath, prefix: [command] };
			return { file: command, prefix: [] };
		}
		if (process.platform === "win32") {
			const shim = path.join(process.env.APPDATA ?? "", "npm", "pi.cmd");
			const content = await fs.readFile(shim, "utf8").catch(() => "");
			const match = /"%~?dp0%\\?([^"]+?\.js)"/i.exec(content);
			if (match) {
				const script = path.join(path.dirname(shim), match[1]);
				await fs.access(script);
				return { file: process.execPath, prefix: [script] };
			}
			throw new Error(`cannot unwrap a spawnable pi command from ${shim}; set WAKE_ALARM_PI_COMMAND to the pi cli.js path`);
		}
		return { file: "pi", prefix: [] };
	})();
	return piLaunch;
}

const spawnExec: ExecFn = (file, args, options) => new Promise((resolve, reject) => {
	const child = spawn(file, args, { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
	let stdout = "";
	let stderr = "";
	let settled = false;
	const cleanup = (): void => {
		clearTimeout(timer);
		options.signal.removeEventListener("abort", onAbort);
	};
	const finish = (fn: (value: never) => void, value: never): void => {
		if (settled) return;
		settled = true;
		cleanup();
		fn(value);
	};
	const overflow = (): void => { child.kill(); finish(reject, new Error(`${file} output exceeded the safety limit`) as never); };
	const timer = setTimeout(() => { child.kill(); finish(reject, new Error(`${file} timed out after ${options.timeout}ms`) as never); }, options.timeout);
	const onAbort = (): void => { child.kill(); finish(reject, new Error(`${file} aborted`) as never); };
	options.signal.addEventListener("abort", onAbort, { once: true });
	child.stdout?.on("data", (chunk) => {
		stdout += chunk;
		if (stdout.length + stderr.length > MAX_EXEC_OUTPUT_CHARS) overflow();
	});
	child.stderr?.on("data", (chunk) => {
		stderr += chunk;
		if (stdout.length + stderr.length > MAX_EXEC_OUTPUT_CHARS) overflow();
	});
	child.on("error", (error) => finish(reject, error as never));
	child.on("close", (code) => finish(resolve, { stdout, stderr, code: code ?? 1 } as never));
});

function runPi(launch: PiLaunch, sessionFile: string, message: string, timeoutMs: number, approve: boolean): Promise<number> {
	return new Promise((resolve) => {
		const child = spawn(launch.file, [...launch.prefix, ...buildResumeArgs(sessionFile, message, { approve })], {
			cwd,
			env: { ...process.env, WAKE_ALARM_PASSIVE: "1" },
			stdio: ["ignore", "pipe", "pipe"],
			windowsHide: true,
		});
		currentChild = child;
		// Ring buffers: only the tails are ever logged, so memory stays bounded.
		let stdout = "";
		let stderr = "";
		let settled = false;
		const finish = (code: number): void => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			if (currentChild === child) currentChild = undefined;
			if (stdout.trim()) log(`wake run stdout tail: ${stdout.trim()}`);
			if (stderr.trim()) log(`wake run stderr tail: ${stderr.trim()}`);
			resolve(code);
		};
		const timer = setTimeout(() => {
			log(`wake run exceeded ${timeoutMs}ms; terminating the woken session`);
			child.kill();
			finish(124);
		}, timeoutMs);
		child.stdout?.on("data", (chunk) => { stdout = (stdout + chunk).slice(-MAX_CHILD_OUTPUT_CHARS); });
		child.stderr?.on("data", (chunk) => { stderr = (stderr + chunk).slice(-MAX_CHILD_OUTPUT_CHARS); });
		child.on("error", (error) => { log(`failed to start ${launch.file}: ${error.message}`); finish(127); });
		child.on("close", (code) => finish(code ?? 1));
	});
}

export interface DaemonEmitDeps {
	getRuntime: () => WakeAlarmRuntime | undefined;
	statePath: string;
	leasePath: string;
	dryRun: boolean;
	spawnDisabled: boolean;
	isStopping: () => boolean;
	log: (message: string) => void;
	runPi: (launch: PiLaunch, sessionFile: string, message: string, timeoutMs: number, approve: boolean) => Promise<number>;
}

/**
 * The daemon-side emit. Fencing comes first: a live session lease means the
 * session owns delivery, so the wake stays in the outbox untouched. The outbox
 * re-check then skips spawning when the wake was already delivered by someone
 * else (a session flush or a manual check) between the scheduler tick and here.
 */
export function createDaemonEmit(deps: DaemonEmitDeps): EmitFn {
	return async (alarm: AlarmState, events: FiredEvent[], now: number): Promise<boolean> => {
		const runtime = deps.getRuntime();
		if (!runtime || deps.isStopping()) return false;
		const lease = await readLeaseFile(deps.leasePath);
		if (leaseCurrentlyAlive(lease)) {
			deps.log(`wake for ${alarm.id} left in the outbox: a live session owns delivery`);
			try { await runtime.reloadFromDisk(); } catch { /* best effort */ }
			return false;
		}
		const expected = alarm.pendingWake?.triggeredAt;
		if (expected !== undefined) {
			const disk = await readStoredAlarms(deps.statePath).catch(() => undefined);
			const record = disk?.find((entry) => entry.id === alarm.id);
			if (!record || record.pendingWake?.triggeredAt !== expected) {
				deps.log(`wake for ${alarm.id} was already delivered or changed; skipping the resume`);
				return true;
			}
		}
		const config = runtime.runtimeConfig;
		if (deps.spawnDisabled || !config.spawnOnWake) {
			deps.log(`wake for ${alarm.id} observed but spawning is disabled; left in the outbox`);
			return false;
		}
		const sessionFile = alarm.ownerSessionFile;
		if (!sessionFile) {
			deps.log(`wake for ${alarm.id} has no owner session; left in the outbox for the next interactive session`);
			return false;
		}
		const message = wakeMessage(alarm, events, now, config.maxEvidenceChars);
		let launch: PiLaunch;
		try { launch = await resolvePiLaunch(config.piCommand); }
		catch (error) {
			deps.log((error as Error).message);
			return false;
		}
		if (deps.dryRun) {
			deps.log(`[dry-run] would run: ${launch.file} ${JSON.stringify([...launch.prefix, ...buildResumeArgs(sessionFile, message, { approve: config.headlessTrust === "always" })])}`);
			return false;
		}
		try { await fs.access(sessionFile); }
		catch {
			deps.log(`owner session file for ${alarm.id} is gone (${sessionFile}); left in the outbox`);
			return false;
		}
		deps.log(`waking session for alarm ${alarm.id} (${events.map((event) => event.kind).join(", ")}): ${sessionFile}`);
		const code = await deps.runPi(launch, sessionFile, message, config.runTimeoutMs, config.headlessTrust === "always");
		deps.log(`wake run for ${alarm.id} exited with code ${code}`);
		// The woken session may have created or changed alarms; reload before the next write.
		try { await runtime.reloadFromDisk(); }
		catch (error) { deps.log(`state reload after wake run failed: ${(error as Error).message}`); }
		return code === 0;
	};
}

async function shutdown(signal: string): Promise<void> {
	if (stopping) return;
	stopping = true;
	log(`received ${signal}; shutting down`);
	if (currentChild) currentChild.kill();
	const runtime = active;
	active = undefined;
	if (runtime) await runtime.stop();
	process.exit(0);
}

async function main(): Promise<void> {
	log(`watching project ${cwd}${dryRun ? " (dry-run)" : ""}${spawnDisabled ? " (spawning disabled)" : ""}`);
	const emit = createDaemonEmit({
		getRuntime: () => active,
		statePath: effectiveStatePath,
		leasePath,
		dryRun,
		spawnDisabled,
		isStopping: () => stopping,
		log,
		runPi,
	});
	while (!stopping) {
		const lease = await readLeaseFile(leasePath);
		if (leaseCurrentlyAlive(lease)) {
			if (active) {
				log("live Pi session lease detected; daemon scheduler standing down");
				const runtime = active;
				active = undefined;
				await runtime.stop();
			}
			await sleep(LEASE_POLL_MS);
			continue;
		}
		if (!active) {
			const runtime = new WakeAlarmRuntime({
				cwd,
				configPath,
				statePath,
				emit,
				execFn: spawnExec,
				schedulingEnabled: true,
				wakeRetry: { delayMs: WAKE_RETRY_DELAY_MS, maxAttempts: WAKE_RETRY_MAX_ATTEMPTS },
			});
			try {
				await runtime.start({ flushPending: false });
				active = runtime;
				log(`daemon active with ${runtime.alarmCount} alarm(s)`);
			} catch (error) {
				log(`activation failed: ${(error as Error).message}; retrying in ${ACTIVATION_RETRY_MS / 1000}s`);
				await sleep(ACTIVATION_RETRY_MS);
				continue;
			}
		}
		await sleep(LEASE_POLL_MS);
	}
}

const isMain = (() => {
	try { return Boolean(process.argv[1]) && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href; }
	catch { return false; }
})();

if (isMain) {
	process.on("SIGINT", () => void shutdown("SIGINT"));
	process.on("SIGTERM", () => void shutdown("SIGTERM"));
	void main();
}
