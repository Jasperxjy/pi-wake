#!/usr/bin/env node
/**
 * Standalone wake-alarm daemon. Watches the same project state as the in-session
 * extension and, while no interactive Pi session holds the session lease, fires
 * due alarms by resuming the alarm's owner session in a headless Pi process:
 *
 *   pi --session <ownerSessionFile> --approve --print "<factual wake message>"
 *
 * The spawned run gets WAKE_ALARM_PASSIVE=1, so its extension instance serves
 * tools but never schedules; this daemon stays the single active scheduler.
 * Run with: node <package>/extensions/pi-wake/daemon.ts  (Node >= 22.18).
 */
import { spawn, type ChildProcess } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import { buildResumeArgs, leaseIsAlive, type AlarmState, type FiredEvent } from "./core.ts";
import { WakeAlarmRuntime, wakeMessage, type ExecFn } from "./runtime.ts";

const LEASE_NAME = "wake-alarm.lock.json";
const LEASE_POLL_MS = 5_000;
const ACTIVATION_RETRY_MS = 10_000;
const WAKE_RETRY_DELAY_MS = 60_000;
const WAKE_RETRY_MAX_ATTEMPTS = 5;
const MAX_CHILD_OUTPUT_CHARS = 2000;

const cwd = process.env.WAKE_ALARM_CWD ? path.resolve(process.env.WAKE_ALARM_CWD) : process.cwd();
const configPath = process.env.WAKE_ALARM_CONFIG_PATH ? path.resolve(process.env.WAKE_ALARM_CONFIG_PATH) : undefined;
const statePath = process.env.WAKE_ALARM_STATE_PATH ? path.resolve(process.env.WAKE_ALARM_STATE_PATH) : undefined;
const leasePath = path.join(cwd, ".pi", LEASE_NAME);
const dryRun = process.env.WAKE_ALARM_SPAWN_DRY_RUN === "1";
const spawnDisabled = process.env.WAKE_ALARM_SPAWN === "0";
const configuredCommand = process.env.WAKE_ALARM_PI_COMMAND;

interface PiLaunch {
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
function resolvePiLaunch(configured?: string): Promise<PiLaunch> {
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

let stopping = false;
let active: WakeAlarmRuntime | undefined;
let currentChild: ChildProcess | undefined;

function log(message: string): void {
	process.stdout.write(`[${new Date().toISOString()}] [wake-alarm-daemon] ${message}\n`);
}

function pidAlive(pid: number): boolean {
	try { process.kill(pid, 0); return true; }
	catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
}

async function readLease(): Promise<{ pid: number; heartbeatAt: number } | undefined> {
	try {
		const raw = JSON.parse(await fs.readFile(leasePath, "utf8")) as { pid?: unknown; heartbeatAt?: unknown };
		if (typeof raw.pid !== "number" || typeof raw.heartbeatAt !== "number") return undefined;
		return { pid: raw.pid, heartbeatAt: raw.heartbeatAt };
	} catch { return undefined; }
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
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
	const timer = setTimeout(() => { child.kill(); finish(reject, new Error(`${file} timed out after ${options.timeout}ms`) as never); }, options.timeout);
	const onAbort = (): void => { child.kill(); finish(reject, new Error(`${file} aborted`) as never); };
	options.signal.addEventListener("abort", onAbort, { once: true });
	child.stdout?.on("data", (chunk) => { stdout += chunk; });
	child.stderr?.on("data", (chunk) => { stderr += chunk; });
	child.on("error", (error) => finish(reject, error as never));
	child.on("close", (code) => finish(resolve, { stdout, stderr, code: code ?? 1 } as never));
});

function runPi(launch: PiLaunch, sessionFile: string, message: string, timeoutMs: number): Promise<number> {
	return new Promise((resolve) => {
		const child = spawn(launch.file, [...launch.prefix, ...buildResumeArgs(sessionFile, message)], {
			cwd,
			env: { ...process.env, WAKE_ALARM_PASSIVE: "1" },
			stdio: ["ignore", "pipe", "pipe"],
			windowsHide: true,
		});
		currentChild = child;
		let stdout = "";
		let stderr = "";
		let settled = false;
		const finish = (code: number): void => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			if (currentChild === child) currentChild = undefined;
			const outTail = stdout.slice(-MAX_CHILD_OUTPUT_CHARS).trim();
			const errTail = stderr.slice(-MAX_CHILD_OUTPUT_CHARS).trim();
			if (outTail) log(`wake run stdout tail: ${outTail}`);
			if (errTail) log(`wake run stderr tail: ${errTail}`);
			resolve(code);
		};
		const timer = setTimeout(() => {
			log(`wake run exceeded ${timeoutMs}ms; terminating the woken session`);
			child.kill();
			finish(124);
		}, timeoutMs);
		child.stdout?.on("data", (chunk) => { stdout += chunk; });
		child.stderr?.on("data", (chunk) => { stderr += chunk; });
		child.on("error", (error) => { log(`failed to start ${launch.file}: ${error.message}`); finish(127); });
		child.on("close", (code) => finish(code ?? 1));
	});
}

async function daemonEmit(alarm: AlarmState, events: FiredEvent[], now: number): Promise<boolean> {
	const runtime = active;
	if (!runtime || stopping) return false;
	const config = runtime.runtimeConfig;
	if (spawnDisabled || !config.spawnOnWake) {
		log(`wake for ${alarm.id} observed but spawning is disabled; left in the outbox`);
		return false;
	}
	const sessionFile = alarm.ownerSessionFile;
	if (!sessionFile) {
		log(`wake for ${alarm.id} has no owner session; left in the outbox for the next interactive session`);
		return false;
	}
	const message = wakeMessage(alarm, events, now, config.maxEvidenceChars);
	let launch: PiLaunch;
	try { launch = await resolvePiLaunch(config.piCommand); }
	catch (error) {
		log((error as Error).message);
		return false;
	}
	if (dryRun) {
		log(`[dry-run] would run: ${launch.file} ${JSON.stringify([...launch.prefix, ...buildResumeArgs(sessionFile, message)])}`);
		return false;
	}
	try { await fs.access(sessionFile); }
	catch {
		log(`owner session file for ${alarm.id} is gone (${sessionFile}); left in the outbox`);
		return false;
	}
	log(`waking session for alarm ${alarm.id} (${events.map((event) => event.kind).join(", ")}): ${sessionFile}`);
	const code = await runPi(launch, sessionFile, message, config.runTimeoutMs);
	log(`wake run for ${alarm.id} exited with code ${code}`);
	// The woken session may have created or changed alarms; reload before the next write.
	try { await runtime.reloadFromDisk(); }
	catch (error) { log(`state reload after wake run failed: ${(error as Error).message}`); }
	return code === 0;
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
	while (!stopping) {
		const lease = await readLease();
		if (lease && leaseIsAlive(lease, Date.now(), pidAlive)) {
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
				emit: daemonEmit,
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

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
void main();
