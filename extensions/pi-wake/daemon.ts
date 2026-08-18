#!/usr/bin/env node
/**
 * Standalone pi-wake daemon. Shares the project alarm state with live sessions
 * through two coordination primitives:
 *
 *   presence registry  (.pi/wake-alarm.sessions/) — which sessions are live
 *   atomic wake claim  (outbox entry claim)       — who delivers this wake
 *
 * The daemon re-reads the state file on every poll (disk state is the source of
 * truth), so alarms created by other sessions after daemon start are adopted
 * automatically. It schedules only alarms whose owner session is not live
 * (ownerless alarms only when no session is live at all). Delivery itself is
 * claimed under the state transaction lock, so a routing overlap can never
 * double-deliver. Owned alarms are delivered by resuming the owner session
 * headlessly:
 *
 *   pi --session <ownerSessionFile> --print "<factual wake message>"
 *
 * Project trust is respected by default (headlessTrust: "saved"); `--approve`
 * is only added when the project config sets "headlessTrust": "always".
 *
 * Run with: pi-wake-daemon (from the project directory), or
 *           node <package>/extensions/pi-wake/daemon.ts   (Node >= 22.19).
 */
import { spawn, type ChildProcess } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildResumeArgs, type AlarmState, type OutboxEntry } from "./core.ts";
import { PRESENCE_DIR_NAME, isSessionFileLive, listLivePresences, type PresenceRecord } from "./presence.ts";
import { WakeAlarmRuntime, type EmitFn, type ExecFn } from "./runtime.ts";

const PRESENCE_POLL_MS = 5_000;
const ACTIVATION_RETRY_MS = 10_000;
const TERMINATION_GRACE_MS = 5_000;
const MAX_CHILD_OUTPUT_CHARS = 2000;
const MAX_EXEC_OUTPUT_CHARS = 1024 * 1024;

const cwd = process.env.WAKE_ALARM_CWD ? path.resolve(process.env.WAKE_ALARM_CWD) : process.cwd();
const configPath = process.env.WAKE_ALARM_CONFIG_PATH ? path.resolve(process.env.WAKE_ALARM_CONFIG_PATH) : undefined;
const statePath = process.env.WAKE_ALARM_STATE_PATH ? path.resolve(process.env.WAKE_ALARM_STATE_PATH) : undefined;
const presenceDir = path.join(cwd, ".pi", PRESENCE_DIR_NAME);
const dryRun = process.env.WAKE_ALARM_SPAWN_DRY_RUN === "1";
const spawnDisabled = process.env.WAKE_ALARM_SPAWN === "0";
const configuredCommand = process.env.WAKE_ALARM_PI_COMMAND;

let stopping = false;
let active: WakeAlarmRuntime | undefined;
let currentChild: ChildProcess | undefined;
let livePresences: PresenceRecord[] = [];

function log(message: string): void {
	process.stdout.write(`[${new Date().toISOString()}] [pi-wake-daemon] ${message}\n`);
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Daemon routing: schedule alarms whose owner session is offline; ownerless only when no session is live. */
export function daemonOwns(alarm: Pick<AlarmState, "ownerSessionFile">, live: readonly PresenceRecord[]): boolean {
	if (alarm.ownerSessionFile === undefined) return live.length === 0;
	return !isSessionFileLive(live, alarm.ownerSessionFile);
}

export interface PiLaunch {
	file: string;
	prefix: string[];
}

const piLaunchCache = new Map<string, Promise<PiLaunch>>();

/**
 * Modern Node refuses to spawn .cmd shims without a shell, so on Windows the
 * npm shim is unwrapped and the pi CLI script is run with this Node directly.
 * WAKE_ALARM_PI_COMMAND / config piCommand may point at a cli.js (run with
 * Node) or at any directly spawnable executable. Failed resolutions are not
 * cached, so fixing the environment does not require a daemon restart.
 */
export function resolvePiLaunch(configured?: string): Promise<PiLaunch> {
	const key = configured ?? configuredCommand ?? "<auto>";
	let cached = piLaunchCache.get(key);
	if (cached) return cached;
	cached = (async (): Promise<PiLaunch> => {
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
	cached.catch(() => piLaunchCache.delete(key));
	piLaunchCache.set(key, cached);
	return cached;
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
		let timedOut = false;
		let graceTimer: ReturnType<typeof setTimeout> | undefined;
		let forceTimer: ReturnType<typeof setTimeout> | undefined;
		const finish = (code: number): void => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			if (graceTimer) clearTimeout(graceTimer);
			if (forceTimer) clearTimeout(forceTimer);
			if (currentChild === child) currentChild = undefined;
			if (stdout.trim()) log(`wake run stdout tail: ${stdout.trim()}`);
			if (stderr.trim()) log(`wake run stderr tail: ${stderr.trim()}`);
			resolve(code);
		};
		// Force-kill the whole process tree on Windows (the direct child only is not
		// enough: pi may have spawned providers/tools under the session).
		const forceKill = (): void => {
			if (process.platform === "win32") {
				spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore", windowsHide: true });
			} else {
				child.kill("SIGKILL");
			}
		};
		const timer = setTimeout(() => {
			timedOut = true;
			log(`wake run exceeded ${timeoutMs}ms; requesting termination of the woken session`);
			child.kill(); // graceful SIGTERM (forceful on Windows, where SIGTERM is not real)
			// Two-phase termination: only after the child actually closes (or is
			// force-killed) is the delivery slot released, so the next attempt cannot
			// start a second Pi on the same session file while the old one still lives.
			graceTimer = setTimeout(() => {
				log("wake run still running after the termination request; force-killing");
				forceKill();
				forceTimer = setTimeout(() => {
					// Even SIGKILL did not close it (e.g. uninterruptible I/O): release the
					// slot rather than hang the daemon forever; the session is unusable anyway.
					log("wake run did not exit after SIGKILL; releasing the delivery slot");
					finish(124);
				}, TERMINATION_GRACE_MS);
				child.once("close", () => { if (forceTimer) clearTimeout(forceTimer); });
			}, TERMINATION_GRACE_MS);
			child.once("close", () => { if (graceTimer) clearTimeout(graceTimer); });
		}, timeoutMs);
		child.stdout?.on("data", (chunk) => { stdout = (stdout + chunk).slice(-MAX_CHILD_OUTPUT_CHARS); });
		child.stderr?.on("data", (chunk) => { stderr = (stderr + chunk).slice(-MAX_CHILD_OUTPUT_CHARS); });
		child.on("error", (error) => { log(`failed to start ${launch.file}: ${error.message}`); finish(127); });
		child.on("close", (code) => finish(timedOut ? 124 : code ?? 1));
	});
}

export interface DaemonEmitDeps {
	getRuntime: () => WakeAlarmRuntime | undefined;
	presenceDir: string;
	dryRun: boolean;
	spawnDisabled: boolean;
	isStopping: () => boolean;
	log: (message: string) => void;
	runPi: (launch: PiLaunch, sessionFile: string, message: string, timeoutMs: number, approve: boolean) => Promise<number>;
}

/**
 * The daemon-side emit, invoked only after this runtime holds the delivery claim
 * for an outbox entry. Returning false releases the claim and keeps the entry
 * in the outbox for a later attempt (or for the next live session of the owner).
 */
export function createDaemonEmit(deps: DaemonEmitDeps): EmitFn {
	return async (entry: OutboxEntry): Promise<boolean> => {
		const runtime = deps.getRuntime();
		if (!runtime || deps.isStopping()) return false;
		const config = runtime.runtimeConfig;
		if (deps.spawnDisabled || !config.spawnOnWake) {
			deps.log(`wake for ${entry.alarmId} observed but spawning is disabled; left in the outbox`);
			return false;
		}
		const sessionFile = entry.ownerSessionFile;
		// Re-check presence between the claim and the spawn: if the owner session came
		// back online in the meantime, leave the wake for the live session instead of
		// spawning a second Pi process against the same session file. Ownerless wakes
		// are daemon-served only while no session is live at all.
		// Fail CLOSED: an unreadable presence registry means "unknown", never "nobody
		// is live" — the wake is already durable, so retrying later costs nothing.
		let live: PresenceRecord[];
		try { live = await listLivePresences(deps.presenceDir); }
		catch (error) {
			deps.log(`cannot verify session presence (${(error as Error).message}); wake left in the outbox`);
			return false;
		}
		if (sessionFile ? isSessionFileLive(live, sessionFile) : live.length > 0) {
			deps.log(`owner of ${entry.alarmId} is now live; wake left in the outbox for the session`);
			return false;
		}
		if (!sessionFile) {
			deps.log(`wake for ${entry.alarmId} has no owner session; left in the outbox for the next interactive session`);
			return false;
		}
		const message = entry.message;
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
			deps.log(`owner session file for ${entry.alarmId} is gone (${sessionFile}); left in the outbox`);
			return false;
		}
		deps.log(`waking session for alarm ${entry.alarmId} (${entry.events.map((event) => event.kind).join(", ")}): ${sessionFile}`);
		const code = await deps.runPi(launch, sessionFile, message, config.runTimeoutMs, config.headlessTrust === "always");
		deps.log(`wake run for ${entry.alarmId} exited with code ${code}`);
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
		presenceDir,
		dryRun,
		spawnDisabled,
		isStopping: () => stopping,
		log,
		runPi,
	});
	while (!stopping) {
		livePresences = await listLivePresences(presenceDir).catch(() => livePresences);
		if (!active) {
			const runtime = new WakeAlarmRuntime({
				cwd,
				configPath,
				statePath,
				emit,
				execFn: spawnExec,
				schedulingEnabled: true,
				claimantId: `daemon:${process.pid}`,
				deliveryTtlMs: () => ((active?.runtimeConfig.runTimeoutMs ?? 1_800_000) + 60_000),
				wakeRetry: { delayMs: 60_000, capMs: 1_800_000 },
				owns: (alarm) => daemonOwns(alarm, livePresences),
			});
			try {
				await runtime.start({ flushPending: false });
				active = runtime;
				log(`daemon active with ${runtime.alarmCount} alarm(s), ${livePresences.length} live session(s)`);
			} catch (error) {
				log(`activation failed: ${(error as Error).message}; retrying in ${ACTIVATION_RETRY_MS / 1000}s`);
				await sleep(ACTIVATION_RETRY_MS);
				continue;
			}
		} else {
			// Disk state is the source of truth: adopt alarms created or changed by
			// other sessions since the last poll, then re-arm the scheduler.
			try { await active.resync(); }
			catch (error) { log(`reconcile failed: ${(error as Error).message}`); }
		}
		await sleep(PRESENCE_POLL_MS);
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
