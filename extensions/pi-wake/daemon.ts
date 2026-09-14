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
import { promises as fs, realpathSync } from "node:fs";
import path from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { buildResumeArgs, type AlarmState, type OutboxEntry } from "./core.ts";
import { PRESENCE_DIR_NAME, clearDaemonHeartbeat, compareVersions, isSessionFileLive, listLivePresences, pidAlive, readDaemonLiveness, readPkgVersion, writeDaemonHeartbeat, type PresenceRecord } from "./presence.ts";
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

/** The package version of the CODE this daemon runs (WAKE_ALARM_PKG_VERSION
 * overrides it for tests). Long-lived daemons survive installs and uninstalls
 * of their host directory, so the version travels with the heartbeat and a
 * newer challenger can take the role over. */
const pkgVersion = (() => {
	const override = process.env.WAKE_ALARM_PKG_VERSION;
	if (override && /^[0-9A-Za-z][0-9A-Za-z.+-]{0,31}$/.test(override)) return override;
	try { return readPkgVersion(path.dirname(fileURLToPath(import.meta.url))); } catch { return "0.0.0-unknown"; }
})();

export { compareVersions } from "./presence.ts";

/** Grace period with zero active alarms and zero live sessions before an idle
 * daemon exits (WAKE_ALARM_IDLE_EXIT_MS overrides it for tests). A future
 * session restarts the daemon when it creates the next alarm. */
const IDLE_EXIT_MS = (() => {
	const raw = Number(process.env.WAKE_ALARM_IDLE_EXIT_MS ?? "");
	return Number.isFinite(raw) && raw > 0 ? raw : 6 * 60 * 60 * 1000;
})();

let degradedReason: string | undefined;
let idleSince: number | undefined;

/** Identity of the project directory at startup: dev/ino/birthtime. Used to
 * detect a vanished OR recreated project (the heartbeat writer recreates
 * <cwd>/.pi, so existence alone is not proof the project still exists). */
const cwdStat = await fs.stat(cwd).catch(() => undefined);

let stopping = false;
let active: WakeAlarmRuntime | undefined;
let currentChild: ChildProcess | undefined;
let livePresences: PresenceRecord[] = [];

const LOG_RING: string[] = [];

function log(message: string): void {
	const line = `[${new Date().toISOString()}] [pi-wake-daemon] ${message}`;
	process.stdout.write(line + "\n");
	LOG_RING.push(line);
	if (LOG_RING.length > 30) LOG_RING.shift();
}

const startedAt = Date.now();

/** Heartbeat + recent log tail, so a dead daemon leaves its last words on disk.
 * Read-before-write: an established daemon must notice a foreign claim BEFORE
 * overwriting it, otherwise a takeover can never succeed against a daemon that
 * only re-reads at startup. Yield rules (their claim is fresh and alive):
 *   - their code version is newer than ours         -> yield (upgrade path);
 *   - we are degraded                               -> yield (replacement path);
 *   - same version and their pid is larger          -> yield (duplicate tie-break). */
async function heartbeat(): Promise<void> {
	try {
		const foreign = await readDaemonLiveness(cwd);
		const theirs = foreign.live ? foreign.heartbeat : undefined;
		if (theirs && theirs.pid !== process.pid) {
			const newer = typeof theirs.pkgVersion === "string" && compareVersions(theirs.pkgVersion, pkgVersion) > 0;
			const sameVersionLargerPid = theirs.pkgVersion === pkgVersion && theirs.pid > process.pid;
			if (newer || degradedReason !== undefined || sameVersionLargerPid) {
				log(`yielding the daemon role to pid ${theirs.pid}${newer ? ` (newer code ${theirs.pkgVersion})` : degradedReason !== undefined ? " (this daemon is degraded)" : " (same version, larger pid)"}; exiting`);
				await shutdown("role-yield");
				return;
			}
		}
	} catch { /* unreadable heartbeat: nothing to yield to */ }
	await writeDaemonHeartbeat(cwd, { version: 1, pid: process.pid, startedAt, heartbeatAt: Date.now(), dryRun, pkgVersion, degraded: degradedReason, logTail: [...LOG_RING] }).catch((error) => log(`heartbeat write failed: ${(error as Error).message}`));
}

/** Stop scheduling but keep heartbeating (marked degraded). Stale in-memory
 * state must never be written back over newer on-disk state, so the runtime is
 * torn down and the main loop re-enters the activation path, which retries from
 * disk and clears the flag on success; sessions seeing a degraded daemon spawn
 * a replacement that takes the role over. */
function degrade(reason: string, runtime?: WakeAlarmRuntime): void {
	degradedReason ??= reason;
	log(`DEGRADED — ${reason}; scheduling stopped until the state becomes readable`);
	const target = runtime ?? active;
	active = undefined;
	if (target) void target.stop().catch(() => undefined);
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
	degrade: (reason: string, runtime?: WakeAlarmRuntime) => void;
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
		catch (error) { deps.degrade(`state reload after wake run failed: ${(error as Error).message}`, runtime); }
		return code === 0;
	};
}

async function shutdown(signal: string): Promise<void> {
	if (stopping) return;
	stopping = true;
	log(`received ${signal}; shutting down`);
	await clearDaemonHeartbeat(cwd, process.pid).catch(() => undefined);
	if (currentChild) currentChild.kill();
	const runtime = active;
	active = undefined;
	if (runtime) await runtime.stop();
	process.exit(0);
}

/**
 * Single-instance guard (heartbeat claim + staggered verify). Two daemons on one
 * project never corrupt state — the transaction lock and delivery claims keep
 * them correct — but they would double the probe traffic forever, and nothing
 * else reaps the duplicate. Protocol:
 *
 *   1. a fresh foreign heartbeat already on disk -> step down, UNLESS we can
 *      take the role over: their code is older (long-lived daemon on stale
 *      code — the 2026-09 incident) or they are degraded (state unreadable);
 *      takeover signals the old daemon and waits briefly for its exit;
 *   2. write OUR pid as a claim, then wait 1-2.5s (jitter staggers near-
 *      simultaneous spawns) and re-read: a healthy NEWER daemon on the file
 *      means we lost the race -> step down; anything else proceeds — the
 *      established daemon's read-before-write tick yields within 5s.
 *
 * The >=1s minimum stagger makes simultaneous double survival impossible, and
 * the per-tick yield check closes the late-starter gap (a challenger slipping
 * between an established daemon's 5s writes never re-read afterwards).
 */
async function singleInstanceGuard(): Promise<void> {
	const pre = await readDaemonLiveness(cwd);
	if (pre.live && pre.heartbeat && pre.heartbeat.pid !== process.pid) {
		const theirs = pre.heartbeat;
		const degraded = typeof theirs.degraded === "string" && theirs.degraded.length > 0;
		const older = typeof theirs.pkgVersion === "string" && compareVersions(pkgVersion, theirs.pkgVersion) > 0;
		if (degraded || older) {
			log(`taking over from daemon pid ${theirs.pid}${older ? ` (older code ${theirs.pkgVersion ?? "?"} < ${pkgVersion})` : " (degraded)"}; signaling it to stop`);
			// SIGTERM runs their graceful shutdown on POSIX; on Windows this is a
		// hard kill — acceptable because heartbeats are pidAlive-gated and wake
		// delivery is claim-based at-least-once (a mid-run kill redelivers later).
			try { process.kill(theirs.pid, "SIGTERM"); } catch { /* already gone */ }
			for (let waited = 0; waited < 3_000 && pidAlive(theirs.pid); waited += 100) await sleep(100);
			if (pidAlive(theirs.pid)) log(`pid ${theirs.pid} still alive after SIGTERM; claiming anyway (its per-tick yield check will settle it)`);
		} else {
			log(`another pi-wake daemon (pid ${theirs.pid}, version ${theirs.pkgVersion ?? "?"}) is already live for this project; stepping down`);
			process.exit(0);
		}
	}
	await heartbeat(); // claim the heartbeat path with OUR pid
	await sleep(1_000 + Math.floor(Math.random() * 1_500));
	const verify = await readDaemonLiveness(cwd);
	if (verify.live && verify.heartbeat && verify.heartbeat.pid !== process.pid) {
		const theirs = verify.heartbeat;
		const newer = typeof theirs.pkgVersion === "string" && compareVersions(theirs.pkgVersion, pkgVersion) > 0;
		if (newer && !theirs.degraded) {
			log(`lost the single-instance race to newer daemon pid ${theirs.pid} (${theirs.pkgVersion}); stepping down`);
			await clearDaemonHeartbeat(cwd, process.pid).catch(() => undefined); // no-op when the winner's pid is on the file
			process.exit(0);
		}
		log(`retaining the claim against daemon pid ${theirs.pid} (same or older code${theirs.degraded ? ", degraded" : ""}); its per-tick check will yield`);
	}
	await heartbeat(); // refresh so the winner never looks stale after the stagger
}

async function main(): Promise<void> {
	log(`pi-wake daemon ${pkgVersion} watching project ${cwd}${dryRun ? " (dry-run)" : ""}${spawnDisabled ? " (spawning disabled)" : ""}`);
	await singleInstanceGuard();
	const emit = createDaemonEmit({
		getRuntime: () => active,
		presenceDir,
		dryRun,
		spawnDisabled,
		isStopping: () => stopping,
		log,
		degrade,
		runPi,
	});
	while (!stopping) {
		// A daemon whose project directory vanished is an immortal orphan (the
		// 2026-09 incident left several watching deleted install directories).
		// IDENTITY, not mere existence: this daemon's own heartbeat writer
		// mkdir-recreates <cwd>/.pi every 5s, so a deleted project can look
		// alive again. The recreated directory has a different dev/ino/birthtime.
		try {
			const stat = await fs.stat(cwd);
			if (cwdStat && (stat.dev !== cwdStat.dev || stat.ino !== cwdStat.ino || stat.birthtimeMs !== cwdStat.birthtimeMs)) throw new Error("replaced");
		} catch {
			log(`project directory is gone or was replaced (${cwd}); exiting so a fresh clone can respawn`);
			await shutdown("project-gone");
			return;
		}
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
				if (degradedReason) { log("recovered: state is readable again, resuming scheduling"); degradedReason = undefined; }
				log(`daemon active with ${runtime.alarmCount} alarm(s), ${livePresences.length} live session(s)`);
			} catch (error) {
				degrade(`state activation failed: ${(error as Error).message}`, runtime);
				await heartbeat(); // degraded heartbeat, so sessions can react and replace us
				await sleep(ACTIVATION_RETRY_MS);
				continue;
			}
		} else {
			// Disk state is the source of truth: adopt alarms created or changed by
			// other sessions since the last poll, then re-arm the scheduler.
			try { await active.resync(); }
			catch (error) {
				degrade(`reconcile failed: ${(error as Error).message}`, active);
				await heartbeat();
				continue;
			}
		}
		// Idle exit: with zero active alarms and zero live sessions this daemon has
		// no possible work; leaving it alive only preserves stale code. The next
		// session that creates an alarm restarts the daemon automatically.
		const activeAlarms = active.alarmDigest().active;
		if (activeAlarms > 0 || livePresences.length > 0) idleSince = undefined;
		else {
			idleSince ??= Date.now();
			if (Date.now() - idleSince >= IDLE_EXIT_MS) {
				log(`no active alarms and no live sessions for ${Math.round((Date.now() - idleSince) / 1000)}s; exiting (idle)`);
				await shutdown("idle-exit");
				return;
			}
		}
		await heartbeat();
		await sleep(PRESENCE_POLL_MS);
	}
}

/**
 * Resolve the invoked entry to its real path. `process.argv[1]` can be a relative
 * path, an absolute path, a SYMLINK (npm links bin entries to dist/daemon.js on
 * POSIX), or a bare command name found via PATH (service managers). Node's
 * `import.meta.url` is the realpath, so all of these must resolve to the same
 * file before the daemon treats itself as the main entry.
 */
function resolveInvoked(argv1: string): string | undefined {
	const candidates: string[] = [path.resolve(argv1)];
	if (!path.isAbsolute(argv1)) {
		for (const dir of (process.env.PATH ?? "").split(path.delimiter)) {
			if (dir) candidates.push(path.join(dir, argv1));
		}
	}
	for (const candidate of candidates) {
		try { return realpathSync(candidate); } catch { /* try next candidate */ }
	}
	return undefined;
}

const isMain = (() => {
	try {
		if (!process.argv[1]) return false;
		const invoked = resolveInvoked(process.argv[1]);
		return invoked !== undefined && import.meta.url === pathToFileURL(invoked).href;
	} catch { return false; }
})();

if (isMain) {
	process.on("SIGINT", () => void shutdown("SIGINT"));
	process.on("SIGTERM", () => void shutdown("SIGTERM"));
	void main();
}
