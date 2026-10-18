import { randomUUID } from "node:crypto";
import { promises as fs, readFileSync } from "node:fs";
import type { FileHandle } from "node:fs/promises";
import path from "node:path";
import { leaseIsAlive } from "./core.ts";

/**
 * Per-session presence registry. Each live session owns exactly one file named
 * after its instance id. Each registration owns a unique temporary file;
 * session heartbeats are coalesced by createPresenceRefresher. There is no
 * acquire or takeover. Leadership for ownerless alarms is computed
 * deterministically (smallest live instance id), not claimed.
 */
export const PRESENCE_DIR_NAME = "wake-alarm.sessions";
export const PRESENCE_MAX_AGE_MS = 60_000;

export interface PresenceRecord {
	version: 1;
	pid: number;
	instanceId: string;
	sessionFile?: string;
	heartbeatAt: number;
}

export function pidAlive(pid: number): boolean {
	try { process.kill(pid, 0); return true; }
	catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
}

async function renameWithRetry(from: string, to: string): Promise<void> {
	let lastError: Error | undefined;
	for (let attempt = 0; attempt < 10; attempt++) {
		try { await fs.rename(from, to); return; }
		catch (error) {
			lastError = error as Error;
			const code = (error as NodeJS.ErrnoException).code;
			if (code !== "EPERM" && code !== "EACCES" && code !== "EBUSY") throw error;
			await new Promise((resolve) => setTimeout(resolve, 25 + attempt * 25));
		}
	}
	throw lastError;
}

type PresenceOperation = "mkdir" | "open" | "write" | "close" | "rename" | "list";

/** Keep the original I/O error for callers without exposing paths in warnings. */
export class PresenceIoError extends Error {
	readonly operation: PresenceOperation;
	readonly code: string | undefined;

	constructor(operation: PresenceOperation, cause: unknown) {
		super(`Presence ${operation} failed`, { cause });
		this.name = "PresenceIoError";
		this.operation = operation;
		const code = (cause as NodeJS.ErrnoException | null)?.code;
		this.code = typeof code === "string" ? code : undefined;
	}
}

/** Atomic registration; overlapping callers never share a temporary file.
 * Direct callers choose commit order. Session heartbeats use the refresher below. */
export async function registerPresence(dir: string, record: PresenceRecord): Promise<void> {
	const file = path.join(dir, `${record.instanceId}.json`);
	const temp = `${file}.tmp-${process.pid}-${randomUUID()}`;
	let operation: PresenceOperation = "mkdir";
	let handle: FileHandle | undefined;
	let ownsTemp = false;
	try {
		await fs.mkdir(dir, { recursive: true, mode: 0o700 });
		operation = "open";
		handle = await fs.open(temp, "wx", 0o600);
		ownsTemp = true;
		operation = "write";
		await handle.writeFile(`${JSON.stringify(record)}\n`, { encoding: "utf8" });
		operation = "close";
		await handle.close();
		handle = undefined;
		operation = "rename";
		await renameWithRetry(temp, file);
		ownsTemp = false;
	} catch (error) {
		throw new PresenceIoError(operation, error);
	} finally {
		// A partial write still owns its file. An unsuccessful exclusive open
		// does not, and must never remove somebody else's existing file.
		if (handle) await handle.close().catch(() => undefined);
		if (ownsTemp) await fs.rm(temp, { force: true }).catch(() => undefined);
	}
}

/** Call only after the owning refresher has closed and drained. */
export async function releasePresence(dir: string, instanceId: string): Promise<void> {
	await fs.rm(path.join(dir, `${instanceId}.json`), { force: true }).catch(() => undefined);
}

export interface PresenceRefresher {
	request(record: PresenceRecord): Promise<void>;
	close(): Promise<void>;
}

/** One running write/list cycle and at most one latest pending heartbeat.
 * Callbacks are synchronous; reentrant requests join the same drain. */
export function createPresenceRefresher(dir: string, callbacks: {
	onLive?: (live: PresenceRecord[]) => void;
	onError?: (error: unknown) => void;
} = {}): PresenceRefresher {
	let closed = false;
	let pending: PresenceRecord | undefined;
	let inFlight: Promise<void> | undefined;
	return {
		request(record) {
			if (closed) return Promise.resolve();
			pending = { ...record };
			if (!inFlight) {
				// Publish the Promise before any work or callback can reenter.
				inFlight = Promise.resolve().then(async () => {
					try {
						while (!closed && pending) {
							const next = pending;
							pending = undefined;
							try {
								await registerPresence(dir, next);
								if (closed) continue;
								const live = await listLivePresences(dir).catch((error) => { throw new PresenceIoError("list", error); });
								if (!closed) callbacks.onLive?.(live);
							} catch (error) {
								if (!closed) callbacks.onError?.(error);
							}
						}
					} finally {
						// No await between observing an empty queue and clearing this
						// state: a new request cannot be stranded in a finally gap.
						inFlight = undefined;
					}
				});
			}
			return inFlight;
		},
		close() {
			closed = true;
			pending = undefined;
			return inFlight ?? Promise.resolve();
		},
	};
}

export function formatPresenceFailure(error: unknown, failures: number): string {
	const operations: readonly string[] = ["mkdir", "open", "write", "close", "rename", "list"];
	const operation = error instanceof PresenceIoError && operations.includes(error.operation) ? error.operation : "refresh";
	const rawCode = (error as NodeJS.ErrnoException | null)?.code;
	const code = typeof rawCode === "string" && /^E[A-Z0-9_]{1,39}$/.test(rawCode) ? rawCode : "UNKNOWN";
	const hint = code === "EACCES" || code === "EPERM"
		? "check directory/file permissions or file locks"
		: code === "ENOENT" && operation === "rename"
			? "the temporary source or parent directory is missing"
			: "check the presence registry filesystem and I/O operation";
	return `Wake alarm: cannot refresh session presence (${operation}: ${code}; ${failures} consecutive failure(s)); the daemon may not detect this session — ${hint}`;
}

/** All currently live presences; stale records are ignored and best-effort removed. */
export async function listLivePresences(dir: string, now: number = Date.now(), maxAgeMs: number = PRESENCE_MAX_AGE_MS): Promise<PresenceRecord[]> {
	let entries: string[];
	try { entries = await fs.readdir(dir); }
	catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
		throw error;
	}
	const live: PresenceRecord[] = [];
	for (const entry of entries) {
		if (!entry.endsWith(".json")) continue;
		const full = path.join(dir, entry);
		let record: PresenceRecord | undefined;
		try {
			const raw = JSON.parse(await fs.readFile(full, "utf8")) as Record<string, unknown>;
			if (raw.version === 1 && typeof raw.pid === "number" && typeof raw.instanceId === "string" && typeof raw.heartbeatAt === "number") {
				record = { version: 1, pid: raw.pid, instanceId: raw.instanceId, sessionFile: typeof raw.sessionFile === "string" ? raw.sessionFile : undefined, heartbeatAt: raw.heartbeatAt };
			}
		} catch { record = undefined; }
		if (record && leaseIsAlive(record, now, pidAlive, maxAgeMs)) {
			live.push(record);
		} else if (record && now - record.heartbeatAt > maxAgeMs * 10) {
			await fs.rm(full, { force: true }).catch(() => undefined);
		}
	}
	return live;
}

/** The deterministic leader among live sessions: the smallest instance id. */
export function leaderInstanceId(live: readonly PresenceRecord[]): string | undefined {
	let leader: string | undefined;
	for (const record of live) if (leader === undefined || record.instanceId < leader) leader = record.instanceId;
	return leader;
}

export function isSessionFileLive(live: readonly PresenceRecord[], sessionFile: string): boolean {
	return live.some((record) => record.sessionFile === sessionFile);
}

/**
 * Daemon heartbeat file (.pi/wake-alarm.daemon.json). The daemon rewrites it on
 * every poll tick (5s) so any process — live session or a freshly started one —
 * can answer "is a daemon watching this project?" with one stat+read. The ring
 * of recent log lines travels with the heartbeat, so a dead daemon leaves its
 * last words on disk for post-mortem diagnosis.
 */
export const DAEMON_HEARTBEAT_NAME = "wake-alarm.daemon.json";
/** A heartbeat older than this is considered dead (3 poll ticks). */
export const DAEMON_HEARTBEAT_FRESH_MS = 15_000;
const DAEMON_LOG_TAIL_LINES = 30;

export interface DaemonHeartbeat {
	version: 1;
	pid: number;
	startedAt: number;
	heartbeatAt: number;
	dryRun: boolean;
	/** pi-wake package version of the daemon CODE that is running. A healthy
	 * daemon holding an older version must yield to a newer challenger, so a
	 * long-lived daemon never wedges a project on stale code (2026-09 incident). */
	pkgVersion?: string;
	/** Set while the daemon stopped scheduling because its state file is
	 * unreadable (it must never write stale memory back over newer disk state).
	 * Sessions treat a degraded daemon as unhealthy and spawn a replacement. */
	degraded?: string;
	logTail: string[];
}

export function daemonHeartbeatPath(cwd: string): string {
	return path.join(cwd, ".pi", DAEMON_HEARTBEAT_NAME);
}

export async function writeDaemonHeartbeat(cwd: string, record: DaemonHeartbeat): Promise<void> {
	const target = daemonHeartbeatPath(cwd);
	const tmp = `${target}.tmp-${process.pid}`;
	await fs.mkdir(path.dirname(target), { recursive: true, mode: 0o700 }).catch(() => undefined);
	await fs.writeFile(tmp, `${JSON.stringify(record)}
`, { mode: 0o600 });
	await renameWithRetry(tmp, target);
}

export async function clearDaemonHeartbeat(cwd: string, pid: number): Promise<void> {
	const target = daemonHeartbeatPath(cwd);
	try {
		const raw = JSON.parse((await fs.readFile(target, "utf8")).trim()) as Partial<DaemonHeartbeat>;
		// Only remove our own file: a newer daemon may have taken over the path.
		if (raw.pid === pid) await fs.rm(target, { force: true });
	} catch { /* absent or unreadable: nothing to clear */ }
}

/** Numeric version compare; positive when a > b. Unknown/short cores pad with
 * zero; a pre-release suffix ("1.0.0-beta") sorts below its release. */
export function compareVersions(a: string, b: string): number {
	const core = (value: string) => value.split("+", 1)[0].split("-", 1)[0].split(".");
	const ac = core(a);
	const bc = core(b);
	for (let index = 0; index < Math.max(ac.length, bc.length); index++) {
		const delta = (Number(ac[index] ?? "0") || 0) - (Number(bc[index] ?? "0") || 0);
		if (delta) return delta;
	}
	const aPre = a.includes("-");
	const bPre = b.includes("-");
	if (aPre !== bPre) return aPre ? -1 : 1;
	return a.localeCompare(b);
}

/** The pi-wake package version reachable from a module directory (dist/ and
 * extensions/pi-wake/ are both one or two levels below the package root). */
export function readPkgVersion(moduleDir: string): string {
	for (const rel of ["../package.json", "../../package.json"]) {
		try {
			const version = (JSON.parse(readFileSync(path.join(moduleDir, rel), "utf8")) as { version?: unknown }).version;
			if (typeof version === "string" && version) return version.slice(0, 32);
		} catch { /* try next candidate */ }
	}
	return "0.0.0-unknown";
}

export interface DaemonLiveness {
	live: boolean;
	heartbeat?: DaemonHeartbeat;
	/** Age of the newest heartbeat in ms (undefined when the file is absent/unreadable). */
	ageMs?: number;
}

export async function readDaemonLiveness(cwd: string): Promise<DaemonLiveness> {
	try {
		const raw = JSON.parse((await fs.readFile(daemonHeartbeatPath(cwd), "utf8")).trim()) as Partial<DaemonHeartbeat>;
		if (typeof raw.heartbeatAt !== "number" || typeof raw.pid !== "number") return { live: false };
		const ageMs = Date.now() - raw.heartbeatAt;
		// Fresh AND the pid still exists: a daemon that was hard-killed
		// (TerminateProcess / SIGKILL never runs the cleanup) leaves a fresh-looking
		// heartbeat behind; its pid being gone is the proof it cannot be writing.
		return {
			live: ageMs >= 0 && ageMs <= DAEMON_HEARTBEAT_FRESH_MS && pidAlive(raw.pid),
			heartbeat: {
				version: 1,
				pid: raw.pid,
				startedAt: raw.startedAt ?? raw.heartbeatAt,
				heartbeatAt: raw.heartbeatAt,
				dryRun: Boolean(raw.dryRun),
				pkgVersion: typeof raw.pkgVersion === "string" ? raw.pkgVersion.slice(0, 64) : undefined,
				degraded: typeof raw.degraded === "string" && raw.degraded ? raw.degraded.slice(0, 256) : undefined,
				logTail: Array.isArray(raw.logTail) ? raw.logTail.slice(-DAEMON_LOG_TAIL_LINES).map((line) => String(line)) : [],
			},
			ageMs,
		};
	} catch {
		return { live: false };
	}
}
