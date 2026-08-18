import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { leaseIsAlive, type AlarmLease } from "./core.ts";

export interface LeaseRecord extends AlarmLease {
	instanceId: string;
	epoch: string;
}

export function pidAlive(pid: number): boolean {
	try { process.kill(pid, 0); return true; }
	catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
}

export async function readLeaseFile(file: string): Promise<LeaseRecord | undefined> {
	try {
		const raw = JSON.parse(await fs.readFile(file, "utf8")) as Record<string, unknown>;
		if (typeof raw.pid !== "number" || typeof raw.heartbeatAt !== "number" || typeof raw.instanceId !== "string" || typeof raw.epoch !== "string") return undefined;
		return {
			version: 1,
			pid: raw.pid,
			instanceId: raw.instanceId,
			epoch: raw.epoch,
			role: "session",
			sessionFile: typeof raw.sessionFile === "string" ? raw.sessionFile : undefined,
			heartbeatAt: raw.heartbeatAt,
		};
	} catch { return undefined; }
}

async function writeLeaseAtomic(file: string, record: LeaseRecord, exclusive: boolean): Promise<boolean> {
	if (exclusive) {
		let handle;
		try {
			handle = await fs.open(file, "wx");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
			throw error;
		}
		try {
			await handle.writeFile(`${JSON.stringify(record)}\n`, { encoding: "utf8" });
		} finally { await handle.close(); }
		return true;
	}
	const temp = `${file}.tmp-${process.pid}-${randomUUID()}`;
	await fs.writeFile(temp, `${JSON.stringify(record)}\n`, { encoding: "utf8", mode: 0o600 });
	let lastError: Error | undefined;
	for (let attempt = 0; attempt < 10; attempt++) {
		try { await fs.rename(temp, file); return true; }
		catch (error) {
			lastError = error as Error;
			const code = (error as NodeJS.ErrnoException).code;
			if (code !== "EPERM" && code !== "EACCES" && code !== "EBUSY") break;
			await new Promise((resolve) => setTimeout(resolve, 25 + attempt * 25));
		}
	}
	await fs.rm(temp, { force: true }).catch(() => undefined);
	throw lastError;
}

export interface LeaseHandle {
	file: string;
	instanceId: string;
	epoch: string;
}

/**
 * Atomically acquire the session lease with a fencing epoch. Exactly one caller
 * wins even when several sessions start concurrently in the same or different
 * processes. A stale lease (dead pid or heartbeat older than the max age) is
 * broken and re-acquired; the exclusive create guarantees a single winner.
 */
export async function tryAcquireLease(file: string, instanceId: string, sessionFile: string | undefined, now: number = Date.now()): Promise<LeaseHandle | undefined> {
	for (let attempt = 0; attempt < 4; attempt++) {
		const epoch = `${now}-${randomUUID()}`;
		const record: LeaseRecord = { version: 1, pid: process.pid, instanceId, epoch, role: "session", sessionFile, heartbeatAt: now };
		if (await writeLeaseAtomic(file, record, true)) return { file, instanceId, epoch };
		const existing = await readLeaseFile(file);
		if (existing) {
			if (existing.instanceId !== instanceId && leaseIsAlive(existing, now, pidAlive)) return undefined;
			// Stale, dead, or our own previous incarnation: break and retry the exclusive create.
			await fs.rm(file, { force: true }).catch(() => undefined);
			continue;
		}
		// Unparsable lease: a concurrent creator may be mid-write (half-born). Only
		// break it when the file is old enough that no live creator can still own it.
		const stat = await fs.stat(file).catch(() => undefined);
		if (!stat) continue; // vanished; retry the exclusive create
		if (Date.now() - stat.mtimeMs < 5_000 && attempt < 3) {
			await new Promise((resolve) => setTimeout(resolve, 50));
			continue;
		}
		await fs.rm(file, { force: true }).catch(() => undefined);
	}
	return undefined;
}

/**
 * Heartbeat for the current holder. Re-fences before writing: if the lease now
 * belongs to another instance/epoch, ownership has been lost and false is
 * returned instead of overwriting another holder's lease.
 */
export async function heartbeatLease(handle: LeaseHandle, sessionFile: string | undefined, now: number = Date.now()): Promise<boolean> {
	const current = await readLeaseFile(handle.file);
	if (!current || current.instanceId !== handle.instanceId || current.epoch !== handle.epoch) return false;
	const record: LeaseRecord = { version: 1, pid: process.pid, instanceId: handle.instanceId, epoch: handle.epoch, role: "session", sessionFile, heartbeatAt: now };
	await writeLeaseAtomic(handle.file, record, false);
	return true;
}

/** Remove the lease only if it is still ours (same instance and epoch). */
export async function releaseLease(handle: LeaseHandle): Promise<void> {
	try {
		const current = await readLeaseFile(handle.file);
		if (current && current.instanceId === handle.instanceId && current.epoch === handle.epoch) await fs.rm(handle.file, { force: true });
	} catch { /* A missing or replaced lease is not ours to remove. */ }
	await fs.rm(`${handle.file}.tmp-${process.pid}`, { force: true }).catch(() => undefined);
}

/** Best-effort cleanup of abandoned lease temp files older than ten minutes. */
export async function cleanStaleLeaseTemps(file: string): Promise<void> {
	try {
		const dir = path.dirname(file);
		const prefix = `${path.basename(file)}.tmp-`;
		for (const entry of await fs.readdir(dir)) {
			if (!entry.startsWith(prefix)) continue;
			const full = path.join(dir, entry);
			const stat = await fs.stat(full).catch(() => undefined);
			if (stat && Date.now() - stat.mtimeMs > 10 * 60_000) await fs.rm(full, { force: true }).catch(() => undefined);
		}
	} catch { /* best effort */ }
}

/** Daemon-side liveness check: a fresh lease with a live pid means a session owns scheduling. */
export function leaseCurrentlyAlive(record: Pick<LeaseRecord, "pid" | "heartbeatAt"> | undefined, now: number = Date.now()): boolean {
	return Boolean(record && leaseIsAlive(record, now, pidAlive));
}
