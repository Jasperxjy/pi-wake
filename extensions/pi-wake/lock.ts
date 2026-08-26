import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import { pidAlive } from "./presence.ts";

/**
 * Cross-process state transaction lock.
 *
 * Correctness rules, in order of importance:
 *  1. Acquire is `open(path, "wx")` — atomic create, exactly one winner.
 *  2. Release only ever deletes a lock file that is still *ours* (same inode and
 *     token), so a recovered-from-crash contender can never delete a successor's
 *     live lock from the `finally` path.
 *  3. Stale takeover is rename-based: the contender renames the lock it judged
 *     stale to a private victim name and then verifies the victim — SAME content
 *     AND SAME inode (inode-only checks are unsound on filesystems that fabricate
 *     colliding inodes, e.g. WSL/DrvFS). If the identity does not match, a live
 *     successor's lock was displaced and is restored (hard link, retried until the
 *     path is free) instead of deleted; a displaced lock is NEVER dropped, so a
 *     contender can never acquire from a live holder's emptied slot. This closes
 *     the classic stat-rm-acquire TOCTOU where two contenders both "recover" the
 *     same dead lock and the second one deletes the first one's fresh lock.
 *  4. A lock whose owner PID is still alive is never judged stale, no matter how
 *     old it is: age-based takeover of a live holder is what allowed a late
 *     release() to delete a successor's lock. Only dead owners are recoverable.
 *  5. Every state write inside the critical section calls `verifyHeld()` first:
 *     if the lock at `path` is no longer the inode we acquired, the operation
 *     aborts instead of writing concurrently with the new holder.
 *
 * Residual (documented, deliberate): between a `verifyHeld()` check and the
 * state-file rename, a takeover that judges us stale (dead pid, or an
 * unparsable lock that crossed staleMs) can still displace us. Holders are
 * alive and critical sections are short, so this requires a pathological pause;
 * the per-alarm revision CAS on top bounds the damage of any such interleave.
 */
export interface LockTestingHooks {
	/** Called after a contender judged the current lock stale, immediately before the takeover rename. */
	beforeTakeover?: () => Promise<void> | void;
	/** Called after a successful (verified) takeover removed the stale lock, before the retry. */
	afterTakeover?: () => Promise<void> | void;
}

export interface StateLockOptions {
	path: string;
	/** Locks older than this (mtime) or with an unparsable owner older than this are stale. Default 30s. */
	staleMs?: number;
	/** Give up acquiring after this long. Default 10s. */
	timeoutMs?: number;
	hooks?: LockTestingHooks;
}

interface LockIdentity {
	token: string;
	ino: number;
}

interface LockObservation {
	ino: number;
	mtimeMs: number;
	text: string;
	parsed: { pid: number; token: string; createdAt: number } | undefined;
}

export class StateLock {
	private readonly lockPath: string;
	private readonly staleMs: number;
	private readonly timeoutMs: number;
	private readonly hooks: LockTestingHooks | undefined;
	private identity: LockIdentity | undefined;

	constructor(options: StateLockOptions) {
		this.lockPath = options.path;
		this.staleMs = options.staleMs ?? 30_000;
		this.timeoutMs = options.timeoutMs ?? 10_000;
		this.hooks = options.hooks;
	}

	get path(): string {
		return this.lockPath;
	}

	private sleep(ms: number): Promise<void> {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	private async observe(): Promise<LockObservation | undefined> {
		let text: string;
		const stat = await fs.stat(this.lockPath).catch(() => undefined);
		if (!stat) return undefined;
		try { text = await fs.readFile(this.lockPath, "utf8"); }
		catch { return undefined; }
		let parsed: LockObservation["parsed"];
		try {
			const raw = JSON.parse(text) as Record<string, unknown>;
			const pid = Number(raw.pid);
			const createdAt = Number(raw.createdAt);
			parsed = typeof raw.token === "string" && Number.isSafeInteger(pid) && pid > 0 && Number.isSafeInteger(createdAt)
				? { pid, token: raw.token, createdAt }
				: undefined;
		} catch { parsed = undefined; }
		return { ino: stat.ino, mtimeMs: stat.mtimeMs, text, parsed };
	}

	private isStale(observed: LockObservation, now: number): boolean {
		if (observed.parsed) {
			// A lock whose owner is still alive is NEVER stale, regardless of age:
			// age-based takeover of a live holder is what lets a still-running release()
			// delete a successor's lock. Only a dead owner (or unparsable old garbage)
			// can be recovered. The cost is that a hung-but-alive holder makes other
			// contenders time out loudly instead of stealing — the safe failure mode.
			return !pidAlive(observed.parsed.pid);
		}
		// Unparsable content: half-born (fresh) locks are never stale; old garbage is.
		return now - observed.mtimeMs > this.staleMs;
	}

	async acquire(): Promise<void> {
		if (this.identity) throw new Error("state lock is already held by this runtime");
		const deadline = Date.now() + this.timeoutMs;
		for (;;) {
			const token = randomUUID();
			try {
				const handle = await fs.open(this.lockPath, "wx");
				try { await handle.writeFile(`${JSON.stringify({ pid: process.pid, token, createdAt: Date.now() })}\n`); }
				finally { await handle.close(); }
				const stat = await fs.stat(this.lockPath).catch(() => undefined);
				this.identity = { token, ino: stat?.ino ?? 0 };
				return;
			} catch (error) {
				const code = (error as NodeJS.ErrnoException).code;
				if (code === "EPERM" || code === "EACCES" || code === "EBUSY") {
					// Windows transient: an AV/indexer can briefly hold the file open.
					// Retry like ordinary contention instead of failing the operation.
					if (Date.now() > deadline) throw new Error("timed out acquiring the wake-alarm state lock");
					await this.sleep(10 + Math.floor(Math.random() * 25));
					continue;
				}
				if (code !== "EEXIST") throw error;
			}
			const now = Date.now();
			const observed = await this.observe();
			if (!observed) continue; // vanished between create attempt and read; retry
			if (this.isStale(observed, now)) {
				await this.hooks?.beforeTakeover?.();
				const victim = `${this.lockPath}.stale-${process.pid}-${token.slice(0, 8)}`;
				try { await fs.rename(this.lockPath, victim); }
				catch { continue; } // someone else already took it over; retry
				const victimStat = await fs.stat(victim).catch(() => undefined);
				if (!victimStat) {
					// Our rename reported success but the victim is gone: the stale lock was
					// concurrently claimed by another contender (Windows can resolve both
					// renames of the same source without error; only one victim survives).
					// Nothing of ours was displaced — just retry the loop; the winner will
					// publish its lock at `path`.
					continue;
				}
				const victimIno = victimStat.ino;
				const sameText = await fs.readFile(victim, "utf8").catch(() => undefined) === observed.text;
				// The victim is the stale lock we judged only if its identity matches what
				// we observed: SAME content AND SAME inode (when the filesystem provides
				// one). Some filesystems fabricate colliding inodes (e.g. WSL/DrvFS), so
				// the content check is load-bearing: an inode match alone must never
				// justify deleting a lock whose text differs — that text is a LIVE
				// successor's lock, and deleting it lets this contender acquire while the
				// real holder is still inside its critical section.
				const confirmedStale = sameText && (observed.ino === 0 || victimIno === 0 || victimIno === observed.ino);
				if (!confirmedStale) {
					// We displaced a live successor's lock, not the stale one we judged.
					// Restore it and never acquire from its emptied slot.
					await this.restoreVictim(victim);
					continue;
				}
				await fs.rm(victim, { force: true }).catch(() => undefined);
				await this.hooks?.afterTakeover?.();
				continue;
			}
			if (Date.now() > deadline) throw new Error("timed out acquiring the wake-alarm state lock");
			await this.sleep(25 + Math.floor(Math.random() * 50));
		}
	}

	/**
	 * Restore a displaced lock, retrying until the path is free, so a live holder is
	 * NEVER dispossessed: dropping a live lock is what lets a contender acquire from
	 * its emptied slot while the holder is still inside its critical section.
	 * Bounded: after 30s the victim is left orphaned (never deleted) and the acquire
	 * retry continues; the displaced holder detects the loss at its next verifyHeld().
	 */
	private async restoreVictim(victim: string): Promise<void> {
		const deadline = Date.now() + 30_000;
		for (;;) {
			if (Date.now() > deadline) return; // leave the victim orphaned; do not delete it
			try {
				await fs.link(victim, this.lockPath);
				await fs.rm(victim, { force: true });
				return;
			} catch (error) {
				const code = (error as NodeJS.ErrnoException).code;
				if (code === "ENOENT") {
					// The displaced lock vanished (concurrent rename); nothing to restore.
					return;
				}
				if (code === "EEXIST") {
					// A contender created a lock in the window; wait for it to release.
					await this.sleep(10 + Math.floor(Math.random() * 25));
					continue;
				}
				if (code === "EPERM" || code === "EACCES" || code === "ENOTSUP" || code === "EOPNOTSUPP") {
					// Filesystem without hard links: move back only when the path is free.
					const occupied = await fs.stat(this.lockPath).catch(() => undefined);
					if (!occupied) {
						try { await fs.rename(victim, this.lockPath); return; }
						catch { /* raced; retry */ }
					}
					await this.sleep(10 + Math.floor(Math.random() * 25));
					continue;
				}
				throw error;
			}
		}
	}

	/** Whether the lock file currently at `path` is still the exact one we acquired (token, and inode when the filesystem provides one). */	private isOurs(observed: LockObservation): boolean {
		if (!this.identity) return false;
		if (observed.parsed?.token !== this.identity.token) return false;
		if (observed.ino !== 0 && this.identity.ino !== 0 && observed.ino !== this.identity.ino) return false;
		return true;
	}

	/**
	 * Abort when the lock file at `path` is no longer the one we acquired.
	 * Call before every state write inside the critical section.
	 */
	async verifyHeld(): Promise<void> {
		if (!this.identity) throw new Error("state lock is not held");
		// A contender's takeover can briefly empty `path` while it restores a lock it
		// displaced (rename-then-verify-then-link). That is transient, not a takeover:
		// retry for a short window before declaring failure. A genuine takeover shows
		// up as a DIFFERENT token at `path` and aborts immediately.
		for (let attempt = 0; ; attempt++) {
			const observed = await this.observe();
			if (observed) {
				if (this.isOurs(observed)) return;
				throw new Error("wake-alarm state lock was taken over; operation aborted");
			}
			if (attempt >= 25) throw new Error("wake-alarm state lock was taken over; operation aborted");
			await this.sleep(2);
		}
	}

	async release(): Promise<void> {
		const identity = this.identity;
		this.identity = undefined;
		if (!identity) return;
		// Like verifyHeld: ride out the transient empty window of a contender's
		// takeover-restore; only remove the lock when it is still ours.
		for (let attempt = 0; ; attempt++) {
			const observed = await this.observe();
			if (observed) {
				if (observed.parsed?.token !== identity.token || (observed.ino !== 0 && identity.ino !== 0 && observed.ino !== identity.ino)) return;
				await fs.rm(this.lockPath, { force: true }).catch(() => undefined);
				return;
			}
			if (attempt >= 25) return;
			await this.sleep(2);
		}
	}
}
