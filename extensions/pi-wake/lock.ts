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
 *     stale to a private victim name and then verifies the victim's inode. The
 *     rename detaches whatever currently sits at `path`; if the inode does not
 *     match the one that was judged stale, a live successor's lock was displaced
 *     and is restored (hard link) instead of deleted. This closes the classic
 *     stat-rm-acquire TOCTOU where two contenders both "recover" the same dead
 *     lock and the second one deletes the first one's fresh lock.
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
				if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
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
				const victimIno = victimStat?.ino ?? 0;
				const sameInode = observed.ino === 0 || victimIno === 0 ? undefined : victimIno === observed.ino;
				const sameText = await fs.readFile(victim, "utf8").catch(() => undefined) === observed.text;
				if (sameInode === false || (sameInode === undefined && !sameText)) {
					// We displaced a live successor's lock, not the stale one we judged.
					// Restore it (hard link fails cleanly if a newer lock already exists).
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

	/** Best-effort restore of a displaced live lock: hard link back, else rename back when the path is free, else drop it. */
	private async restoreVictim(victim: string): Promise<void> {
		try {
			await fs.link(victim, this.lockPath);
			await fs.rm(victim, { force: true });
			return;
		} catch (error) {
			const code = (error as NodeJS.ErrnoException).code;
			if (code !== "EEXIST" && code !== "EPERM" && code !== "EACCES" && code !== "ENOTSUP" && code !== "EOPNOTSUPP") {
				// Last resort: move it back only when nothing else acquired in the meantime.
				const occupied = await fs.stat(this.lockPath).catch(() => undefined);
				if (!occupied) await fs.rename(victim, this.lockPath).catch(() => undefined);
				return;
			}
		}
		// A successor lock already exists; drop the displaced one. Its holder detects
		// the loss at the next verifyHeld() and aborts its write.
		await fs.rm(victim, { force: true }).catch(() => undefined);
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
		const observed = await this.observe();
		if (!observed || !this.isOurs(observed)) throw new Error("wake-alarm state lock was taken over; operation aborted");
	}

	async release(): Promise<void> {
		const identity = this.identity;
		this.identity = undefined;
		if (!identity) return;
		const observed = await this.observe();
		if (!observed || observed.parsed?.token !== identity.token || (observed.ino !== 0 && identity.ino !== 0 && observed.ino !== identity.ino)) return;
		await fs.rm(this.lockPath, { force: true }).catch(() => undefined);
	}
}
