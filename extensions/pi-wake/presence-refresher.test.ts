import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { createPresenceRefresher, releasePresence, type PresenceRecord, type PresenceRefresher } from "./presence.ts";

function deferred() {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => { resolve = done; });
	return { promise, resolve };
}
function record(instanceId: string, heartbeatAt = Date.now()): PresenceRecord {
	return { version: 1, pid: process.pid, instanceId, heartbeatAt };
}

test("refresher serializes A/B/C as A/C and snapshots the latest pending record", { timeout: 5_000 }, async (t) => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-refresher-latest-"));
	const entered = [deferred(), deferred()];
	const resume = [deferred(), deferred()];
	const writes: number[] = [];
	let running = 0;
	let peak = 0;
	const rename = fs.rename;
	t.mock.method(fs, "rename", async (from: Parameters<typeof fs.rename>[0], to: Parameters<typeof fs.rename>[1]) => {
		running++;
		peak = Math.max(peak, running);
		const slot = writes.length;
		writes.push(JSON.parse(await fs.readFile(from, "utf8")).heartbeatAt);
		entered[slot]?.resolve();
		await resume[slot]?.promise;
		try { await rename(from, to); } finally { running--; }
	});
	const refresher = createPresenceRefresher(dir);
	const base = Date.now();
	let drain: Promise<void> | undefined;
	try {
		drain = refresher.request(record("one", base));
		await entered[0].promise;
		assert.equal(refresher.request(record("one", base + 1)), drain);
		// Arrival order, not the wall clock, defines the latest request.
		const latest = record("one", base - 1);
		assert.equal(refresher.request(latest), drain);
		latest.heartbeatAt = base + 999;
		resume[0].resolve();
		await entered[1].promise;
		let settled = false;
		void drain.then(() => { settled = true; });
		await Promise.resolve();
		assert.equal(settled, false, "the shared drain includes the latest pending write");
		assert.deepEqual(writes, [base, base - 1]);
		resume[1].resolve();
		await drain;
		assert.equal(peak, 1);
		assert.equal(JSON.parse(await fs.readFile(path.join(dir, "one.json"), "utf8")).heartbeatAt, base - 1);
	} finally {
		for (const gate of resume) gate.resolve();
		await refresher.close();
		await drain;
		await fs.rm(dir, { recursive: true, force: true });
	}
});

for (const failFirst of [false, true]) {
	test(`refresher preserves synchronous ${failFirst ? "failure" : "success"} callback reentry in the same drain`, { timeout: 5_000 }, async (t) => {
		const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-refresher-reentry-"));
		const secondEntered = deferred();
		const resume = deferred();
		const writes: number[] = [];
		const base = Date.now();
		let childDrain: Promise<void> | undefined;
		let refresher: PresenceRefresher;
		let errors = 0;
		const reenter = () => { if (writes.length === 1) childDrain = refresher.request(record("one", base + 1)); };
		refresher = createPresenceRefresher(dir, { onLive: reenter, onError: () => { errors++; reenter(); } });
		const rename = fs.rename;
		t.mock.method(fs, "rename", async (from: Parameters<typeof fs.rename>[0], to: Parameters<typeof fs.rename>[1]) => {
			writes.push(JSON.parse(await fs.readFile(from, "utf8")).heartbeatAt);
			if (writes.length === 1 && failFirst) throw Object.assign(new Error("injected"), { code: "EIO" });
			if (writes.length === 2) { secondEntered.resolve(); await resume.promise; }
			await rename(from, to);
		});
		const drain = refresher.request(record("one", base));
		try {
			await secondEntered.promise;
			assert.equal(childDrain, drain);
			let settled = false;
			void drain.then(() => { settled = true; });
			await Promise.resolve();
			assert.equal(settled, false);
			resume.resolve();
			await drain;
			assert.deepEqual(writes, [base, base + 1]);
			assert.equal(errors, failFirst ? 1 : 0);
		} finally {
			resume.resolve();
			await refresher.close();
			await fs.rm(dir, { recursive: true, force: true });
		}
	});
}

for (const phase of ["rename", "list"] as const) {
	test(`close drains ${phase}, discards pending requests and suppresses late callbacks`, { timeout: 5_000 }, async (t) => {
		const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-refresher-close-"));
		const entered = deferred();
		const resume = deferred();
		const callbacks: unknown[] = [];
		let writes = 0;
		const rename = fs.rename;
		const readdir = fs.readdir;
		t.mock.method(fs, "rename", async (from: Parameters<typeof fs.rename>[0], to: Parameters<typeof fs.rename>[1]) => {
			writes++;
			if (phase === "rename") { entered.resolve(); await resume.promise; }
			await rename(from, to);
		});
		if (phase === "list") t.mock.method(fs, "readdir", async (...args: Parameters<typeof fs.readdir>) => {
			entered.resolve();
			await resume.promise;
			throw Object.assign(new Error("late list failure"), { code: "EIO" });
		});
		const refresher = createPresenceRefresher(dir, { onLive: (live) => callbacks.push(live), onError: (error) => callbacks.push(error) });
		const drain = refresher.request(record("one"));
		try {
			await entered.promise;
			refresher.request(record("one", Date.now() + 1));
			const closing = refresher.close();
			assert.equal(closing, drain);
			assert.equal(refresher.close(), closing);
			await refresher.request(record("one", Date.now() + 2));
			resume.resolve();
			await closing;
			assert.equal(writes, 1);
			assert.deepEqual(callbacks, []);
			await releasePresence(dir, "one");
			assert.deepEqual(await readdir(dir), []);
		} finally {
			resume.resolve();
			await refresher.close();
			await fs.rm(dir, { recursive: true, force: true });
		}
	});
}

test("different instance refreshers advance independently and a failed drain can recover", { timeout: 5_000 }, async (t) => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-refresher-isolation-"));
	const entered = deferred();
	const resume = deferred();
	const errors: unknown[] = [];
	const a = createPresenceRefresher(dir);
	const b = createPresenceRefresher(dir, { onError: (error) => errors.push(error) });
	const rename = fs.rename;
	let failB = true;
	t.mock.method(fs, "rename", async (from: Parameters<typeof fs.rename>[0], to: Parameters<typeof fs.rename>[1]) => {
		if (to === path.join(dir, "a.json")) { entered.resolve(); await resume.promise; }
		if (to === path.join(dir, "b.json") && failB) { failB = false; throw Object.assign(new Error("injected"), { code: "EIO" }); }
		await rename(from, to);
	});
	const drainA = a.request(record("a"));
	try {
		await entered.promise;
		await b.request(record("b"));
		assert.equal(errors.length, 1);
		await b.request(record("b"));
		assert.equal(JSON.parse(await fs.readFile(path.join(dir, "b.json"), "utf8")).instanceId, "b");
		resume.resolve();
		await drainA;
	} finally {
		resume.resolve();
		await Promise.all([a.close(), b.close()]);
		await fs.rm(dir, { recursive: true, force: true });
	}
});
