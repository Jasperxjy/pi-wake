import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { formatPresenceFailure, PresenceIoError, registerPresence, type PresenceRecord } from "./presence.ts";

function deferred() {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => { resolve = done; });
	return { promise, resolve };
}

function record(instanceId: string, heartbeatAt = Date.now()): PresenceRecord {
	return { version: 1, pid: process.pid, instanceId, heartbeatAt };
}

test("same-instance registrations own separate temporary files under forced overlap", { timeout: 5_000 }, async (t) => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-overlap-"));
	const target = path.join(dir, "same.json");
	const bothRenaming = deferred();
	const rename = fs.rename;
	const sources: string[] = [];
	t.mock.method(fs, "rename", async (from: Parameters<typeof fs.rename>[0], to: Parameters<typeof fs.rename>[1]) => {
		if (to === target) {
			sources.push(String(from));
			if (sources.length === 2) bothRenaming.resolve();
			await bothRenaming.promise;
		}
		await rename(from, to);
	});
	try {
		const outcomes = await Promise.allSettled([registerPresence(dir, record("same", 1)), registerPresence(dir, record("same", 2))]);
		assert.deepEqual(outcomes.map((outcome) => outcome.status), ["fulfilled", "fulfilled"]);
		assert.equal(new Set(sources).size, 2, "each invocation owns its rename source");
		assert.ok([1, 2].includes(JSON.parse(await fs.readFile(target, "utf8")).heartbeatAt));
		assert.deepEqual(await fs.readdir(dir), ["same.json"]);
	} finally {
		bothRenaming.resolve();
		await fs.rm(dir, { recursive: true, force: true });
	}
});

for (const phase of ["mkdir", "open", "write", "close", "rename"] as const) {
	test(`registration preserves the original ${phase} error and cleans only owned files`, async (t) => {
		const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-failure-"));
		const target = path.join(dir, "one.json");
		await fs.writeFile(target, "previous complete record\n");
		const failure = Object.assign(new Error("sensitive /private/session.jsonl"), { code: "EIO" });
		const open = fs.open;
		let closes = 0;
		if (phase === "mkdir") t.mock.method(fs, "mkdir", async () => { throw failure; });
		if (phase === "open") t.mock.method(fs, "open", async () => { throw failure; });
		if (phase === "write" || phase === "close") t.mock.method(fs, "open", async (...args: Parameters<typeof fs.open>) => {
			const handle = await open(...args);
			const close = handle.close.bind(handle);
			t.mock.method(handle, "close", async () => {
				closes++;
				if (phase === "close" && closes === 1) throw failure;
				await close();
			});
			if (phase === "write") t.mock.method(handle, "writeFile", async () => { throw failure; });
			return handle;
		});
		if (phase === "rename") t.mock.method(fs, "rename", async () => { throw failure; });
		try {
			await assert.rejects(registerPresence(dir, record("one")), (error: unknown) => {
				assert.ok(error instanceof PresenceIoError);
				assert.equal(error.operation, phase);
				assert.equal(error.code, "EIO");
				assert.equal(error.cause, failure);
				return true;
			});
			assert.deepEqual(await fs.readdir(dir), ["one.json"]);
			assert.equal(await fs.readFile(target, "utf8"), "previous complete record\n");
			if (phase === "write" || phase === "close") assert.ok(closes >= 1, "opened handles are closed on failure");
		} finally { await fs.rm(dir, { recursive: true, force: true }); }
	});
}

test("exclusive-open EEXIST does not remove the file belonging to another writer", async (t) => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-eexist-"));
	let foreign: Parameters<typeof fs.open>[0] | undefined;
	t.mock.method(fs, "open", async (file: Parameters<typeof fs.open>[0]) => {
		foreign = file;
		await fs.writeFile(file, "foreign-owned");
		throw Object.assign(new Error("already exists"), { code: "EEXIST" });
	});
	try {
		await assert.rejects(registerPresence(dir, record("one")), { code: "EEXIST", operation: "open" });
		assert.ok(foreign);
		assert.equal(await fs.readFile(foreign, "utf8"), "foreign-owned");
	} finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test("one failed registration cannot clean up a concurrent writer's temporary file", { timeout: 5_000 }, async (t) => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-owner-"));
	const both = deferred();
	const resumeSecond = deferred();
	const rename = fs.rename;
	let count = 0;
	let secondSource: Parameters<typeof fs.rename>[0] | undefined;
	t.mock.method(fs, "rename", async (from: Parameters<typeof fs.rename>[0], to: Parameters<typeof fs.rename>[1]) => {
		const index = ++count;
		if (index === 2) { secondSource = from; both.resolve(); }
		await both.promise;
		if (index === 1) throw Object.assign(new Error("first writer failed"), { code: "EIO" });
		await resumeSecond.promise;
		await rename(from, to);
	});
	const first = registerPresence(dir, record("one", 1));
	const firstOutcome = first.then(() => undefined, (error: unknown) => error);
	const second = registerPresence(dir, record("one", 2));
	try {
		await both.promise;
		assert.ok(await firstOutcome instanceof PresenceIoError);
		assert.ok(secondSource);
		await fs.access(secondSource);
		resumeSecond.resolve();
		await second;
		assert.deepEqual(await fs.readdir(dir), ["one.json"]);
	} finally {
		both.resolve();
		resumeSecond.resolve();
		await Promise.allSettled([first, second]);
		await fs.rm(dir, { recursive: true, force: true });
	}
});

test("registration preserves restrictive modes and isolates different instance ids", { skip: process.platform === "win32" }, async () => {
	const root = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-modes-"));
	const dir = path.join(root, "registry");
	try {
		await Promise.all(Array.from({ length: 100 }, (_, index) => registerPresence(dir, record(`instance-${index}`))));
		assert.equal((await fs.readdir(dir)).length, 100);
		assert.equal((await fs.stat(dir)).mode & 0o777, 0o700);
		assert.equal((await fs.stat(path.join(dir, "instance-0.json"))).mode & 0o777, 0o600);
	} finally { await fs.rm(root, { recursive: true, force: true }); }
});

test("registration retains transient rename retries but does not retry ENOENT", async (t) => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-retry-"));
	const rename = fs.rename;
	let attempts = 0;
	let missing = false;
	t.mock.method(fs, "rename", async (from: Parameters<typeof fs.rename>[0], to: Parameters<typeof fs.rename>[1]) => {
		attempts++;
		if (missing) throw Object.assign(new Error("missing"), { code: "ENOENT" });
		if (attempts <= 3) throw Object.assign(new Error("transient"), { code: ["EPERM", "EACCES", "EBUSY"][attempts - 1] });
		await rename(from, to);
	});
	try {
		await registerPresence(dir, record("one"));
		assert.equal(attempts, 4);
		missing = true;
		attempts = 0;
		await assert.rejects(registerPresence(dir, record("one")), { operation: "rename", code: "ENOENT" });
		assert.equal(attempts, 1);
		assert.deepEqual(await fs.readdir(dir), ["one.json"]);
	} finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test("presence warnings expose operation and errno without paths, messages or control characters", () => {
	const source = Object.assign(new Error("token=secret /private/session.jsonl\u001b[31m"), { code: "ENOENT", path: "/private/source" });
	const missing = formatPresenceFailure(new PresenceIoError("rename", source), 1);
	assert.match(missing, /rename: ENOENT/);
	assert.match(missing, /temporary source or parent directory/);
	assert.doesNotMatch(missing, /secret|private|\u001b|check.*permissions/);
	assert.match(formatPresenceFailure(new PresenceIoError("open", Object.assign(new Error(), { code: "EACCES" })), 5), /open: EACCES.*5 consecutive.*permissions/);
	const list = formatPresenceFailure(new PresenceIoError("list", Object.assign(new Error(), { code: "EIO" })), 2);
	assert.match(list, /list: EIO/);
	assert.doesNotMatch(list, /cannot write|permissions/);
	assert.match(formatPresenceFailure({ code: "EIO\nsecret" }, 1), /refresh: UNKNOWN/);
	assert.doesNotMatch(formatPresenceFailure({ code: "EIO\nsecret" }, 1), /secret|\n/);
});

test("200 overlapping same-instance registrations all complete with one intact final file", { timeout: 10_000 }, async () => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-stress-"));
	try {
		const outcomes = await Promise.allSettled(Array.from({ length: 200 }, (_, index) => registerPresence(dir, record("same", index))));
		assert.ok(outcomes.every((outcome) => outcome.status === "fulfilled"));
		assert.deepEqual(await fs.readdir(dir), ["same.json"]);
		const final = JSON.parse(await fs.readFile(path.join(dir, "same.json"), "utf8"));
		assert.equal(final.instanceId, "same");
		assert.ok(Number.isInteger(final.heartbeatAt) && final.heartbeatAt >= 0 && final.heartbeatAt < 200);
	} finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test("a cleanup error does not mask the original rename failure", async (t) => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-presence-cleanup-"));
	const failure = Object.assign(new Error("rename failed"), { code: "EIO" });
	t.mock.method(fs, "rename", async () => { throw failure; });
	const cleanup = t.mock.method(fs, "rm", async () => { throw Object.assign(new Error("cleanup failed"), { code: "EACCES" }); });
	try {
		await assert.rejects(registerPresence(dir, record("one")), (error: unknown) => {
			assert.ok(error instanceof PresenceIoError);
			assert.equal(error.code, "EIO");
			assert.equal(error.cause, failure);
			return true;
		});
	} finally {
		cleanup.mock.restore();
		await fs.rm(dir, { recursive: true, force: true });
	}
});
