import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import {
	DAEMON_HEARTBEAT_FRESH_MS,
	clearDaemonHeartbeat,
	daemonHeartbeatPath,
	readDaemonLiveness,
	writeDaemonHeartbeat,
	type DaemonHeartbeat,
} from "./presence.ts";

async function makeDir(): Promise<string> {
	return fs.mkdtemp(path.join(tmpdir(), "wake-presence-test-"));
}

function beat(overrides: Partial<DaemonHeartbeat> = {}): DaemonHeartbeat {
	return { version: 1, pid: process.pid, startedAt: 1_000, heartbeatAt: Date.now(), dryRun: false, logTail: ["line-a", "line-b"], ...overrides };
}

test("daemon heartbeat round-trips and liveness follows freshness", async () => {
	const dir = await makeDir();
	await writeDaemonHeartbeat(dir, beat());
	const fresh = await readDaemonLiveness(dir);
	assert.equal(fresh.live, true, "a just-written heartbeat is live");
	assert.equal(fresh.heartbeat?.pid, process.pid);
	assert.deepEqual(fresh.heartbeat?.logTail, ["line-a", "line-b"]);
	assert.ok((fresh.ageMs ?? Infinity) < DAEMON_HEARTBEAT_FRESH_MS);
	// A heartbeat older than the freshness window reads as dead, with age exposed.
	await writeDaemonHeartbeat(dir, beat({ heartbeatAt: Date.now() - DAEMON_HEARTBEAT_FRESH_MS - 5_000 }));
	const stale = await readDaemonLiveness(dir);
	assert.equal(stale.live, false);
	assert.ok((stale.ageMs ?? 0) > DAEMON_HEARTBEAT_FRESH_MS);
	assert.equal(stale.heartbeat?.logTail.length, 2, "the log tail survives for post-mortem");
	// Absent or garbage files degrade to "not live", never throw.
	await fs.rm(daemonHeartbeatPath(dir));
	assert.equal((await readDaemonLiveness(dir)).live, false);
	await fs.writeFile(daemonHeartbeatPath(dir), "{not json");
	assert.equal((await readDaemonLiveness(dir)).live, false);
});

test("clearDaemonHeartbeat removes only its own file", async () => {
	const dir = await makeDir();
	await writeDaemonHeartbeat(dir, beat({ pid: 4242 }));
	await clearDaemonHeartbeat(dir, 1111); // a different (newer) daemon's takeover must survive
	const kept = await readDaemonLiveness(dir);
	assert.equal(kept.live, true, "a fresh foreign heartbeat is untouched");
	assert.equal(kept.heartbeat?.pid, 4242);
	await clearDaemonHeartbeat(dir, 4242);
	await assert.rejects(fs.readFile(daemonHeartbeatPath(dir)), /ENOENT/, "own heartbeat removed on shutdown");
});
