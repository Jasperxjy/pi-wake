import test from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { compareVersions } from "./daemon.ts";
import { readDaemonLiveness } from "./presence.ts";

/**
 * Daemon lifecycle tests (0.2.5): the four ways a daemon must END, so the
 * 2026-09 incident (16 immortal daemons, one wedging a project on stale code)
 * cannot recur. Each test spawns real daemon processes from source.
 */
const daemonEntry = fileURLToPath(new URL("./daemon.ts", import.meta.url));

function startDaemon(extraEnv: NodeJS.ProcessEnv, options: { cwd?: string } = {}): ChildProcess {
	return spawn(process.execPath, [daemonEntry], {
		cwd: options.cwd,
		env: { ...process.env, WAKE_ALARM_SPAWN_DRY_RUN: "1", ...extraEnv },
		stdio: ["ignore", "pipe", "pipe"],
		windowsHide: true,
	});
}

async function makeDir(): Promise<string> {
	return fs.mkdtemp(path.join(tmpdir(), "pi-wake-dlife-"));
}

async function waitFor(condition: () => boolean | Promise<boolean>, label: string, timeoutMs = 20_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await condition()) return;
		await new Promise((resolve) => setTimeout(resolve, 200));
	}
	throw new Error(`timed out waiting for ${label}`);
}

function exitOf(child: ChildProcess): Promise<number | null> {
	return new Promise((resolve) => {
		if (child.exitCode !== null) resolve(child.exitCode);
		else child.once("close", (code) => resolve(code));
	});
}

test("compareVersions orders releases, pre-releases, and unknown cores", () => {
	assert.ok(compareVersions("0.2.4", "0.2.2") > 0);
	assert.ok(compareVersions("0.2.2", "0.2.4") < 0);
	assert.ok(compareVersions("1.0.0", "1.0.0-beta") > 0, "a pre-release sorts below its release");
	assert.ok(compareVersions("0.10.0", "0.9.9") > 0, "numeric, not lexicographic");
	assert.ok(compareVersions("0.2.4", "0.2.4") === 0);
	assert.ok(compareVersions("0.2.4.1", "0.2.4") > 0, "longer cores pad with zero");
});

test("a newer daemon takes the role over from an older one", { timeout: 90_000 }, async () => {
	const dir = await makeDir();
	const first = startDaemon({ WAKE_ALARM_CWD: dir, WAKE_ALARM_PKG_VERSION: "0.2.2" });
	try {
		await waitFor(async () => (await readDaemonLiveness(dir)).heartbeat?.pkgVersion === "0.2.2", "daemon A to claim with its code version");
		const second = startDaemon({ WAKE_ALARM_CWD: dir, WAKE_ALARM_PKG_VERSION: "9.9.9" });
		try {
			// The takeover signals the older daemon; the exit code differs by
			// platform (graceful SIGTERM shutdown vs Windows hard kill) — only
			// the END matters here.
			await Promise.race([exitOf(first), new Promise<never>((_, reject) => setTimeout(() => reject(new Error("daemon A did not exit after takeover")), 30_000))]);
			await waitFor(async () => {
				const liveness = await readDaemonLiveness(dir);
				return liveness.live && liveness.heartbeat?.pid === second.pid && liveness.heartbeat?.pkgVersion === "9.9.9";
			}, "daemon B to own the heartbeat");
		} finally {
			second.kill();
			await exitOf(second);
		}
	} finally {
		first.kill();
		await exitOf(first);
	}
});

test("a degraded daemon is replaced: it yields and the replacement activates", { timeout: 90_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, ".pi", "wake-alarm.state.json");
	await fs.mkdir(path.dirname(statePath), { recursive: true });
	// A record no version can restore: the daemon must degrade, not wedge.
	await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms: [{ id: "bad", kind: "nonsense" }], outbox: [] })}\n`);
	const env = { WAKE_ALARM_CWD: dir, WAKE_ALARM_PKG_VERSION: "0.2.5" };
	const first = startDaemon(env);
	try {
		await waitFor(async () => {
			const liveness = await readDaemonLiveness(dir);
			return liveness.live && Boolean(liveness.heartbeat?.degraded);
		}, "daemon A to report itself degraded", 30_000);
		// The state becomes readable again (e.g. an upgraded daemon wrote it);
		// the replacement must activate cleanly and take over the role.
		await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms: [], outbox: [] })}\n`);
		const second = startDaemon(env);
		try {
			await Promise.race([exitOf(first), new Promise<never>((_, reject) => setTimeout(() => reject(new Error("degraded daemon A did not yield")), 30_000))]);
			await waitFor(async () => {
				const liveness = await readDaemonLiveness(dir);
				return liveness.live && liveness.heartbeat?.pid === second.pid && !liveness.heartbeat?.degraded;
			}, "daemon B to own a healthy heartbeat");
		} finally {
			second.kill();
			await exitOf(second);
		}
	} finally {
		first.kill();
		await exitOf(first);
	}
});

test("an idle daemon (no active alarms, no sessions) exits and clears its heartbeat", { timeout: 90_000 }, async () => {
	const dir = await makeDir();
	const statePath = path.join(dir, ".pi", "wake-alarm.state.json");
	await fs.mkdir(path.dirname(statePath), { recursive: true });
	await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms: [], outbox: [] })}\n`);
	const child = startDaemon({ WAKE_ALARM_CWD: dir, WAKE_ALARM_STATE_PATH: statePath, WAKE_ALARM_IDLE_EXIT_MS: "1500" });
	const code = await Promise.race([
		exitOf(child),
		new Promise<never>((_, reject) => setTimeout(() => reject(new Error("idle daemon did not exit within 30s")), 30_000)),
	]);
	assert.equal(code, 0, "idle exit is graceful");
	// Graceful shutdown removes its own heartbeat: nothing may look live afterwards.
	assert.equal((await readDaemonLiveness(dir)).live, false);
});

test("a daemon whose project directory vanished exits instead of orphaning", { timeout: 90_000 }, async () => {
	const parent = await makeDir();
	const project = path.join(parent, "project");
	const statePath = path.join(project, ".pi", "wake-alarm.state.json");
	await fs.mkdir(path.dirname(statePath), { recursive: true });
	await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms: [], outbox: [] })}\n`);
	// Spawn with the PARENT as process cwd: Windows refuses to delete a
	// directory some process holds as its own cwd, so the daemon must reach the
	// project only via WAKE_ALARM_CWD (exactly how orphaned daemons behave).
	const child = startDaemon({ WAKE_ALARM_CWD: project, WAKE_ALARM_IDLE_EXIT_MS: "600000" }, { cwd: parent });
	try {
		await waitFor(async () => (await readDaemonLiveness(project)).live, "the daemon to claim the project");
		let removed = false;
		for (let attempt = 0; attempt < 10 && !removed; attempt++) {
			try { await fs.rm(project, { recursive: true, force: true }); removed = true; }
			catch { await new Promise((resolve) => setTimeout(resolve, 300)); }
		}
		assert.ok(removed, "the project directory could be removed while the daemon runs");
		const code = await Promise.race([
			exitOf(child),
			new Promise<never>((_, reject) => setTimeout(() => reject(new Error("daemon did not exit after its project vanished")), 30_000)),
		]);
		assert.equal(code, 0, "project-gone exit is graceful");
	} finally {
		child.kill();
		await exitOf(child);
	}
});
