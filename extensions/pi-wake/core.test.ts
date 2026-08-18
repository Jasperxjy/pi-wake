import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import {
	MAX_TIMER_DELAY_MS,
	applyBaseline,
	applyCheckFailure,
	applyContainerDeadline,
	applyProbe,
	applyTimer,
	buildResumeArgs,
	createContainerAlarm,
	createOutboxEntry,
	createTimerAlarm,
	deadlineAfter,
	fileReadWindow,
	leaseIsAlive,
	nextAlarmDueAt,
	parseAbsoluteTime,
	parseDuration,
	restoreAlarmState,
	restoreOutbox,
	resumeAlarm,
	scanNewLog,
	timerDelay,
	validateAlarmId,
	validateAlarmName,
	validateContainer,
	validateContainerEvents,
	validateHost,
	validateOwnerSessionFile,
	validatePollingDuration,
	validateRemoteLogPath,
	validateRemoteLogRoots,
	validateUser,
	wakeMessage,
	type ContainerAlarmState,
	type FiredEvent,
	type OutboxEntry,
	type ProbeResult,
} from "./core.ts";

const encoder = new TextEncoder();
const allowedRoots = ["/data/probes/"];

function runningProbe(overrides: Partial<ProbeResult> = {}): ProbeResult {
	return {
		exists: true,
		containerId: "a".repeat(64),
		running: true,
		status: "running",
		containerStatus: "running",
		startedAt: "2026-01-01T00:00:00.000Z",
		exitCode: 0,
		oomKilled: false,
		logMode: "application-file",
		selectedLogPath: "/data/probes/data/run.log",
		logFileId: "1:100",
		logOffset: 0,
		logBytes: new Uint8Array(),
		tail: "healthy",
		...overrides,
	};
}

function makeContainer(events: string[], overrides: Partial<Parameters<typeof createContainerAlarm>[0]> = {}): ContainerAlarmState {
	return createContainerAlarm({ id: "watch", name: "Named condition", container: "job", events, now: 1000, statusPollMs: 1000, ...overrides });
}

test("durations, absolute times, deadlines, and Node timer bounds are validated", () => {
	assert.equal(parseDuration("30m"), 1_800_000);
	assert.equal(parseDuration("1.5h"), 5_400_000);
	assert.equal(parseAbsoluteTime("2026-01-01T00:00:00Z"), Date.parse("2026-01-01T00:00:00Z"));
	for (const bad of ["30", "0s", "-1m", "forever", 0, 1.5]) assert.throws(() => parseDuration(bad));
	assert.throws(() => parseAbsoluteTime("tomorrow sometime"));
	assert.equal(validatePollingDuration(MAX_TIMER_DELAY_MS), MAX_TIMER_DELAY_MS);
	assert.throws(() => validatePollingDuration(MAX_TIMER_DELAY_MS + 1));
	assert.equal(timerDelay(100, 200), 0);
	assert.equal(timerDelay(MAX_TIMER_DELAY_MS + 100, 0), MAX_TIMER_DELAY_MS);
	assert.equal(deadlineAfter(100, 50), 150);
	assert.throws(() => deadlineAfter(Number.MAX_SAFE_INTEGER - 10, 20));
});

test("one-shot timer fires only when due and never creates a periodic wake", () => {
	const timer = createTimerAlarm({ id: "pomodoro", name: "Half hour elapsed", now: 1000, afterMs: 1800 });
	assert.equal(timer.dueAt, 2800);
	assert.equal(nextAlarmDueAt(timer), 2800);
	assert.deepEqual(applyTimer(timer, 2799).events, []);
	const fired = applyTimer(timer, 2800);
	assert.deepEqual(fired.events.map((event) => event.kind), ["timer"]);
	assert.equal(fired.state.active, false);
	assert.equal(fired.state.triggeredAt, 2800);
	assert.equal(nextAlarmDueAt(fired.state), undefined);
	assert.deepEqual(applyTimer(fired.state, 9999).events, []);
	assert.throws(() => resumeAlarm(fired.state, 10_000), /reset/);
	assert.throws(() => createTimerAlarm({ id: "bad", name: "Bad", now: 0 }));
	assert.throws(() => createTimerAlarm({ id: "bad", name: "Bad", now: 0, afterMs: 1, at: 2 }));
});

test("identifiers, names, host, user, events, and remote paths reject injection", () => {
	assert.equal(validateContainer("train_job-1.2"), "train_job-1.2");
	assert.equal(validateAlarmId("alarm_1"), "alarm_1");
	assert.equal(validateAlarmName("Check experiment"), "Check experiment");
	assert.equal(validateHost("192.168.50.213"), "192.168.50.213");
	assert.equal(validateUser("testuser"), "testuser");
	assert.deepEqual(validateContainerEvents(["exit", "log-error", "connection-failure"]), ["exit", "log-error", "connection-failure"]);
	assert.deepEqual(validateRemoteLogRoots(allowedRoots), allowedRoots);
	assert.equal(validateRemoteLogPath("/data/probes/data/run.log", allowedRoots), "/data/probes/data/run.log");
	for (const bad of ["job;docker stop x", "$(id)", "job/name", " job name "]) assert.throws(() => validateContainer(bad));
	assert.throws(() => validateAlarmName("bad\nname"));
	assert.throws(() => validateContainerEvents(["exit", "exit"]));
	assert.throws(() => validateContainerEvents(["unknown"]));
	for (const bad of ["relative.log", "/data/probes/../secret", "/etc/passwd"]) assert.throws(() => validateRemoteLogPath(bad, allowedRoots));
	assert.throws(() => validateHost("host;reboot"));
	assert.throws(() => validateUser("user@host"));
});

test("container configuration requires matching literal/deadline parameters", () => {
	assert.throws(() => makeContainer(["log-match"]), /logPattern/);
	assert.throws(() => makeContainer(["exit"], { logPattern: "DONE" }), /log-match/);
	assert.throws(() => makeContainer(["deadline"]), /deadline/);
	assert.throws(() => makeContainer(["exit"], { deadlineMs: 10 }), /deadline/);
	const configured = makeContainer(["log-match", "deadline"], { logPattern: "DONE", deadlineMs: 5000, policy: "keep" });
	assert.equal(configured.deadlineAt, 6000);
	assert.equal(configured.policy, "keep");
});

test("baseline advances logs without firing historical error or literal matches", () => {
	const alarm = makeContainer(["log-error", "log-match"], { logPattern: "DONE" });
	const baseline = applyBaseline(alarm, runningProbe({ logOffset: 100, logBytes: encoder.encode("old RuntimeError: x\nold DONE\n") }), 2000);
	assert.equal(baseline.logOffset, 100);
	assert.deepEqual(baseline.eventFingerprints, {});
	const noAppend = applyProbe(baseline, runningProbe({ logOffset: 100, logBytes: new Uint8Array() }), 3000);
	assert.deepEqual(noAppend.events, []);
});

test("new log scanning catches split Traceback and split literal without carry retriggers", () => {
	const traceFirst = scanNewLog("", encoder.encode("progress\nTrace"), "DONE");
	assert.equal(traceFirst.hasError, false);
	const traceSecond = scanNewLog(traceFirst.carry, encoder.encode("back (most recent call last):\n"), "DONE");
	assert.equal(traceSecond.hasError, true);
	const literalFirst = scanNewLog(traceSecond.carry, encoder.encode("progress\nDO"), "DONE");
	assert.equal(literalFirst.hasLiteral, false);
	const literalSecond = scanNewLog(literalFirst.carry, encoder.encode("NE\n"), "DONE");
	assert.equal(literalSecond.hasLiteral, true);
	const third = scanNewLog(literalSecond.carry, encoder.encode("ordinary line\n"), "DONE");
	assert.equal(third.hasError, false);
	assert.equal(third.hasLiteral, false);
	assert.equal(scanNewLog("", encoder.encode("metric_error_rate=1"), "DONE").hasError, false);
});

test("event filtering distinguishes normal exit from abnormal/nonzero/OOM", () => {
	const normalOnly = applyBaseline(makeContainer(["exit"]), runningProbe(), 1000);
	const clean = applyProbe(normalOnly, runningProbe({ running: false, status: "exited", containerStatus: "exited", exitCode: 0 }), 2000);
	assert.deepEqual(clean.events.map((event) => event.kind), ["exit"]);
	const abnormalIgnored = applyProbe(applyBaseline(makeContainer(["exit"]), runningProbe(), 1000), runningProbe({ running: false, status: "exited", containerStatus: "exited", exitCode: 2 }), 2000);
	assert.deepEqual(abnormalIgnored.events, []);
	const abnormal = applyProbe(applyBaseline(makeContainer(["abnormal"]), runningProbe(), 1000), runningProbe({ running: false, status: "exited", containerStatus: "exited", exitCode: 137, oomKilled: true }), 2000);
	assert.deepEqual(abnormal.events.map((event) => event.kind), ["abnormal"]);
	const unknownExitCode = applyProbe(applyBaseline(makeContainer(["exit"]), runningProbe(), 1000), runningProbe({ running: false, status: "exited", containerStatus: "exited", exitCode: undefined }), 2000);
	assert.deepEqual(unknownExitCode.events, [], "an unknown exit code must not be reported as a clean exit");
	for (const status of ["paused", "created", "removing"]) {
		const notExited = applyProbe(applyBaseline(makeContainer(["exit"]), runningProbe(), 1000), runningProbe({ running: false, status, containerStatus: status }), 2000);
		assert.deepEqual(notExited.events, [], `${status} is not a clean exit`);
	}
});

test("pause policy pauses after a trigger while keep policy dedupes stable terminal state", () => {
	const pausedBase = applyBaseline(makeContainer(["exit"]), runningProbe(), 1000);
	const paused = applyProbe(pausedBase, runningProbe({ running: false, status: "exited", containerStatus: "exited" }), 2000);
	assert.equal(paused.state.active, false);
	assert.match(paused.state.pauseReason ?? "", /triggered: exit/);
	const resumed = resumeAlarm(paused.state, 3000) as ContainerAlarmState;
	const stableAfterResume = applyProbe(resumed, runningProbe({ running: false, status: "exited", containerStatus: "exited" }), 3000);
	assert.deepEqual(stableAfterResume.events, []);

	const keepBase = applyBaseline(makeContainer(["exit"], { policy: "keep" }), runningProbe(), 1000);
	const firstExit = applyProbe(keepBase, runningProbe({ running: false, status: "exited", containerStatus: "exited" }), 2000);
	assert.equal(firstExit.state.active, true);
	assert.deepEqual(firstExit.events.map((event) => event.kind), ["exit"]);
	assert.deepEqual(applyProbe(firstExit.state, runningProbe({ running: false, status: "exited", containerStatus: "exited" }), 3000).events, []);
	const healthyAgain = applyProbe(firstExit.state, runningProbe({ startedAt: "2026-01-02T00:00:00Z" }), 4000);
	const secondExit = applyProbe(healthyAgain.state, runningProbe({ running: false, status: "exited", containerStatus: "exited", startedAt: "2026-01-02T00:00:00Z" }), 5000);
	assert.deepEqual(secondExit.events.map((event) => event.kind), ["exit"]);
});

test("container deadline is independently schedulable and fires once without a remote probe", () => {
	const alarm = makeContainer(["deadline"], { deadlineMs: 5000 });
	assert.equal(nextAlarmDueAt(alarm), 1000, "initial baseline/check remains immediately due");
	const baselined = applyBaseline(alarm, runningProbe(), 1000);
	assert.equal(nextAlarmDueAt(baselined), 2000, "poll precedes the deadline");
	const before = { ...baselined, nextCheckAt: 9000 };
	assert.equal(nextAlarmDueAt(before), 6000, "deadline schedules independently of the next remote poll");
	assert.deepEqual(applyContainerDeadline(before, 5999).events, []);
	const fired = applyContainerDeadline(before, 6000);
	assert.deepEqual(fired.events.map((event) => event.kind), ["deadline"]);
	assert.equal(fired.state.active, false);
	assert.deepEqual(applyContainerDeadline(fired.state, 7000).events, []);
});

test("missing, replaced, deadline, literal, and error conditions are OR-combined and deduped", () => {
	const alarm = applyBaseline(makeContainer(["missing", "replaced", "deadline", "log-error", "log-match"], { policy: "keep", logPattern: "READY", deadlineMs: 5000 }), runningProbe(), 1000);
	const logsAndDeadline = applyProbe(alarm, runningProbe({ logOffset: 30, logBytes: encoder.encode("READY\nValueError: bad\n") }), 6000);
	assert.deepEqual(logsAndDeadline.events.map((event) => event.kind).sort(), ["deadline", "log-error", "log-match"]);
	assert.deepEqual(applyProbe(logsAndDeadline.state, runningProbe({ logOffset: 30 }), 7000).events, []);
	const missing = applyProbe(logsAndDeadline.state, runningProbe({ exists: false, running: false, status: "missing", containerStatus: "missing" }), 8000);
	assert.deepEqual(missing.events.map((event) => event.kind), ["missing"]);
	assert.deepEqual(applyProbe(missing.state, runningProbe({ exists: false, running: false, status: "missing", containerStatus: "missing" }), 9000).events, []);
	const replaced = applyProbe(missing.state, runningProbe({ exists: false, running: false, status: "replaced", containerStatus: "replaced", containerId: "b".repeat(64) }), 10_000);
	assert.deepEqual(replaced.events.map((event) => event.kind), ["replaced"]);
});

test("reset/rebaseline can rebind a replacement and clears event dedupe", () => {
	const original = applyBaseline(makeContainer(["replaced", "log-error"], { policy: "keep" }), runningProbe({ containerId: "a".repeat(64), logOffset: 20 }), 1000);
	const replaced = applyProbe(original, runningProbe({ exists: false, running: false, status: "replaced", containerStatus: "replaced", containerId: "b".repeat(64), logOffset: 20 }), 2000);
	assert.deepEqual(replaced.events.map((event) => event.kind), ["replaced"]);
	const fresh = makeContainer(["replaced", "log-error"], { policy: "keep", now: 3000 });
	const rebound = applyBaseline(fresh, runningProbe({ containerId: "b".repeat(64), logOffset: 40, logBytes: encoder.encode("historical Error: ignored") }), 3000);
	assert.equal(rebound.containerId, "b".repeat(64));
	assert.deepEqual(rebound.eventFingerprints, {});
	assert.deepEqual(applyProbe(rebound, runningProbe({ containerId: "b".repeat(64), logOffset: 40 }), 4000).events, []);
});

test("bounded connection failures wake only when configured and fire once per outage", () => {
	const unconfigured = applyCheckFailure(applyCheckFailure(makeContainer(["exit"]), 2000, 2, "offline").state, 3000, 2, "offline");
	assert.deepEqual(unconfigured.events, []);
	assert.equal(unconfigured.state.active, true);
	assert.equal(unconfigured.state.consecutiveFailures, 2, "unconfigured failures remain bounded while polling can continue");
	const pauseAlarm = makeContainer(["exit", "connection-failure"]);
	const one = applyCheckFailure(pauseAlarm, 2000, 3, "offline");
	const two = applyCheckFailure(one.state, 3000, 3, "offline");
	const three = applyCheckFailure(two.state, 4000, 3, "offline");
	assert.deepEqual(one.events, []);
	assert.deepEqual(two.events, []);
	assert.deepEqual(three.events.map((event) => event.kind), ["connection-failure"]);
	assert.equal(three.state.active, false);
	const keep = makeContainer(["exit", "connection-failure"], { policy: "keep" });
	const k1 = applyCheckFailure(keep, 2000, 1, "offline");
	assert.equal(k1.state.active, true);
	assert.deepEqual(k1.events.map((event) => event.kind), ["connection-failure"]);
	assert.deepEqual(applyCheckFailure(k1.state, 3000, 1, "offline").events, []);
	const recovered = applyProbe(k1.state, runningProbe(), 4000);
	assert.equal(recovered.state.failureNotified, false);
	assert.deepEqual(applyCheckFailure(recovered.state, 5000, 1, "offline again").events.map((event) => event.kind), ["connection-failure"]);
});

test("file identity replacement resets carry and adopts the new device/inode", () => {
	const alarm = applyBaseline(makeContainer(["log-error"], { policy: "keep" }), runningProbe({ logFileId: "1:100", logOffset: 5, logBytes: encoder.encode("Trace") }), 1000);
	const rotated = applyProbe(alarm, runningProbe({ logFileId: "1:200", logReset: true, logOffset: 10, logBytes: encoder.encode("back only\n") }), 2000);
	assert.equal(rotated.state.logFileId, "1:200");
	assert.deepEqual(rotated.events, [], "old-file carry must not combine with a replacement file");
});

test("file windows cover baseline, append, and truncation", () => {
	assert.deepEqual(fileReadWindow(100, 0, true, 20), { start: 80, nextOffset: 100, reset: false });
	assert.deepEqual(fileReadWindow(125, 100, false, 20), { start: 100, nextOffset: 100, reset: false });
	assert.deepEqual(fileReadWindow(12, 125, false, 20), { start: 0, nextOffset: 0, reset: true });
});

test("strict restoration accepts valid timer/container states and rejects malformed or legacy fields", () => {
	const timer = JSON.parse(JSON.stringify(createTimerAlarm({ id: "timer", name: "Timer", now: 1000, afterMs: 5000 })));
	assert.equal(restoreAlarmState(timer, allowedRoots).kind, "timer");
	// An alarm must no longer carry an embedded pendingWake: wakes live in the outbox.
	const legacyPendingTimer = { ...timer, pendingWake: { triggeredAt: 6000, events: [{ kind: "timer", fingerprint: "timer:timer:6000" }] } };
	assert.throws(() => restoreAlarmState(legacyPendingTimer, allowedRoots), /unknown alarm field.*pendingWake/);
	const valid = JSON.parse(JSON.stringify(applyBaseline(makeContainer(["log-match", "deadline"], { logPattern: "DONE", deadlineMs: 5000, logPath: "/data/probes/data/run.log", allowedRemoteLogRoots: allowedRoots }), runningProbe({ logOffset: 10 }), 1000)));
	assert.equal(restoreAlarmState(valid, allowedRoots).kind, "container");
	for (const mutation of [
		(value: Record<string, unknown>) => { delete value.nextCheckAt; },
		(value: Record<string, unknown>) => { value.statusPollMs = MAX_TIMER_DELAY_MS + 1; },
		(value: Record<string, unknown>) => { value.active = "yes"; },
		(value: Record<string, unknown>) => { value.logOffset = -1; },
		(value: Record<string, unknown>) => { value.logPath = "/etc/passwd"; },
		(value: Record<string, unknown>) => { value.semanticReview = "20m"; },
		(value: Record<string, unknown>) => { value.events = ["log-match"]; delete value.logPattern; },
	]) {
		const invalid = structuredClone(valid) as Record<string, unknown>;
		mutation(invalid);
		assert.throws(() => restoreAlarmState(invalid, allowedRoots));
	}
});

test("outbox entries are durable facts independent of the alarm state: same kind may occur many times", () => {
	const timer = createTimerAlarm({ id: "t1", name: "Timer", now: 1000, afterMs: 5000 });
	const timerEvent: FiredEvent = { kind: "timer", fingerprint: "timer:t1:6000" };
	const first = createOutboxEntry(timer, [timerEvent], 6000);
	assert.match(first.eventId, /^t1:6000:/);
	// A second, independent occurrence of the SAME kind is a separate entry and must restore cleanly.
	const second = createOutboxEntry(timer, [{ kind: "timer", fingerprint: "timer:t1:9000" }], 9000);
	const restored = restoreOutbox([first, second]);
	assert.equal(restored.length, 2);
	assert.deepEqual(restored.map((entry) => entry.events[0].fingerprint), [timerEvent.fingerprint, "timer:t1:9000"]);
	// Entries never touch the alarm: a reset timer plus an old wake stays valid.
	assert.equal(restoreAlarmState(JSON.parse(JSON.stringify(createTimerAlarm({ id: "t1", name: "Timer", now: 7000, afterMs: 5000 }))), allowedRoots).kind, "timer");

	const exitEvent: FiredEvent = { kind: "exit", fingerprint: "exit:abc:2026-01-01T00:00:00Z:0" };
	const entry = createOutboxEntry({ ...timer, kind: "container", container: "job", events: ["exit"], policy: "pause", statusPollMs: 60_000, nextCheckAt: 2000, logOffset: 0, scanCarry: "", eventFingerprints: {}, consecutiveFailures: 0, failureNotified: false } as ContainerAlarmState, [exitEvent], 2000);
	assert.equal(entry.alarmName, "Timer");
	// Duplicate kinds WITHIN one entry are still rejected.
	const duplicate = { ...entry, events: [exitEvent, { kind: "exit", fingerprint: "exit:def" }] };
	assert.throws(() => restoreOutbox([duplicate]), /duplicate kinds/);
	for (const mutation of [
		(value: OutboxEntry) => ({ ...value, eventId: "bad event id!" }),
		(value: OutboxEntry) => ({ ...value, events: [] }),
		(value: OutboxEntry) => ({ ...value, message: "x".repeat(4001) }),
		(value: OutboxEntry) => ({ ...value, claim: { claimantId: "a", token: "t" } } as unknown as OutboxEntry),
	]) {
		assert.throws(() => restoreOutbox([mutation(entry)]));
	}
});

test("embedded file probe resets a still-larger replacement by device/inode identity", () => {
	const source = readFileSync(new URL("./runtime.ts", import.meta.url), "utf8");
	const scriptMatch = source.match(/const PROBE_SCRIPT = String\.raw`([\s\S]*?)`;\r?\n\r?\nconst PROBE_LOADER/);
	assert.ok(scriptMatch, "embedded PROBE_SCRIPT must remain extractable");
	const scriptBase64 = Buffer.from(scriptMatch[1], "utf8").toString("base64");
	const appPath = "/data/probes/run.log";
	const cid = "a".repeat(64);
	const payload = Buffer.from(JSON.stringify({ container: "job", expectedId: cid, logPath: appPath, allowedRemoteLogRoots: allowedRoots, offset: 100, fileId: "1:100", baseline: false, readLogs: true, maxBytes: 4096, tailBytes: 32 })).toString("base64");
	const inspectJson = JSON.stringify([{ Id: cid, LogPath: "", State: { Running: true, Status: "running", ExitCode: 0, OOMKilled: false, StartedAt: "2026-01-01T00:00:00Z" } }]);
	const wrapper = [
		"import base64,builtins,io,os,subprocess,sys",
		`app_path=${JSON.stringify(appPath)}`,
		`inspect_json=${JSON.stringify(inspectJson)}`,
		"content=b'x'*200",
		"class Result:",
		"    returncode=0",
		"    stdout=inspect_json.encode('utf-8')",
		"    stderr=b''",
		"class Stat:",
		"    st_size=200",
		"    st_dev=1",
		"    st_ino=200",
		"subprocess.run=lambda *args,**kwargs: Result()",
		"os.stat=lambda value: Stat() if value == app_path else (_ for _ in ()).throw(FileNotFoundError(value))",
		"builtins.open=lambda value,*args,**kwargs: io.BytesIO(content) if value == app_path else (_ for _ in ()).throw(FileNotFoundError(value))",
		`sys.argv=['probe',${JSON.stringify(payload)}]`,
		`exec(base64.b64decode(${JSON.stringify(scriptBase64)}))`,
	].join("\n");
	const result = spawnSync(process.platform === "win32" ? "python" : "python3", ["-c", wrapper], { encoding: "utf8" });
	assert.equal(result.status, 0, result.stderr || result.error?.message || "spawn failed");
	const response = JSON.parse(result.stdout.trim()) as { probeError?: string; logReset?: boolean; logFileId?: string; logOffset?: number };
	assert.equal(response.probeError, undefined);
	assert.equal(response.logReset, true);
	assert.equal(response.logFileId, "1:200");
	assert.equal(response.logOffset, 200);
});

test("embedded Docker fallback retains the last consumed cursor on overflow and uses inspected ID", () => {
	const source = readFileSync(new URL("./runtime.ts", import.meta.url), "utf8");
	const scriptMatch = source.match(/const PROBE_SCRIPT = String\.raw`([\s\S]*?)`;\r?\n\r?\nconst PROBE_LOADER/);
	assert.ok(scriptMatch, "embedded PROBE_SCRIPT must remain extractable");
	const scriptBase64 = Buffer.from(scriptMatch[1], "utf8").toString("base64");
	const cid = "a".repeat(64);
	const cursor = "2026-01-01T00:00:00.000000000Z";
	const firstStamp = "2026-01-01T00:00:01.000000000Z";
	const firstLine = `${firstStamp} first\n`;
	const secondLine = "2026-01-01T00:00:02.000000000Z second\n";
	const payload = Buffer.from(JSON.stringify({ container: "job", expectedId: cid, allowedRemoteLogRoots: allowedRoots, offset: 0, cursor, baseline: false, readLogs: true, maxBytes: Buffer.byteLength(firstLine), tailBytes: 1024 })).toString("base64");
	const inspectJson = JSON.stringify([{ Id: cid, LogPath: "", State: { Running: true, Status: "running", ExitCode: 0, OOMKilled: false, StartedAt: "2026-01-01T00:00:00Z" } }]);
	const wrapper = [
		"import base64,io,subprocess,sys",
		`inspect_json=${JSON.stringify(inspectJson)}`,
		`lines=${JSON.stringify(firstLine + secondLine)}.encode('utf-8')`,
		`expected_id=${JSON.stringify(cid)}`,
		"class Result:",
		"    returncode=0",
		"    stdout=inspect_json.encode('utf-8')",
		"    stderr=b''",
		"class Proc:",
		"    def __init__(self,args):",
		"        if args[-1] != expected_id: raise RuntimeError('docker logs did not use inspected ID')",
		"        self.stdout=io.BytesIO(lines)",
		"        self.terminated=False",
		"    def terminate(self): self.terminated=True",
		"    def kill(self): self.terminated=True",
		"    def wait(self,timeout=None): return -15 if self.terminated else 0",
		"subprocess.run=lambda *args,**kwargs: Result()",
		"subprocess.Popen=lambda args,**kwargs: Proc(args)",
		`sys.argv=['probe',${JSON.stringify(payload)}]`,
		`exec(base64.b64decode(${JSON.stringify(scriptBase64)}))`,
	].join("\n");
	const result = spawnSync(process.platform === "win32" ? "python" : "python3", ["-c", wrapper], { encoding: "utf8" });
	assert.equal(result.status, 0, result.stderr || result.error?.message || "spawn failed");
	const response = JSON.parse(result.stdout.trim()) as { probeError?: string; logCursor?: string; logBase64?: string };
	assert.equal(response.probeError, undefined);
	assert.equal(response.logCursor, firstStamp);
	assert.equal(Buffer.from(response.logBase64 ?? "", "base64").toString("utf8"), firstLine);
});

test("embedded remote probe rejects an allowed-root symlink resolving outside", () => {
	const source = readFileSync(new URL("./runtime.ts", import.meta.url), "utf8");
	const scriptMatch = source.match(/const PROBE_SCRIPT = String\.raw`([\s\S]*?)`;\r?\n\r?\nconst PROBE_LOADER/);
	assert.ok(scriptMatch, "embedded PROBE_SCRIPT must remain extractable");
	const scriptBase64 = Buffer.from(scriptMatch[1], "utf8").toString("base64");
	const appPath = "/data/probes/link.log";
	const payload = Buffer.from(JSON.stringify({ container: "job", logPath: appPath, allowedRemoteLogRoots: allowedRoots, offset: 0, baseline: true, readLogs: true, maxBytes: 4096, tailBytes: 1024 })).toString("base64");
	const wrapper = [
		"import base64,os,subprocess,sys",
		`app_path=${JSON.stringify(appPath)}`,
		"real_realpath=os.path.realpath",
		"def fake_realpath(value):",
		"    if value == app_path: return '/etc/passwd'",
		"    if value.rstrip('/') == '/data/probes': return '/data/probes'",
		"    return real_realpath(value)",
		"os.path.realpath=fake_realpath",
		"def fail_run(*args,**kwargs): raise RuntimeError('docker inspect should not run')",
		"subprocess.run=fail_run",
		`sys.argv=['probe',${JSON.stringify(payload)}]`,
		`exec(base64.b64decode(${JSON.stringify(scriptBase64)}))`,
	].join("\n");
	const result = spawnSync(process.platform === "win32" ? "python" : "python3", ["-c", wrapper], { encoding: "utf8" });
	assert.equal(result.status, 0, result.stderr || result.error?.message || "spawn failed");
	const response = JSON.parse(result.stdout.trim()) as { probeError?: string };
	assert.match(response.probeError ?? "", /outside allowedRemoteLogRoots/);
	assert.doesNotMatch(response.probeError ?? "", /docker inspect should not run/);
});

test("embedded remote probe fails closed for unavailable explicit application logs", () => {
	const source = readFileSync(new URL("./runtime.ts", import.meta.url), "utf8");
	const scriptMatch = source.match(/const PROBE_SCRIPT = String\.raw`([\s\S]*?)`;\r?\n\r?\nconst PROBE_LOADER/);
	assert.ok(scriptMatch, "embedded PROBE_SCRIPT must remain extractable");
	const scriptBase64 = Buffer.from(scriptMatch[1], "utf8").toString("base64");
	const payload = Buffer.from(JSON.stringify({ container: "job", logPath: "/data/probes/definitely-missing-alarm-test.log", allowedRemoteLogRoots: allowedRoots, offset: 0, baseline: true, readLogs: true, maxBytes: 4096, tailBytes: 1024 })).toString("base64");
	const inspectJson = JSON.stringify([{ Id: "a".repeat(64), LogPath: "/var/lib/docker/fallback.log", State: { Running: true, Status: "running", ExitCode: 0, OOMKilled: false, StartedAt: "2026-01-01T00:00:00Z" } }]);
	const wrapper = [
		"import base64,subprocess,sys",
		`inspect_json=${JSON.stringify(inspectJson)}`,
		"class Result:",
		"    returncode=0",
		"    stdout=inspect_json.encode('utf-8')",
		"    stderr=b''",
		"subprocess.run=lambda *args,**kwargs: Result()",
		"def fail_popen(*args,**kwargs):",
		"    raise RuntimeError('docker fallback was used')",
		"subprocess.Popen=fail_popen",
		`sys.argv=['probe',${JSON.stringify(payload)}]`,
		`exec(base64.b64decode(${JSON.stringify(scriptBase64)}))`,
	].join("\n");
	const result = spawnSync(process.platform === "win32" ? "python" : "python3", ["-c", wrapper], { encoding: "utf8" });
	assert.equal(result.status, 0, result.stderr || result.error?.message || "spawn failed");
	const response = JSON.parse(result.stdout.trim()) as { probeError?: string };
	assert.match(response.probeError ?? "", /configured application log unavailable/);
	assert.doesNotMatch(response.probeError ?? "", /docker fallback was used/);
});

test("ownerSessionFile is validated, created, and restored", () => {
	assert.equal(validateOwnerSessionFile("C:\\Users\\x\\.pi\\agent\\sessions\\s.jsonl"), "C:\\Users\\x\\.pi\\agent\\sessions\\s.jsonl");
	assert.equal(validateOwnerSessionFile("/home/u/.pi/agent/sessions/s.jsonl"), "/home/u/.pi/agent/sessions/s.jsonl");
	assert.equal(validateOwnerSessionFile("\\\\server\\share\\s.jsonl"), "\\\\server\\share\\s.jsonl");
	for (const bad of ["", "relative/s.jsonl", "C:rel.jsonl", "x".repeat(4097), "bad\0path"]) assert.throws(() => validateOwnerSessionFile(bad));

	const timer = createTimerAlarm({ id: "t1", name: "Timer", now: 1000, afterMs: 1000, ownerSessionFile: "/sessions/a.jsonl" });
	assert.equal(timer.ownerSessionFile, "/sessions/a.jsonl");
	const restoredTimer = restoreAlarmState(JSON.parse(JSON.stringify(timer)), allowedRoots);
	assert.equal(restoredTimer.ownerSessionFile, "/sessions/a.jsonl");

	const container = createContainerAlarm({ id: "c1", name: "Watch", container: "job", events: ["exit"], now: 1000, ownerSessionFile: "/sessions/b.jsonl" });
	const restoredContainer = restoreAlarmState(JSON.parse(JSON.stringify(container)), allowedRoots);
	assert.equal(restoredContainer.ownerSessionFile, "/sessions/b.jsonl");

	const legacy = createTimerAlarm({ id: "t2", name: "Legacy", now: 1000, afterMs: 1000 });
	assert.equal(restoreAlarmState(JSON.parse(JSON.stringify(legacy)), allowedRoots).ownerSessionFile, undefined);

	const corrupt = JSON.parse(JSON.stringify(timer));
	corrupt.ownerSessionFile = "relative.jsonl";
	assert.throws(() => restoreAlarmState(corrupt, allowedRoots), /ownerSessionFile/);
	const unknown = JSON.parse(JSON.stringify(timer));
	unknown.ownerSession = "/sessions/a.jsonl";
	assert.throws(() => restoreAlarmState(unknown, allowedRoots), /unknown alarm field/);
});

test("claim fields round-trip through strict outbox restore", () => {
	const timer = createTimerAlarm({ id: "t1", name: "Timer", now: 1000, afterMs: 1000, ownerSessionFile: "/sessions/a.jsonl" });
	const entry: OutboxEntry = {
		eventId: "t1:2000:abc",
		alarmId: "t1",
		alarmName: "Timer",
		ownerSessionFile: "/sessions/a.jsonl",
		triggeredAt: 2000,
		events: [{ kind: "timer", fingerprint: "timer:t1:2000" }],
		message: "[Wake alarm] Timer (t1)\nTriggered at: ...",
		claim: { claimantId: "session:abc", token: "tok-1", expiresAt: 999_999 },
	};
	const restored = restoreOutbox([entry])[0];
	assert.equal(restored.claim?.claimantId, "session:abc");
	assert.equal(restored.claim?.token, "tok-1");
	assert.equal(restored.message, entry.message);
	const badClaim = { ...entry, claim: { claimantId: "a", token: "t", expiresAt: 999_999, extra: true } } as OutboxEntry;
	assert.throws(() => restoreOutbox([badClaim]), /unknown/);
	const badClaimField = { ...entry, claim: { claimantId: "a", token: "t", expiresAt: "soon" } } as unknown as OutboxEntry;
	assert.throws(() => restoreOutbox([badClaimField]), /expiresAt/);
	// Alarm revision is unaffected by outbox facts.
	const fired = { ...timer, active: false, triggeredAt: 2000, lastTriggeredAt: 2000, revision: 7 };
	assert.equal(restoreAlarmState(JSON.parse(JSON.stringify(fired)), allowedRoots).revision, 7);
});

test("includeWakeEvidence false keeps evidence out of the wake message but stored for opt-in retrieval", () => {
	const container = applyProbe(applyBaseline(makeContainer(["log-error"]), runningProbe({ logBytes: encoder.encode("Traceback: kaboom\n") }), 1000), runningProbe({ logOffset: 20, logBytes: encoder.encode("ok\nTraceback: second\n") }), 2000);
	const event = container.events.find((candidate) => candidate.kind === "log-error");
	assert.ok(event?.evidence, "the fired log-error event carries evidence");
	const sanitized = wakeMessage(container.state, [event], 2000, 1000, false);
	assert.ok(!sanitized.includes(event.evidence!), "includeWakeEvidence:false excludes the raw log text from the wake message");
	const withEvidence = wakeMessage(container.state, [event], 2000, 1000, true);
	assert.ok(withEvidence.includes("Traceback"), "includeWakeEvidence:true includes it");
	// The outbox entry built with evidence disabled still retains the evidence for the evidence action.
	const entry = createOutboxEntry(container.state, [event], 2000, { includeEvidence: false });
	assert.ok(!entry.message.includes("Traceback"));
	assert.equal(entry.events[0].evidence, event.evidence);
});

test("absolute times require an explicit timezone", () => {
	assert.equal(parseAbsoluteTime("2026-01-01T09:00:00+08:00"), Date.parse("2026-01-01T09:00:00+08:00"));
	assert.equal(parseAbsoluteTime("2026-01-01T09:00:00Z"), Date.parse("2026-01-01T09:00:00Z"));
	assert.throws(() => parseAbsoluteTime("2026-01-01T09:00:00"), /timezone/);
	assert.throws(() => parseAbsoluteTime("2026-01-01 09:00"), /timezone/);
});

test("session lease liveness requires a fresh heartbeat and a live pid", () => {
	const alive = () => true;
	const dead = () => false;
	const now = 1_000_000;
	assert.equal(leaseIsAlive({ pid: 123, heartbeatAt: now - 1000 }, now, alive), true);
	assert.equal(leaseIsAlive({ pid: 123, heartbeatAt: now - 59_000 }, now, alive), true);
	assert.equal(leaseIsAlive({ pid: 123, heartbeatAt: now - 61_000 }, now, alive), false);
	assert.equal(leaseIsAlive({ pid: 123, heartbeatAt: now + 61_000 }, now, alive), false);
	assert.equal(leaseIsAlive({ pid: 123, heartbeatAt: now - 1000 }, now, dead), false);
	assert.equal(leaseIsAlive({ pid: -1, heartbeatAt: now }, now, alive), false);
});

test("headless session resume arguments keep the wake message positional", () => {
	assert.deepEqual(buildResumeArgs("C:\\Users\\x\\s.jsonl", "[Wake alarm] t1 fired"), ["--session", "C:\\Users\\x\\s.jsonl", "--print", "[Wake alarm] t1 fired"]);
	assert.deepEqual(buildResumeArgs("/s.jsonl", "msg", { approve: true }), ["--session", "/s.jsonl", "--approve", "--print", "msg"]);
	assert.throws(() => buildResumeArgs("relative.jsonl", "msg"));
	assert.throws(() => buildResumeArgs("/s.jsonl", ""));
	assert.throws(() => buildResumeArgs("/s.jsonl", "x".repeat(4001)));
});
