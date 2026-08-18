import { createHash } from "node:crypto";
import path from "node:path";

export const DEFAULT_STATUS_POLL_MS = 60_000;
export const MAX_TIMER_DELAY_MS = 2_147_483_647;
export const ERROR_PATTERN = /(?:Traceback|(?:[A-Z][A-Za-z]*)?Error)(?::|\b)/;

export type TriggerPolicy = "pause" | "keep";
export type ContainerEventKind = "exit" | "abnormal" | "missing" | "replaced" | "log-error" | "log-match" | "deadline" | "connection-failure";
export type FiredEventKind = "timer" | ContainerEventKind;
export type LogMode = "application-file" | "docker-file" | "docker-logs";

interface AlarmBase {
	id: string;
	name: string;
	kind: "timer" | "container";
	active: boolean;
	createdAt: number;
	pauseReason?: string;
	lastTriggeredAt?: number;
	pendingWake?: PendingWake;
	ownerSessionFile?: string;
}

export interface TimerAlarmState extends AlarmBase {
	kind: "timer";
	dueAt: number;
	triggeredAt?: number;
}

export interface ContainerAlarmState extends AlarmBase {
	kind: "container";
	container: string;
	containerId?: string;
	logPath?: string;
	logMode?: LogMode;
	selectedLogPath?: string;
	logFileId?: string;
	events: ContainerEventKind[];
	policy: TriggerPolicy;
	logPattern?: string;
	deadlineAt?: number;
	statusPollMs: number;
	nextCheckAt: number;
	logOffset: number;
	logCursor?: string;
	scanCarry: string;
	eventFingerprints: Partial<Record<FiredEventKind, string>>;
	consecutiveFailures: number;
	failureNotified: boolean;
	lastCheckAt?: number;
	lastContainerStatus?: string;
	lastStartedAt?: string;
	lastExitCode?: number;
	lastOomKilled?: boolean;
	lastEvidence?: string;
}

export type AlarmState = TimerAlarmState | ContainerAlarmState;

export interface ProbeResult {
	exists: boolean;
	containerId?: string;
	running: boolean;
	status: string;
	containerStatus: string;
	startedAt?: string;
	exitCode?: number;
	oomKilled?: boolean;
	logMode?: LogMode;
	selectedLogPath?: string;
	logFileId?: string;
	logOffset: number;
	logCursor?: string;
	logReset?: boolean;
	logBytes: Uint8Array;
	tail: string;
}

export interface FiredEvent {
	kind: FiredEventKind;
	fingerprint: string;
	evidence?: string;
}

export interface PendingWake {
	triggeredAt: number;
	events: FiredEvent[];
}

const CONTAINER_EVENTS: readonly ContainerEventKind[] = ["exit", "abnormal", "missing", "replaced", "log-error", "log-match", "deadline", "connection-failure"];
const FIRED_EVENTS: readonly FiredEventKind[] = ["timer", ...CONTAINER_EVENTS];
const DURATION_RE = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/i;
const DURATION_MULTIPLIERS: Record<string, number> = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 };
const CONTAINER_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
const ALARM_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/;
const HOST_RE = /^(?:[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?|\[[0-9A-Fa-f:]+\])$/;
const USER_RE = /^[A-Za-z_][A-Za-z0-9_-]{0,31}$/;

export function parseDuration(value: string | number, label = "duration"): number {
	if (typeof value === "number") {
		if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${label} must be a positive integer number of milliseconds`);
		return value;
	}
	const match = DURATION_RE.exec(value.trim());
	if (!match) throw new Error(`${label} must use ms, s, m, h, or d (for example 30m)`);
	const result = Number(match[1]) * DURATION_MULTIPLIERS[match[2].toLowerCase()];
	if (!Number.isSafeInteger(result) || result <= 0) throw new Error(`${label} is outside the supported range`);
	return result;
}

export function parseAbsoluteTime(value: string, label = "at"): number {
	const result = Date.parse(value);
	if (!Number.isSafeInteger(result) || result < 0) throw new Error(`${label} must be a valid absolute timestamp`);
	return result;
}

export function validatePollingDuration(value: number, label = "statusPoll"): number {
	if (!Number.isSafeInteger(value) || value <= 0 || value > MAX_TIMER_DELAY_MS) {
		throw new Error(`${label} must be positive milliseconds no greater than ${MAX_TIMER_DELAY_MS}`);
	}
	return value;
}

export function timerDelay(dueAt: number, now: number): number {
	if (!Number.isFinite(dueAt) || !Number.isFinite(now)) throw new Error("timer deadline and current time must be finite numbers");
	return Math.min(MAX_TIMER_DELAY_MS, Math.max(0, dueAt - now));
}

export function deadlineAfter(now: number, duration: number, label = "deadline"): number {
	if (!Number.isSafeInteger(now) || now < 0 || !Number.isSafeInteger(duration) || duration <= 0) {
		throw new Error(`${label} requires a non-negative safe start time and a positive safe duration`);
	}
	const deadline = now + duration;
	if (!Number.isSafeInteger(deadline)) throw new Error(`${label} is outside the supported timestamp range`);
	return deadline;
}

export function validateContainer(value: string): string {
	const result = value.trim();
	if (!CONTAINER_RE.test(result)) throw new Error("container must be a Docker name or hexadecimal ID without spaces or shell characters");
	return result;
}

export function validateAlarmId(value: string): string {
	const result = value.trim();
	if (!ALARM_ID_RE.test(result)) throw new Error("alarm id must be 1-64 letters, digits, dot, underscore, or hyphen");
	return result;
}

export function validateAlarmName(value: string): string {
	const result = value.trim();
	if (!result || result.length > 160 || /[\x00-\x1F\x7F]/.test(result)) throw new Error("alarm name must be 1-160 printable characters");
	return result;
}

export function validateHost(value: string): string {
	const result = value.trim();
	if (!HOST_RE.test(result) || result.includes("..")) throw new Error("host is not a valid hostname or IP literal");
	return result;
}

export function validateUser(value: string): string {
	const result = value.trim();
	if (!USER_RE.test(result)) throw new Error("SSH user is invalid");
	return result;
}

function normalizeRemoteRoot(value: string): string {
	if (typeof value !== "string" || value.includes("\0") || !path.posix.isAbsolute(value)) throw new Error("each allowedRemoteLogRoots entry must be an absolute POSIX path");
	const normalized = path.posix.normalize(value);
	return normalized === "/" ? normalized : `${normalized.replace(/\/+$/, "")}/`;
}

export function validateRemoteLogRoots(value: unknown): string[] {
	if (!Array.isArray(value) || value.length === 0) throw new Error("allowedRemoteLogRoots must be a non-empty array of absolute POSIX paths");
	return value.map((entry) => normalizeRemoteRoot(entry as string));
}

function validatePosixFilePath(value: string, label: string): string {
	if (typeof value !== "string" || value.includes("\0") || !path.posix.isAbsolute(value)) throw new Error(`${label} must be an absolute POSIX path`);
	const normalized = path.posix.normalize(value);
	if (normalized !== value || value.endsWith("/")) throw new Error(`${label} must be a normalized absolute POSIX file path`);
	return normalized;
}

export function validateRemoteLogPath(value: string, allowedRoots: readonly string[]): string {
	const normalized = validatePosixFilePath(value, "logPath");
	const roots = validateRemoteLogRoots([...allowedRoots]);
	if (!roots.some((root) => root === "/" || normalized.startsWith(root))) throw new Error(`logPath must be under an allowedRemoteLogRoots entry (${roots.join(", ")})`);
	return normalized;
}

export function validateContainerEvents(value: readonly string[]): ContainerEventKind[] {
	if (!Array.isArray(value) || value.length === 0) throw new Error("events must contain at least one container event");
	const result = value.map((event) => {
		if (!(CONTAINER_EVENTS as readonly string[]).includes(event)) throw new Error(`unsupported container event: ${event}`);
		return event as ContainerEventKind;
	});
	if (new Set(result).size !== result.length) throw new Error("events must not contain duplicates");
	return result;
}

export function validateLogPattern(value: string): string {
	if (!value || value.length > 256 || value.includes("\0")) throw new Error("logPattern must be a non-empty literal no longer than 256 characters");
	return value;
}

export function validateLogFileId(value: string): string {
	if (!/^\d+:\d+$/.test(value) || value.length > 128) throw new Error("logFileId must be a device:inode identifier");
	return value;
}

export function decodeNewLog(bytes: Uint8Array): string {
	return new TextDecoder("utf-8", { fatal: false }).decode(bytes);
}

function firstNewMatch(combined: string, previousLength: number, matcher: RegExp | string): { index: number; length: number } | undefined {
	if (typeof matcher === "string") {
		let index = combined.indexOf(matcher);
		while (index >= 0) {
			if (index + matcher.length > previousLength) return { index, length: matcher.length };
			index = combined.indexOf(matcher, index + 1);
		}
		return undefined;
	}
	const match = [...combined.matchAll(new RegExp(matcher.source, "g"))].find((candidate) => (candidate.index ?? 0) + candidate[0].length > previousLength);
	return match ? { index: match.index ?? 0, length: match[0].length } : undefined;
}

function matchEvidence(combined: string, match: { index: number; length: number }): string {
	const lineStart = Math.max(0, combined.lastIndexOf("\n", match.index - 1) + 1);
	const lineEndCandidate = combined.indexOf("\n", match.index + match.length);
	const lineEnd = lineEndCandidate < 0 ? combined.length : lineEndCandidate;
	return combined.slice(lineStart, lineEnd).slice(-500);
}

export function scanNewLog(previousCarry: string, bytes: Uint8Array, literal?: string): {
	text: string;
	hasError: boolean;
	hasLiteral: boolean;
	carry: string;
	errorEvidence?: string;
	literalEvidence?: string;
	fingerprint: string;
} {
	const text = decodeNewLog(bytes);
	const combined = previousCarry + text;
	const errorMatch = firstNewMatch(combined, previousCarry.length, ERROR_PATTERN);
	const literalMatch = literal ? firstNewMatch(combined, previousCarry.length, literal) : undefined;
	const carryLength = Math.max(64, literal ? Math.min(256, literal.length - 1) : 0);
	return {
		text,
		hasError: Boolean(errorMatch),
		hasLiteral: Boolean(literalMatch),
		carry: combined.slice(-carryLength),
		errorEvidence: errorMatch ? matchEvidence(combined, errorMatch) : undefined,
		literalEvidence: literalMatch ? matchEvidence(combined, literalMatch) : undefined,
		fingerprint: createHash("sha256").update(text).digest("hex").slice(0, 16),
	};
}

export function fileReadWindow(size: number, requested: number, baseline: boolean, tailBytes: number): { start: number; nextOffset: number; reset: boolean } {
	for (const [label, value] of [["file size", size], ["requested offset", requested], ["tail bytes", tailBytes]] as const) {
		if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${label} must be a non-negative safe integer`);
	}
	const reset = requested > size;
	const start = baseline ? Math.max(0, size - tailBytes) : (reset ? 0 : requested);
	return { start, nextOffset: baseline ? size : start, reset };
}

export function createTimerAlarm(input: { id: string; name: string; now: number; afterMs?: number; at?: number; ownerSessionFile?: string }): TimerAlarmState {
	if ((input.afterMs === undefined) === (input.at === undefined)) throw new Error("set_timer requires exactly one of after or at");
	if (!Number.isSafeInteger(input.now) || input.now < 0) throw new Error("alarm creation time must be a non-negative safe integer");
	const dueAt = input.afterMs === undefined ? input.at! : deadlineAfter(input.now, input.afterMs, "timer due time");
	if (!Number.isSafeInteger(dueAt) || dueAt < 0) throw new Error("timer due time must be a non-negative safe timestamp");
	const ownerSessionFile = input.ownerSessionFile === undefined ? undefined : validateOwnerSessionFile(input.ownerSessionFile);
	return { id: validateAlarmId(input.id), name: validateAlarmName(input.name), kind: "timer", active: true, createdAt: input.now, dueAt, ownerSessionFile };
}

export function createContainerAlarm(input: {
	id: string;
	name: string;
	container: string;
	events: readonly string[];
	policy?: TriggerPolicy;
	logPattern?: string;
	logPath?: string;
	allowedRemoteLogRoots?: readonly string[];
	now: number;
	statusPollMs?: number;
	deadlineMs?: number;
	ownerSessionFile?: string;
}): ContainerAlarmState {
	if (!Number.isSafeInteger(input.now) || input.now < 0) throw new Error("alarm creation time must be a non-negative safe integer");
	const events = validateContainerEvents(input.events);
	if (events.includes("log-match") !== (input.logPattern !== undefined)) throw new Error("log-match requires logPattern, and logPattern requires the log-match event");
	if (events.includes("deadline") !== (input.deadlineMs !== undefined)) throw new Error("deadline requires a relative deadline, and a relative deadline requires the deadline event");
	const policy = input.policy ?? "pause";
	if (policy !== "pause" && policy !== "keep") throw new Error("policy must be pause or keep");
	return {
		id: validateAlarmId(input.id),
		name: validateAlarmName(input.name),
		kind: "container",
		active: true,
		createdAt: input.now,
		ownerSessionFile: input.ownerSessionFile === undefined ? undefined : validateOwnerSessionFile(input.ownerSessionFile),
		container: validateContainer(input.container),
		events,
		policy,
		logPattern: input.logPattern === undefined ? undefined : validateLogPattern(input.logPattern),
		logPath: input.logPath ? validateRemoteLogPath(input.logPath, input.allowedRemoteLogRoots ?? []) : undefined,
		deadlineAt: input.deadlineMs === undefined ? undefined : deadlineAfter(input.now, input.deadlineMs, "container deadline"),
		statusPollMs: validatePollingDuration(input.statusPollMs ?? DEFAULT_STATUS_POLL_MS),
		nextCheckAt: input.now,
		logOffset: 0,
		scanCarry: "",
		eventFingerprints: {},
		consecutiveFailures: 0,
		failureNotified: false,
	};
}

export function applyTimer(alarm: TimerAlarmState, now: number): { state: TimerAlarmState; events: FiredEvent[] } {
	if (!alarm.active || alarm.triggeredAt !== undefined || now < alarm.dueAt) return { state: alarm, events: [] };
	const event = { kind: "timer" as const, fingerprint: `timer:${alarm.id}:${alarm.dueAt}` };
	return { state: { ...alarm, active: false, triggeredAt: now, lastTriggeredAt: now, pauseReason: "timer fired" }, events: [event] };
}

export function applyBaseline(alarm: ContainerAlarmState, probe: ProbeResult, now: number): ContainerAlarmState {
	const scan = scanNewLog("", probe.logBytes, alarm.logPattern);
	return {
		...alarm,
		containerId: probe.containerId,
		logMode: probe.logMode,
		selectedLogPath: probe.selectedLogPath,
		logFileId: probe.logFileId,
		logOffset: probe.logOffset,
		logCursor: probe.logCursor,
		scanCarry: scan.carry,
		consecutiveFailures: 0,
		failureNotified: false,
		lastCheckAt: now,
		lastContainerStatus: probe.containerStatus,
		lastStartedAt: probe.startedAt,
		lastExitCode: probe.exitCode,
		lastOomKilled: probe.oomKilled,
		nextCheckAt: deadlineAfter(now, alarm.statusPollMs, "next check"),
	};
}

function addEvent(alarm: ContainerAlarmState, next: ContainerAlarmState, events: FiredEvent[], kind: ContainerEventKind, fingerprint: string, evidence?: string): void {
	if (!alarm.events.includes(kind) || next.eventFingerprints[kind] === fingerprint) return;
	next.eventFingerprints[kind] = fingerprint;
	events.push({ kind, fingerprint, evidence });
}

export function applyContainerDeadline(alarm: ContainerAlarmState, now: number): { state: ContainerAlarmState; events: FiredEvent[] } {
	if (!alarm.active || alarm.deadlineAt === undefined || now < alarm.deadlineAt) return { state: alarm, events: [] };
	const fingerprint = `deadline:${alarm.id}:${alarm.deadlineAt}`;
	if (alarm.eventFingerprints.deadline === fingerprint) return { state: alarm, events: [] };
	const eventFingerprints = { ...alarm.eventFingerprints, deadline: fingerprint };
	const event: FiredEvent = { kind: "deadline", fingerprint };
	return {
		state: {
			...alarm,
			eventFingerprints,
			lastTriggeredAt: now,
			active: alarm.policy === "pause" ? false : alarm.active,
			pauseReason: alarm.policy === "pause" ? "triggered: deadline" : alarm.pauseReason,
		},
		events: [event],
	};
}

export function applyProbe(alarm: ContainerAlarmState, probe: ProbeResult, now: number): { state: ContainerAlarmState; events: FiredEvent[] } {
	const eventFingerprints = { ...alarm.eventFingerprints };
	const next: ContainerAlarmState = {
		...alarm,
		containerId: alarm.containerId ?? probe.containerId,
		logMode: probe.logMode ?? alarm.logMode,
		selectedLogPath: probe.selectedLogPath ?? alarm.selectedLogPath,
		logFileId: probe.logFileId ?? alarm.logFileId,
		logOffset: probe.logOffset,
		logCursor: probe.logMode && probe.logMode !== "docker-logs" ? undefined : (probe.logCursor ?? alarm.logCursor),
		eventFingerprints,
		consecutiveFailures: 0,
		failureNotified: false,
		lastCheckAt: now,
		lastContainerStatus: probe.containerStatus,
		lastStartedAt: probe.startedAt ?? alarm.lastStartedAt,
		lastExitCode: probe.exitCode,
		lastOomKilled: probe.oomKilled,
		nextCheckAt: deadlineAfter(now, alarm.statusPollMs, "next check"),
	};
	delete eventFingerprints["connection-failure"];
	if (probe.logReset) {
		delete eventFingerprints["log-error"];
		delete eventFingerprints["log-match"];
	}
	const scan = scanNewLog(probe.logReset ? "" : alarm.scanCarry, probe.logBytes, alarm.logPattern);
	next.scanCarry = scan.carry;
	const fired: FiredEvent[] = [];
	if (scan.hasError && probe.logBytes.byteLength > 0) addEvent(alarm, next, fired, "log-error", `log-error:${probe.logOffset}:${scan.fingerprint}`, scan.errorEvidence);
	if (scan.hasLiteral && probe.logBytes.byteLength > 0) addEvent(alarm, next, fired, "log-match", `log-match:${probe.logOffset}:${scan.fingerprint}`, scan.literalEvidence);

	const replaced = probe.status === "replaced";
	const missing = probe.status === "missing";
	const abnormal = probe.exists && (Boolean(probe.oomKilled) || (probe.exitCode !== undefined && probe.exitCode !== 0) || ["dead", "restarting"].includes(probe.containerStatus));
	const cleanExit = probe.exists && probe.containerStatus === "exited" && !probe.running && probe.exitCode === 0 && !abnormal;
	if (!missing) delete eventFingerprints.missing;
	if (!replaced) delete eventFingerprints.replaced;
	if (!cleanExit) delete eventFingerprints.exit;
	if (!abnormal) delete eventFingerprints.abnormal;
	if (missing) addEvent(alarm, next, fired, "missing", `missing:${alarm.container}`);
	if (replaced) addEvent(alarm, next, fired, "replaced", `replaced:${alarm.containerId ?? "unbound"}:${probe.containerId ?? "unknown"}`);
	if (cleanExit) addEvent(alarm, next, fired, "exit", `exit:${probe.containerId ?? alarm.containerId ?? alarm.container}:${probe.startedAt ?? "unknown"}:${probe.exitCode}`);
	if (abnormal) addEvent(alarm, next, fired, "abnormal", `abnormal:${probe.containerId ?? alarm.containerId ?? alarm.container}:${probe.startedAt ?? "unknown"}:${probe.containerStatus}:${probe.exitCode ?? "unknown"}:${Boolean(probe.oomKilled)}`);
	if (alarm.deadlineAt !== undefined && now >= alarm.deadlineAt) addEvent(alarm, next, fired, "deadline", `deadline:${alarm.id}:${alarm.deadlineAt}`);

	if (fired.length) {
		next.lastTriggeredAt = now;
		next.lastEvidence = fired.find((event) => event.evidence)?.evidence;
		if (alarm.policy === "pause") {
			next.active = false;
			next.pauseReason = `triggered: ${fired.map((event) => event.kind).join(", ")}`;
		}
	}
	return { state: next, events: fired };
}

export function applyCheckFailure(alarm: ContainerAlarmState, now: number, maxConsecutiveFailures: number, reason: string): { state: ContainerAlarmState; events: FiredEvent[] } {
	const failures = Math.min(alarm.consecutiveFailures + 1, maxConsecutiveFailures);
	const reached = failures >= maxConsecutiveFailures;
	const configured = alarm.events.includes("connection-failure");
	const shouldFire = configured && reached && !alarm.failureNotified;
	const eventFingerprints = { ...alarm.eventFingerprints };
	if (shouldFire) eventFingerprints["connection-failure"] = `connection-failure:${alarm.id}:${now}`;
	const next: ContainerAlarmState = {
		...alarm,
		active: shouldFire && alarm.policy === "pause" ? false : alarm.active,
		consecutiveFailures: failures,
		failureNotified: configured && reached,
		eventFingerprints,
		lastCheckAt: now,
		lastContainerStatus: `connection failure (${failures}/${maxConsecutiveFailures})`,
		nextCheckAt: deadlineAfter(now, alarm.statusPollMs, "next check"),
		lastTriggeredAt: shouldFire ? now : alarm.lastTriggeredAt,
		lastEvidence: shouldFire ? reason : alarm.lastEvidence,
		pauseReason: shouldFire && alarm.policy === "pause" ? "triggered: connection-failure" : alarm.pauseReason,
	};
	return { state: next, events: shouldFire ? [{ kind: "connection-failure", fingerprint: eventFingerprints["connection-failure"]!, evidence: reason }] : [] };
}

export function resumeAlarm(alarm: AlarmState, now: number): AlarmState {
	if (alarm.kind === "timer" && alarm.triggeredAt !== undefined) throw new Error("a fired timer must be reset with a new after or at value");
	return alarm.kind === "timer"
		? { ...alarm, active: true, pauseReason: undefined }
		: { ...alarm, active: true, pauseReason: undefined, consecutiveFailures: 0, failureNotified: false, nextCheckAt: now };
}

function requiredString(record: Record<string, unknown>, name: string, maxLength: number): string {
	const value = record[name];
	if (typeof value !== "string" || !value || value.length > maxLength) throw new Error(`${name} must be a non-empty string no longer than ${maxLength} characters`);
	return value;
}

function optionalString(record: Record<string, unknown>, name: string, maxLength: number): string | undefined {
	return name in record ? requiredString(record, name, maxLength) : undefined;
}

function requiredBoundedString(record: Record<string, unknown>, name: string, maxLength: number): string {
	const value = record[name];
	if (typeof value !== "string" || value.length > maxLength) throw new Error(`${name} must be a string no longer than ${maxLength} characters`);
	return value;
}

function requiredInteger(record: Record<string, unknown>, name: string, min = 0, max = Number.MAX_SAFE_INTEGER): number {
	const value = record[name];
	if (!Number.isSafeInteger(value) || (value as number) < min || (value as number) > max) throw new Error(`${name} must be an integer from ${min} to ${max}`);
	return value as number;
}

function optionalInteger(record: Record<string, unknown>, name: string, min = 0, max = Number.MAX_SAFE_INTEGER): number | undefined {
	return name in record ? requiredInteger(record, name, min, max) : undefined;
}

function restorePendingWake(record: Record<string, unknown>): PendingWake | undefined {
	if (!("pendingWake" in record)) return undefined;
	const raw = record.pendingWake;
	if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("pendingWake must be an object");
	const pending = raw as Record<string, unknown>;
	assertKnown(pending, ["triggeredAt", "events"]);
	if (!Array.isArray(pending.events) || pending.events.length === 0 || pending.events.length > FIRED_EVENTS.length) throw new Error("pendingWake.events must be a non-empty bounded array");
	const events = pending.events.map((value) => {
		if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("pendingWake event must be an object");
		const event = value as Record<string, unknown>;
		assertKnown(event, ["kind", "fingerprint", "evidence"]);
		const kind = requiredString(event, "kind", 32);
		if (!(FIRED_EVENTS as readonly string[]).includes(kind)) throw new Error("pendingWake event kind is invalid");
		return {
			kind: kind as FiredEventKind,
			fingerprint: requiredString(event, "fingerprint", 256),
			evidence: optionalString(event, "evidence", 2000),
		};
	});
	if (new Set(events.map((event) => event.kind)).size !== events.length) throw new Error("pendingWake events must not contain duplicate kinds");
	return { triggeredAt: requiredInteger(pending, "triggeredAt"), events };
}

function assertKnown(record: Record<string, unknown>, fields: readonly string[]): void {
	const allowed = new Set(fields);
	const unknown = Object.keys(record).filter((name) => !allowed.has(name));
	if (unknown.length) throw new Error(`unknown alarm field(s): ${unknown.join(", ")}`);
}

function restoreBase(record: Record<string, unknown>): AlarmBase {
	if (typeof record.active !== "boolean") throw new Error("active must be boolean");
	const kind = requiredString(record, "kind", 16);
	if (kind !== "timer" && kind !== "container") throw new Error("kind must be timer or container");
	return {
		id: validateAlarmId(requiredString(record, "id", 64)),
		name: validateAlarmName(requiredString(record, "name", 160)),
		kind,
		active: record.active,
		createdAt: requiredInteger(record, "createdAt"),
		pauseReason: optionalString(record, "pauseReason", 1024),
		lastTriggeredAt: optionalInteger(record, "lastTriggeredAt"),
		pendingWake: restorePendingWake(record),
		ownerSessionFile: (() => { const file = optionalString(record, "ownerSessionFile", 4096); return file ? validateOwnerSessionFile(file) : undefined; })(),
	};
}

export function restoreAlarmState(value: unknown, allowedRemoteLogRoots: readonly string[]): AlarmState {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("alarm must be an object");
	const record = value as Record<string, unknown>;
	const base = restoreBase(record);
	const baseFields = ["id", "name", "kind", "active", "createdAt", "pauseReason", "lastTriggeredAt", "pendingWake", "ownerSessionFile"];
	if (base.kind === "timer") {
		assertKnown(record, [...baseFields, "dueAt", "triggeredAt"]);
		const triggeredAt = optionalInteger(record, "triggeredAt");
		if (triggeredAt !== undefined && base.active) throw new Error("a triggered timer must not be active");
		if (base.pendingWake && (base.pendingWake.events.length !== 1 || base.pendingWake.events[0].kind !== "timer")) throw new Error("timer pendingWake must contain only the timer event");
		if (base.pendingWake && (triggeredAt !== base.pendingWake.triggeredAt || base.lastTriggeredAt !== base.pendingWake.triggeredAt)) throw new Error("timer pendingWake timestamps are inconsistent");
		return { ...base, kind: "timer", dueAt: requiredInteger(record, "dueAt"), triggeredAt };
	}
	assertKnown(record, [...baseFields, "container", "containerId", "logPath", "logMode", "selectedLogPath", "logFileId", "events", "policy", "logPattern", "deadlineAt", "statusPollMs", "nextCheckAt", "logOffset", "logCursor", "scanCarry", "eventFingerprints", "consecutiveFailures", "failureNotified", "lastCheckAt", "lastContainerStatus", "lastStartedAt", "lastExitCode", "lastOomKilled", "lastEvidence"]);
	if (!Array.isArray(record.events)) throw new Error("events must be an array");
	const events = validateContainerEvents(record.events as string[]);
	const policy = requiredString(record, "policy", 16);
	if (policy !== "pause" && policy !== "keep") throw new Error("policy must be pause or keep");
	if (typeof record.failureNotified !== "boolean") throw new Error("failureNotified must be boolean");
	const fingerprintsRaw = record.eventFingerprints;
	if (!fingerprintsRaw || typeof fingerprintsRaw !== "object" || Array.isArray(fingerprintsRaw)) throw new Error("eventFingerprints must be an object");
	const eventFingerprints: Partial<Record<FiredEventKind, string>> = {};
	for (const [kind, fingerprint] of Object.entries(fingerprintsRaw)) {
		if (kind === "timer" || !(FIRED_EVENTS as readonly string[]).includes(kind) || !events.includes(kind as ContainerEventKind) || typeof fingerprint !== "string" || !fingerprint || fingerprint.length > 256) throw new Error("eventFingerprints contains an invalid or unconfigured entry");
		eventFingerprints[kind as FiredEventKind] = fingerprint;
	}
	if (record.failureNotified && !events.includes("connection-failure")) throw new Error("failureNotified requires the connection-failure event");
	if (base.pendingWake && (base.lastTriggeredAt !== base.pendingWake.triggeredAt || base.pendingWake.events.some((event) => eventFingerprints[event.kind] !== event.fingerprint))) throw new Error("container pendingWake does not match durable event state");
	const logMode = optionalString(record, "logMode", 32);
	if (logMode && !(["application-file", "docker-file", "docker-logs"] as string[]).includes(logMode)) throw new Error("logMode is invalid");
	const lastStartedAt = optionalString(record, "lastStartedAt", 64);
	if (lastStartedAt && !Number.isFinite(Date.parse(lastStartedAt))) throw new Error("lastStartedAt must be a valid timestamp");
	const logCursor = optionalString(record, "logCursor", 128);
	if (logCursor && !Number.isFinite(Date.parse(logCursor))) throw new Error("logCursor must be a valid timestamp");
	const logPath = optionalString(record, "logPath", 4096);
	const logPattern = optionalString(record, "logPattern", 256);
	const deadlineAt = optionalInteger(record, "deadlineAt");
	if (base.pendingWake && base.pendingWake.events.some((event) => event.kind === "timer" || !events.includes(event.kind as ContainerEventKind))) throw new Error("container pendingWake contains an unconfigured event");
	if (events.includes("log-match") !== (logPattern !== undefined)) throw new Error("restored log-match configuration is inconsistent");
	if (events.includes("deadline") !== (deadlineAt !== undefined) || (deadlineAt !== undefined && deadlineAt <= base.createdAt)) throw new Error("restored deadline configuration is inconsistent");
	if ("lastOomKilled" in record && typeof record.lastOomKilled !== "boolean") throw new Error("lastOomKilled must be boolean");
	return {
		...base,
		kind: "container",
		container: validateContainer(requiredString(record, "container", 128)),
		containerId: (() => { const id = optionalString(record, "containerId", 128); return id ? validateContainer(id) : undefined; })(),
		logPath: logPath ? validateRemoteLogPath(logPath, allowedRemoteLogRoots) : undefined,
		logMode: logMode as LogMode | undefined,
		selectedLogPath: (() => { const selected = optionalString(record, "selectedLogPath", 4096); return selected ? validatePosixFilePath(selected, "selectedLogPath") : undefined; })(),
		logFileId: (() => { const fileId = optionalString(record, "logFileId", 128); return fileId ? validateLogFileId(fileId) : undefined; })(),
		events,
		policy,
		logPattern: logPattern ? validateLogPattern(logPattern) : undefined,
		deadlineAt,
		statusPollMs: validatePollingDuration(requiredInteger(record, "statusPollMs", 1), "statusPollMs"),
		nextCheckAt: requiredInteger(record, "nextCheckAt"),
		logOffset: requiredInteger(record, "logOffset"),
		logCursor,
		scanCarry: requiredBoundedString(record, "scanCarry", 256),
		eventFingerprints,
		consecutiveFailures: requiredInteger(record, "consecutiveFailures"),
		failureNotified: record.failureNotified,
		lastCheckAt: optionalInteger(record, "lastCheckAt"),
		lastContainerStatus: optionalString(record, "lastContainerStatus", 1024),
		lastStartedAt,
		lastExitCode: optionalInteger(record, "lastExitCode", 0, 2_147_483_647),
		lastOomKilled: record.lastOomKilled as boolean | undefined,
		lastEvidence: optionalString(record, "lastEvidence", 2000),
	};
}

export function nextAlarmDueAt(alarm: AlarmState): number | undefined {
	if (alarm.pendingWake) return 0;
	if (!alarm.active) return undefined;
	if (alarm.kind === "timer") return alarm.triggeredAt === undefined ? alarm.dueAt : undefined;
	const deadlinePending = alarm.deadlineAt !== undefined && alarm.eventFingerprints.deadline !== `deadline:${alarm.id}:${alarm.deadlineAt}`;
	return deadlinePending ? Math.min(alarm.nextCheckAt, alarm.deadlineAt!) : alarm.nextCheckAt;
}

export function sanitizeExcerpt(value: string, maxChars: number): string {
	const withoutAnsi = value.replace(/\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])/g, "");
	const safe = withoutAnsi.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, "�");
	return safe.length <= maxChars ? safe : `…${safe.slice(-(maxChars - 1))}`;
}

const OWNER_SESSION_FILE_RE = /^(?:[A-Za-z]:[\\/]|\\\\|\/)/;

export function validateOwnerSessionFile(value: string): string {
	const result = value.trim();
	if (!result || result.length > 4096 || /[\x00-\x1F\x7F]/.test(result)) throw new Error("ownerSessionFile must be 1-4096 printable characters");
	if (!OWNER_SESSION_FILE_RE.test(result)) throw new Error("ownerSessionFile must be an absolute path");
	return result;
}

export interface AlarmLease {
	version: 1;
	pid: number;
	instanceId?: string;
	role: "session";
	sessionFile?: string;
	heartbeatAt: number;
}

export const LEASE_MAX_AGE_MS = 60_000;

export function leaseIsAlive(lease: Pick<AlarmLease, "pid" | "heartbeatAt">, now: number, pidAlive: (pid: number) => boolean, maxAgeMs: number = LEASE_MAX_AGE_MS): boolean {
	if (!Number.isSafeInteger(lease.pid) || lease.pid <= 0) return false;
	if (!Number.isSafeInteger(lease.heartbeatAt) || !Number.isSafeInteger(now)) return false;
	if (now - lease.heartbeatAt > maxAgeMs || lease.heartbeatAt - now > maxAgeMs) return false;
	return pidAlive(lease.pid);
}

export function buildResumeArgs(sessionFile: string, message: string): string[] {
	if (!message || message.length > 4000) throw new Error("wake message must be 1-4000 characters");
	return ["--session", validateOwnerSessionFile(sessionFile), "--approve", "--print", message];
}
