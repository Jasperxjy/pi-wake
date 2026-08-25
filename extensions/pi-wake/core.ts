import { createHash, randomUUID } from "node:crypto";
import path from "node:path";

export const DEFAULT_STATUS_POLL_MS = 60_000;
export const MIN_STATUS_POLL_MS = 1_000;
export const MAX_TIMER_DELAY_MS = 2_147_483_647;
export const ERROR_PATTERN = /(?:Traceback|(?:[A-Z][A-Za-z]*)?Error)(?::|\b)/;

export type TriggerPolicy = "pause" | "keep";
export type ContainerEventKind = "exit" | "abnormal" | "missing" | "replaced" | "log-error" | "log-match" | "deadline" | "connection-failure";
export type FiredEventKind = "timer" | ContainerEventKind | "group" | "condition";
export type LogMode = "application-file" | "docker-file" | "docker-logs";
export type GroupCondition = "any_terminal" | "all_terminal" | "any_abnormal" | "n_of_m_terminal";
export type ConditionKind = "exists" | "contains" | "min_size";

interface AlarmBase {
	id: string;
	name: string;
	kind: "timer" | "container" | "group" | "condition";
	active: boolean;
	createdAt: number;
	pauseReason?: string;
	lastTriggeredAt?: number;
	ownerSessionFile?: string;
	/** Optimistic-concurrency revision; assigned by the state store (absent/0 for legacy). */
	revision?: number;
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
	/** When set, this alarm is a member of the group alarm with this id: fires are
	 * recorded in state but produce NO individual wake — the group emits the summary. */
	groupId?: string;
	/** When set, exit/abnormal wake evidence includes the last N lines of the container log. */
	logTailLines?: number;
}

/**
 * Batch barrier: watches a set of member container alarms and emits ONE summary
 * wake when the configured condition first becomes true. Members are created by
 * `watch_container_group` and produce no individual wakes while they belong to
 * a group.
 */
export interface GroupAlarmState extends AlarmBase {
	kind: "group";
	memberIds: string[];
	condition: GroupCondition;
	/** Members required to be terminal for n_of_m_terminal; default: all. */
	required: number;
	statusPollMs: number;
	nextCheckAt: number;
	/** When the condition was first met; used with coalesceWindowMs. */
	conditionMetAt?: number;
	/** Wait up to this long after the condition is first met before firing, so the
	 * summary can include stragglers; all-terminal always fires immediately. */
	coalesceWindowMs?: number;
	/** Set when the group fired; the group never re-fires until reset. */
	firedAt?: number;
	/** Last evaluation summary, shown by list/check. */
	summary?: string;
}

/**
 * Remote completion-condition watch: polls a file on the probe host (under
 * allowedRemoteLogRoots) and fires once when the condition holds. This is the
 * "result file exists / contains marker / size threshold" primitive for
 * experiments whose true completion is a file, not a container state.
 */
export interface ConditionAlarmState extends AlarmBase {
	kind: "condition";
	path: string;
	condition: ConditionKind;
	/** Literal substring required by "contains". */
	value?: string;
	/** Byte threshold required by "min_size". */
	minSize?: number;
	statusPollMs: number;
	nextCheckAt: number;
	satisfiedAt?: number;
	lastSatisfied?: boolean;
	lastSize?: number;
	lastEvidence?: string;
}

export type AlarmState = TimerAlarmState | ContainerAlarmState | GroupAlarmState | ConditionAlarmState;

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

export interface WakeClaim {
	claimantId: string;
	token: string;
	expiresAt: number;
}

/**
 * A durable, not-yet-delivered wake. Outbox entries are independent of the alarm's
 * current state: they record what HAPPENED, so a later pause/reset/remove of the
 * alarm can never corrupt or lose them, and one event kind may appear in many
 * entries (one per occurrence).
 */
export interface OutboxEntry {
	eventId: string;
	alarmId: string;
	alarmName: string;
	ownerSessionFile?: string;
	triggeredAt: number;
	events: FiredEvent[];
	/** Factual wake message snapshot built at fire time (bounded to 4000 chars). */
	message: string;
	/** Atomic delivery claim, written under the state transaction lock. */
	claim?: WakeClaim;
}

const CONTAINER_EVENTS: readonly ContainerEventKind[] = ["exit", "abnormal", "missing", "replaced", "log-error", "log-match", "deadline", "connection-failure"];
const FIRED_EVENTS: readonly FiredEventKind[] = ["timer", ...CONTAINER_EVENTS, "group", "condition"];
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
	const trimmed = value.trim();
	if (!/(?:[zZ]|[+-]\d{2}:?\d{2})$/.test(trimmed)) throw new Error(`${label} must be an ISO 8601 timestamp with an explicit timezone (trailing Z or ±hh:mm offset)`);
	const result = Date.parse(trimmed);
	if (!Number.isSafeInteger(result) || result < 0) throw new Error(`${label} must be a valid absolute timestamp`);
	return result;
}

export function validatePollingDuration(value: number, label = "statusPoll"): number {
	if (!Number.isSafeInteger(value) || value < MIN_STATUS_POLL_MS || value > MAX_TIMER_DELAY_MS) {
		throw new Error(`${label} must be an integer between ${MIN_STATUS_POLL_MS}ms (1s) and ${MAX_TIMER_DELAY_MS}ms`);
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
	groupId?: string;
	logTailLines?: number;
}): ContainerAlarmState {
	if (!Number.isSafeInteger(input.now) || input.now < 0) throw new Error("alarm creation time must be a non-negative safe integer");
	const events = validateContainerEvents(input.events);
	if (events.includes("log-match") !== (input.logPattern !== undefined)) throw new Error("log-match requires logPattern, and logPattern requires the log-match event");
	if (events.includes("deadline") !== (input.deadlineMs !== undefined)) throw new Error("deadline requires a relative deadline, and a relative deadline requires the deadline event");
	const policy = input.policy ?? "pause";
	if (policy !== "pause" && policy !== "keep") throw new Error("policy must be pause or keep");
	if (input.logTailLines !== undefined && (!Number.isSafeInteger(input.logTailLines) || input.logTailLines < 1 || input.logTailLines > 200)) throw new Error("logTailLines must be an integer from 1 to 200");
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
		groupId: input.groupId === undefined ? undefined : validateAlarmId(input.groupId),
		logTailLines: input.logTailLines,
	};
}

/** The last N lines of a text, bounded for wake evidence. */
export function tailLines(text: string, lines: number): string {
	if (!text) return "";
	const parts = text.split(/\r?\n/);
	return parts.slice(-lines).join("\n").trim();
}

export function createGroupAlarm(input: {
	id: string;
	name: string;
	memberIds: readonly string[];
	condition: GroupCondition;
	required?: number;
	now: number;
	statusPollMs?: number;
	coalesceWindowMs?: number;
	ownerSessionFile?: string;
}): GroupAlarmState {
	if (!Number.isSafeInteger(input.now) || input.now < 0) throw new Error("alarm creation time must be a non-negative safe integer");
	if (!Array.isArray(input.memberIds) || input.memberIds.length < 1 || input.memberIds.length > 64) throw new Error("a group needs 1-64 member alarms");
	const memberIds = input.memberIds.map((id) => validateAlarmId(id));
	if (new Set(memberIds).size !== memberIds.length) throw new Error("group members must be unique");
	const condition = input.condition;
	if (!["any_terminal", "all_terminal", "any_abnormal", "n_of_m_terminal"].includes(condition)) throw new Error("condition must be any_terminal, all_terminal, any_abnormal, or n_of_m_terminal");
	const required = input.required === undefined ? memberIds.length : input.required;
	if (!Number.isSafeInteger(required) || required < 1 || required > memberIds.length) throw new Error(`required must be an integer from 1 to ${memberIds.length}`);
	if (input.coalesceWindowMs !== undefined && (!Number.isSafeInteger(input.coalesceWindowMs) || input.coalesceWindowMs < 0 || input.coalesceWindowMs > MAX_TIMER_DELAY_MS)) throw new Error("coalesceWindow is outside the supported range");
	return {
		id: validateAlarmId(input.id),
		name: validateAlarmName(input.name),
		kind: "group",
		active: true,
		createdAt: input.now,
		ownerSessionFile: input.ownerSessionFile === undefined ? undefined : validateOwnerSessionFile(input.ownerSessionFile),
		memberIds,
		condition,
		required,
		statusPollMs: validatePollingDuration(input.statusPollMs ?? DEFAULT_STATUS_POLL_MS),
		nextCheckAt: input.now,
		coalesceWindowMs: input.coalesceWindowMs,
	};
}

export function createConditionAlarm(input: {
	id: string;
	name: string;
	path: string;
	condition: ConditionKind;
	value?: string;
	minSize?: number;
	allowedRemoteLogRoots?: readonly string[];
	now: number;
	statusPollMs?: number;
	ownerSessionFile?: string;
}): ConditionAlarmState {
	if (!Number.isSafeInteger(input.now) || input.now < 0) throw new Error("alarm creation time must be a non-negative safe integer");
	const condition = input.condition;
	if (!["exists", "contains", "min_size"].includes(condition)) throw new Error("condition must be exists, contains, or min_size");
	if (condition === "contains" && (!input.value || input.value.length > 256 || input.value.includes("\0"))) throw new Error("contains requires a literal value no longer than 256 characters");
	if (condition === "min_size" && (!Number.isSafeInteger(input.minSize) || (input.minSize as number) < 1)) throw new Error("min_size requires a positive integer minSize");
	return {
		id: validateAlarmId(input.id),
		name: validateAlarmName(input.name),
		kind: "condition",
		active: true,
		createdAt: input.now,
		ownerSessionFile: input.ownerSessionFile === undefined ? undefined : validateOwnerSessionFile(input.ownerSessionFile),
		path: validateRemoteLogPath(input.path, input.allowedRemoteLogRoots ?? []),
		condition,
		value: condition === "contains" ? input.value : undefined,
		minSize: condition === "min_size" ? input.minSize : undefined,
		statusPollMs: validatePollingDuration(input.statusPollMs ?? DEFAULT_STATUS_POLL_MS),
		nextCheckAt: input.now,
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
	const exitFingerprint = `exit:${probe.containerId ?? alarm.containerId ?? alarm.container}:${probe.startedAt ?? "unknown"}:${probe.exitCode}`;
	const abnormalFingerprint = `abnormal:${probe.containerId ?? alarm.containerId ?? alarm.container}:${probe.startedAt ?? "unknown"}:${probe.containerStatus}:${probe.exitCode ?? "unknown"}:${Boolean(probe.oomKilled)}`;
	// Optional bounded log tail attached to exit/abnormal wake evidence.
	const tailEvidence = alarm.logTailLines ? tailLines(probe.tail, alarm.logTailLines) : undefined;
	if (cleanExit) addEvent(alarm, next, fired, "exit", exitFingerprint, tailEvidence);
	if (abnormal) addEvent(alarm, next, fired, "abnormal", abnormalFingerprint, tailEvidence);
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
	if (alarm.kind === "group" && alarm.firedAt !== undefined) throw new Error("a completed group must be reset, not resumed");
	if (alarm.kind === "condition" && alarm.satisfiedAt !== undefined) throw new Error("a satisfied condition alarm must be reset, not resumed");
	if (alarm.kind === "timer") return { ...alarm, active: true, pauseReason: undefined };
	if (alarm.kind === "group") return { ...alarm, active: true, pauseReason: undefined, nextCheckAt: now };
	if (alarm.kind === "condition") return { ...alarm, active: true, pauseReason: undefined, nextCheckAt: now };
	return { ...alarm, active: true, pauseReason: undefined, consecutiveFailures: 0, failureNotified: false, nextCheckAt: now };
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

function restoreOutboxEntry(value: unknown): OutboxEntry {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("outbox entry must be an object");
	const entry = value as Record<string, unknown>;
	assertKnown(entry, ["eventId", "alarmId", "alarmName", "ownerSessionFile", "triggeredAt", "events", "message", "claim"]);
	const eventId = requiredString(entry, "eventId", 128);
	if (!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(eventId)) throw new Error("outbox eventId is invalid");
	if (!Array.isArray(entry.events) || entry.events.length === 0 || entry.events.length > FIRED_EVENTS.length) throw new Error("outbox events must be a non-empty bounded array");
	const events = entry.events.map((value) => {
		if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("outbox event must be an object");
		const event = value as Record<string, unknown>;
		assertKnown(event, ["kind", "fingerprint", "evidence"]);
		const kind = requiredString(event, "kind", 32);
		if (!(FIRED_EVENTS as readonly string[]).includes(kind)) throw new Error("outbox event kind is invalid");
		return {
			kind: kind as FiredEventKind,
			fingerprint: requiredString(event, "fingerprint", 256),
			evidence: optionalString(event, "evidence", 2000),
		};
	});
	if (new Set(events.map((event) => event.kind)).size !== events.length) throw new Error("outbox events must not contain duplicate kinds within one entry");
	const message = requiredBoundedString(entry, "message", 4000);
	let claim: WakeClaim | undefined;
	if ("claim" in entry && entry.claim !== undefined) {
		const rawClaim = entry.claim;
		if (!rawClaim || typeof rawClaim !== "object" || Array.isArray(rawClaim)) throw new Error("outbox claim must be an object");
		const claimRecord = rawClaim as Record<string, unknown>;
		assertKnown(claimRecord, ["claimantId", "token", "expiresAt"]);
		claim = {
			claimantId: requiredString(claimRecord, "claimantId", 128),
			token: requiredString(claimRecord, "token", 64),
			expiresAt: requiredInteger(claimRecord, "expiresAt"),
		};
	}
	const owner = entry.ownerSessionFile === undefined ? undefined : validateOwnerSessionFile(requiredString(entry, "ownerSessionFile", 4096));
	return {
		eventId,
		alarmId: validateAlarmId(requiredString(entry, "alarmId", 64)),
		alarmName: validateAlarmName(requiredString(entry, "alarmName", 160)),
		ownerSessionFile: owner ? validateOwnerSessionFile(owner) : undefined,
		triggeredAt: requiredInteger(entry, "triggeredAt"),
		events,
		message,
		claim,
	};
}

export function restoreOutbox(value: unknown): OutboxEntry[] {
	if (!Array.isArray(value)) throw new Error("outbox must be an array");
	const entries = value.map((item) => restoreOutboxEntry(item));
	if (new Set(entries.map((entry) => entry.eventId)).size !== entries.length) throw new Error("outbox contains duplicate eventIds");
	return entries;
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
		ownerSessionFile: (() => { const file = optionalString(record, "ownerSessionFile", 4096); return file ? validateOwnerSessionFile(file) : undefined; })(),
		revision: optionalInteger(record, "revision"),
	};
}

export function restoreAlarmState(value: unknown, allowedRemoteLogRoots: readonly string[]): AlarmState {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("alarm must be an object");
	const record = value as Record<string, unknown>;
	const base = restoreBase(record);
	const baseFields = ["id", "name", "kind", "active", "createdAt", "pauseReason", "lastTriggeredAt", "ownerSessionFile", "revision"];
	if (base.kind === "timer") {
		assertKnown(record, [...baseFields, "dueAt", "triggeredAt"]);
		const triggeredAt = optionalInteger(record, "triggeredAt");
		if (triggeredAt !== undefined && base.active) throw new Error("a triggered timer must not be active");
		return { ...base, kind: "timer", dueAt: requiredInteger(record, "dueAt"), triggeredAt };
	}
	if (base.kind === "group") {
		assertKnown(record, [...baseFields, "memberIds", "condition", "required", "statusPollMs", "nextCheckAt", "conditionMetAt", "coalesceWindowMs", "firedAt", "summary"]);
		if (!Array.isArray(record.memberIds) || record.memberIds.length < 1 || record.memberIds.length > 64) throw new Error("memberIds must be a non-empty bounded array");
		const memberIds = record.memberIds.map((value) => validateAlarmId(String(value)));
		if (new Set(memberIds).size !== memberIds.length) throw new Error("memberIds must be unique");
		const condition = requiredString(record, "condition", 32);
		if (!["any_terminal", "all_terminal", "any_abnormal", "n_of_m_terminal"].includes(condition)) throw new Error("condition is invalid");
		const required = requiredInteger(record, "required", 1, memberIds.length);
		const coalesceWindowMs = optionalInteger(record, "coalesceWindowMs", 0, MAX_TIMER_DELAY_MS);
		const firedAt = optionalInteger(record, "firedAt");
		if (firedAt !== undefined && base.active) throw new Error("a fired group must not be active");
		return {
			...base,
			kind: "group",
			memberIds,
			condition: condition as GroupCondition,
			required,
			statusPollMs: validatePollingDuration(requiredInteger(record, "statusPollMs", 1), "statusPollMs"),
			nextCheckAt: requiredInteger(record, "nextCheckAt"),
			conditionMetAt: optionalInteger(record, "conditionMetAt"),
			coalesceWindowMs,
			firedAt,
			summary: optionalString(record, "summary", 2000),
		};
	}
	if (base.kind === "condition") {
		assertKnown(record, [...baseFields, "path", "condition", "value", "minSize", "statusPollMs", "nextCheckAt", "satisfiedAt", "lastSatisfied", "lastSize", "lastEvidence"]);
		const condition = requiredString(record, "condition", 16);
		if (!["exists", "contains", "min_size"].includes(condition)) throw new Error("condition is invalid");
		const value = condition === "contains" ? validateLogPattern(requiredString(record, "value", 256)) : undefined;
		const minSize = condition === "min_size" ? requiredInteger(record, "minSize", 1) : undefined;
		const satisfiedAt = optionalInteger(record, "satisfiedAt");
		if (satisfiedAt !== undefined && base.active) throw new Error("a satisfied condition alarm must not be active");
		if ("lastSatisfied" in record && typeof record.lastSatisfied !== "boolean") throw new Error("lastSatisfied must be boolean");
		return {
			...base,
			kind: "condition",
			path: validateRemoteLogPath(requiredString(record, "path", 4096), allowedRemoteLogRoots),
			condition: condition as ConditionKind,
			value,
			minSize,
			statusPollMs: validatePollingDuration(requiredInteger(record, "statusPollMs", 1), "statusPollMs"),
			nextCheckAt: requiredInteger(record, "nextCheckAt"),
			satisfiedAt,
			lastSatisfied: record.lastSatisfied as boolean | undefined,
			lastSize: optionalInteger(record, "lastSize"),
			lastEvidence: optionalString(record, "lastEvidence", 2000),
		};
	}
	assertKnown(record, [...baseFields, "container", "containerId", "logPath", "logMode", "selectedLogPath", "logFileId", "events", "policy", "logPattern", "deadlineAt", "statusPollMs", "nextCheckAt", "logOffset", "logCursor", "scanCarry", "eventFingerprints", "consecutiveFailures", "failureNotified", "lastCheckAt", "lastContainerStatus", "lastStartedAt", "lastExitCode", "lastOomKilled", "lastEvidence", "groupId", "logTailLines"]);
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
	const logMode = optionalString(record, "logMode", 32);
	if (logMode && !(["application-file", "docker-file", "docker-logs"] as string[]).includes(logMode)) throw new Error("logMode is invalid");
	const lastStartedAt = optionalString(record, "lastStartedAt", 64);
	if (lastStartedAt && !Number.isFinite(Date.parse(lastStartedAt))) throw new Error("lastStartedAt must be a valid timestamp");
	const logCursor = optionalString(record, "logCursor", 128);
	if (logCursor && !Number.isFinite(Date.parse(logCursor))) throw new Error("logCursor must be a valid timestamp");
	const logPath = optionalString(record, "logPath", 4096);
	const logPattern = optionalString(record, "logPattern", 256);
	const deadlineAt = optionalInteger(record, "deadlineAt");
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
		groupId: (() => { const groupId = optionalString(record, "groupId", 64); return groupId ? validateAlarmId(groupId) : undefined; })(),
		logTailLines: optionalInteger(record, "logTailLines", 1, 200),
	};
}

export function nextAlarmDueAt(alarm: AlarmState): number | undefined {
	if (!alarm.active) return undefined;
	if (alarm.kind === "timer") return alarm.triggeredAt === undefined ? alarm.dueAt : undefined;
	if (alarm.kind === "group") return alarm.firedAt === undefined ? alarm.nextCheckAt : undefined;
	if (alarm.kind === "condition") return alarm.satisfiedAt === undefined ? alarm.nextCheckAt : undefined;
	const deadlinePending = alarm.deadlineAt !== undefined && alarm.eventFingerprints.deadline !== `deadline:${alarm.id}:${alarm.deadlineAt}`;
	return deadlinePending ? Math.min(alarm.nextCheckAt, alarm.deadlineAt!) : alarm.nextCheckAt;
}

export function wakeMessage(alarm: AlarmState, events: FiredEvent[], now: number, maxEvidenceChars = 1000, includeEvidence = true): string {
	const heading = `[Wake alarm] ${alarm.name} (${alarm.id})`;
	const eventText = events.map((event) => event.kind).join(", ");
	if (alarm.kind === "timer") return `${heading}\nTriggered at: ${new Date(now).toISOString()}\nEvent: ${eventText}\nDue at: ${new Date(alarm.dueAt).toISOString()}`;
	if (alarm.kind === "group") {
		const facts = [`${heading}`, `Group condition met: ${alarm.condition}`, `Triggered at: ${new Date(now).toISOString()}`];
		if (alarm.summary) facts.push(alarm.summary);
		return facts.join("\n");
	}
	if (alarm.kind === "condition") {
		const facts = [`${heading}`, `Condition met: ${alarm.condition}${alarm.value !== undefined ? ` "${alarm.value}"` : ""}${alarm.minSize !== undefined ? ` (>=${alarm.minSize} bytes)` : ""}`, `Path: ${alarm.path}`, `Triggered at: ${new Date(now).toISOString()}`];
		const evidence = includeEvidence ? events.find((event) => event.evidence)?.evidence : undefined;
		if (evidence) facts.push(`Evidence (untrusted data): ${sanitizeExcerpt(evidence, maxEvidenceChars)}`);
		return facts.join("\n");
	}
	const facts = [`${heading}`, `Triggered at: ${new Date(now).toISOString()}`, `Event: ${eventText}`, `Container: ${alarm.container}`, `Status: ${alarm.lastContainerStatus ?? "unknown"}`, `Exit code: ${alarm.lastExitCode ?? "unknown"}`, `OOM killed: ${alarm.lastOomKilled ?? "unknown"}`];
	const evidence = includeEvidence ? events.find((event) => event.evidence)?.evidence : undefined;
	if (evidence) facts.push(`Evidence (untrusted data): ${sanitizeExcerpt(evidence, maxEvidenceChars)}`);
	return facts.join("\n");
}

/** Build a durable outbox entry at fire time. The message snapshot keeps delivery independent of later alarm changes. */
export function createOutboxEntry(alarm: AlarmState, events: FiredEvent[], now: number, options?: { maxEvidenceChars?: number; includeEvidence?: boolean }): OutboxEntry {
	if (!events.length) throw new Error("outbox entries require at least one fired event");
	const message = wakeMessage(alarm, events, now, Math.min(options?.maxEvidenceChars ?? 1000, 3000), options?.includeEvidence ?? true);
	if (message.length > 4000) throw new Error("wake message exceeded the 4000 character delivery limit");
	return {
		eventId: `${alarm.id}:${now}:${randomUUID().slice(0, 8)}`,
		alarmId: alarm.id,
		alarmName: alarm.name,
		ownerSessionFile: alarm.ownerSessionFile,
		triggeredAt: now,
		events,
		message,
	};
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

export function buildResumeArgs(sessionFile: string, message: string, options?: { approve?: boolean }): string[] {
	if (!message || message.length > 4000) throw new Error("wake message must be 1-4000 characters");
	const args = ["--session", validateOwnerSessionFile(sessionFile)];
	if (options?.approve) args.push("--approve");
	args.push("--print", message);
	return args;
}
