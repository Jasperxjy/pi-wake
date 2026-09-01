import { promises as fs } from "node:fs";
import path from "node:path";
import { deflateSync } from "node:zlib";
import { randomUUID } from "node:crypto";
import { pidAlive } from "./presence.ts";
import { StateLock, type LockTestingHooks } from "./lock.ts";
import {
	applyBaseline,
	applyCheckFailure,
	applyContainerDeadline,
	applyProbe,
	applyTimer,
	createConditionAlarm,
	createContainerAlarm,
	createGroupAlarm,
	createOutboxEntry,
	createTimerAlarm,
	formatLocalTime,
	tailLines,
	decodeNewLog,
	nextAlarmDueAt,
	parseAbsoluteTime,
	parseDuration,
	restoreAlarmState,
	restoreOutbox,
	resumeAlarm,
	sanitizeExcerpt,
	persistedEvidence,
	clampPersistedState,
	timerDelay,
	validateAlarmId,
	validateContainer,
	validateHost,
	validateLogFileId,
	validatePollingDuration,
	validateRemoteLogPath,
	validateRemoteLogRoots,
	validateUser,
	wakeMessage,
	type AlarmState,
	type ConditionAlarmState,
	type ConditionKind,
	type ContainerAlarmState,
	type ContainerEventKind,
	type FiredEvent,
	type GroupAlarmState,
	type GroupCondition,
	type OutboxEntry,
	type ProbeResult,
} from "./core.ts";

export interface RemoteConfig {
	host: string;
	user: string;
	port: number;
	identityFile: string;
	identityPath: string;
	allowedRemoteLogRoots: string[];
	sshAttempts: number;
	sshBackoffMs: number;
	connectTimeoutSeconds: number;
	maxConsecutiveFailures: number;
}

export interface RuntimeConfig {
	statusPollMs: number;
	maxLogBytes: number;
	maxEvidenceChars: number;
	/** Hard cap on undelivered outbox entries; overflow pauses the producing alarm instead of silently dropping. */
	maxOutboxEntries: number;
	maxOutboxEntriesPerAlarm: number;
	remote?: RemoteConfig;
	piCommand?: string;
	spawnOnWake: boolean;
	/** Sessions auto-start the project daemon when no live daemon heartbeat exists (set false to manage the daemon yourself). */
	spawnDaemon: boolean;
	/** Status bar / widget display language: "auto" (system locale), "en", or "zh". Default "auto". */
	uiLanguage: "auto" | "en" | "zh";
	runTimeoutMs: number;
	headlessTrust: "saved" | "always";
	includeWakeEvidence: boolean;
}

/**
 * Durable shared state, version 3. The on-disk file is the source of truth;
 * each runtime keeps only a cache of it.
 *
 *   alarms — the current world: every alarm and its latest revision
 *   outbox — facts that HAPPENED but have not yet been delivered: one entry per
 *            wake occurrence, independent of the alarm's current state, so a
 *            later pause/reset/remove can never corrupt or lose a wake, and one
 *            event kind may legitimately appear in many entries.
 */
export interface StoredState {
	version: 3;
	alarms: AlarmState[];
	outbox: OutboxEntry[];
}

export interface ToolParams {
	action: "set_timer" | "watch_container" | "watch_container_group" | "watch_condition" | "list" | "check" | "pause" | "resume" | "reset" | "remove" | "evidence" | "list_wakes" | "drop_wake" | "purge_wakes" | "ack" | "set_language";
	id?: string;
	name?: string;
	after?: string;
	at?: string;
	container?: string;
	containers?: string[];
	events?: ContainerEventKind[];
	policy?: "pause" | "keep";
	logPath?: string;
	logPattern?: string;
	deadline?: string;
	statusPoll?: string;
	eventId?: string;
	condition?: string;
	required?: number;
	coalesceWindow?: string;
	logTailLines?: number;
	path?: string;
	value?: string;
	minSize?: number;
	/** set_language only: display language for the status bar and widget ("auto" follows the system locale). */
	language?: "auto" | "en" | "zh";
	/** Absolute ISO timestamp or relative duration; condition files older than this never satisfy. */
	ignoreBefore?: string;
	purgePendingEvents?: boolean;
}

export interface ActionContext {
	ownerSessionFile?: string;
}

export type ExecFn = (file: string, args: string[], options: { signal: AbortSignal; timeout: number }) => Promise<{ stdout: string; stderr: string; code: number }>;

/** Returns false when the wake could not be delivered; the outbox record is then kept for retry. */
export type EmitFn = (entry: OutboxEntry) => Promise<boolean | void> | boolean | void;

export interface WakeRetryPolicy {
	delayMs: number;
	capMs: number;
}

export interface RuntimeOptions {
	cwd: string;
	configPath?: string;
	statePath?: string;
	emit: EmitFn;
	execFn: ExecFn;
	/** When false the runtime loads state and serves actions but never schedules or fires alarms. */
	schedulingEnabled?: boolean;
	/**
	 * Ownership routing: only matching alarms (and their outbox entries) are
	 * scheduled or wake-flushed by this runtime. Sessions match their own
	 * ownerSessionFile (plus ownerless alarms when they lead the live-presence
	 * registry); the daemon matches alarms whose owner session is not live.
	 * Correctness never depends on this filter — the atomic wake claim under the
	 * state lock serializes any routing overlap.
	 */
	owns?: (alarm: Pick<AlarmState, "ownerSessionFile">) => boolean;
	/** Identity used in wake-delivery claims. Defaults to a per-runtime unique id. */
	claimantId?: string;
	/** How long a delivery claim stays valid; another claimant may take over after expiry. Defaults to 60s. */
	deliveryTtlMs?: number | (() => number);
	/** Backoff applied to undeliverable wakes (linear, capped). Default: 5s delay, 30m cap. */
	wakeRetry?: WakeRetryPolicy;
	/**
	 * Deferred delivery completion: emit() returning true only means "handed to
	 * the host" — the outbox entry is kept (claim held) until confirmDelivery()
	 * is called with proof the message entered the conversation. Host-side loss
	 * (abort clears queued messages; crash evaporates them) therefore falls back
	 * to redelivery instead of losing the wake. onDeliveryCycleSettled() releases
	 * unconfirmed pendings for a backoff retry when the agent run ends without
	 * having echoed them. The daemon does not use this: its emit resolves only
	 * after the woken process exited 0, which already proves persistence.
	 */
	deferDeliveryCompletion?: boolean;
	/** Test hooks for the state-lock stale takeover. */
	lockHooks?: LockTestingHooks;
}

const CONFIG_NAME = "wake-alarm.json";
const STATE_NAME = "wake-alarm.state.json";
const MAX_SSH_OUTPUT = 512 * 1024;
const PROBE_SCRIPT = String.raw`
import base64,datetime,json,os,posixpath,subprocess,sys

def out(value):
    print(json.dumps(value,separators=(",",":")))

def utc_cursor():
    now=datetime.datetime.now(datetime.timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.")+f"{now.microsecond:06d}000Z"

def allowed_file_path(value,roots):
    if not isinstance(value,str) or not value.startswith("/") or posixpath.normpath(value) != value:
        return False
    candidate=os.path.realpath(value)
    for root in roots:
        resolved_root=os.path.realpath(root)
        try:
            if os.path.commonpath([candidate,resolved_root]) == resolved_root:
                return True
        except ValueError:
            pass
    return False

try:
    req=json.loads(base64.b64decode(sys.argv[1],validate=True).decode("utf-8"))
    roots=req.get("allowedRemoteLogRoots") or []
    if req.get("conditionPath"):
        cpath=req["conditionPath"]
        if not allowed_file_path(cpath,roots):
            raise ValueError("condition path is outside allowedRemoteLogRoots")
        try:
            cstat=os.stat(cpath)
            csize=cstat.st_size
            with open(cpath,"rb",buffering=0) as handle:
                handle.seek(max(0,csize-int(req["tailBytes"])))
                ctail=handle.read(int(req["tailBytes"]))
            out({"exists":True,"size":csize,"mtime":int(cstat.st_mtime*1000),"tailBase64":base64.b64encode(ctail).decode("ascii")})
        except OSError:
            out({"exists":False,"size":0,"tailBase64":""})
        sys.exit(0)
    name=req["container"]
    app_path=req.get("logPath")
    if app_path and not allowed_file_path(app_path,roots):
        raise ValueError("application logPath is outside allowedRemoteLogRoots")
    inspected=subprocess.run(["docker","inspect",name],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=12)
    if inspected.returncode != 0:
        message=inspected.stderr.decode("utf-8","replace")[-500:]
        if "No such" in message or "not found" in message.lower():
            out({"exists":False,"running":False,"status":"missing","containerStatus":"missing","logOffset":req["offset"],"tail":"","logMode":None,"selectedLogPath":app_path})
            sys.exit(0)
        raise RuntimeError("docker inspect failed: "+message)
    info=json.loads(inspected.stdout)[0]
    cid=info.get("Id","")
    state=info.get("State") or {}
    expected=req.get("expectedId")
    if expected and cid != expected:
        out({"exists":False,"containerId":cid,"running":False,"status":"replaced","containerStatus":"replaced","exitCode":state.get("ExitCode"),"oomKilled":bool(state.get("OOMKilled")),"startedAt":state.get("StartedAt"),"logOffset":req["offset"],"tail":"","logMode":None,"selectedLogPath":app_path})
        sys.exit(0)
    if not req.get("readLogs"):
        out({"exists":True,"containerId":cid,"running":bool(state.get("Running")),"status":state.get("Status","unknown"),"containerStatus":state.get("Status","unknown"),"startedAt":state.get("StartedAt"),"exitCode":state.get("ExitCode"),"oomKilled":bool(state.get("OOMKilled")),"logOffset":req["offset"],"logBase64":"","tailBase64":""})
        sys.exit(0)
    docker_log_path=info.get("LogPath") or ""
    baseline=bool(req.get("baseline"))
    requested=int(req.get("offset",0))
    max_bytes=int(req["maxBytes"])
    tail_bytes=int(req["tailBytes"])
    cursor=req.get("cursor")
    next_cursor=cursor
    expected_file_id=req.get("fileId")
    next_file_id=None
    file_error=None
    selected_path=None
    mode=None
    candidates=[]
    if app_path:
        candidates.append((app_path,"application-file"))
    if docker_log_path and docker_log_path != app_path:
        candidates.append((docker_log_path,"docker-file"))
    for candidate,candidate_mode in candidates:
        try:
            stat=os.stat(candidate)
            size=stat.st_size
            candidate_file_id=f"{stat.st_dev}:{stat.st_ino}"
            reset=requested > size or bool(expected_file_id and expected_file_id != candidate_file_id)
            start=max(0,size-tail_bytes) if baseline else (0 if reset else requested)
            with open(candidate,"rb",buffering=0) as handle:
                handle.seek(start)
                data=handle.read(max_bytes)
                next_offset=size if baseline else start+len(data)
                tail_start=max(0,size-tail_bytes)
                handle.seek(tail_start)
                tail=handle.read(tail_bytes)
            mode=candidate_mode
            selected_path=candidate
            next_file_id=candidate_file_id
            next_cursor=None
            break
        except OSError as exc:
            if candidate_mode == "application-file":
                raise RuntimeError("configured application log unavailable: "+str(exc))
            file_error=exc
    if mode is None:
        mode="docker-logs"
        reset=False
        fence=utc_cursor()
        args=["docker","logs","--timestamps"]
        if baseline or not cursor:
            args += ["--tail","200"]
        else:
            args += ["--since",str(cursor)]
        args.append(cid)
        proc=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        chunks=[]
        used=0
        latest=str(cursor or "")
        overflow=False
        assert proc.stdout is not None
        while True:
            line=proc.stdout.readline()
            if not line:
                break
            stamp,sep,_rest=line.partition(b" ")
            stamp_text=stamp.decode("ascii","ignore")
            if not sep or "T" not in stamp_text:
                continue
            if cursor and stamp_text <= str(cursor):
                continue
            if used+len(line) > max_bytes:
                overflow=True
                proc.terminate()
                break
            chunks.append(line)
            used += len(line)
            if stamp_text > latest:
                latest=stamp_text
        try:
            return_code=proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill(); return_code=proc.wait()
        if return_code not in (0,-15):
            detail=str(file_error) if file_error else "no readable log file"
            raise RuntimeError("docker logs failed after file probe: "+detail)
        data=b"".join(chunks)
        tail=data[-tail_bytes:]
        if req.get("tailLinesReq"):
            proc2=subprocess.run(["docker","logs","--tail",str(int(req["tailLinesReq"])),cid],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=12)
            if proc2.returncode in (0,-15):
                tail=proc2.stdout[-tail_bytes:]
        next_offset=requested+len(data)
        next_file_id=None
        if baseline:
            next_cursor=fence
        elif overflow:
            next_cursor=latest or cursor
        else:
            next_cursor=max(latest,fence)
    out({
        "exists":True,
        "containerId":cid,
        "running":bool(state.get("Running")),
        "status":state.get("Status","unknown"),
        "containerStatus":state.get("Status","unknown"),
        "startedAt":state.get("StartedAt"),
        "exitCode":state.get("ExitCode"),
        "oomKilled":bool(state.get("OOMKilled")),
        "logMode":mode,
        "selectedLogPath":selected_path,
        "logFileId":next_file_id,
        "logOffset":next_offset,
        "logCursor":next_cursor,
        "logReset":reset,
        "logBase64":base64.b64encode(data).decode("ascii"),
        "tailBase64":base64.b64encode(tail).decode("ascii")
    })
except Exception as exc:
    out({"probeError":str(exc)[:800]})
`;

const PROBE_LOADER = `import base64,zlib;exec(zlib.decompress(base64.b64decode("${deflateSync(Buffer.from(PROBE_SCRIPT)).toString("base64")}")))`;
export const ACTION_ENUM = ["set_timer", "watch_container", "watch_container_group", "watch_condition", "list", "check", "pause", "resume", "reset", "remove", "evidence", "list_wakes", "drop_wake", "purge_wakes", "ack", "set_language"] as const;
const ACTION_FIELDS: Record<ToolParams["action"], readonly (keyof ToolParams)[]> = {
	set_timer: ["action", "id", "name", "after", "at"],
	watch_container: ["action", "id", "name", "container", "events", "policy", "logPath", "logPattern", "deadline", "statusPoll", "logTailLines"],
	watch_container_group: ["action", "id", "name", "containers", "condition", "required", "coalesceWindow", "statusPoll", "logTailLines"],
	watch_condition: ["action", "id", "name", "path", "condition", "value", "minSize", "ignoreBefore", "statusPoll"],
	list: ["action"],
	check: ["action", "id"],
	pause: ["action", "id"],
	resume: ["action", "id"],
	reset: ["action", "id", "after", "at"],
	remove: ["action", "id", "purgePendingEvents"],
	evidence: ["action", "id"],
	list_wakes: ["action"],
	drop_wake: ["action", "eventId"],
	purge_wakes: ["action", "id"],
	ack: ["action", "id"],
	set_language: ["action", "language"],
};

function shellSingleQuote(value: string): string {
	return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function asInt(value: unknown, name: string, min: number, max: number): number {
	if (!Number.isSafeInteger(value) || (value as number) < min || (value as number) > max) throw new Error(`${name} must be an integer from ${min} to ${max}`);
	return value as number;
}

/** ignoreBefore accepts an absolute ISO timestamp ("2026-08-27T00:00:00Z") or a
 * relative duration ("5m" = files older than 5 minutes ago). Both bound the
 * same hazard: a stale marker from a previous run must not satisfy the watch. */
function parseIgnoreBefore(value: string): number {
	const trimmed = value.trim();
	if (/(?:[zZ]|[+-]\d{2}:?\d{2})$/.test(trimmed)) return parseAbsoluteTime(trimmed, "ignoreBefore");
	const cutoff = Date.now() - parseDuration(trimmed, "ignoreBefore");
	if (!Number.isSafeInteger(cutoff) || cutoff < 0) throw new Error("ignoreBefore must resolve to a non-negative epoch-ms timestamp");
	return cutoff;
}

async function parseRemoteConfig(value: unknown, configDir: string): Promise<RemoteConfig | undefined> {
	if (value === undefined) return undefined;
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("remote must be an object with host, user, and identityFile");
	const parsed = value as Record<string, unknown>;
	const KNOWN_REMOTE = ["host", "user", "port", "identityFile", "allowedRemoteLogRoots", "sshAttempts", "sshBackoffMs", "connectTimeoutSeconds", "maxConsecutiveFailures"];
	const unknownRemote = Object.keys(parsed).filter((key) => !KNOWN_REMOTE.includes(key));
	if (unknownRemote.length) throw new Error(`remote contains unknown field(s): ${unknownRemote.join(", ")}`);
	for (const forbidden of ["password", "passphrase", "privateKey", "privateKeyContents"]) if (forbidden in parsed) throw new Error(`remote accepts an identityFile path only; ${forbidden} is forbidden`);
	const identityFile = String(parsed.identityFile ?? "");
	if (!identityFile || identityFile.includes("\0")) throw new Error("remote.identityFile must be a private key path");
	const identityPath = path.resolve(configDir, identityFile);
	if (identityPath.endsWith(".pub")) throw new Error("remote.identityFile must refer to the private key path, not the .pub file");
	await fs.access(identityPath);
	return {
		host: validateHost(String(parsed.host ?? "")),
		user: validateUser(String(parsed.user ?? "")),
		port: asInt(parsed.port ?? 22, "remote.port", 1, 65535),
		identityFile,
		identityPath,
		allowedRemoteLogRoots: parsed.allowedRemoteLogRoots === undefined ? [] : validateRemoteLogRoots(parsed.allowedRemoteLogRoots),
		sshAttempts: asInt(parsed.sshAttempts ?? 3, "remote.sshAttempts", 1, 5),
		sshBackoffMs: asInt(parsed.sshBackoffMs ?? 1000, "remote.sshBackoffMs", 0, 30_000),
		connectTimeoutSeconds: asInt(parsed.connectTimeoutSeconds ?? 10, "remote.connectTimeoutSeconds", 1, 60),
		maxConsecutiveFailures: asInt(parsed.maxConsecutiveFailures ?? 3, "remote.maxConsecutiveFailures", 1, 20),
	};
}

/**
 * Remote-section guard with a coach-style error: instead of only stating that
 * the config is missing, the message carries the minimal working shape and the
 * one non-obvious semantic (identityFile resolves relative to the .pi/ dir).
 */
function requireRemote(action: string, config: RuntimeConfig): RemoteConfig {
	if (config.remote) return config.remote;
	throw new Error(`${action} probes Docker/files on a REMOTE host over SSH, which needs a "remote" section in .pi/${CONFIG_NAME}, e.g. {"remote":{"host":"gpu.example.com","user":"me","identityFile":"id_rsa","allowedRemoteLogRoots":["/data/results/"]}} — identityFile resolves RELATIVE to the .pi/ directory (use "../keys/id_rsa" for a key stored elsewhere); key auth only, no passwords`);
}

function validateActionParams(params: ToolParams): void {
	const allowed = new Set<string>(ACTION_FIELDS[params.action]);
	const irrelevant = Object.entries(params).filter(([key, value]) => !allowed.has(key) && value !== undefined).map(([key]) => key);
	if (irrelevant.length) {
		const accepted = ACTION_FIELDS[params.action].filter((field) => field !== "action").join(", ");
		throw new Error(`${params.action} does not accept: ${irrelevant.join(", ")} (accepted: ${accepted})`);
	}
}

/**
 * Read-only snapshot for diagnosis when no runtime is alive (e.g. the extension
 * failed its session_start, or the agent is inspecting a foreign project).
 * Best effort: an unreadable state yields an explicit error text, never a throw,
 * and never schedules or delivers anything.
 */
export async function readOnlySnapshot(options: { cwd: string; statePath?: string; filterId?: string }): Promise<string> {
	const statePath = options.statePath ?? path.join(options.cwd, ".pi", STATE_NAME);
	const header = (body: string): string => `[read-only, no live session] ${body}`;
	let saved: StoredState | undefined;
	try { saved = await readStoredState(statePath); }
	catch (error) { return header(`cannot read ${path.basename(statePath)}: ${(error as Error).message}; the state file must be repaired before alarms can be restored`); }
	if (!saved) return header("no state file yet — nothing is on disk");
	const alarms = options.filterId ? saved.alarms.filter((alarm) => alarm.id === options.filterId || alarm.id.startsWith(`${options.filterId}-`)) : saved.alarms;
	if (!alarms.length) return header(options.filterId ? `unknown alarm: ${options.filterId}` : "0 alarms");
	const lines = alarms.map((alarm) => { try { return alarmSummary(alarm); } catch { return `${alarm.id ?? "?"}: (summary unavailable) ${JSON.stringify(alarm).slice(0, 300)}`; } });
	if (!options.filterId && saved.alarms.length > alarms.length) lines.push(`(plus ${saved.alarms.length - alarms.length} filtered alarm(s))`);
	if (saved.outbox.length) lines.push(`${saved.outbox.length} undelivered wake(s) in the outbox — see list_wakes in a live session`);
	return header(lines.join("\n"));
}

/** Read the stored alarm list and outbox without validation; undefined when the state file is absent. One retry tolerates a rename race. */
export async function readStoredState(statePath: string): Promise<StoredState | undefined> {
	let text: string;
	try { text = await fs.readFile(statePath, "utf8"); }
	catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
	let saved: Record<string, unknown>;
	try { saved = JSON.parse(text) as Record<string, unknown>; }
	catch {
		await new Promise((resolve) => setTimeout(resolve, 50));
		saved = JSON.parse(await fs.readFile(statePath, "utf8")) as Record<string, unknown>;
	}
	if (saved.version !== 3 || !Array.isArray(saved.alarms) || !Array.isArray(saved.outbox)) throw new Error("unsupported state content");
	return saved as unknown as StoredState;
}

export interface AlarmDigestEntry {
	id: string;
	name: string;
	kind: AlarmState["kind"];
	active: boolean;
	/** Canonical English status line (logs, tests, and a display fallback). */
	detail: string;
	/** Language-neutral facts so the display layer can render any locale. */
	dueInMs?: number;
	containerStatus?: string;
	failures?: number;
	conditionSize?: number;
	conditionSatisfied?: boolean;
}

export interface AlarmDigest {
	active: number;
	paused: number;
	pendingWakes: number;
	nextDue?: { id: string; name: string; inMs: number };
	entries: AlarmDigestEntry[];
}

/** Compact human delay for display: "40s", "4m", "1h 5m", "3d 4h". */
export function formatDelay(ms: number): string {
	const abs = Math.max(0, Math.round(ms / 1000));
	if (abs < 60) return `${abs}s`;
	const minutes = Math.floor(abs / 60);
	if (minutes < 60) return `${minutes}m`;
	const hours = Math.floor(minutes / 60);
	if (hours < 24) return `${hours}h ${minutes % 60}m`;
	return `${Math.floor(hours / 24)}d ${hours % 24}h`;
}

export function alarmSummary(alarm: AlarmState): string {
	const lifecycle = alarm.active ? "active" : `paused${alarm.pauseReason ? ` (${alarm.pauseReason})` : ""}`;
	if (alarm.kind === "timer") return `${alarm.id}: ${alarm.name} — timer ${lifecycle}; due=${formatLocalTime(alarm.dueAt)}${alarm.triggeredAt === undefined ? "" : `; fired=${formatLocalTime(alarm.triggeredAt)}`}`;
	if (alarm.kind === "group") return `${alarm.id}: ${alarm.name} — group ${lifecycle}; condition=${alarm.condition}${alarm.condition === "n_of_m_terminal" ? ` (${alarm.required}/${alarm.memberIds.length})` : ` (${alarm.memberIds.length} members)`}; ${alarm.summary ?? "not yet evaluated"}${alarm.firedAt !== undefined ? `; fired=${formatLocalTime(alarm.firedAt)}` : ""}`;
	if (alarm.kind === "condition") return `${alarm.id}: ${alarm.name} — condition ${lifecycle}; ${alarm.condition}${alarm.value !== undefined ? ` "${alarm.value}"` : ""}${alarm.minSize !== undefined ? ` >=${alarm.minSize}B` : ""} on ${alarm.path}; size=${alarm.lastSize ?? "unknown"}${alarm.satisfiedAt !== undefined ? `; satisfied=${formatLocalTime(alarm.satisfiedAt)}` : ""}`;
	const source = alarm.selectedLogPath ?? alarm.logPath ?? "Docker stdout";
	return `${alarm.id}: ${alarm.name} — container ${lifecycle}; target=${alarm.container}; events=${alarm.events.join(",")}; policy=${alarm.policy}; status=${alarm.lastContainerStatus ?? "unchecked"}; log=${alarm.logMode ?? "pending"}:${source}; failures=${alarm.consecutiveFailures}`;
}

export class WakeAlarmRuntime {
	private readonly options: RuntimeOptions;
	private readonly schedulingEnabled: boolean;
	private config: RuntimeConfig | undefined;
	private readonly statePath: string;
	private readonly stateLock: StateLock;
	private readonly alarms = new Map<string, AlarmState>();
	private readonly outbox = new Map<string, OutboxEntry>();
	private scheduler: ReturnType<typeof setTimeout> | undefined;
	private stopped = true;
	private initialized = false;
	private operation: Promise<void> = Promise.resolve();
	private retiredLegacyState = false;
	private readonly controllers = new Set<AbortController>();
	private readonly wakeRetry = new Map<string, { attempts: number; nextAt: number }>();
	/** Entries handed to the host but not yet confirmed as part of the conversation. */
	private readonly pendingConfirmations = new Map<string, { token: string; sentAt: number }>();
	private readonly dirtyIds = new Set<string>();
	private readonly deletedIds = new Set<string>();
	private readonly createIds = new Set<string>();
	private readonly forceIds = new Set<string>();
	private readonly intentMap = new Map<string, (disk: AlarmState) => AlarmState>();
	private readonly baseRevisions = new Map<string, number>();
	private readonly claimantId: string;

	constructor(options: RuntimeOptions) {
		this.options = options;
		this.schedulingEnabled = options.schedulingEnabled !== false;
		this.statePath = options.statePath ?? path.join(options.cwd, ".pi", STATE_NAME);
		this.claimantId = options.claimantId ?? `runtime:${process.pid}:${randomUUID().slice(0, 8)}`;
		this.stateLock = new StateLock({ path: `${this.statePath}.lock`, hooks: options.lockHooks });
	}

	get alarmCount(): number {
		return this.alarms.size;
	}

	get outboxCount(): number {
		return this.outbox.size;
	}

	get retiredLegacy(): boolean {
		return this.retiredLegacyState;
	}

	/**
	 * Read-only display snapshot for UI surfaces (status bar / widget): counts,
	 * the next deterministic deadline, and one line per active alarm. Pure
	 * in-memory — no locks, no disk, safe to call at UI refresh cadence.
	 */
	alarmDigest(): AlarmDigest {
		const entries: AlarmDigestEntry[] = [];
		let nextDue: AlarmDigest["nextDue"];
		for (const alarm of this.alarms.values()) {
			if (!alarm.active) continue;
			let detail = "";
			const facts: Partial<AlarmDigestEntry> = {};
			let inMs: number | undefined;
			if (alarm.kind === "timer") {
				inMs = alarm.dueAt - Date.now();
				detail = inMs >= 0 ? `in ${formatDelay(inMs)}` : `overdue ${formatDelay(-inMs)}`;
				facts.dueInMs = inMs;
				if (nextDue === undefined || inMs < nextDue.inMs) nextDue = { id: alarm.id, name: alarm.name, inMs };
			} else if (alarm.kind === "container") {
				const fails = alarm.consecutiveFailures > 0 ? ` · fail ${alarm.consecutiveFailures}` : "";
				detail = `${alarm.lastContainerStatus ?? "unchecked"}${fails}`;
				facts.containerStatus = alarm.lastContainerStatus ?? "unchecked";
				facts.failures = alarm.consecutiveFailures;
			} else if (alarm.kind === "group") {
				detail = alarm.summary ?? `${alarm.memberIds.length} members`;
			} else {
				detail = alarm.satisfiedAt !== undefined ? "satisfied" : `waiting · ${alarm.lastSize ?? "?"}B`;
				facts.conditionSatisfied = alarm.satisfiedAt !== undefined;
				facts.conditionSize = alarm.lastSize;
			}
			entries.push({ id: alarm.id, name: alarm.name, kind: alarm.kind, active: true, detail, ...facts });
		}
		entries.sort((a, b) => (a.kind === "timer" ? 0 : 1) - (b.kind === "timer" ? 0 : 1) || a.id.localeCompare(b.id));
		return {
			active: entries.length,
			paused: [...this.alarms.values()].filter((alarm) => !alarm.active).length,
			pendingWakes: this.outbox.size,
			nextDue,
			entries,
		};
	}

	get runtimeConfig(): RuntimeConfig {
		if (!this.config) throw new Error("wake alarm config is unavailable");
		return this.config;
	}

	private serialize<T>(fn: () => Promise<T>): Promise<T> {
		const result = this.operation.then(fn, fn);
		this.operation = result.then(() => undefined, () => undefined);
		return result;
	}

	private async initialize(): Promise<void> {
		if (this.initialized) return;
		const cwd = this.options.cwd;
		const configPath = this.options.configPath ?? path.join(cwd, ".pi", CONFIG_NAME);
		let parsed: Record<string, unknown> = {};
		try {
			parsed = JSON.parse(await fs.readFile(configPath, "utf8")) as Record<string, unknown>;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new Error(`Cannot read ${CONFIG_NAME}: ${(error as Error).message}`);
		}
		const KNOWN_KEYS = ["remote", "statusPoll", "maxLogBytes", "maxEvidenceChars", "maxOutboxEntries", "maxOutboxEntriesPerAlarm", "piCommand", "spawnOnWake", "spawnDaemon", "runTimeout", "headlessTrust", "includeWakeEvidence", "uiLanguage"];
		const unknownKeys = Object.keys(parsed).filter((key) => !KNOWN_KEYS.includes(key));
		if (unknownKeys.length) throw new Error(`${CONFIG_NAME} contains unknown field(s): ${unknownKeys.join(", ")}`);
		for (const retired of ["semanticReview", "maximumRuntime"]) if (retired in parsed) throw new Error(`${retired} is retired; wake alarms fire only for explicitly configured timers or conditions`);
		if (parsed.piCommand !== undefined && (typeof parsed.piCommand !== "string" || !parsed.piCommand || parsed.piCommand.length > 512 || parsed.piCommand.includes("\0"))) throw new Error("piCommand must be a non-empty command path no longer than 512 characters");
		if (parsed.spawnOnWake !== undefined && typeof parsed.spawnOnWake !== "boolean") throw new Error("spawnOnWake must be boolean");
		if (parsed.spawnDaemon !== undefined && typeof parsed.spawnDaemon !== "boolean") throw new Error("spawnDaemon must be boolean");
		if (parsed.uiLanguage !== undefined && parsed.uiLanguage !== "auto" && parsed.uiLanguage !== "en" && parsed.uiLanguage !== "zh") throw new Error("uiLanguage must be \"auto\", \"en\", or \"zh\"");
		if (parsed.includeWakeEvidence !== undefined && typeof parsed.includeWakeEvidence !== "boolean") throw new Error("includeWakeEvidence must be boolean");
		if (parsed.headlessTrust !== undefined && parsed.headlessTrust !== "saved" && parsed.headlessTrust !== "always") throw new Error("headlessTrust must be \"saved\" or \"always\"");
		const maxOutboxEntries = asInt(parsed.maxOutboxEntries ?? 1000, "maxOutboxEntries", 1, 100_000);
		// The per-alarm default follows the global cap, so setting only maxOutboxEntries
		// to a small value cannot produce a per-alarm default that exceeds it.
		const maxOutboxEntriesPerAlarm = parsed.maxOutboxEntriesPerAlarm === undefined
			? Math.min(100, maxOutboxEntries)
			: asInt(parsed.maxOutboxEntriesPerAlarm, "maxOutboxEntriesPerAlarm", 1, maxOutboxEntries);
		this.config = {
			statusPollMs: validatePollingDuration(parseDuration((parsed.statusPoll ?? "60s") as string | number, "statusPoll")),
			maxLogBytes: asInt(parsed.maxLogBytes ?? 65_536, "maxLogBytes", 1024, 262_144),
			maxEvidenceChars: asInt(parsed.maxEvidenceChars ?? 1000, "maxEvidenceChars", 100, 4000),
			maxOutboxEntries,
			maxOutboxEntriesPerAlarm,
			remote: await parseRemoteConfig(parsed.remote, path.dirname(configPath)),
			piCommand: parsed.piCommand as string | undefined,
			spawnOnWake: parsed.spawnOnWake === undefined ? true : (parsed.spawnOnWake as boolean),
			spawnDaemon: parsed.spawnDaemon === undefined ? true : (parsed.spawnDaemon as boolean),
			uiLanguage: parsed.uiLanguage === undefined ? "auto" : (parsed.uiLanguage as "auto" | "en" | "zh"),
			runTimeoutMs: parseDuration((parsed.runTimeout ?? "30m") as string | number, "runTimeout"),
			headlessTrust: parsed.headlessTrust === undefined ? "saved" : (parsed.headlessTrust as "saved" | "always"),
			includeWakeEvidence: parsed.includeWakeEvidence === undefined ? true : (parsed.includeWakeEvidence as boolean),
		};
		await fs.mkdir(path.dirname(this.statePath), { recursive: true, mode: 0o700 });
		await this.loadStateFromDisk();
		this.initialized = true;
	}

	private async loadStateFromDisk(): Promise<void> {
		this.alarms.clear();
		this.outbox.clear();
		try {
			const raw = JSON.parse(await fs.readFile(this.statePath, "utf8")) as Record<string, unknown>;
			if (raw.version === 1 || raw.version === 2) {
				await this.migrateStateUnderLock();
				return;
			}
			if (clampPersistedState(raw) > 0) {
				// Legacy state with overlong evidence: rewrite the clamp under the
				// state lock (migrateStateUnderLock re-reads the freshest file inside
				// the critical section and clamps again idempotently), so restore
				// succeeds instead of failing on a too-long evidence string.
				await this.migrateStateUnderLock();
				return;
			}
			await this.adoptVersionedState(raw);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new Error(`Cannot restore ${path.basename(this.statePath)}: ${(error as Error).message}`);
		}
	}

	private async adoptVersionedState(raw: Record<string, unknown>): Promise<void> {
		if (raw.version !== 3 || !Array.isArray(raw.alarms) || !Array.isArray(raw.outbox)) throw new Error("unsupported state version");
		for (const [index, value] of raw.alarms.entries()) {
			try {
				const restored = restoreAlarmState(value, this.runtimeConfig.remote?.allowedRemoteLogRoots ?? []);
				if (this.alarms.has(restored.id)) throw new Error(`duplicate alarm id: ${restored.id}`);
				this.alarms.set(restored.id, restored);
				this.baseRevisions.set(restored.id, restored.revision ?? 0);
			} catch (error) {
				throw new Error(`alarms[${index}] is invalid: ${(error as Error).message}; repair or remove ${path.basename(this.statePath)}`);
			}
		}
		try {
			for (const entry of restoreOutbox(raw.outbox)) this.outbox.set(entry.eventId, entry);
		} catch (error) {
			throw new Error(`outbox is invalid: ${(error as Error).message}; repair or remove ${path.basename(this.statePath)}`);
		}
	}

	/**
	 * v1 (retire) and v2 (embedded pendingWake -> outbox) rewrites happen under the
	 * state lock and re-read the FRESHEST file inside the critical section, so a
	 * concurrent writer that already migrated is adopted instead of double-migrated.
	 */
	private async migrateStateUnderLock(): Promise<void> {
		await this.withStateLock(async () => {
			const raw = JSON.parse(await fs.readFile(this.statePath, "utf8")) as Record<string, unknown>;
			if (raw.version === 3) {
				const fixed = clampPersistedState(raw);
				if (fixed > 0) {
					await this.writeFullStateLocked(raw.alarms as unknown as AlarmState[], raw.outbox as unknown as OutboxEntry[]);
					console.log(`[pi-wake] migrated ${fixed} overlong evidence field(s) in ${path.basename(this.statePath)}`);
				}
				await this.adoptVersionedState(raw);
				return;
			}
			if (raw.version === 1) {
				await fs.rm(this.statePath, { force: true });
				this.retiredLegacyState = true;
				return;
			}
			if (raw.version !== 2 || !Array.isArray(raw.alarms)) throw new Error("unsupported state version");
			const migrated = this.migrateV2(raw);
			await this.writeFullStateLocked(migrated.alarms, migrated.outbox);
			for (const alarm of migrated.alarms) { this.alarms.set(alarm.id, alarm); this.baseRevisions.set(alarm.id, alarm.revision ?? 0); }
			for (const entry of migrated.outbox) this.outbox.set(entry.eventId, entry);
		});
	}

	/** Pure v2 -> v3 conversion: move each alarm's embedded pendingWake into the outbox. */
	private migrateV2(raw: Record<string, unknown>): { alarms: AlarmState[]; outbox: OutboxEntry[] } {
		const source = Array.isArray(raw.alarms) ? raw.alarms : [];
		const migratedAlarms: unknown[] = [];
		const migratedOutbox: OutboxEntry[] = [];
		for (const value of source) {
			if (!value || typeof value !== "object" || Array.isArray(value)) { migratedAlarms.push(value); continue; }
			const record = value as Record<string, unknown>;
			const { pendingWake, ...rest } = record;
			migratedAlarms.push(rest);
			if (pendingWake && typeof pendingWake === "object" && !Array.isArray(pendingWake)) {
				const pending = pendingWake as Record<string, unknown>;
				if (Array.isArray(pending.events) && pending.events.length) {
					const triggeredAt = Number.isSafeInteger(pending.triggeredAt) ? (pending.triggeredAt as number) : Date.now();
					const claim = pending.claim as OutboxEntry["claim"] | undefined;
					const alarm = restoreAlarmState(rest, this.runtimeConfig.remote?.allowedRemoteLogRoots ?? []);
					migratedOutbox.push({
						eventId: `${String(rest.id)}:${triggeredAt}:migrated-${migratedOutbox.length + 1}`,
						alarmId: alarm.id,
						alarmName: alarm.name,
						ownerSessionFile: alarm.ownerSessionFile,
						triggeredAt,
						events: pending.events as FiredEvent[],
						message: wakeMessage(alarm, pending.events as FiredEvent[], triggeredAt, this.runtimeConfig.maxEvidenceChars, this.runtimeConfig.includeWakeEvidence),
						claim,
					});
				}
			}
		}
		const restored: AlarmState[] = [];
		for (const [index, value] of migratedAlarms.entries()) {
			try {
				restored.push(restoreAlarmState(value, this.runtimeConfig.remote?.allowedRemoteLogRoots ?? []));
			} catch (error) {
				throw new Error(`alarms[${index}] is invalid: ${(error as Error).message}; repair or remove ${path.basename(this.statePath)}`);
			}
		}
		restoreOutbox(migratedOutbox);
		return { alarms: restored, outbox: migratedOutbox };
	}

	/**
	 * Re-read the state file and merge it into the local caches. Disk state is the
	 * source of truth: new alarms appear, removed alarms vanish, and every alarm
	 * whose revision advanced is replaced wholesale. This is what lets a long-lived
	 * daemon (and any session) discover alarms created by other sessions after it
	 * started, and what keeps the CAS base revisions fresh.
	 *
	 * Called before every action and every scheduler tick, and periodically by the
	 * daemon. Skipped while this runtime still has uncommitted local mutations.
	 */
	async reconcileFromDisk(): Promise<void> {
		if (!this.initialized) throw new Error("wake alarm runtime is not initialized");
		if (this.dirtyIds.size || this.deletedIds.size) return;
		const disk = await readStoredState(this.statePath).catch((error) => {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
			throw error;
		});
		if (!disk) {
			this.alarms.clear();
			this.outbox.clear();
			this.baseRevisions.clear();
			return;
		}
		const diskById = new Map<string, AlarmState>(disk.alarms.map((alarm) => [alarm.id, alarm]));
		for (const [id] of [...this.alarms]) {
			if (!diskById.has(id)) { this.alarms.delete(id); this.baseRevisions.delete(id); }
		}
		for (const alarm of disk.alarms) {
			const local = this.alarms.get(alarm.id);
			if (!local || (local.revision ?? 0) !== (alarm.revision ?? 0)) {
				this.alarms.set(alarm.id, alarm);
				this.baseRevisions.set(alarm.id, alarm.revision ?? 0);
			}
		}
		this.outbox.clear();
		for (const entry of disk.outbox) this.outbox.set(entry.eventId, entry);
		for (const eventId of [...this.wakeRetry.keys()]) if (!this.outbox.has(eventId)) this.wakeRetry.delete(eventId);
	}

	/** Re-read disk state and re-arm the scheduler; used by the daemon's periodic poll. */
	async resync(): Promise<void> {
		await this.reconcileFromDisk();
		this.schedule();
	}

	/**
	 * Full reload (alarms and outbox). Called by an emit implementation after a
	 * woken session had the chance to change alarms. Must be called from inside a
	 * serialized runtime operation (e.g. emit).
	 */
	async reloadFromDisk(): Promise<void> {
		if (!this.initialized) throw new Error("wake alarm runtime is not initialized");
		this.dirtyIds.clear();
		this.deletedIds.clear();
		this.createIds.clear();
		this.forceIds.clear();
		this.baseRevisions.clear();
		await this.loadStateFromDisk();
	}

	/**
	 * Cross-process state transaction lock. Atomic rename alone only guarantees
	 * readers never see half a file; it does NOT serialize the read-merge-write
	 * cycle, so concurrent writers in different processes could lose each other's
	 * alarms. All state mutations go through this exclusive lock file.
	 *
	 * The lock uses inode-verified rename-based stale takeover, so a crash-recovery
	 * contender can never delete a successor's live lock (see lock.ts).
	 */
	private async withStateLock<T>(fn: () => Promise<T>): Promise<T> {
		await this.stateLock.acquire();
		try {
			await this.stateLock.verifyHeld();
			return await fn();
		} finally {
			await this.stateLock.release();
		}
	}

	/** Windows can transiently refuse a rename while an indexer/AV holds a handle; retry briefly. */
	private async renameWithRetry(from: string, to: string): Promise<void> {
		let lastError: Error | undefined;
		for (let attempt = 0; attempt < 10; attempt++) {
			try { await fs.rename(from, to); return; }
			catch (error) {
				lastError = error as Error;
				const code = (error as NodeJS.ErrnoException).code;
				if (code !== "EPERM" && code !== "EACCES" && code !== "EBUSY") throw error;
				await new Promise((resolve) => setTimeout(resolve, 25 + attempt * 25));
			}
		}
		throw lastError;
	}

	/** Write the complete version-3 state atomically. Only call while holding the state lock. */
	private async writeFullStateLocked(alarms: AlarmState[], outbox: OutboxEntry[]): Promise<void> {
		await this.stateLock.verifyHeld();
		const temp = `${this.statePath}.tmp-${process.pid}-${Date.now()}`;
		const data: StoredState = { version: 3, alarms, outbox };
		// Serialization boundary guard: never let overlong evidence reach disk.
		const clamped = clampPersistedState(data);
		if (clamped > 0) console.warn(`[pi-wake] clamped ${clamped} overlong evidence field(s) before persisting state`);
		await fs.writeFile(temp, `${JSON.stringify(data, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
		try { await this.renameWithRetry(temp, this.statePath); }
		catch (error) { await fs.rm(temp, { force: true }); throw error; }
	}

	private async readDiskState(): Promise<StoredState> {
		return (await readStoredState(this.statePath)) ?? { version: 3, alarms: [], outbox: [] };
	}

	private async readLatestAlarm(id: string): Promise<AlarmState | undefined> {
		return (await this.readDiskState()).alarms.find((alarm) => alarm.id === id);
	}

	private adopt(disk: AlarmState): void {
		this.alarms.set(disk.id, disk);
		this.baseRevisions.set(disk.id, disk.revision ?? 0);
	}

	/**
	 * Merge-save with per-alarm revision CAS, under the cross-process state lock.
	 * - fresh create colliding with an existing id on disk -> "already exists" error
	 * - scheduler-derived update on a stale base -> adopt the disk version (re-derived next tick)
	 * - user-intent update on a stale base -> merge onto the latest revision
	 *
	 * The outbox is fully independent of alarm mutations: removing an alarm stops
	 * FUTURE events only; its undelivered wakes stay durable and are still delivered.
	 */
	private async saveState(): Promise<void> {
		if (!this.dirtyIds.size && !this.deletedIds.size) return;
		await this.withStateLock(async () => {
			const diskState = await this.readDiskState();
			const byId = new Map<string, AlarmState>(diskState.alarms.map((alarm) => [alarm.id, alarm]));
			for (const id of this.deletedIds) {
				byId.delete(id);
				this.baseRevisions.delete(id);
			}
			for (const id of this.dirtyIds) {
				const mine = this.alarms.get(id);
				if (!mine) continue;
				const disk = byId.get(id);
				if (this.createIds.has(id) && disk) throw new Error(`alarm already exists: ${id}`);
				if (!disk) {
					if (this.createIds.has(id) || this.forceIds.has(id) || (this.baseRevisions.get(id) ?? 0) === 0) {
						const next = { ...mine, revision: 1 };
						byId.set(id, next);
						this.alarms.set(id, next);
						this.baseRevisions.set(id, 1);
					} else {
						this.alarms.delete(id);
						this.baseRevisions.delete(id);
					}
					continue;
				}
				const diskRevision = disk.revision ?? 0;
				const base = this.baseRevisions.get(id) ?? 0;
				if (diskRevision === base) {
					const next = { ...mine, revision: base + 1 };
					byId.set(id, next);
					this.alarms.set(id, next);
					this.baseRevisions.set(id, base + 1);
					continue;
				}
				if (this.forceIds.has(id)) {
					// User intent is re-applied onto the freshest disk state, never overlaid
					// from a stale snapshot.
					const intent = this.intentMap.get(id);
					const applied = intent ? intent(disk) : mine;
					const merged = { ...applied, revision: diskRevision + 1 } as AlarmState;
					byId.set(id, merged);
					this.alarms.set(id, merged);
					this.baseRevisions.set(id, diskRevision + 1);
				} else {
					this.alarms.set(id, disk);
					this.baseRevisions.set(id, diskRevision);
				}
			}
			await this.writeFullStateLocked([...byId.values()], diskState.outbox);
			this.outbox.clear();
			for (const entry of diskState.outbox) this.outbox.set(entry.eventId, entry);
			this.dirtyIds.clear();
			this.deletedIds.clear();
			this.createIds.clear();
			this.forceIds.clear();
			this.intentMap.clear();
		});
	}

	/**
	 * Atomically claim the delivery of one outbox entry. Returns the claim token,
	 * or undefined when another live claimant holds it or the entry is gone.
	 */
	private async claimOutboxEntry(eventId: string): Promise<string | undefined> {
		return this.withStateLock(async () => {
			const diskState = await this.readDiskState();
			const entry = diskState.outbox.find((candidate) => candidate.eventId === eventId);
			if (!entry) return undefined;
			const now = Date.now();
			const existing = entry.claim;
			if (existing && existing.claimantId !== this.claimantId && existing.expiresAt > now) return undefined;
			const option = this.options.deliveryTtlMs;
			const ttl = typeof option === "function" ? option() : (option ?? 60_000);
			const token = randomUUID();
			const next: OutboxEntry = { ...entry, claim: { claimantId: this.claimantId, token, expiresAt: now + ttl } };
			await this.writeFullStateLocked(diskState.alarms, diskState.outbox.map((candidate) => (candidate.eventId === eventId ? next : candidate)));
			this.outbox.set(eventId, next);
			return token;
		});
	}

	/** Remove a delivered outbox entry; only the claim holder can complete. */
	private async completeOutboxEntry(eventId: string, token: string): Promise<boolean> {
		return this.withStateLock(async () => {
			const diskState = await this.readDiskState();
			const entry = diskState.outbox.find((candidate) => candidate.eventId === eventId);
			if (entry?.claim?.token !== token) return false;
			const outbox = diskState.outbox.filter((candidate) => candidate.eventId !== eventId);
			await this.writeFullStateLocked(diskState.alarms, outbox);
			this.outbox.delete(eventId);
			this.wakeRetry.delete(eventId);
			return true;
		});
	}

	/** Release a claim after failed delivery; the entry stays for the next attempt. */
	private async releaseOutboxEntryClaim(eventId: string, token: string): Promise<void> {
		await this.withStateLock(async () => {
			const diskState = await this.readDiskState();
			const entry = diskState.outbox.find((candidate) => candidate.eventId === eventId);
			if (entry?.claim?.token !== token) return;
			const next: OutboxEntry = { ...entry, claim: undefined };
			await this.writeFullStateLocked(diskState.alarms, diskState.outbox.map((candidate) => (candidate.eventId === eventId ? next : candidate)));
			this.outbox.set(eventId, next);
		});
	}

	private sleep(ms: number, signal: AbortSignal): Promise<void> {
		return new Promise((resolve, reject) => {
			if (signal.aborted) return reject(new Error("wake alarm shutdown"));
			const timer = setTimeout(resolve, ms);
			signal.addEventListener("abort", () => { clearTimeout(timer); reject(new Error("wake alarm shutdown")); }, { once: true });
		});
	}

	private async probe(alarm: ContainerAlarmState, baseline = false, rebind = false): Promise<ProbeResult> {
		const config = this.runtimeConfig;
		const remote = requireRemote("watch_container", config);
		const payload = Buffer.from(JSON.stringify({
			container: alarm.container,
			expectedId: rebind ? undefined : alarm.containerId,
			logPath: alarm.logPath,
			allowedRemoteLogRoots: remote.allowedRemoteLogRoots,
			offset: rebind ? 0 : alarm.logOffset,
			cursor: rebind ? undefined : alarm.logCursor,
			fileId: rebind ? undefined : alarm.logFileId,
			baseline,
			readLogs: Boolean(alarm.logPath) || alarm.events.includes("log-error") || alarm.events.includes("log-match") || alarm.logTailLines !== undefined,
			maxBytes: config.maxLogBytes,
			tailBytes: Math.min(config.maxLogBytes, 8192),
			tailLinesReq: alarm.logTailLines,
		})).toString("base64");
		const remoteCommand = `python3 -c ${shellSingleQuote(PROBE_LOADER)} ${shellSingleQuote(payload)}`;
		let lastError = "SSH probe failed";
		for (let attempt = 1; attempt <= remote.sshAttempts; attempt++) {
			const controller = new AbortController();
			this.controllers.add(controller);
			try {
				const result = await this.options.execFn("ssh", ["-i", remote.identityPath, "-p", String(remote.port), "-o", "BatchMode=yes", "-o", `ConnectTimeout=${remote.connectTimeoutSeconds}`, "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=1", `${remote.user}@${remote.host}`, remoteCommand], { signal: controller.signal, timeout: (remote.connectTimeoutSeconds + 20) * 1000 });
				if (result.stdout.length > MAX_SSH_OUTPUT || result.stderr.length > MAX_SSH_OUTPUT) throw new Error("SSH probe output exceeded safety limit");
				if (result.code !== 0) throw new Error(`ssh exited ${result.code}: ${sanitizeExcerpt(result.stderr, 800)}`);
				const raw = JSON.parse(result.stdout.trim()) as Record<string, unknown>;
				if (raw.probeError) throw new Error(String(raw.probeError));
				const logOffset = Number(raw.logOffset ?? alarm.logOffset);
				if (!Number.isSafeInteger(logOffset) || logOffset < 0) throw new Error("remote probe returned an invalid logOffset");
				const logMode = raw.logMode == null ? undefined : String(raw.logMode);
				if (logMode && !["application-file", "docker-file", "docker-logs"].includes(logMode)) throw new Error("remote probe returned an invalid logMode");
				const exitCode = raw.exitCode == null ? undefined : Number(raw.exitCode);
				if (exitCode !== undefined && (!Number.isSafeInteger(exitCode) || exitCode < 0 || exitCode > 2_147_483_647)) throw new Error("remote probe returned an invalid exitCode");
				const logFileId = raw.logFileId == null ? undefined : validateLogFileId(String(raw.logFileId));
				return {
					exists: Boolean(raw.exists),
					containerId: raw.containerId ? validateContainer(String(raw.containerId)) : undefined,
					running: Boolean(raw.running),
					status: String(raw.status ?? "unknown"),
					containerStatus: String(raw.containerStatus ?? raw.status ?? "unknown"),
					startedAt: raw.startedAt ? String(raw.startedAt) : undefined,
					exitCode,
					oomKilled: raw.oomKilled == null ? undefined : Boolean(raw.oomKilled),
					logMode: logMode as ProbeResult["logMode"],
					selectedLogPath: raw.selectedLogPath == null ? undefined : String(raw.selectedLogPath),
					logFileId,
					logOffset,
					logCursor: raw.logCursor ? String(raw.logCursor) : undefined,
					logReset: Boolean(raw.logReset),
					logBytes: Buffer.from(String(raw.logBase64 ?? ""), "base64"),
					tail: decodeNewLog(Buffer.from(String(raw.tailBase64 ?? ""), "base64")),
				};
			} catch (error) {
				lastError = sanitizeExcerpt((error as Error).message, 800);
				if (this.stopped) throw new Error("wake alarm shutdown");
				if (attempt < remote.sshAttempts) await this.sleep(remote.sshBackoffMs * attempt, controller.signal);
			} finally { this.controllers.delete(controller); }
		}
		throw new Error(lastError);
	}

	/** Poll a remote completion-condition file (exists / contains / min_size). Runs outside the state lock. */
	private async probeCondition(alarm: ConditionAlarmState): Promise<{ exists: boolean; size: number; tail: string; mtime?: number }> {
		const config = this.runtimeConfig;
		const remote = requireRemote("watch_condition", config);
		const payload = Buffer.from(JSON.stringify({
			conditionPath: alarm.path,
			allowedRemoteLogRoots: remote.allowedRemoteLogRoots,
			tailBytes: Math.min(config.maxLogBytes, 8192),
		})).toString("base64");
		const remoteCommand = `python3 -c ${shellSingleQuote(PROBE_LOADER)} ${shellSingleQuote(payload)}`;
		let lastError = "SSH probe failed";
		for (let attempt = 1; attempt <= remote.sshAttempts; attempt++) {
			const controller = new AbortController();
			this.controllers.add(controller);
			try {
				const result = await this.options.execFn("ssh", ["-i", remote.identityPath, "-p", String(remote.port), "-o", "BatchMode=yes", "-o", `ConnectTimeout=${remote.connectTimeoutSeconds}`, "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=1", `${remote.user}@${remote.host}`, remoteCommand], { signal: controller.signal, timeout: (remote.connectTimeoutSeconds + 20) * 1000 });
				if (result.stdout.length > MAX_SSH_OUTPUT || result.stderr.length > MAX_SSH_OUTPUT) throw new Error("SSH probe output exceeded safety limit");
				if (result.code !== 0) throw new Error(`ssh exited ${result.code}: ${sanitizeExcerpt(result.stderr, 800)}`);
				const raw = JSON.parse(result.stdout.trim()) as Record<string, unknown>;
				if (raw.probeError) throw new Error(String(raw.probeError));
				const size = Number(raw.size ?? 0);
				if (!Number.isSafeInteger(size) || size < 0) throw new Error("remote probe returned an invalid size");
				const mtime = raw.mtime === undefined || raw.mtime === null ? undefined : Number(raw.mtime);
				if (mtime !== undefined && (!Number.isSafeInteger(mtime) || mtime < 0)) throw new Error("remote probe returned an invalid mtime");
				return {
					exists: Boolean(raw.exists),
					size,
					mtime,
					tail: decodeNewLog(Buffer.from(String(raw.tailBase64 ?? ""), "base64")),
				};
			} catch (error) {
				lastError = sanitizeExcerpt((error as Error).message, 800);
				if (this.stopped) throw new Error("wake alarm shutdown");
				if (attempt < remote.sshAttempts) await this.sleep(remote.sshBackoffMs * attempt, controller.signal);
			} finally { this.controllers.delete(controller); }
		}
		throw new Error(lastError);
	}

	private outboxOptions(): { maxEvidenceChars: number; includeWakeEvidence: boolean } {
		return { maxEvidenceChars: this.runtimeConfig.maxEvidenceChars, includeWakeEvidence: this.runtimeConfig.includeWakeEvidence };
	}

	/**
	 * Outbox capacity guard: when undelivered wakes already saturate the configured
	 * caps, a NEW occurrence is not silently dropped — the producing alarm is paused
	 * with an explicit overflow reason instead, so the user sees exactly what happened.
	 * Existing entries are never discarded (at-least-once).
	 */
	private outboxOverflowed(diskState: StoredState, alarmId: string): boolean {
		return diskState.outbox.length >= this.runtimeConfig.maxOutboxEntries
			|| diskState.outbox.filter((entry) => entry.alarmId === alarmId).length >= this.runtimeConfig.maxOutboxEntriesPerAlarm;
	}

	/** Commit a fire transition and (when emitting) append the outbox entry in one locked transaction. */
	/** Every persisted change bumps the alarm's revision: revision is the version signal for all caches. */
	private bump<T extends AlarmState>(alarm: T, patch: Partial<T>): T {
		// Durable evidence fields must stay within the 2000-char restore cap:
		// every state mutation funnels through here, so clamp at the boundary.
		const clamped: Partial<T> = { ...patch };
		const durable = clamped as unknown as { lastEvidence?: string; summary?: string };
		if (typeof durable.lastEvidence === "string") durable.lastEvidence = persistedEvidence(durable.lastEvidence);
		if (typeof durable.summary === "string") durable.summary = persistedEvidence(durable.summary);
		return { ...alarm, ...clamped, revision: (alarm.revision ?? 0) + 1 } as T;
	}

	private outboxCapacityFree(disk: StoredState, alarmId: string, extra: number): boolean {
		return disk.outbox.length + extra <= this.runtimeConfig.maxOutboxEntries
			&& disk.outbox.filter((entry) => entry.alarmId === alarmId).length + extra <= this.runtimeConfig.maxOutboxEntriesPerAlarm;
	}

	/**
	 * Multi-alarm transaction: acquire the state lock, let the caller build the full
	 * new alarms/outbox arrays from the freshest disk state, write ONCE (atomic
	 * rename), and adopt every changed alarm. This is the single write path for
	 * group create/complete/pause/resume/reset/remove and member nudges — a group
	 * is a compound object and must never be written member-by-member.
	 */
	private async commitAlarmSet<T>(fn: (disk: StoredState) => Promise<{ alarms: AlarmState[]; outbox: OutboxEntry[]; adopted: Map<string, AlarmState>; adoptedOutbox?: OutboxEntry[]; result: T }>, options?: { held?: boolean }): Promise<T> {
		const run = async (): Promise<T> => {
			const disk = await this.readDiskState();
			const outcome = await fn(disk);
			await this.writeFullStateLocked(outcome.alarms, outcome.outbox);
			for (const [alarmId, alarm] of outcome.adopted) this.adopt(alarm);
			if (outcome.adoptedOutbox) {
				this.outbox.clear();
				for (const entry of outcome.adoptedOutbox) this.outbox.set(entry.eventId, entry);
			}
			return outcome.result;
		};
		// Fire/observation paths that already run inside a caller-held lock pass
		// held: true; the barrier/condition ticks acquire the lock themselves.
		if (options?.held) return run();
		return this.withStateLock(run);
	}

	private async commitFire(id: string, next: AlarmState, events: FiredEvent[], now: number, shouldEmit: boolean): Promise<AlarmState> {
		// The caller passes the post-decision state WITHOUT a revision; the transaction
		// bumps it from the freshest disk value. Called under the caller's lock.
		return this.commitAlarmSet(async (disk) => {
			const diskAlarm = disk.alarms.find((alarm) => alarm.id === id);
			// Group members record fires in state but produce NO individual wake; the
			// owning group emits the single summary wake. The group is nudged (with its
			// revision bumped) so the next scheduler tick re-evaluates the barrier.
			const memberGroupId = diskAlarm !== undefined && diskAlarm.kind === "container" ? diskAlarm.groupId : undefined;
			const groupAlarm = memberGroupId ? disk.alarms.find((alarm) => alarm.id === memberGroupId && alarm.kind === "group") : undefined;
			const alarms = disk.alarms.map((alarm) => {
				if (alarm.id === id) return this.bump(alarm, next);
				if (groupAlarm && alarm.id === memberGroupId) return this.bump(alarm, { nextCheckAt: now });
				return alarm;
			});
			const adopted = new Map<string, AlarmState>([[id, alarms.find((alarm) => alarm.id === id)!]]);
			if (groupAlarm) adopted.set(memberGroupId!, alarms.find((alarm) => alarm.id === memberGroupId)!);
			if (!shouldEmit || !events.length || memberGroupId !== undefined) {
				return { alarms, outbox: disk.outbox, adopted, result: adopted.get(id)! };
			}
			if (!this.outboxCapacityFree(disk, id, 1)) {
				// Outbox at capacity: PAUSE WITHOUT CONSUMING the occurrence. The pre-fire
				// alarm state (triggeredAt, event fingerprints, log cursor, …) is kept
				// intact and only active/pauseReason change, so once the user frees outbox
				// capacity (drop_wake) and resumes, the very same occurrence fires again
				// and produces its wake. Persisting the post-fire state here would mark
				// the event as consumed without a durable wake — losing it forever.
				const paused = this.bump(diskAlarm!, { active: false, pauseReason: "outbox overflow: undelivered wakes; drop_wake or purge_wakes to free capacity, then resume" });
				const overflowAlarms = disk.alarms.map((alarm) => (alarm.id === id ? paused : alarm));
				return { alarms: overflowAlarms, outbox: disk.outbox, adopted: new Map([[id, paused]]), result: paused };
			}
			const entry = createOutboxEntry(adopted.get(id)!, events, now, this.outboxOptions());
			return { alarms, outbox: [...disk.outbox, entry], adopted, adoptedOutbox: [...disk.outbox, entry], result: adopted.get(id)! };
		}, { held: true });
	}

	/**
	 * Ownership test: is this alarm still a member THIS group owns? A same-id alarm
	 * that is not a container, or whose groupId points elsewhere, is a replacement,
	 * NOT a member — group lifecycle must never mutate it.
	 */
	private ownedGroupMember(alarm: AlarmState | undefined, group: GroupAlarmState): alarm is ContainerAlarmState {
		return alarm !== undefined && alarm.kind === "container" && alarm.groupId === group.id && group.memberIds.includes(alarm.id);
	}

	/**
	 * Batch-barrier evaluation. Reads the freshest member states under the state
	 * lock, computes the condition summary, and either advances the poll cadence,
	 * waits out a coalesce window, fires the single group wake, or — when the
	 * outbox is full at the moment the condition is met — FREEZES the occurrence:
	 * members are paused, the summary is kept, and the group only retries the
	 * outbox slot, so a restarted container can never erase an already-happened
	 * abnormal exit. Every changed alarm (group + members) gets a revision bump.
	 * A member whose alarm is missing, not a container, or no longer pointing at
	 * this group is an integrity failure, never a terminal result.
	 */
	private async evaluateGroup(id: string, shouldEmit = true): Promise<{ alarm: GroupAlarmState; events: FiredEvent[] }> {
		const current = this.alarms.get(id);
		if (!current || current.kind !== "group") throw new Error(`unknown group alarm: ${id}`);
		return this.commitAlarmSet(async (disk) => {
			const group = disk.alarms.find((candidate) => candidate.id === id);
			if (!group || group.kind !== "group") return { alarms: disk.alarms, outbox: disk.outbox, adopted: new Map(), result: { alarm: (group as GroupAlarmState | undefined) ?? (current as GroupAlarmState), events: [] } };
			if (!group.active || group.firedAt !== undefined) return { alarms: disk.alarms, outbox: disk.outbox, adopted: new Map([[id, group]]), result: { alarm: group, events: [] } };
			const now = Date.now();
			const groupMember = (memberId: string): ContainerAlarmState | undefined => {
				const candidate = disk.alarms.find((alarm) => alarm.id === memberId);
				return this.ownedGroupMember(candidate, group) ? candidate : undefined;
			};
			// Frozen occurrence: the condition was already met but the outbox had no
			// capacity. This fast path runs BEFORE any member/integrity check: the
			// fact HAPPENED, and the current member world is no longer its precondition
			// — a member removed or replaced while frozen can never erase it.
			if (group.pendingFire === true) {
				if (shouldEmit && !this.outboxCapacityFree(disk, id, 1)) {
					const next = this.bump(group, { nextCheckAt: now + 5_000 });
					const alarms = disk.alarms.map((candidate) => (candidate.id === id ? next : candidate));
					return { alarms, outbox: disk.outbox, adopted: new Map([[id, next]]), result: { alarm: next, events: [] } };
				}
				// Fire with the FROZEN summary and the FROZEN occurrence time.
				const occurredAt = group.pendingFireAt ?? now;
				const fired = this.bump(group, { active: false, pauseReason: `group condition met: ${group.condition}`, firedAt: occurredAt, conditionMetAt: undefined, pendingFire: false, pendingFireAt: undefined, lastTriggeredAt: occurredAt });
				const event: FiredEvent = { kind: "group", fingerprint: `group:${id}:${group.condition}:${occurredAt}`, evidence: group.summary };
				const entry = createOutboxEntry(fired, [event], occurredAt, this.outboxOptions());
				const alarms = disk.alarms.map((candidate) => (candidate.id === id ? fired : candidate));
				return { alarms, outbox: shouldEmit ? [...disk.outbox, entry] : disk.outbox, adopted: new Map([[id, fired]]), adoptedOutbox: shouldEmit ? [...disk.outbox, entry] : undefined, result: { alarm: fired, events: [event] } };
			}
			const badMembers = group.memberIds.filter((memberId) => groupMember(memberId) === undefined);
			if (badMembers.length > 0) {
				// Integrity failure: a member that is missing, replaced by another kind,
				// or no longer attached to this group is NEVER a terminal result. Only
				// ownership-valid members are paused; a same-id replacement is untouched.
				const diagnostic = `group integrity failure: invalid member alarm(s) ${badMembers.join(", ")}`;
				const next = this.bump(group, { active: false, pauseReason: diagnostic, summary: diagnostic, pendingFire: false });
				const alarms = disk.alarms.map((candidate) => candidate.id === id ? next : this.ownedGroupMember(candidate, group) ? this.bump(candidate, { active: false, pauseReason: "group integrity failure" }) : candidate);
				const adopted = new Map(alarms.filter((candidate) => candidate.id === id || this.ownedGroupMember(candidate, group)).map((candidate) => [candidate.id, candidate]));
				return { alarms, outbox: disk.outbox, adopted, result: { alarm: next, events: [] } };
			}
			const members = group.memberIds.map(groupMember).filter((member): member is ContainerAlarmState => member !== undefined);
			let terminal = 0;
			let exit0 = 0;
			let abnormal = 0;
			let missing = 0;
			let replaced = 0;
			const lines: string[] = [];
			for (const member of members) {
				const fps = member.eventFingerprints;
				const isTerminal = Boolean(fps.exit || fps.abnormal || fps.missing || fps.replaced);
				const isExit0 = Boolean(fps.exit) && member.lastExitCode === 0 && !member.lastOomKilled;
				const isAbnormal = Boolean(fps.abnormal) || (member.lastExitCode !== undefined && member.lastExitCode !== 0) || Boolean(member.lastOomKilled) || member.lastContainerStatus === "dead" || member.lastContainerStatus === "restarting";
				if (isTerminal) terminal++;
				if (isExit0) exit0++;
				if (isAbnormal) abnormal++;
				if (fps.missing) missing++;
				if (fps.replaced) replaced++;
				const status = fps.missing ? "missing" : fps.replaced ? "replaced" : member.lastContainerStatus ?? "running";
				const code = member.lastExitCode !== undefined ? `, code ${member.lastExitCode}` : "";
				const line = `${member.id} (${member.container}): ${status}${code}`;
				lines.push(line);
				// Bounded per-member log tail from logTailLines evidence, so a group wake
				// can answer "what did it print at the end" for each member.
				if (member.logTailLines && member.lastEvidence) {
					const tail = sanitizeExcerpt(member.lastEvidence, 300);
					if (tail) lines.push(`    tail: ${tail.replace(/\n/g, "\n    ")}`);
				}
			}
			const counts = `${terminal}/${group.memberIds.length} terminal; ${exit0} exit 0; ${abnormal} abnormal; ${missing} missing; ${replaced} replaced`;
			const conditionMet = group.condition === "any_terminal" ? terminal >= 1
				: group.condition === "all_terminal" ? terminal === group.memberIds.length
				: group.condition === "any_abnormal" ? abnormal >= 1
				: terminal >= group.required;
			const allTerminal = terminal === group.memberIds.length;
			if (!conditionMet) {
				const next = this.bump(group, { summary: counts, conditionMetAt: undefined, pendingFire: false, nextCheckAt: now + group.statusPollMs });
				const alarms = disk.alarms.map((candidate) => (candidate.id === id ? next : candidate));
				return { alarms, outbox: disk.outbox, adopted: new Map([[id, next]]), result: { alarm: next, events: [] } };
			}
			const metAt = group.conditionMetAt ?? now;
			const shouldFire = allTerminal || group.coalesceWindowMs === undefined || now >= metAt + group.coalesceWindowMs;
			if (!shouldFire) {
				const next = this.bump(group, { summary: counts, conditionMetAt: metAt, nextCheckAt: metAt + group.coalesceWindowMs! });
				const alarms = disk.alarms.map((candidate) => (candidate.id === id ? next : candidate));
				return { alarms, outbox: disk.outbox, adopted: new Map([[id, next]]), result: { alarm: next, events: [] } };
			}
			const memberLines = lines.slice(0, 12);
			if (lines.length > 12) memberLines.push(`… ${lines.length - 12} more`);
			const firedSummary = `${counts}\nMembers:\n${memberLines.join("\n")}`.slice(0, 2000);
			if (shouldEmit && !this.outboxCapacityFree(disk, id, 1)) {
				// Freeze the occurrence: pause members so a container restart can never
				// delete the fingerprints that justify this wake, keep the summary, and
				// retry only the outbox slot.
				const frozen = this.bump(group, { summary: firedSummary, pendingFire: true, pendingFireAt: now, conditionMetAt: undefined, nextCheckAt: now + 5_000 });
				const alarms = disk.alarms.map((candidate) => candidate.id === id ? frozen : this.ownedGroupMember(candidate, group) ? this.bump(candidate, { active: false, pauseReason: "group completed (awaiting outbox capacity)" }) : candidate);
				const adopted = new Map(alarms.filter((candidate) => candidate.id === id || this.ownedGroupMember(candidate, group)).map((candidate) => [candidate.id, candidate]));
				return { alarms, outbox: disk.outbox, adopted, result: { alarm: frozen, events: [] } };
			}
			// Fire: the group pauses (never re-fires until reset) and its members pause too.
			const fired = this.bump(group, {
				active: false,
				pauseReason: `group condition met: ${group.condition}`,
				summary: firedSummary,
				firedAt: now,
				conditionMetAt: undefined,
				pendingFire: false,
				lastTriggeredAt: now,
			});
			const event: FiredEvent = { kind: "group", fingerprint: `group:${id}:${group.condition}:${now}`, evidence: counts };
			const entry = createOutboxEntry(fired, [event], now, this.outboxOptions());
			const alarms = disk.alarms.map((candidate) => {
				if (candidate.id === id) return fired;
				if (this.ownedGroupMember(candidate, group)) return this.bump(candidate, { active: false, pauseReason: "group completed" });
				return candidate;
			});
			const adopted = new Map(alarms.filter((candidate) => candidate.id === id || this.ownedGroupMember(candidate, group)).map((candidate) => [candidate.id, candidate]));
			return {
				alarms,
				outbox: shouldEmit ? [...disk.outbox, entry] : disk.outbox,
				adopted,
				adoptedOutbox: shouldEmit ? [...disk.outbox, entry] : undefined,
				result: { alarm: fired, events: [event] },
			};
		});
	}

	/**
	 * Two-phase condition check, mirroring checkContainer's optimistic concurrency:
	 * a frozen (pending) occurrence is handled UNDER THE LOCK BEFORE any SSH probe,
	 * so a remote host that goes offline after the occurrence can never block its
	 * delivery. Only a not-yet-satisfied (or unfrozen) condition probes remotely,
	 * and the probe result is committed only if the alarm's revision is unchanged.
	 */
	private async checkCondition(id: string, shouldEmit: boolean): Promise<{ alarm: ConditionAlarmState; events: FiredEvent[] }> {
		const current = this.alarms.get(id);
		if (!current || current.kind !== "condition") throw new Error(`unknown condition alarm: ${id}`);
		if (!current.active) return { alarm: current, events: [] };
		const phaseA = await this.withStateLock(async (): Promise<{ handled: boolean; alarm: ConditionAlarmState; events: FiredEvent[]; revision: number }> => {
			const disk = await this.readDiskState();
			const diskAlarm = disk.alarms.find((candidate) => candidate.id === id);
			if (!diskAlarm || diskAlarm.kind !== "condition" || !diskAlarm.active) {
				if (diskAlarm) this.adopt(diskAlarm);
				return { handled: true, alarm: (diskAlarm as ConditionAlarmState | undefined) ?? current, events: [], revision: -1 };
			}
			const pending = this.pendingConditionOutcome(disk, diskAlarm, shouldEmit);
			if (pending) {
				await this.writeFullStateLocked(pending.alarms, pending.outbox);
				for (const [alarmId, alarm] of pending.adopted) this.adopt(alarm);
				if (pending.adoptedOutbox) {
					this.outbox.clear();
					for (const entry of pending.adoptedOutbox) this.outbox.set(entry.eventId, entry);
				}
				return { handled: true, alarm: pending.result.alarm, events: pending.result.events, revision: -1 };
			}
			return { handled: false, alarm: current, events: [], revision: diskAlarm.revision ?? 0 };
		});
		if (phaseA.handled) return { alarm: phaseA.alarm, events: phaseA.events };
		// SSH probe outside the lock.
		let result: { exists: boolean; size: number; tail: string; mtime?: number };
		try { result = await this.probeCondition(current); }
		catch (error) {
			if (this.stopped) return { alarm: current, events: [] };
			const reason = sanitizeExcerpt((error as Error).message, 800);
			return this.commitAlarmSet(async (disk) => {
				const diskAlarm = disk.alarms.find((candidate) => candidate.id === id);
				if (!diskAlarm || diskAlarm.kind !== "condition" || (diskAlarm.revision ?? 0) !== phaseA.revision) {
					if (diskAlarm) this.adopt(diskAlarm);
					return { alarms: disk.alarms, outbox: disk.outbox, adopted: new Map(), result: { alarm: (diskAlarm as ConditionAlarmState | undefined) ?? current, events: [] } };
				}
				const pending = this.pendingConditionOutcome(disk, diskAlarm, shouldEmit);
				if (pending) return pending;
				const next = this.bump(diskAlarm, { lastEvidence: persistedEvidence(reason), nextCheckAt: Date.now() + diskAlarm.statusPollMs });
				const alarms = disk.alarms.map((candidate) => (candidate.id === id ? next : candidate));
				return { alarms, outbox: disk.outbox, adopted: new Map([[id, next]]), result: { alarm: next, events: [] } };
			});
		}
		// Phase B: locked CAS commit of the probe result.
		return this.commitAlarmSet(async (disk) => {
			const diskAlarm = disk.alarms.find((candidate) => candidate.id === id);
			if (!diskAlarm || diskAlarm.kind !== "condition" || (diskAlarm.revision ?? 0) !== phaseA.revision) {
				if (diskAlarm) this.adopt(diskAlarm);
				return { alarms: disk.alarms, outbox: disk.outbox, adopted: new Map(), result: { alarm: (diskAlarm as ConditionAlarmState | undefined) ?? current, events: [] } };
			}
			// Another runtime may have frozen the occurrence while we probed.
			const pending = this.pendingConditionOutcome(disk, diskAlarm, shouldEmit);
			if (pending) return pending;
			const now = Date.now();
			// Stale-marker guard: with ignoreBefore set, a file that existed BEFORE the
			// cutoff (a marker from a previous run) never satisfies the condition.
			const stale = result.exists && diskAlarm.ignoreBefore !== undefined && result.mtime !== undefined && result.mtime < diskAlarm.ignoreBefore;
			const satisfied = !stale && (diskAlarm.condition === "exists" ? result.exists
				: diskAlarm.condition === "contains" ? result.exists && result.tail.includes(diskAlarm.value ?? "")
				: result.exists && result.size >= (diskAlarm.minSize ?? 0));
			// The evidence records BOTH clocks: when the probe detected it (detection)
			// and the file mtime (occurrence), so a backfilled satisfiedAt cannot be
			// mistaken for the moment the event happened.
			const occurredNote = result.mtime !== undefined ? `\n(file mtime: ${new Date(result.mtime).toISOString()})` : "";
			const tailEvidence = result.exists ? persistedEvidence(tailLines(result.tail, 10) + occurredNote) : undefined;
			if (!satisfied) {
				const next = this.bump(diskAlarm, { lastSatisfied: false, lastSize: result.size, lastEvidence: tailEvidence, nextCheckAt: now + diskAlarm.statusPollMs });
				const alarms = disk.alarms.map((candidate) => (candidate.id === id ? next : candidate));
				return { alarms, outbox: disk.outbox, adopted: new Map([[id, next]]), result: { alarm: next, events: [] } };
			}
			if (shouldEmit && !this.outboxCapacityFree(disk, id, 1)) {
				// Freeze the occurrence at THIS moment; only the outbox slot is retried.
				const frozen = this.bump(diskAlarm, { lastSatisfied: true, pendingSatisfiedAt: now, lastSize: result.size, lastEvidence: tailEvidence, nextCheckAt: now + 5_000 });
				const alarms = disk.alarms.map((candidate) => (candidate.id === id ? frozen : candidate));
				return { alarms, outbox: disk.outbox, adopted: new Map([[id, frozen]]), result: { alarm: frozen, events: [] } };
			}
			const fired = this.bump(diskAlarm, { lastSatisfied: true, lastSize: result.size, lastEvidence: tailEvidence, active: false, pauseReason: `condition met: ${diskAlarm.condition}`, satisfiedAt: now, nextCheckAt: now + diskAlarm.statusPollMs });
			const event: FiredEvent = { kind: "condition", fingerprint: `condition:${id}:${now}`, evidence: tailEvidence };
			const entry = createOutboxEntry(fired, [event], now, this.outboxOptions());
			const alarms = disk.alarms.map((candidate) => (candidate.id === id ? fired : candidate));
			return {
				alarms,
				outbox: shouldEmit ? [...disk.outbox, entry] : disk.outbox,
				adopted: new Map([[id, fired]]),
				adoptedOutbox: shouldEmit ? [...disk.outbox, entry] : undefined,
				result: { alarm: fired, events: [event] },
			};
		});
	}

	/**
	 * The frozen-occurrence handling shared by both phases: a condition that WAS
	 * satisfied (pendingSatisfiedAt set, satisfiedAt unset) only retries the outbox
	 * slot — it never re-probes the remote world. Fires with the frozen occurrence
	 * timestamp. Returns undefined when there is no pending occurrence.
	 */
	private pendingConditionOutcome(disk: StoredState, diskAlarm: ConditionAlarmState, shouldEmit: boolean): { alarms: AlarmState[]; outbox: OutboxEntry[]; adopted: Map<string, AlarmState>; adoptedOutbox?: OutboxEntry[]; result: { alarm: ConditionAlarmState; events: FiredEvent[] } } | undefined {
		if (!(diskAlarm.lastSatisfied === true && diskAlarm.satisfiedAt === undefined)) return undefined;
		const now = Date.now();
		if (shouldEmit && !this.outboxCapacityFree(disk, diskAlarm.id, 1)) {
			const next = this.bump(diskAlarm, { nextCheckAt: now + 5_000 });
			return { alarms: disk.alarms.map((candidate) => (candidate.id === diskAlarm.id ? next : candidate)), outbox: disk.outbox, adopted: new Map([[diskAlarm.id, next]]), result: { alarm: next, events: [] } };
		}
		const occurredAt = diskAlarm.pendingSatisfiedAt ?? now;
		const fired = this.bump(diskAlarm, { active: false, pauseReason: `condition met: ${diskAlarm.condition}`, satisfiedAt: occurredAt, pendingSatisfiedAt: undefined });
		const event: FiredEvent = { kind: "condition", fingerprint: `condition:${diskAlarm.id}:${occurredAt}`, evidence: diskAlarm.lastEvidence };
		const entry = createOutboxEntry(fired, [event], occurredAt, this.outboxOptions());
		return {
			alarms: disk.alarms.map((candidate) => (candidate.id === diskAlarm.id ? fired : candidate)),
			outbox: shouldEmit ? [...disk.outbox, entry] : disk.outbox,
			adopted: new Map([[diskAlarm.id, fired]]),
			adoptedOutbox: shouldEmit ? [...disk.outbox, entry] : undefined,
			result: { alarm: fired, events: [event] },
		};
	}

	/** Remove a specific undelivered wake (explicit management action; never implied by alarm removal). */
	private async dropOutboxEntry(eventId: string): Promise<void> {
		await this.withStateLock(async () => {
			const diskState = await this.readDiskState();
			const entry = diskState.outbox.find((candidate) => candidate.eventId === eventId);
			if (!entry) throw new Error(`unknown wake: ${eventId}`);
			const outbox = diskState.outbox.filter((candidate) => candidate.eventId !== eventId);
			await this.writeFullStateLocked(diskState.alarms, outbox);
			this.outbox.delete(eventId);
			this.wakeRetry.delete(eventId);
		});
	}

	/** Remove every undelivered wake of an alarm (explicit management action). Returns the number dropped. */
	private async purgeOutboxEntries(alarmId: string): Promise<number> {
		let dropped = 0;
		await this.withStateLock(async () => {
			const diskState = await this.readDiskState();
			const outbox = diskState.outbox.filter((candidate) => candidate.alarmId !== alarmId);
			dropped = diskState.outbox.length - outbox.length;
			if (!dropped) return;
			await this.writeFullStateLocked(diskState.alarms, outbox);
			this.outbox.clear();
			for (const entry of outbox) this.outbox.set(entry.eventId, entry);
			for (const eventId of [...this.wakeRetry.keys()]) if (!this.outbox.has(eventId)) this.wakeRetry.delete(eventId);
		});
		return dropped;
	}

	private async tryEmit(entry: OutboxEntry): Promise<boolean> {
		try { return (await this.options.emit(entry)) !== false; }
		catch { return false; }
	}

	private noteWakeRetry(eventId: string): void {
		const policy = this.options.wakeRetry ?? { delayMs: 5_000, capMs: 1_800_000 };
		const attempts = (this.wakeRetry.get(eventId)?.attempts ?? 0) + 1;
		const nextAt = Date.now() + Math.min(policy.delayMs * attempts, policy.capMs);
		this.wakeRetry.set(eventId, { attempts, nextAt });
	}

	private snapshotMutations(): { alarms: Map<string, AlarmState>; dirtyIds: Set<string>; deletedIds: Set<string>; createIds: Set<string>; forceIds: Set<string>; intentMap: Map<string, (disk: AlarmState) => AlarmState>; baseRevisions: Map<string, number> } {
		return {
			alarms: new Map(this.alarms),
			dirtyIds: new Set(this.dirtyIds),
			deletedIds: new Set(this.deletedIds),
			createIds: new Set(this.createIds),
			forceIds: new Set(this.forceIds),
			intentMap: new Map(this.intentMap),
			baseRevisions: new Map(this.baseRevisions),
		};
	}

	private restoreMutations(snapshot: ReturnType<WakeAlarmRuntime["snapshotMutations"]>): void {
		this.alarms.clear(); for (const [id, alarm] of snapshot.alarms) this.alarms.set(id, alarm);
		this.dirtyIds.clear(); for (const id of snapshot.dirtyIds) this.dirtyIds.add(id);
		this.deletedIds.clear(); for (const id of snapshot.deletedIds) this.deletedIds.add(id);
		this.createIds.clear(); for (const id of snapshot.createIds) this.createIds.add(id);
		this.forceIds.clear(); for (const id of snapshot.forceIds) this.forceIds.add(id);
		this.intentMap.clear(); for (const [id, intent] of snapshot.intentMap) this.intentMap.set(id, intent);
		this.baseRevisions.clear(); for (const [id, revision] of snapshot.baseRevisions) this.baseRevisions.set(id, revision);
	}

	private async replaceAlarm(id: string, next: AlarmState, options?: { force?: boolean; intent?: (disk: AlarmState) => AlarmState }): Promise<void> {
		const snapshot = this.snapshotMutations();
		const previous = this.alarms.get(id);
		this.alarms.set(id, next);
		this.dirtyIds.add(id);
		this.deletedIds.delete(id);
		if (previous === undefined) this.createIds.add(id);
		if (options?.force) this.forceIds.add(id);
		if (options?.intent) this.intentMap.set(id, options.intent);
		try { await this.saveState(); }
		catch (error) { this.restoreMutations(snapshot); throw error; }
	}

	private async removeAlarm(id: string): Promise<number> {
		const previous = this.alarms.get(id);
		if (!previous) throw new Error(`unknown alarm: ${id}`);
		// Removing an alarm stops FUTURE events; its undelivered outbox wakes are
		// historical facts and stay durable for delivery by the daemon or the owner.
		const pendingBefore = [...this.outbox.values()].filter((entry) => entry.alarmId === id).length;
		const snapshot = this.snapshotMutations();
		this.alarms.delete(id);
		this.dirtyIds.delete(id);
		this.createIds.delete(id);
		this.deletedIds.add(id);
		try { await this.saveState(); }
		catch (error) { this.restoreMutations(snapshot); throw error; }
		return pendingBefore;
	}

	private ownsAlarm(alarm: AlarmState): boolean {
		return this.options.owns ? this.options.owns(alarm) : true;
	}

	private ownsEntry(entry: OutboxEntry): boolean {
		return this.options.owns ? this.options.owns({ ownerSessionFile: entry.ownerSessionFile }) : true;
	}

	private ownedEntries(alarmId: string): OutboxEntry[] {
		return [...this.outbox.values()].filter((entry) => entry.alarmId === alarmId && this.ownsEntry(entry)).sort((a, b) => a.triggeredAt - b.triggeredAt);
	}

	/**
	 * Claim, deliver, and settle one outbox entry. Delivery itself happens outside
	 * the state lock; the claim written under the lock guarantees single delivery.
	 */
	private async deliverOutboxEntry(eventId: string): Promise<void> {
		if (this.stopped || !this.outbox.has(eventId)) return;
		const token = await this.claimOutboxEntry(eventId);
		if (!token) { this.noteWakeRetry(eventId); return; }
		const entry = this.outbox.get(eventId);
		if (!entry) return;
		const delivered = await this.tryEmit(entry);
		if (delivered && this.options.deferDeliveryCompletion) {
			// Handed to the host, NOT yet delivered: the entry stays durable on disk
			// (claim held) until the host echoes the message into the conversation.
			this.pendingConfirmations.set(eventId, { token, sentAt: Date.now() });
		} else if (delivered) await this.completeOutboxEntry(eventId, token);
		else { await this.releaseOutboxEntryClaim(eventId, token); this.noteWakeRetry(eventId); }
	}

	/** Proof from the host that a handed-off wake entered the conversation: complete it. Idempotent. */
	async confirmDelivery(eventId: string): Promise<boolean> {
		const pending = this.pendingConfirmations.get(eventId);
		if (!pending) return false;
		this.pendingConfirmations.delete(eventId);
		try { return await this.completeOutboxEntry(eventId, pending.token); }
		catch { return false; }
	}

	/**
	 * The host's agent run settled: every steer message queued before the run had
	 * its echo by now (in-order events), so anything still pending was lost host-side
	 * (abort clears the queue; consumption never happened). Release the claims and
	 * retry with backoff — the wake is a durable fact and must still be delivered.
	 */
	onDeliveryCycleSettled(): void {
		for (const [eventId, pending] of [...this.pendingConfirmations]) {
			this.pendingConfirmations.delete(eventId);
			void this.releaseOutboxEntryClaim(eventId, pending.token).catch(() => undefined);
			this.noteWakeRetry(eventId);
		}
		// The scheduler may have gone dormant (a fully-pending outbox arms no
		// timer); re-arm it so the backoff retry actually fires.
		this.schedule();
	}

	private async flushPendingWakes(): Promise<void> {
		for (const entry of [...this.outbox.values()].sort((a, b) => a.triggeredAt - b.triggeredAt)) {
			if (this.stopped) return;
			if (!this.ownsEntry(entry)) continue;
			if (this.pendingConfirmations.has(entry.eventId)) continue; // already handed to the host, awaiting its echo
			try { await this.deliverOutboxEntry(entry.eventId); }
			catch { /* The scheduler retries pending outbox delivery with backoff. */ }
		}
	}

	/**
	 * Timer check: recompute the fire decision from the freshest persisted state
	 * under the transaction lock. A stale in-memory snapshot can therefore never
	 * fire a timer that was paused or reset by another runtime in the meantime:
	 * `applyTimer` is applied to the latest disk alarm, not to the local cache.
	 */
	private async checkTimer(id: string, shouldEmit: boolean): Promise<{ alarm: AlarmState; events: FiredEvent[] }> {
		const current = this.alarms.get(id);
		if (!current || current.kind !== "timer") throw new Error(`unknown timer alarm: ${id}`);
		return this.withStateLock(async () => {
			const disk = await this.readLatestAlarm(id);
			if (!disk) { this.alarms.delete(id); this.baseRevisions.delete(id); return { alarm: current, events: [] }; }
			if (disk.kind !== "timer") { this.adopt(disk); return { alarm: disk, events: [] }; }
			const now = Date.now();
			const decision = applyTimer(disk, now);
			if (!decision.events.length) { this.adopt(disk); return { alarm: disk, events: [] }; }
			const next: AlarmState = { ...decision.state, revision: (disk.revision ?? 0) + 1 };
			const committed = await this.commitFire(id, next, decision.events, now, shouldEmit);
			return { alarm: committed, events: decision.events };
		});
	}

	/**
	 * Container check with optimistic concurrency: the time-based deadline decision
	 * is computed under the lock, then the SSH probe runs OUTSIDE it (a probe can
	 * take many seconds and must not hold the lock), and finally the result is
	 * committed only if the world did not move: same revision, same active flag.
	 * Otherwise the probe is discarded and the freshest disk state adopted.
	 */
	private async checkContainer(id: string, shouldEmit: boolean, observePaused = false): Promise<{ alarm: ContainerAlarmState; events: FiredEvent[] }> {
		const current = this.alarms.get(id);
		if (!current || current.kind !== "container") throw new Error(`unknown container alarm: ${id}`);
		if (!current.active && !observePaused) return { alarm: current, events: [] };
		const phaseA = await this.withStateLock(async (): Promise<{ base: ContainerAlarmState | undefined; revision: number; deadline: { state: ContainerAlarmState; events: FiredEvent[] }; committed: { alarm: ContainerAlarmState; events: FiredEvent[] } | undefined }> => {
			const disk = await this.readLatestAlarm(id);
			if (!disk) { this.alarms.delete(id); this.baseRevisions.delete(id); return { base: undefined, revision: -1, deadline: { state: current, events: [] }, committed: undefined }; }
			if (disk.kind !== "container") { this.adopt(disk); return { base: undefined, revision: -1, deadline: { state: current, events: [] }, committed: undefined }; }
			const preservePaused = !disk.active && observePaused;
			const effective: ContainerAlarmState = preservePaused ? { ...disk, active: true, pauseReason: undefined } : disk;
			const deadline = applyContainerDeadline(effective, Date.now());
			if (deadline.events.length && !deadline.state.active && shouldEmit) {
				// Deadline fired with the pause policy: commit the transition and the
				// wake in one locked transaction (time-based, SSH not required).
				const next: ContainerAlarmState = { ...deadline.state, revision: (disk.revision ?? 0) + 1 };
				const committed = await this.commitFire(id, next, deadline.events, Date.now(), true);
				return { base: undefined, revision: -1, deadline: { state: current, events: [] }, committed: { alarm: committed as ContainerAlarmState, events: deadline.events } };
			}
			return { base: disk, revision: disk.revision ?? 0, deadline, committed: undefined };
		});
		if (phaseA.committed) return phaseA.committed;
		if (!phaseA.base) return { alarm: current, events: [] };
		let probeResult: ProbeResult | undefined;
		let failureReason: string | undefined;
		try { probeResult = await this.probe(phaseA.deadline.state); }
		catch (error) {
			if (this.stopped) return { alarm: current, events: [] };
			failureReason = sanitizeExcerpt((error as Error).message, 800);
		}
		return this.withStateLock(async (): Promise<{ alarm: ContainerAlarmState; events: FiredEvent[] }> => {
			const disk = await this.readLatestAlarm(id);
			if (!disk) { this.alarms.delete(id); this.baseRevisions.delete(id); return { alarm: current, events: [] }; }
			if (disk.kind !== "container" || (disk.revision ?? 0) !== phaseA.revision || disk.active !== phaseA.base!.active) {
				this.adopt(disk);
				return { alarm: disk as ContainerAlarmState, events: [] };
			}
			const preservePaused = !disk.active && observePaused;
			const effective: ContainerAlarmState = preservePaused ? { ...disk, active: true, pauseReason: undefined } : disk;
			const now2 = Date.now();
			// Recompute BOTH the time-based deadline and the probe-derived decision on the
			// freshest disk state (structurally equal to the pre-probe base, but the
			// deadline may have newly crossed while the probe ran). Committing from the
			// fresh deadline state also records the deadline fingerprint exactly once.
			const deadline = applyContainerDeadline(effective, now2);
			const applied = probeResult
				? applyProbe(deadline.state, probeResult, now2)
				: applyCheckFailure(deadline.state, now2, this.runtimeConfig.remote?.maxConsecutiveFailures ?? 3, failureReason ?? "SSH probe failed");
			const events = [...deadline.events, ...applied.events.filter((event) => !deadline.events.some((other) => other.fingerprint === event.fingerprint))];
			const nextBase: ContainerAlarmState = preservePaused ? { ...applied.state, active: false, pauseReason: disk.pauseReason } : applied.state;
			const next: ContainerAlarmState = { ...nextBase, revision: (disk.revision ?? 0) + 1 };
			const committed = await this.commitFire(id, next, events, Date.now(), shouldEmit);
			return { alarm: committed as ContainerAlarmState, events };
		});
	}

	private effectiveDueAt(alarm: AlarmState): number | undefined {
		return nextAlarmDueAt(alarm);
	}

	private effectiveEntryDue(entry: OutboxEntry): number | undefined {
		if (this.pendingConfirmations.has(entry.eventId)) return undefined;
		const retry = this.wakeRetry.get(entry.eventId);
		return retry && retry.nextAt > Date.now() ? retry.nextAt : entry.triggeredAt;
	}

	private scheduleRetry(): void {
		if (this.stopped || !this.schedulingEnabled) return;
		if (this.scheduler) clearTimeout(this.scheduler);
		this.scheduler = setTimeout(() => this.schedule(), 1000);
	}

	private schedule(): void {
		if (this.scheduler) clearTimeout(this.scheduler);
		this.scheduler = undefined;
		if (this.stopped || !this.schedulingEnabled) return;
		// Outbox delivery is independent of alarm existence: a removed alarm's
		// undelivered wakes are still due and still delivered.
		const due = [
			...[...this.alarms.values()].filter((alarm) => this.ownsAlarm(alarm)).map((alarm) => this.effectiveDueAt(alarm)),
			...[...this.outbox.values()].filter((entry) => this.ownsEntry(entry)).map((entry) => this.effectiveEntryDue(entry)),
		].filter((value): value is number => value !== undefined);
		if (!due.length) return;
		this.scheduler = setTimeout(() => {
			this.scheduler = undefined;
			this.serialize(async () => {
				await this.reconcileFromDisk();
				const now = Date.now();
				let failed = false;
				// Deliver due outbox entries first, regardless of whether their alarm still exists.
				for (const entry of [...this.outbox.values()]) {
					if (this.stopped || !this.ownsEntry(entry)) continue;
					const entryDue = this.effectiveEntryDue(entry);
					if (entryDue === undefined || entryDue > now) continue;
					try { await this.deliverOutboxEntry(entry.eventId); }
					catch { failed = true; }
				}
				for (const alarm of [...this.alarms.values()]) {
					if (this.stopped || !this.ownsAlarm(alarm)) continue;
					const alarmDue = this.effectiveDueAt(alarm);
					if (alarmDue === undefined || alarmDue > now) continue;
					try {
						const current = this.alarms.get(alarm.id);
						if (this.stopped || !current) continue;
						if (current.kind === "timer") await this.checkTimer(current.id, true);
						else if (current.kind === "group") await this.evaluateGroup(current.id, true);
						else if (current.kind === "condition") await this.checkCondition(current.id, true);
						else await this.checkContainer(current.id, true);
					} catch {
						failed = true;
					}
				}
				if (failed) throw new Error("one or more due wake alarms failed; retrying with backoff");
			}).then(() => this.schedule(), () => this.scheduleRetry());
		}, timerDelay(Math.min(...due), Date.now()));
	}

	private async setTimer(params: ToolParams, context?: ActionContext): Promise<AlarmState> {
		if (!params.id || !params.name) throw new Error("id and name are required for set_timer");
		const id = validateAlarmId(params.id);
		if (this.alarms.has(id)) throw new Error(`alarm already exists: ${id}`);
		const alarm = createTimerAlarm({ id, name: params.name, now: Date.now(), afterMs: params.after === undefined ? undefined : parseDuration(params.after, "after"), at: params.at === undefined ? undefined : parseAbsoluteTime(params.at), ownerSessionFile: context?.ownerSessionFile });
		await this.replaceAlarm(id, alarm); this.schedule(); return alarm;
	}

	private async watchContainer(params: ToolParams, context?: ActionContext): Promise<AlarmState> {
		const config = this.runtimeConfig;
		const remote = requireRemote("watch_container", config);
		if (!params.id || !params.name || !params.container || !params.events) throw new Error("id, name, container, and events are required for watch_container");
		const id = validateAlarmId(params.id);
		if (this.alarms.has(id)) throw new Error(`alarm already exists: ${id}`);
		let alarm = createContainerAlarm({ id, name: params.name, container: params.container, events: params.events, policy: params.policy, logPath: params.logPath ? validateRemoteLogPath(params.logPath, remote.allowedRemoteLogRoots) : undefined, logPattern: params.logPattern, allowedRemoteLogRoots: remote.allowedRemoteLogRoots, now: Date.now(), statusPollMs: params.statusPoll ? parseDuration(params.statusPoll, "statusPoll") : config.statusPollMs, deadlineMs: params.deadline ? parseDuration(params.deadline, "deadline") : undefined, ownerSessionFile: context?.ownerSessionFile, logTailLines: params.logTailLines });
		const baseline = await this.probe(alarm, true, true);
		if (!baseline.exists) throw new Error(`container does not exist: ${alarm.container}`);
		alarm = applyBaseline(alarm, baseline, Date.now());
		await this.replaceAlarm(id, alarm); this.schedule(); return alarm;
	}

	private async watchContainerGroup(params: ToolParams, context?: ActionContext): Promise<AlarmState> {
		const config = this.runtimeConfig;
		const remote = requireRemote("watch_container_group", config);
		if (!params.id || !params.name || !params.containers?.length) throw new Error("id, name, and containers are required for watch_container_group");
		if (params.containers.length < 2 || params.containers.length > 64) throw new Error("a group needs 2-64 containers");
		if (new Set(params.containers).size !== params.containers.length) throw new Error("containers must be unique");
		const id = validateAlarmId(params.id);
		if (this.alarms.has(id)) throw new Error(`alarm already exists: ${id}`);
		const condition = (params.condition ?? "all_terminal") as GroupCondition;
		if (!["any_terminal", "all_terminal", "any_abnormal", "n_of_m_terminal"].includes(condition)) throw new Error("condition must be any_terminal, all_terminal, any_abnormal, or n_of_m_terminal");
		const statusPollMs = params.statusPoll ? parseDuration(params.statusPoll, "statusPoll") : config.statusPollMs;
		const coalesceWindowMs = params.coalesceWindow ? parseDuration(params.coalesceWindow, "coalesceWindow") : undefined;
		// Probe EVERY member first (baseline, outside the lock); the whole group
		// creation fails if any container is missing.
		const memberAlarms: ContainerAlarmState[] = [];
		for (const [index, container] of params.containers.entries()) {
			const memberId = `${id}-${index + 1}`;
			let member = createContainerAlarm({ id: memberId, name: `${params.name} #${index + 1}`, container, events: ["exit", "abnormal", "missing", "replaced"], policy: "keep", allowedRemoteLogRoots: remote.allowedRemoteLogRoots, now: Date.now(), statusPollMs, ownerSessionFile: context?.ownerSessionFile, groupId: id, logTailLines: params.logTailLines });
			const baseline = await this.probe(member, true, true);
			if (!baseline.exists) throw new Error(`container does not exist: ${container}`);
			memberAlarms.push(applyBaseline(member, baseline, Date.now()));
		}
		const group = createGroupAlarm({ id, name: params.name, memberIds: memberAlarms.map((member) => member.id), condition, required: params.required, now: Date.now(), statusPollMs, coalesceWindowMs, ownerSessionFile: context?.ownerSessionFile });
		// ONE transaction: all members + the group appear together or not at all —
		// a partial write must never leave orphan members whose wakes are suppressed
		// by a group that does not exist.
		const groupResult = await this.commitAlarmSet(async (disk) => {
			if (disk.alarms.some((alarm) => alarm.id === id)) throw new Error(`alarm already exists: ${id}`);
			for (const member of memberAlarms) {
				if (disk.alarms.some((alarm) => alarm.id === member.id)) throw new Error(`member alarm already exists: ${member.id}`);
			}
			const alarms = [...disk.alarms, ...memberAlarms, group];
			const adopted = new Map<string, AlarmState>([[id, group], ...memberAlarms.map((member) => [member.id, member] as [string, AlarmState])]);
			return { alarms, outbox: disk.outbox, adopted, result: group };
		});
		this.schedule();
		return groupResult;
	}

	private async watchCondition(params: ToolParams, context?: ActionContext): Promise<AlarmState> {
		const config = this.runtimeConfig;
		const remote = requireRemote("watch_condition", config);
		if (!params.id || !params.name || !params.path || !params.condition) throw new Error("id, name, path, and condition are required for watch_condition");
		const id = validateAlarmId(params.id);
		if (this.alarms.has(id)) throw new Error(`alarm already exists: ${id}`);
		const alarm = createConditionAlarm({ id, name: params.name, path: params.path, condition: params.condition as ConditionKind, value: params.value, minSize: params.minSize, ignoreBefore: params.ignoreBefore === undefined ? undefined : parseIgnoreBefore(params.ignoreBefore), allowedRemoteLogRoots: remote.allowedRemoteLogRoots, now: Date.now(), statusPollMs: params.statusPoll ? parseDuration(params.statusPoll, "statusPoll") : config.statusPollMs, ownerSessionFile: context?.ownerSessionFile });
		await this.replaceAlarm(id, alarm);
		this.schedule();
		return alarm;
	}

	private async resetOne(id: string, params: ToolParams): Promise<AlarmState> {
		const config = this.runtimeConfig;
		const current = this.alarms.get(id);
		if (!current) throw new Error(`unknown alarm: ${id}`);
		if (current.kind === "timer") {
			const reset = createTimerAlarm({ id: current.id, name: current.name, now: Date.now(), afterMs: params.after === undefined ? undefined : parseDuration(params.after, "after"), at: params.at === undefined ? undefined : parseAbsoluteTime(params.at), ownerSessionFile: current.ownerSessionFile });
			this.wakeRetry.delete(id);
			await this.replaceAlarm(id, reset, { force: true, intent: () => reset }); this.schedule(); return reset;
		}
		if (current.kind === "group") {
			if (params.after !== undefined || params.at !== undefined) throw new Error("group reset does not accept after or at");
			// Phase A: snapshot the group AND its members (revision/kind/groupId) BEFORE
			// the (slow) rebaseline probes, so a concurrent remove/recreate cannot be
			// overwritten by this reset (ABA).
			const snapshot = await this.readDiskState();
			const snapGroup = snapshot.alarms.find((candidate) => candidate.id === id);
			if (!snapGroup || snapGroup.kind !== "group") throw new Error(`unknown group alarm: ${id}`);
			const snapMembers = new Map<string, ContainerAlarmState>();
			for (const memberId of snapGroup.memberIds) {
				const member = snapshot.alarms.find((candidate) => candidate.id === memberId);
				if (!member || member.kind !== "container" || member.groupId !== id) throw new Error(`group member ${memberId} is invalid; remove and recreate the group`);
				snapMembers.set(memberId, member);
			}
			// Rebaseline EVERY member (fresh probes, cleared fingerprints/status).
			const freshMembers: ContainerAlarmState[] = [];
			for (const memberId of snapGroup.memberIds) {
				const member = snapMembers.get(memberId)!;
				let fresh = createContainerAlarm({ id: member.id, name: member.name, container: member.container, events: member.events, policy: member.policy, allowedRemoteLogRoots: config.remote?.allowedRemoteLogRoots ?? [], now: Date.now(), statusPollMs: member.statusPollMs, ownerSessionFile: member.ownerSessionFile, groupId: member.groupId, logTailLines: member.logTailLines });
				const baseline = await this.probe(fresh, true, true);
				if (!baseline.exists) throw new Error(`container does not exist: ${member.container}`);
				freshMembers.push(applyBaseline(fresh, baseline, Date.now()));
			}
			// Phase B: re-validate under the lock; any change discards the reset. The
			// reset is a PATCH applied to the validated disk group — never a stale
			// local snapshot — so an incarnation change is fully closed.
			const resetPatch: Partial<GroupAlarmState> = { active: true, pauseReason: undefined, firedAt: undefined, conditionMetAt: undefined, pendingFire: false, pendingFireAt: undefined, lastTriggeredAt: undefined, nextCheckAt: Date.now() };
			const reset = await this.commitAlarmSet(async (disk) => {
				const diskGroup = disk.alarms.find((candidate) => candidate.id === id);
				if (!diskGroup || diskGroup.kind !== "group" || (diskGroup.revision ?? 0) !== (snapGroup.revision ?? 0) || diskGroup.createdAt !== snapGroup.createdAt) throw new Error("group changed concurrently; retry reset");
				for (const memberId of snapGroup.memberIds) {
					const diskMember = disk.alarms.find((candidate) => candidate.id === memberId);
					const snap = snapMembers.get(memberId)!;
					if (!diskMember || diskMember.kind !== "container" || diskMember.groupId !== id || (diskMember.revision ?? 0) !== (snap.revision ?? 0) || diskMember.createdAt !== snap.createdAt) throw new Error(`group member ${memberId} changed concurrently; retry reset`);
				}
				const alarms = disk.alarms.map((candidate) => {
					const fresh = freshMembers.find((member) => member.id === candidate.id);
					if (fresh) return this.bump(candidate, fresh);
					if (candidate.id === id) return this.bump(candidate, resetPatch);
					return candidate;
				});
				const adopted = new Map(alarms.filter((candidate) => candidate.id === id || snapGroup.memberIds.includes(candidate.id)).map((candidate) => [candidate.id, candidate]));
				return { alarms, outbox: disk.outbox, adopted, result: alarms.find((candidate) => candidate.id === id)! as GroupAlarmState };
			});
			this.schedule();
			return reset;
		}
		if (current.kind === "condition") {
			if (params.after !== undefined || params.at !== undefined) throw new Error("condition reset does not accept after or at");
			const reset: ConditionAlarmState = { ...current, active: true, pauseReason: undefined, satisfiedAt: undefined, pendingSatisfiedAt: undefined, lastSatisfied: false, lastTriggeredAt: undefined, nextCheckAt: Date.now() };
			await this.replaceAlarm(id, reset, { force: true, intent: () => reset }); this.schedule(); return reset;
		}
		if (params.after !== undefined || params.at !== undefined) throw new Error("container reset does not accept after or at");
		const deadlineMs = current.deadlineAt === undefined ? undefined : current.deadlineAt - current.createdAt;
		let reset = createContainerAlarm({ id: current.id, name: current.name, container: current.container, events: current.events, policy: current.policy, logPath: current.logPath, logPattern: current.logPattern, allowedRemoteLogRoots: config.remote?.allowedRemoteLogRoots ?? [], now: Date.now(), statusPollMs: current.statusPollMs, deadlineMs, ownerSessionFile: current.ownerSessionFile, groupId: current.groupId, logTailLines: current.logTailLines });
		const baseline = await this.probe(reset, true, true);
		if (!baseline.exists) throw new Error(`container does not exist: ${reset.container}`);
		reset = applyBaseline(reset, baseline, Date.now());
		this.wakeRetry.delete(id);
		await this.replaceAlarm(id, reset, { force: true, intent: () => reset }); this.schedule(); return reset;
	}

	async runAction(params: ToolParams, context?: ActionContext): Promise<string> {
		if (!this.initialized) throw new Error("wake alarm runtime is not initialized");
		if (!params || !(ACTION_ENUM as readonly string[]).includes(params.action)) throw new Error("invalid wake alarm action");
		validateActionParams(params);
		return this.serialize(async () => {
			// Always act on the freshest disk truth: alarms created by other sessions
			// appear here, and locally-deleted alarms are not resurrected by stale caches.
			await this.reconcileFromDisk();
			switch (params.action) {
				case "set_timer": return `Set ${alarmSummary(await this.setTimer(params, context))}`;
				case "watch_container": return `Set ${alarmSummary(await this.watchContainer(params, context))}`;
				case "watch_container_group": return `Set ${alarmSummary(await this.watchContainerGroup(params, context))}`;
				case "watch_condition": return `Set ${alarmSummary(await this.watchCondition(params, context))}`;
				case "list": return this.alarms.size ? [...this.alarms.values()].map(alarmSummary).join("\n") : "No wake alarms.";
				case "set_language": throw new Error("set_language is handled by the session shell (it owns the UI preference); call it from a live session");
				case "check": {
					const ids = params.id ? [validateAlarmId(params.id)] : [...this.alarms.keys()];
					if (!ids.length) return "No wake alarms.";
					const lines: string[] = [];
					for (const id of ids) {
						let current = this.alarms.get(id);
						if (!current) throw new Error(`unknown alarm: ${id}`);
						for (const entry of this.ownedEntries(id)) {
							const retry = this.wakeRetry.get(entry.eventId);
							if (retry && retry.nextAt > Date.now()) continue;
							await this.deliverOutboxEntry(entry.eventId);
						}
						current = this.alarms.get(id);
						if (!current) continue;
						const result = current.kind === "timer" ? await this.checkTimer(id, false)
							: current.kind === "group" ? await this.evaluateGroup(id, false)
							: current.kind === "condition" ? await this.checkCondition(id, false)
							: await this.checkContainer(id, false, true);
						const pending = this.ownedEntries(id).length ? "; wake pending for the owner session or daemon" : "";
						lines.push(`${alarmSummary(result.alarm)}${result.events.length ? `; observed=${result.events.map((event) => event.kind).join(",")}` : ""}${pending}`);
					}
					this.schedule(); return lines.join("\n");
				}
				case "pause": {
					if (!params.id) throw new Error("id is required for pause");
					const id = validateAlarmId(params.id); const current = this.alarms.get(id);
					if (!current) throw new Error(`unknown alarm: ${id}`);
					if (current.kind === "group") {
						// Group lifecycle controls members: pausing the barrier stops member polling too.
						// The affected set is derived from the FRESH disk group inside the lock, and only
						// ownership-valid members are touched — a same-id replacement is never mutated.
						const count = await this.commitAlarmSet(async (disk) => {
							const diskGroup = disk.alarms.find((candidate) => candidate.id === id);
							if (!diskGroup || diskGroup.kind !== "group") throw new Error(`unknown group alarm: ${id}`);
							const invalid = diskGroup.memberIds.filter((memberId) => !this.ownedGroupMember(disk.alarms.find((candidate) => candidate.id === memberId), diskGroup));
							if (invalid.length > 0) throw new Error(`group integrity failure: invalid member alarm(s) ${invalid.join(", ")}`);
							const memberIds = new Set(diskGroup.memberIds);
							const alarms = disk.alarms.map((candidate) => (candidate.id === id || memberIds.has(candidate.id)) ? this.bump(candidate, { active: false, pauseReason: "paused explicitly" }) : candidate);
							const adopted = new Map(alarms.filter((candidate) => candidate.id === id || memberIds.has(candidate.id)).map((candidate) => [candidate.id, candidate]));
							return { alarms, outbox: disk.outbox, adopted, result: diskGroup.memberIds.length };
						});
						this.schedule(); return `Paused ${id} (group and ${count} member(s)).`;
					}
					await this.replaceAlarm(id, { ...current, active: false, pauseReason: "paused explicitly" }, { force: true, intent: (disk) => ({ ...disk, active: false, pauseReason: "paused explicitly" }) }); this.schedule(); return `Paused ${id}.`;
				}
				case "resume": {
					if (!params.id) throw new Error("id is required for resume");
					const id = validateAlarmId(params.id); const current = this.alarms.get(id);
					if (!current) throw new Error(`unknown alarm: ${id}`);
					if (current.kind === "group") {
						if (current.firedAt !== undefined) throw new Error("a completed group must be reset, not resumed");
						const count = await this.commitAlarmSet(async (disk) => {
							const diskGroup = disk.alarms.find((candidate) => candidate.id === id);
							if (!diskGroup || diskGroup.kind !== "group") throw new Error(`unknown group alarm: ${id}`);
							if (diskGroup.firedAt !== undefined) throw new Error("a completed group must be reset, not resumed");
							const invalid = diskGroup.memberIds.filter((memberId) => !this.ownedGroupMember(disk.alarms.find((candidate) => candidate.id === memberId), diskGroup));
							if (invalid.length > 0) throw new Error(`group integrity failure: invalid member alarm(s) ${invalid.join(", ")}`);
							const memberIds = new Set(diskGroup.memberIds);
							const alarms = disk.alarms.map((candidate) => {
								if (candidate.id === id || memberIds.has(candidate.id)) return this.bump(candidate, resumeAlarm(candidate, Date.now()));
								return candidate;
							});
							const adopted = new Map(alarms.filter((candidate) => candidate.id === id || memberIds.has(candidate.id)).map((candidate) => [candidate.id, candidate]));
							return { alarms, outbox: disk.outbox, adopted, result: diskGroup.memberIds.length };
						});
						this.schedule(); return `Resumed ${id} (group and ${count} member(s)).`;
					}
					const resumed = resumeAlarm(current, Date.now()); await this.replaceAlarm(id, resumed, { force: true, intent: (disk) => resumeAlarm(disk, Date.now()) }); this.schedule(); return `Resumed ${id}.`;
				}
				case "reset": {
					if (!params.id) throw new Error("id is required for reset");
					return `Reset ${alarmSummary(await this.resetOne(validateAlarmId(params.id), params))}`;
				}
				case "remove": {
					if (!params.id) throw new Error("id is required for remove");
					const id = validateAlarmId(params.id);
					// The removal set is derived from the FRESH disk group inside the lock:
					// a stale local member list must never delete a successor group's members.
					let pending = 0;
					let purged = 0;
					const removeIds = await this.commitAlarmSet(async (disk) => {
						const diskAlarm = disk.alarms.find((candidate) => candidate.id === id);
						const ids = new Set<string>([id]);
						// Only ownership-valid members are removed with the group: a same-id
						// replacement (e.g. an unrelated timer) is NEVER a casualty.
						if (diskAlarm?.kind === "group") {
							for (const memberId of diskAlarm.memberIds) {
								if (this.ownedGroupMember(disk.alarms.find((candidate) => candidate.id === memberId), diskAlarm)) ids.add(memberId);
							}
						}
						// Alarms AND (optionally) their wakes are removed in ONE transaction:
						// a failure cannot leave alarms alive with wakes purged, or orphan members.
						pending = disk.outbox.filter((entry) => ids.has(entry.alarmId)).length;
						const alarms = disk.alarms.filter((candidate) => !ids.has(candidate.id));
						const outbox = params.purgePendingEvents === true ? disk.outbox.filter((entry) => !ids.has(entry.alarmId)) : disk.outbox;
						purged = params.purgePendingEvents === true ? disk.outbox.length - outbox.length : 0;
						return { alarms, outbox, adopted: new Map(), adoptedOutbox: outbox, result: ids };
					});
					for (const removedId of removeIds) { this.alarms.delete(removedId); this.baseRevisions.delete(removedId); this.wakeRetry.delete(removedId); }
					for (const eventId of [...this.wakeRetry.keys()]) if (!this.outbox.has(eventId)) this.wakeRetry.delete(eventId);
					this.schedule();
					const wakeNote = pending ? `${pending} undelivered wake(s) left in the outbox` : "";
					const purgeNote = purged ? `${wakeNote ? "; " : ""}purged ${purged} wake(s)` : "";
					return `Removed ${id}${wakeNote || purgeNote ? ` (${wakeNote}${purgeNote})` : ""}.`;
				}
				case "ack": {
					if (!params.id) throw new Error("id is required for ack");
					const id = validateAlarmId(params.id);
					// The purge set is derived from the FRESH disk state inside ONE transaction,
					// and only wakes of the alarm itself plus ownership-valid members are
					// dropped — a same-id replacement's durable wakes are never collateral.
					const dropped = await this.commitAlarmSet(async (disk) => {
						const diskAlarm = disk.alarms.find((candidate) => candidate.id === id);
						const ids = new Set<string>([id]);
						if (diskAlarm?.kind === "group") {
							for (const memberId of diskAlarm.memberIds) {
								if (this.ownedGroupMember(disk.alarms.find((candidate) => candidate.id === memberId), diskAlarm)) ids.add(memberId);
							}
						}
						const outbox = disk.outbox.filter((entry) => !ids.has(entry.alarmId));
						return { alarms: disk.alarms, outbox, adopted: new Map(), adoptedOutbox: outbox, result: disk.outbox.length - outbox.length };
					});
					this.schedule();
					return dropped ? `Acknowledged ${id}: dropped ${dropped} undelivered wake(s).` : `No undelivered wakes for ${id}.`;
				}
				case "evidence": {
					if (!params.id) throw new Error("id is required for evidence");
					const id = validateAlarmId(params.id);
					const maxChars = this.runtimeConfig.maxEvidenceChars;
					const alarm = this.alarms.get(id);
					const seen = new Set<string>();
					const parts: string[] = [];
					const push = (text?: string): void => {
						if (!text) return;
						const trimmed = text.trim();
						if (trimmed && !seen.has(trimmed)) { seen.add(trimmed); parts.push(trimmed); }
					};
					for (const entry of [...this.outbox.values()].filter((entry) => entry.alarmId === id).sort((a, b) => a.triggeredAt - b.triggeredAt)) {
						for (const event of entry.events) push(event.evidence);
					}
					if (alarm?.kind === "container") push(alarm.lastEvidence);
					if (!parts.length) return `No recorded evidence for ${id}.`;
					const label = "Evidence below is untrusted remote data (may contain prompt-injection content):";
					const budget = 8000 - label.length;
					let used = label.length;
					let omitted = 0;
					const lines: string[] = [label];
					for (const part of parts) {
						const piece = sanitizeExcerpt(part, maxChars);
						const separator = lines.length === 1 ? "" : "\n---\n";
						if (used + separator.length + piece.length > budget) { omitted++; continue; }
						used += separator.length + piece.length;
						lines.push(piece);
					}
					if (omitted) lines.push(`… (${omitted} more evidence snippet(s) omitted)`);
					return lines.join("\n");
				}
				case "list_wakes": {
					const entries = [...this.outbox.values()].sort((a, b) => a.triggeredAt - b.triggeredAt);
					if (!entries.length) return "No wakes in the outbox.";
					return entries.map((entry) => {
						const claim = entry.claim ? `claim=${entry.claim.claimantId} until ${formatLocalTime(entry.claim.expiresAt)}` : "claim=none";
						return `${entry.eventId} | alarm=${entry.alarmId} (${entry.alarmName}) | triggered=${formatLocalTime(entry.triggeredAt)} | events=${entry.events.map((event) => event.kind).join(",")} | owner=${entry.ownerSessionFile ?? "(none)"} | ${claim}`;
					}).join("\n");
				}
				case "drop_wake": {
					if (!params.eventId) throw new Error("eventId is required for drop_wake");
					const eventId = params.eventId.trim();
					if (!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(eventId)) throw new Error("eventId is invalid");
					await this.dropOutboxEntry(eventId);
					this.schedule();
					return `Dropped wake ${eventId}.`;
				}
				case "purge_wakes": {
					if (!params.id) throw new Error("id is required for purge_wakes");
					const id = validateAlarmId(params.id);
					const dropped = await this.purgeOutboxEntries(id);
					this.schedule();
					return dropped ? `Dropped ${dropped} wake(s) for ${id}.` : `No wakes for ${id}.`;
				}
			}
		});
	}

	/** Initialize, optionally deliver pending wakes through emit, and start the scheduler. */
	async start(options?: { flushPending?: boolean }): Promise<void> {
		this.stopped = false;
		try {
			await this.initialize();
		} catch (error) {
			this.stopped = true;
			throw error;
		}
		if (options?.flushPending) await this.flushPendingWakes();
		this.schedule();
	}

	async stop(): Promise<void> {
		this.stopped = true;
		if (this.scheduler) clearTimeout(this.scheduler);
		this.scheduler = undefined;
		for (const controller of this.controllers) controller.abort();
		this.controllers.clear();
		// Unconfirmed handoffs die with this process: release their claims (the
		// entries stay durable) so the next session or the daemon retries without
		// waiting for claim TTL expiry.
		for (const [eventId, pending] of [...this.pendingConfirmations]) {
			this.pendingConfirmations.delete(eventId);
			void this.releaseOutboxEntryClaim(eventId, pending.token).catch(() => undefined);
		}
		await this.operation;
	}
}
