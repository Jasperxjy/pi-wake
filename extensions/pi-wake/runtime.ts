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
	createContainerAlarm,
	createOutboxEntry,
	createTimerAlarm,
	decodeNewLog,
	nextAlarmDueAt,
	parseAbsoluteTime,
	parseDuration,
	restoreAlarmState,
	restoreOutbox,
	resumeAlarm,
	sanitizeExcerpt,
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
	type ContainerAlarmState,
	type ContainerEventKind,
	type FiredEvent,
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
	remote?: RemoteConfig;
	piCommand?: string;
	spawnOnWake: boolean;
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
	action: "set_timer" | "watch_container" | "list" | "check" | "pause" | "resume" | "reset" | "remove" | "evidence";
	id?: string;
	name?: string;
	after?: string;
	at?: string;
	container?: string;
	events?: ContainerEventKind[];
	policy?: "pause" | "keep";
	logPath?: string;
	logPattern?: string;
	deadline?: string;
	statusPoll?: string;
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
    name=req["container"]
    app_path=req.get("logPath")
    roots=req.get("allowedRemoteLogRoots") or []
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
export const ACTION_ENUM = ["set_timer", "watch_container", "list", "check", "pause", "resume", "reset", "remove", "evidence"] as const;
const ACTION_FIELDS: Record<ToolParams["action"], readonly (keyof ToolParams)[]> = {
	set_timer: ["action", "id", "name", "after", "at"],
	watch_container: ["action", "id", "name", "container", "events", "policy", "logPath", "logPattern", "deadline", "statusPoll"],
	list: ["action"],
	check: ["action", "id"],
	pause: ["action", "id"],
	resume: ["action", "id"],
	reset: ["action", "id", "after", "at"],
	remove: ["action", "id"],
	evidence: ["action", "id"],
};

function shellSingleQuote(value: string): string {
	return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function asInt(value: unknown, name: string, min: number, max: number): number {
	if (!Number.isSafeInteger(value) || (value as number) < min || (value as number) > max) throw new Error(`${name} must be an integer from ${min} to ${max}`);
	return value as number;
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

function validateActionParams(params: ToolParams): void {
	const allowed = new Set<string>(ACTION_FIELDS[params.action]);
	const irrelevant = Object.entries(params).filter(([key, value]) => !allowed.has(key) && value !== undefined).map(([key]) => key);
	if (irrelevant.length) throw new Error(`${params.action} does not accept: ${irrelevant.join(", ")}`);
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

export function alarmSummary(alarm: AlarmState): string {
	const lifecycle = alarm.active ? "active" : `paused${alarm.pauseReason ? ` (${alarm.pauseReason})` : ""}`;
	if (alarm.kind === "timer") return `${alarm.id}: ${alarm.name} — timer ${lifecycle}; due=${new Date(alarm.dueAt).toISOString()}${alarm.triggeredAt === undefined ? "" : `; fired=${new Date(alarm.triggeredAt).toISOString()}`}`;
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
		const KNOWN_KEYS = ["remote", "statusPoll", "maxLogBytes", "maxEvidenceChars", "piCommand", "spawnOnWake", "runTimeout", "headlessTrust", "includeWakeEvidence"];
		const unknownKeys = Object.keys(parsed).filter((key) => !KNOWN_KEYS.includes(key));
		if (unknownKeys.length) throw new Error(`${CONFIG_NAME} contains unknown field(s): ${unknownKeys.join(", ")}`);
		for (const retired of ["semanticReview", "maximumRuntime"]) if (retired in parsed) throw new Error(`${retired} is retired; wake alarms fire only for explicitly configured timers or conditions`);
		if (parsed.piCommand !== undefined && (typeof parsed.piCommand !== "string" || !parsed.piCommand || parsed.piCommand.length > 512 || parsed.piCommand.includes("\0"))) throw new Error("piCommand must be a non-empty command path no longer than 512 characters");
		if (parsed.spawnOnWake !== undefined && typeof parsed.spawnOnWake !== "boolean") throw new Error("spawnOnWake must be boolean");
		if (parsed.includeWakeEvidence !== undefined && typeof parsed.includeWakeEvidence !== "boolean") throw new Error("includeWakeEvidence must be boolean");
		if (parsed.headlessTrust !== undefined && parsed.headlessTrust !== "saved" && parsed.headlessTrust !== "always") throw new Error("headlessTrust must be \"saved\" or \"always\"");
		this.config = {
			statusPollMs: validatePollingDuration(parseDuration((parsed.statusPoll ?? "60s") as string | number, "statusPoll")),
			maxLogBytes: asInt(parsed.maxLogBytes ?? 65_536, "maxLogBytes", 1024, 262_144),
			maxEvidenceChars: asInt(parsed.maxEvidenceChars ?? 1000, "maxEvidenceChars", 100, 4000),
			remote: await parseRemoteConfig(parsed.remote, path.dirname(configPath)),
			piCommand: parsed.piCommand as string | undefined,
			spawnOnWake: parsed.spawnOnWake === undefined ? true : (parsed.spawnOnWake as boolean),
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
			if (raw.version === 1) {
				await fs.rm(this.statePath, { force: true });
				this.retiredLegacyState = true;
				return;
			}
			if (raw.version === 2) {
				await this.migrateLegacyState(raw);
				return;
			}
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
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new Error(`Cannot restore ${path.basename(this.statePath)}: ${(error as Error).message}`);
		}
	}

	/** One-time v2 -> v3 migration: move each alarm's embedded pendingWake into the outbox, then rewrite as version 3. */
	private async migrateLegacyState(raw: Record<string, unknown>): Promise<void> {
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
		await this.withStateLock(async () => {
			if (await readStoredState(this.statePath)) return; // another runtime already migrated
			const restored: AlarmState[] = [];
			for (const [index, value] of migratedAlarms.entries()) {
				try {
					restored.push(restoreAlarmState(value, this.runtimeConfig.remote?.allowedRemoteLogRoots ?? []));
				} catch (error) {
					throw new Error(`alarms[${index}] is invalid: ${(error as Error).message}; repair or remove ${path.basename(this.statePath)}`);
				}
			}
			restoreOutbox(migratedOutbox);
			await this.writeFullStateLocked(restored, migratedOutbox);
			for (const alarm of restored) { this.alarms.set(alarm.id, alarm); this.baseRevisions.set(alarm.id, alarm.revision ?? 0); }
			for (const entry of migratedOutbox) this.outbox.set(entry.eventId, entry);
		});
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
	 * The outbox is disk-authoritative here: alarm mutations never rewrite outbox
	 * facts, except that removing an alarm also drops its undelivered wakes.
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
			const outbox = diskState.outbox.filter((entry) => !this.deletedIds.has(entry.alarmId));
			await this.writeFullStateLocked([...byId.values()], outbox);
			this.outbox.clear();
			for (const entry of outbox) this.outbox.set(entry.eventId, entry);
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
		const remote = config.remote;
		if (!remote) throw new Error(`watch_container requires a remote SSH section in .pi/${CONFIG_NAME}`);
		const payload = Buffer.from(JSON.stringify({
			container: alarm.container,
			expectedId: rebind ? undefined : alarm.containerId,
			logPath: alarm.logPath,
			allowedRemoteLogRoots: remote.allowedRemoteLogRoots,
			offset: rebind ? 0 : alarm.logOffset,
			cursor: rebind ? undefined : alarm.logCursor,
			fileId: rebind ? undefined : alarm.logFileId,
			baseline,
			readLogs: Boolean(alarm.logPath) || alarm.events.includes("log-error") || alarm.events.includes("log-match"),
			maxBytes: config.maxLogBytes,
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

	private outboxOptions(): { maxEvidenceChars: number; includeWakeEvidence: boolean } {
		return { maxEvidenceChars: this.runtimeConfig.maxEvidenceChars, includeWakeEvidence: this.runtimeConfig.includeWakeEvidence };
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

	private async replaceAlarm(id: string, next: AlarmState, options?: { force?: boolean; intent?: (disk: AlarmState) => AlarmState }): Promise<void> {
		const previous = this.alarms.get(id);
		this.alarms.set(id, next);
		this.dirtyIds.add(id);
		this.deletedIds.delete(id);
		if (previous === undefined) this.createIds.add(id);
		if (options?.force) this.forceIds.add(id);
		if (options?.intent) this.intentMap.set(id, options.intent);
		try { await this.saveState(); }
		catch (error) {
			if (previous) { this.alarms.set(id, previous); this.dirtyIds.add(id); } else { this.alarms.delete(id); this.dirtyIds.delete(id); }
			this.createIds.delete(id);
			this.forceIds.delete(id);
			this.intentMap.delete(id);
			throw error;
		}
	}

	private async removeAlarm(id: string): Promise<void> {
		const previous = this.alarms.get(id);
		if (!previous) throw new Error(`unknown alarm: ${id}`);
		this.alarms.delete(id);
		this.dirtyIds.delete(id);
		this.createIds.delete(id);
		this.deletedIds.add(id);
		for (const eventId of [...this.outbox.keys()]) {
			if (this.outbox.get(eventId)?.alarmId === id) this.outbox.delete(eventId);
		}
		for (const eventId of [...this.wakeRetry.keys()]) if (!this.outbox.has(eventId)) this.wakeRetry.delete(eventId);
		try { await this.saveState(); }
		catch (error) { this.alarms.set(id, previous); this.deletedIds.delete(id); this.dirtyIds.add(id); throw error; }
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
		if (delivered) await this.completeOutboxEntry(eventId, token);
		else { await this.releaseOutboxEntryClaim(eventId, token); this.noteWakeRetry(eventId); }
	}

	private async flushPendingWakes(): Promise<void> {
		for (const entry of [...this.outbox.values()].sort((a, b) => a.triggeredAt - b.triggeredAt)) {
			if (this.stopped) return;
			if (!this.ownsEntry(entry)) continue;
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
			const diskState = await this.readDiskState();
			const alarms = diskState.alarms.map((alarm) => (alarm.id === id ? next : alarm));
			if (shouldEmit) {
				const entry = createOutboxEntry(decision.state, decision.events, now, this.outboxOptions());
				await this.writeFullStateLocked(alarms, [...diskState.outbox, entry]);
				this.outbox.set(entry.eventId, entry);
			} else {
				await this.writeFullStateLocked(alarms, diskState.outbox);
			}
			this.adopt(next);
			return { alarm: next, events: decision.events };
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
				const entry = createOutboxEntry(deadline.state, deadline.events, Date.now(), this.outboxOptions());
				const diskState = await this.readDiskState();
				await this.writeFullStateLocked(diskState.alarms.map((alarm) => (alarm.id === id ? next : alarm)), [...diskState.outbox, entry]);
				this.adopt(next);
				this.outbox.set(entry.eventId, entry);
				return { base: undefined, revision: -1, deadline: { state: current, events: [] }, committed: { alarm: next, events: deadline.events } };
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
			const diskState = await this.readDiskState();
			const alarms = diskState.alarms.map((alarm) => (alarm.id === id ? next : alarm));
			if (shouldEmit && events.length) {
				const entry = createOutboxEntry(next, events, Date.now(), this.outboxOptions());
				await this.writeFullStateLocked(alarms, [...diskState.outbox, entry]);
				this.outbox.set(entry.eventId, entry);
			} else {
				await this.writeFullStateLocked(alarms, diskState.outbox);
			}
			this.adopt(next);
			return { alarm: next, events };
		});
	}

	private effectiveDueAt(alarm: AlarmState): number | undefined {
		const alarmDue = nextAlarmDueAt(alarm);
		let entryDue: number | undefined;
		for (const entry of this.ownedEntries(alarm.id)) {
			const retry = this.wakeRetry.get(entry.eventId);
			const due = retry && retry.nextAt > Date.now() ? retry.nextAt : entry.triggeredAt;
			entryDue = entryDue === undefined ? due : Math.min(entryDue, due);
		}
		if (alarmDue === undefined) return entryDue;
		return entryDue === undefined ? alarmDue : Math.min(alarmDue, entryDue);
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
		const owned = [...this.alarms.values()].filter((alarm) => this.ownsAlarm(alarm));
		const due = owned.map((alarm) => this.effectiveDueAt(alarm)).filter((value): value is number => value !== undefined);
		if (!due.length) return;
		this.scheduler = setTimeout(() => {
			this.scheduler = undefined;
			this.serialize(async () => {
				await this.reconcileFromDisk();
				const now = Date.now();
				let failed = false;
				for (const alarm of [...this.alarms.values()]) {
					if (this.stopped || !this.ownsAlarm(alarm)) continue;
					const dueAt = this.effectiveDueAt(alarm);
					if (dueAt === undefined || dueAt > now) continue;
					try {
						for (const entry of this.ownedEntries(alarm.id)) {
							const retry = this.wakeRetry.get(entry.eventId);
							const entryDue = retry && retry.nextAt > Date.now() ? retry.nextAt : entry.triggeredAt;
							if (entryDue <= Date.now()) await this.deliverOutboxEntry(entry.eventId);
						}
						const current = this.alarms.get(alarm.id);
						if (this.stopped || !current) continue;
						const alarmDue = nextAlarmDueAt(current);
						if (alarmDue === undefined || alarmDue > Date.now()) continue;
						if (current.kind === "timer") await this.checkTimer(current.id, true);
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
		if (!config.remote) throw new Error(`watch_container requires a remote SSH section in .pi/${CONFIG_NAME}`);
		if (!params.id || !params.name || !params.container || !params.events) throw new Error("id, name, container, and events are required for watch_container");
		const id = validateAlarmId(params.id);
		if (this.alarms.has(id)) throw new Error(`alarm already exists: ${id}`);
		let alarm = createContainerAlarm({ id, name: params.name, container: params.container, events: params.events, policy: params.policy, logPath: params.logPath ? validateRemoteLogPath(params.logPath, config.remote.allowedRemoteLogRoots) : undefined, logPattern: params.logPattern, allowedRemoteLogRoots: config.remote.allowedRemoteLogRoots, now: Date.now(), statusPollMs: params.statusPoll ? parseDuration(params.statusPoll, "statusPoll") : config.statusPollMs, deadlineMs: params.deadline ? parseDuration(params.deadline, "deadline") : undefined, ownerSessionFile: context?.ownerSessionFile });
		const baseline = await this.probe(alarm, true, true);
		if (!baseline.exists) throw new Error(`container does not exist: ${alarm.container}`);
		alarm = applyBaseline(alarm, baseline, Date.now());
		await this.replaceAlarm(id, alarm); this.schedule(); return alarm;
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
		if (params.after !== undefined || params.at !== undefined) throw new Error("container reset does not accept after or at");
		const deadlineMs = current.deadlineAt === undefined ? undefined : current.deadlineAt - current.createdAt;
		let reset = createContainerAlarm({ id: current.id, name: current.name, container: current.container, events: current.events, policy: current.policy, logPath: current.logPath, logPattern: current.logPattern, allowedRemoteLogRoots: config.remote?.allowedRemoteLogRoots ?? [], now: Date.now(), statusPollMs: current.statusPollMs, deadlineMs, ownerSessionFile: current.ownerSessionFile });
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
				case "list": return this.alarms.size ? [...this.alarms.values()].map(alarmSummary).join("\n") : "No wake alarms.";
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
						const result = current.kind === "timer" ? await this.checkTimer(id, false) : await this.checkContainer(id, false, true);
						const pending = this.ownedEntries(id).length ? "; wake pending for the owner session or daemon" : "";
						lines.push(`${alarmSummary(result.alarm)}${result.events.length ? `; observed=${result.events.map((event) => event.kind).join(",")}` : ""}${pending}`);
					}
					this.schedule(); return lines.join("\n");
				}
				case "pause": {
					if (!params.id) throw new Error("id is required for pause");
					const id = validateAlarmId(params.id); const current = this.alarms.get(id);
					if (!current) throw new Error(`unknown alarm: ${id}`);
					await this.replaceAlarm(id, { ...current, active: false, pauseReason: "paused explicitly" }, { force: true, intent: (disk) => ({ ...disk, active: false, pauseReason: "paused explicitly" }) }); this.schedule(); return `Paused ${id}.`;
				}
				case "resume": {
					if (!params.id) throw new Error("id is required for resume");
					const id = validateAlarmId(params.id); const current = this.alarms.get(id);
					if (!current) throw new Error(`unknown alarm: ${id}`);
					const resumed = resumeAlarm(current, Date.now()); await this.replaceAlarm(id, resumed, { force: true, intent: (disk) => resumeAlarm(disk, Date.now()) }); this.schedule(); return `Resumed ${id}.`;
				}
				case "reset": {
					if (!params.id) throw new Error("id is required for reset");
					return `Reset ${alarmSummary(await this.resetOne(validateAlarmId(params.id), params))}`;
				}
				case "remove": {
					if (!params.id) throw new Error("id is required for remove");
					const id = validateAlarmId(params.id); await this.removeAlarm(id); this.schedule(); return `Removed ${id}.`;
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
					return parts.map((part) => sanitizeExcerpt(part, maxChars)).join("\n---\n");
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
		await this.operation;
	}
}
