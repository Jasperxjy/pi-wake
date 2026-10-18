import { randomUUID } from "node:crypto";
import path from "node:path";
import { StringEnum, Type } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { OutboxEntry } from "./core.ts";
import { spawn } from "node:child_process";
import { access } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import {
	detectSystemLanguage,
	formatFooterStatus,
	formatWidgetLines,
	readPrefs,
	resolveLanguage,
	updatePrefs,
	writePrefs,
	type UiDisplay,
} from "./ui-text.ts";
import { PRESENCE_DIR_NAME, compareVersions, createPresenceRefresher, formatPresenceFailure, leaderInstanceId, readDaemonLiveness, readPkgVersion, releasePresence, type PresenceRefresher } from "./presence.ts";
import { ACTION_DESCRIPTION, PROMPT_GUIDELINES, PROMPT_SNIPPET, TOOL_DESCRIPTION } from "./prompts.ts";
import {
	ACTION_ENUM,
	WakeAlarmRuntime,
	readOnlySnapshot,
	type AlarmDigest,
	type ToolParams,
} from "./runtime.ts";

const PRESENCE_HEARTBEAT_MS = 15_000;
const TOOL_PARAMETERS = Type.Object({
	action: StringEnum(ACTION_ENUM, { description: ACTION_DESCRIPTION }),
	id: Type.Optional(Type.String({ description: "Alarm ID. Required for alarm creation and alarm-targeted lifecycle/outbox actions; optional for check. Not used by list, list_wakes, drop_wake, or set_language." })),
	name: Type.Optional(Type.String({ description: "Short factual alarm name, required when creating an alarm" })),
	after: Type.Optional(Type.String({ description: "Relative timer delay, e.g. 30m; timer reset also accepts it" })),
	at: Type.Optional(Type.String({ description: "Absolute ISO-8601 timer timestamp with an explicit timezone, e.g. 2026-09-01T18:00:00+08:00 or 2026-09-01T10:00:00Z; exactly one of after or at for timers" })),
	container: Type.Optional(Type.String({ description: "Docker container name or ID for watch_container" })),
	containers: Type.Optional(Type.Array(Type.String({ description: "Container names for watch_container_group (unique, 2-64)" }), { minItems: 2, maxItems: 64, description: "Batch containers" })),
	events: Type.Optional(Type.Array(StringEnum(["exit", "abnormal", "missing", "replaced", "log-error", "log-match", "deadline", "connection-failure"] as const), { minItems: 1, maxItems: 8, description: "OR-combined container events. exit = clean exit code 0 only; abnormal = nonzero/OOM/dead/restarting. Use both exit+abnormal for 'wake when the container finishes regardless of outcome'" })),
	policy: Type.Optional(StringEnum(["pause", "keep"] as const, { description: "pause after a trigger (default), or keep monitoring with dedupe" })),
	logPath: Type.Optional(Type.String({ description: "Authoritative absolute remote application log path" })),
	logPattern: Type.Optional(Type.String({ description: "Literal (not regex) required with log-match" })),
	deadline: Type.Optional(Type.String({ description: "Relative container-watch deadline required with the deadline event" })),
	statusPoll: Type.Optional(Type.String({ description: "Deterministic model-free condition polling interval, e.g. 60s; minimum 1s" })),
	eventId: Type.Optional(Type.String({ description: "Outbox wake eventId; required for drop_wake" })),
	condition: Type.Optional(StringEnum(["any_terminal", "all_terminal", "any_abnormal", "n_of_m_terminal", "exists", "contains", "min_size"] as const, { description: "watch_container_group: any_terminal | all_terminal | any_abnormal | n_of_m_terminal. watch_condition: exists | contains | min_size" })),
	required: Type.Optional(Type.Integer({ description: "Members that must be terminal for n_of_m_terminal (default: all)" })),
	coalesceWindow: Type.Optional(Type.String({ description: "Group wake coalescing window, e.g. 30s; all-terminal fires immediately" })),
	logTailLines: Type.Optional(Type.Integer({ description: "Attach the last N container log lines to exit/abnormal wake evidence (1-200)" })),
	path: Type.Optional(Type.String({ description: "Absolute remote file path for watch_condition (must be under allowedRemoteLogRoots)" })),
	value: Type.Optional(Type.String({ description: "Literal substring for the contains condition" })),
	minSize: Type.Optional(Type.Integer({ description: "Byte threshold for the min_size condition" })),
	ignoreBefore: Type.Optional(Type.String({ description: "watch_condition only: ignore files last modified before this time (absolute ISO timestamp with timezone, or relative like '5m'); guards against stale markers from previous runs" })),
	purgePendingEvents: Type.Optional(Type.Boolean({ description: "For remove only: also discard undelivered wakes of the removed alarm; for a group, also its ownership-valid members" })),
	language: Type.Optional(StringEnum(["auto", "en", "zh"] as const, { description: "set_language only: status bar / widget display language ('auto' follows the system locale)" })),
});

export default function wakeAlarmExtension(pi: ExtensionAPI) {
	const instanceId = randomUUID();
	let runtime: WakeAlarmRuntime | undefined;
	let ownerSessionFile: string | undefined;
	let presenceDir: string | undefined;
	let presenceHeartbeat: ReturnType<typeof setInterval> | undefined;
	let isLeader = false;
	let passive = false;
	let presenceFailures = 0;
	let presenceWarned = false;
	let uiNotify: ((message: string) => void) | undefined;
	let cwd = "";
	let presenceRefresher: PresenceRefresher | undefined;
	let generation = 0;
	let closing = true;
	let runtimeReady = false;
	// Unknown until this session's runtime has finished loading its config.
	let spawnDaemon: boolean | undefined;
	let shutdownPromise: Promise<void> | undefined;

	function refreshPresence(): Promise<void> {
		if (closing || !presenceRefresher) return Promise.resolve();
		return presenceRefresher.request({ version: 1, pid: process.pid, instanceId, sessionFile: ownerSessionFile, heartbeatAt: Date.now() });
	}

	function stopHeartbeat(): void {
		if (presenceHeartbeat) clearInterval(presenceHeartbeat);
		presenceHeartbeat = undefined;
	}

	function startHeartbeat(): void {
		stopHeartbeat();
		const epoch = generation;
		presenceHeartbeat = setInterval(() => {
			if (!closing && epoch === generation) void refreshPresence();
		}, PRESENCE_HEARTBEAT_MS);
		if (typeof presenceHeartbeat.unref === "function") presenceHeartbeat.unref();
	}

	// Deferred delivery confirmation: sessionEmit only hands the wake to pi. The
	// outbox entry is completed when the message is ECHOED into the conversation
	// (message_end with our eventId); an agent run that settles without echoing
	// means the host dropped it (abort clears queued messages) — release + retry.
	pi.on("message_end", (event) => {
		const message = (event as { message?: { customType?: string; details?: { eventId?: string } } }).message;
		const eventId = message?.customType === "wake-alarm" ? message.details?.eventId : undefined;
		if (eventId) { void runtime?.confirmDelivery(eventId); void refreshAlarmWidget(); }
	});
	pi.on("agent_settled", () => { runtime?.onDeliveryCycleSettled(); });

	function sessionEmit(entry: OutboxEntry): boolean {
		// The outbox entry carries a bounded message snapshot built at fire time with
		// the configured evidence policy; delivery never recomputes it from the
		// current alarm state, so a pause/reset in the meantime cannot change it.
		// deliverAs "steer": a wake that fires while the agent is mid-turn is injected
		// into the RUNNING turn at the next model step (the agent loop drains the
		// steering queue before every LLM request) — the wake interleaves between
		// tool calls instead of piling up at the turn boundary. An idle session
		// still gets triggerTurn => an immediate new turn.
		pi.sendMessage({ customType: "wake-alarm", content: entry.message, display: true, details: { alarmId: entry.alarmId, eventId: entry.eventId, events: entry.events.map((event) => event.kind) } }, { triggerTurn: true, deliverAs: "steer" });
		return true;
	}

	async function runAction(params: ToolParams): Promise<string> {
		if (params.action === "set_language") {
			// Display preference is owned by the shell (it renders the widget), not the runtime.
			const value = params.language ?? "auto";
			if (value !== "auto" && value !== "en" && value !== "zh") throw new Error("language must be auto, en, or zh");
			await updatePrefs(cwd, { language: value });
			await refreshAlarmWidget();
			const effective = value === "auto" ? detectSystemLanguage() : value;
			return effective === "zh"
				? `显示语言已切换为中文（${value === "auto" ? "自动跟随系统" : "手动设置"}）。`
				: `Display language set to English (${value === "auto" ? "auto, follows the system locale" : "manual"}).`;
		}
		if (!runtime) {
			// Diagnostic entry points stay usable even when the runtime is dead (failed
			// session_start, foreign project): list/list_wakes/check degrade to a
			// read-only disk snapshot instead of a hard refusal.
			if (params.action === "list" || params.action === "check") return readOnlySnapshot({ cwd, filterId: params.id });
			if (params.action === "list_wakes") return readOnlySnapshot({ cwd });
			throw new Error("wake alarm has not received session_start; list/list_wakes/check work read-only");
		}
		return runtime.runAction(params, { ownerSessionFile });
	}

	// ---- daemon liveness + auto-spawn -------------------------------------
	// Wakes that fire while ALL sessions are closed are delivered by the daemon.
	// Sessions therefore (a) surface a missing daemon and (b) start one, so the
	// "set a watch, close the laptop, never get woken" failure mode cannot happen
	// silently. spawnDaemon:false (or WAKE_ALARM_NO_AUTOSPAWN=1) keeps manual control.
	let daemonSpawnedAt = 0;
	let daemonWarned = false;

	type DaemonTarget = { cwd: string; passive: boolean; spawnDaemon: boolean | undefined };

	function daemonTarget(): DaemonTarget { return { cwd, passive, spawnDaemon }; }

	function daemonAutoSpawnAllowed(target: DaemonTarget): boolean {
		return !target.passive && target.spawnDaemon === true && process.env.WAKE_ALARM_NO_AUTOSPAWN !== "1";
	}

	/** Prefer the built daemon when the package ships dist/ (published install);
	 * fall back to the TS source (repo checkout, Node >= 22.19 type stripping). */
	let daemonEntryCache: string | undefined;

	async function resolveDaemonEntry(): Promise<string | undefined> {
		if (daemonEntryCache) return daemonEntryCache;
		// jiti may load this extension as CJS where import.meta.url is unavailable;
		// fall back to __filename before giving up (the note path still degrades safely).
		let base: string | undefined;
		try { base = fileURLToPath(new URL(".", import.meta.url)); } catch { /* CJS host */ }
		if (!base) { try { const filename = (globalThis as Record<string, unknown>).__filename; if (typeof filename === "string") base = path.dirname(filename); } catch { /* no CJS handle either */ } }
		if (!base) return undefined;
		for (const candidate of [path.resolve(base, "..", "..", "dist", "daemon.js"), path.resolve(base, "daemon.ts")]) {
			try { await access(candidate); daemonEntryCache = candidate; return candidate; } catch { /* try next */ }
		}
		return undefined;
	}

	async function ensureDaemon(target = daemonTarget(), handoff = false): Promise<string> {
		const epoch = generation;
		const allowed = () => daemonAutoSpawnAllowed(target) && (handoff || (!closing && epoch === generation));
		if (!allowed()) return daemonWarned ? "" : "daemon auto-start disabled";
		const liveness = await readDaemonLiveness(target.cwd);
		if (!allowed()) return "";
		// A degraded daemon (unreadable state, scheduling stopped) is NOT healthy:
		// replacing it is allowed, and the fresh daemon's guard takes the role over.
		if (liveness.live && !liveness.heartbeat?.degraded) return "";
		if (Date.now() - daemonSpawnedAt < 60_000) return "";
		const entry = await resolveDaemonEntry();
		if (!allowed()) return "";
		if (!entry) return "no pi-wake daemon entry found (package incomplete?)";
		try {
			const child = spawn(process.execPath, [entry], { cwd: target.cwd, detached: true, stdio: "ignore", windowsHide: true, env: { ...process.env, WAKE_ALARM_CWD: target.cwd } });
			child.unref();
			daemonSpawnedAt = Date.now();
			return `started pi-wake daemon (pid ${child.pid ?? "?"})`;
		} catch (error) {
			return `cannot start the pi-wake daemon: ${(error as Error).message}`;
		}
	}

	// A daemon must not linger in a state newer code can fix: the 15s presence
	// tick watches for a DEGRADED heartbeat (scheduling stopped) or an OLDER
	// code version (long-lived daemon predating an upgrade — the 2026-09
	// incident) and spawns a replacement, whose guard takes the role over.
	// Replacement spawns are rate-limited hard: a state unreadable by the
	// CURRENT code would otherwise respawn a doomed daemon every tick forever.
	let daemonDegradedSpawnedAt = 0;
	const DAEMON_DEGRADED_RESPAWN_MS = 300_000;
	const ownPkgVersion = readPkgVersion(path.dirname(fileURLToPath(import.meta.url)));
	async function maybeReplaceDaemon(): Promise<void> {
		const epoch = generation;
		const target = daemonTarget();
		const allowed = () => !closing && epoch === generation && daemonAutoSpawnAllowed(target);
		if (!allowed()) return;
		let liveness;
		try { liveness = await readDaemonLiveness(target.cwd); } catch { return; }
		if (!allowed() || !liveness.live || !liveness.heartbeat) return;
		const reason = liveness.heartbeat.degraded
			? `degraded (${liveness.heartbeat.degraded})`
			: liveness.heartbeat.pkgVersion && compareVersions(ownPkgVersion, liveness.heartbeat.pkgVersion) > 0
				? `older code (${liveness.heartbeat.pkgVersion} < ${ownPkgVersion})`
				: undefined;
		if (!reason) return;
		if (Date.now() - daemonDegradedSpawnedAt < DAEMON_DEGRADED_RESPAWN_MS) return;
		const entry = await resolveDaemonEntry();
		if (!allowed() || !entry) return;
		try {
			const child = spawn(process.execPath, [entry], { cwd: target.cwd, detached: true, stdio: "ignore", windowsHide: true, env: { ...process.env, WAKE_ALARM_CWD: target.cwd } });
			child.unref();
			daemonDegradedSpawnedAt = Date.now();
			uiNotify?.(`Wake alarm: daemon is ${reason}; started a replacement (pid ${child.pid ?? "?"})`);
		} catch { /* the next tick retries after the cooldown */ }
	}

	async function daemonNote(action: string): Promise<string> {
		if (action !== "set_timer" && action !== "watch_container" && action !== "watch_container_group" && action !== "watch_condition") return "";
		const epoch = generation;
		const outcome = await ensureDaemon();
		if (closing || epoch !== generation || !outcome) return "";
		if (outcome.startsWith("started")) return ` (${outcome}; it delivers wakes while all sessions are closed)`;
		if (!daemonWarned) {
			daemonWarned = true;
			uiNotify?.(`Wake alarm: ${outcome}; wakes that fire while all sessions are closed stay in the outbox until the next session`);
		}
		return ` (note: ${outcome})`;
	}

	// ---- status bar + widget (works in the TUI and in pi-web via RPC) --------
	// Plain text only: RPC widgets accept string arrays (component factories are
	// ignored over RPC), and ANSI colors would render as garbage on the web side.
	const STATUS_KEY = "wake";
	const WIDGET_KEY = "wake-alarms";
	const WIDGET_MAX_ENTRIES = 5;
	let uiSet: { setStatus: (key: string, text: string | undefined) => void; setWidget: (key: string, lines: string[] | undefined) => void } | undefined;

	async function refreshAlarmWidget(): Promise<void> {
		const epoch = generation;
		const ui = uiSet;
		const runtimeNow = runtime;
		const projectCwd = cwd;
		// The first presence is written before runtime creation. Do not let an
		// early empty snapshot clear a later, fully initialized widget.
		if (closing || !ui || !runtimeNow || !runtimeReady) return;
		const current = () => !closing && epoch === generation && uiSet === ui && runtime === runtimeNow && runtimeReady;
		let daemonLive = false;
		let daemonDegraded = false;
		try {
			const liveness = await readDaemonLiveness(projectCwd);
			if (!current()) return;
			daemonDegraded = liveness.live && Boolean(liveness.heartbeat?.degraded);
			daemonLive = liveness.live && !daemonDegraded;
		}
		catch { /* display only */ }
		if (!current()) return;
		const digest: AlarmDigest = runtimeNow.alarmDigest();
		if (!digest || digest.active === 0) {
			ui.setStatus(STATUS_KEY, undefined);
			ui.setWidget(WIDGET_KEY, undefined);
			return;
		}
		// Language: tool preference (set_language) > config uiLanguage > system locale.
		const prefs = await readPrefs(projectCwd).catch(() => undefined);
		if (!current()) return;
		const language = resolveLanguage(prefs?.language, runtimeNow.runtimeConfig.uiLanguage, detectSystemLanguage());
		const render = { language, daemonLive, degraded: daemonDegraded };
		const display: UiDisplay = prefs?.display ?? "full";
		if (display === "off") {
			ui.setStatus(STATUS_KEY, undefined);
			ui.setWidget(WIDGET_KEY, undefined);
			return;
		}
		ui.setStatus(STATUS_KEY, formatFooterStatus(digest, render));
		// short = footer only; full = footer + the detail table.
		ui.setWidget(WIDGET_KEY, display === "full" ? formatWidgetLines(digest, { ...render, maxEntries: WIDGET_MAX_ENTRIES }) : undefined);
	}

	pi.registerTool({
		name: "wake_alarm",
		label: "Wake Alarm",
		description: TOOL_DESCRIPTION,
		promptSnippet: PROMPT_SNIPPET,
		promptGuidelines: [...PROMPT_GUIDELINES],
		parameters: TOOL_PARAMETERS,
		async execute(_toolCallId, params) {
			const details: { action: string; error?: boolean } = { action: params.action as string };
			try {
				const text = await runAction(params as ToolParams);
				const note = await daemonNote(params.action as string).catch(() => "");
				void refreshAlarmWidget();
				return { content: [{ type: "text" as const, text: note ? `${text}${note}` : text }], details };
			}
			catch (error) { return { content: [{ type: "text" as const, text: `Wake alarm error: ${(error as Error).message}` }], details: { ...details, error: true } }; }
		},
	});

	pi.registerCommand("wake-alarm", {
		description: "Manage wake alarms; also show/short/close to control the status bar + widget",
		handler: async (args, ctx) => {
			const trimmed = args.trim();
			try {
				// Display-mode controls: the widget and footer are shared screen real
				// estate, so users can fold them away without touching the alarms.
				const DISPLAY_WORDS: Record<string, UiDisplay> = { show: "full", detail: "full", short: "short", close: "off", hide: "off" };
				const word = trimmed.toLowerCase();
				const display = DISPLAY_WORDS[word];
				if (display) {
					await updatePrefs(cwd, { display });
					await refreshAlarmWidget();
					const zh = resolveLanguage((await readPrefs(cwd).catch(() => undefined))?.language, runtime?.runtimeConfig.uiLanguage, detectSystemLanguage()) === "zh";
					const confirmations: Record<UiDisplay, [string, string]> = {
						full: ["Widget + status bar shown.", "已恢复完整显示（状态栏 + 表格）。"],
						short: ["Compact mode: status bar only.", "已切换为简洁显示（仅状态栏）。"],
						off: ["Display off: no status bar, no widget (alarms keep running).", "已关闭显示（闹钟照常运行，可用 /wake-alarm show 恢复）。"],
					};
					ctx.ui.notify(zh ? confirmations[display][1] : confirmations[display][0], "info");
					return;
				}
				let params: ToolParams;
				if (trimmed.startsWith("{")) params = JSON.parse(trimmed) as ToolParams;
				else {
					const [action, id] = trimmed.split(/\s+/);
					if (!(action === "list" || action === "check" || action === "pause" || action === "resume" || action === "remove")) throw new Error("usage: /wake-alarm <JSON request> | list | check [id] | pause|resume|remove <id> | show|short|close (display)");
					params = { action, id: id || undefined } as ToolParams;
				}
				ctx.ui.notify(await runAction(params), "info");
			} catch (error) { ctx.ui.notify((error as Error).message, "error"); }
		},
	});

	pi.on("session_start", async (_event, ctx: ExtensionContext) => {
		const epoch = ++generation;
		const current = () => !closing && epoch === generation;
		closing = false;
		runtimeReady = false;
		spawnDaemon = undefined;
		shutdownPromise = undefined;
		cwd = ctx.cwd;
		passive = process.env.WAKE_ALARM_PASSIVE === "1";
		ownerSessionFile = ctx.sessionManager.getSessionFile() ?? undefined;
		isLeader = false;
		presenceDir = undefined;
		presenceFailures = 0;
		presenceWarned = false;
		uiNotify = ctx.hasUI ? (message) => ctx.ui.notify(message, "warning") : undefined;
		// hasUI covers TUI and RPC (pi-web); print mode (daemon-woken runs) has none.
		uiSet = ctx.hasUI ? { setStatus: (key, text) => ctx.ui.setStatus(key, text), setWidget: (key, lines) => ctx.ui.setWidget(key, lines) } : undefined;
		// Presence is established BEFORE any scheduling or wake flushing: each live
		// session owns one registry file, so this never contends with other sessions.
		if (!passive) {
			presenceDir = path.join(ctx.cwd, ".pi", PRESENCE_DIR_NAME);
			presenceRefresher = createPresenceRefresher(presenceDir, {
				onLive: (live) => {
					if (!current()) return;
					isLeader = leaderInstanceId(live) === instanceId;
					presenceFailures = 0;
					presenceWarned = false;
					void refreshAlarmWidget();
					void maybeReplaceDaemon();
				},
				onError: (error) => {
					if (!current()) return;
					presenceFailures++;
					if (!presenceWarned || presenceFailures % 5 === 0) {
						presenceWarned = true;
						uiNotify?.(formatPresenceFailure(error, presenceFailures));
					}
				},
			});
			await refreshPresence();
			if (!current()) return;
		}
		const starting = new WakeAlarmRuntime({
			cwd: ctx.cwd,
			emit: sessionEmit,
			deferDeliveryCompletion: true,
			execFn: (file, args, options) => pi.exec(file, args, options),
			schedulingEnabled: !passive,
			owns: (alarm) => alarm.ownerSessionFile === undefined ? isLeader : alarm.ownerSessionFile === ownerSessionFile,
			claimantId: `session:${instanceId}`,
		});
		runtime = starting;
		try {
			await starting.start({ flushPending: !passive });
			if (!current()) { await starting.stop(); return; }
			spawnDaemon = starting.runtimeConfig.spawnDaemon;
			runtimeReady = true;
			if (passive) return;
			startHeartbeat();
			void refreshAlarmWidget();
			if (ctx.hasUI && !isLeader) ctx.ui.notify("Wake alarm: another live session leads legacy alarms; this session schedules its own.", "info");
			if (ctx.hasUI && runtime.retiredLegacy) ctx.ui.notify("Wake alarm retired the incompatible v1 periodic watcher state; no alarm was migrated.", "info");
			if (ctx.hasUI && runtime.alarmCount) ctx.ui.notify(`Wake alarm restored ${runtime.alarmCount} alarm(s).`, "info");
		} catch (error) {
			if (!current()) { await starting.stop().catch(() => undefined); return; }
			closing = true;
			const failedEpoch = ++generation;
			stopHeartbeat();
			const failedPresence = presenceRefresher;
			const failedDir = presenceDir;
			presenceRefresher = undefined;
			presenceDir = undefined;
			const drained = failedPresence?.close();
			runtime = undefined;
			runtimeReady = false;
			spawnDaemon = undefined;
			uiSet = undefined;
			uiNotify = undefined;
			try { await starting.stop(); }
			finally {
				try { await drained; }
				finally { if (failedDir) await releasePresence(failedDir, instanceId); }
			}
			if (ctx.hasUI && generation === failedEpoch) ctx.ui.notify(`Wake alarm disabled: ${(error as Error).message}`, "error");
		}
	});

	pi.on("session_shutdown", () => {
		if (shutdownPromise) return shutdownPromise;
		// Snapshot configuration before detaching runtime. Unknown config never
		// authorizes a daemon, and reading this snapshot cannot invoke a getter.
		const target = daemonTarget();
		closing = true;
		generation++;
		const presence = presenceRefresher;
		const dir = presenceDir;
		presenceRefresher = undefined;
		presenceDir = undefined;
		const drained = presence?.close();
		stopHeartbeat();
		try {
			uiSet?.setStatus(STATUS_KEY, undefined);
			uiSet?.setWidget(WIDGET_KEY, undefined);
		} catch { /* UI cleanup must not prevent presence cleanup. */ }
		uiSet = undefined;
		uiNotify = undefined;
		const current = runtime;
		runtime = undefined;
		runtimeReady = false;
		spawnDaemon = undefined;
		isLeader = false;
		shutdownPromise = (async () => {
			try {
				if (current) await current.stop();
				await drained;
				// Explicit handoff is allowed after closing, but still respects the
				// original session's passive/config/environment restrictions.
				await ensureDaemon(target, true).catch(() => "");
			} finally {
				try { await drained; }
				finally { if (dir) await releasePresence(dir, instanceId); }
			}
		})();
		return shutdownPromise;
	});
}
