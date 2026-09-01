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
import {
	PRESENCE_DIR_NAME,
	leaderInstanceId,
	listLivePresences,
	readDaemonLiveness,
	registerPresence,
	releasePresence,
} from "./presence.ts";
import {
	ACTION_ENUM,
	WakeAlarmRuntime,
	readOnlySnapshot,
	type AlarmDigest,
	type ToolParams,
} from "./runtime.ts";

const PRESENCE_HEARTBEAT_MS = 15_000;
const TOOL_PARAMETERS = Type.Object({
	action: StringEnum(ACTION_ENUM),
	id: Type.Optional(Type.String({ description: "Alarm ID; required except for list and optional for check" })),
	name: Type.Optional(Type.String({ description: "Short factual alarm name, required when creating an alarm" })),
	after: Type.Optional(Type.String({ description: "Relative timer delay, e.g. 30m; timer reset also accepts it" })),
	at: Type.Optional(Type.String({ description: "Absolute timer timestamp; exactly one of after or at for timers" })),
	container: Type.Optional(Type.String({ description: "Docker container name or ID for watch_container" })),
	containers: Type.Optional(Type.Array(Type.String({ description: "Container names for watch_container_group (unique, 2-64)" }), { minItems: 2, maxItems: 64, description: "Batch containers" })),
	events: Type.Optional(Type.Array(StringEnum(["exit", "abnormal", "missing", "replaced", "log-error", "log-match", "deadline", "connection-failure"] as const), { minItems: 1, maxItems: 8, description: "OR-combined container events" })),
	policy: Type.Optional(StringEnum(["pause", "keep"] as const, { description: "pause after a trigger (default), or keep monitoring with dedupe" })),
	logPath: Type.Optional(Type.String({ description: "Authoritative absolute remote application log path" })),
	logPattern: Type.Optional(Type.String({ description: "Literal (not regex) required with log-match" })),
	deadline: Type.Optional(Type.String({ description: "Relative container-watch deadline required with the deadline event" })),
	statusPoll: Type.Optional(Type.String({ description: "Deterministic model-free condition polling interval, e.g. 60s" })),
	eventId: Type.Optional(Type.String({ description: "Outbox wake eventId; required for drop_wake" })),
	condition: Type.Optional(StringEnum(["any_terminal", "all_terminal", "any_abnormal", "n_of_m_terminal", "exists", "contains", "min_size"] as const, { description: "Group barrier or remote file condition" })),
	required: Type.Optional(Type.Integer({ description: "Members that must be terminal for n_of_m_terminal (default: all)" })),
	coalesceWindow: Type.Optional(Type.String({ description: "Group wake coalescing window, e.g. 30s; all-terminal fires immediately" })),
	logTailLines: Type.Optional(Type.Integer({ description: "Attach the last N container log lines to exit/abnormal wake evidence (1-200)" })),
	path: Type.Optional(Type.String({ description: "Absolute remote file path for watch_condition (must be under allowedRemoteLogRoots)" })),
	value: Type.Optional(Type.String({ description: "Literal substring for the contains condition" })),
	minSize: Type.Optional(Type.Integer({ description: "Byte threshold for the min_size condition" })),
	ignoreBefore: Type.Optional(Type.String({ description: "watch_condition only: ignore files last modified before this time (absolute ISO timestamp with timezone, or relative like '5m'); guards against stale markers from previous runs" })),
	purgePendingEvents: Type.Optional(Type.Boolean({ description: "remove also clears this alarm's undelivered wakes" })),
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

	async function refreshPresence(): Promise<void> {
		if (!presenceDir) return;
		try {
			await registerPresence(presenceDir, { version: 1, pid: process.pid, instanceId, sessionFile: ownerSessionFile, heartbeatAt: Date.now() });
			const live = await listLivePresences(presenceDir);
			// Ownerless (legacy) alarms are scheduled by exactly one live session: the
			// deterministic leader (smallest instance id). No acquisition, no fencing.
			isLeader = leaderInstanceId(live) === instanceId;
			presenceFailures = 0;
			presenceWarned = false;
			void refreshAlarmWidget();
		} catch {
			// Presence is the daemon's only signal that this session has a real Pi
			// process. A silent failure here would let the daemon treat the session as
			// offline and spawn a second Pi on the same session file, so consecutive
			// failures must be visible instead of swallowed.
			presenceFailures++;
			if (!presenceWarned || presenceFailures % 5 === 0) {
				presenceWarned = true;
				uiNotify?.(`Wake alarm: cannot write session presence (${presenceFailures} consecutive failure(s)); the daemon may not detect this session — check write access to ${presenceDir}`);
			}
		}
	}

	function stopHeartbeat(): void {
		if (presenceHeartbeat) clearInterval(presenceHeartbeat);
		presenceHeartbeat = undefined;
	}

	function startHeartbeat(): void {
		stopHeartbeat();
		presenceHeartbeat = setInterval(() => void refreshPresence(), PRESENCE_HEARTBEAT_MS);
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

	function daemonAutoSpawnAllowed(): boolean {
		if (passive || process.env.WAKE_ALARM_NO_AUTOSPAWN === "1") return false;
		const config = runtime?.runtimeConfig;
		return config ? config.spawnDaemon : true;
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

	async function ensureDaemon(): Promise<string> {
		if (!daemonAutoSpawnAllowed()) return daemonWarned ? "" : "daemon auto-start disabled";
		const liveness = await readDaemonLiveness(cwd);
		if (liveness.live) return "";
		if (Date.now() - daemonSpawnedAt < 60_000) return "";
		const entry = await resolveDaemonEntry();
		if (!entry) return "no pi-wake daemon entry found (package incomplete?)";
		try {
			const child = spawn(process.execPath, [entry], { cwd, detached: true, stdio: "ignore", windowsHide: true, env: { ...process.env, WAKE_ALARM_CWD: cwd } });
			child.unref();
			daemonSpawnedAt = Date.now();
			return `started pi-wake daemon (pid ${child.pid ?? "?"})`;
		} catch (error) {
			return `cannot start the pi-wake daemon: ${(error as Error).message}`;
		}
	}

	async function daemonNote(action: string): Promise<string> {
		if (action !== "set_timer" && action !== "watch_container" && action !== "watch_container_group" && action !== "watch_condition") return "";
		const outcome = await ensureDaemon();
		if (!outcome) return "";
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
		// Capture the UI handle up front: an in-flight refresh must survive a
		// session_shutdown that nulls uiSet while our awaits are suspended.
		const ui = uiSet;
		if (!ui) return;
		let daemonLive = false;
		try { daemonLive = (await readDaemonLiveness(cwd)).live; }
		catch { /* display only */ }
		const runtimeNow = runtime;
		const digest: AlarmDigest | undefined = runtimeNow?.alarmDigest();
		if (!digest || digest.active === 0) {
			ui.setStatus(STATUS_KEY, undefined);
			ui.setWidget(WIDGET_KEY, undefined);
			return;
		}
		// Language: tool preference (set_language) > config uiLanguage > system locale.
		const prefs = await readPrefs(cwd).catch(() => undefined);
		const language = resolveLanguage(prefs?.language, runtimeNow?.runtimeConfig.uiLanguage, detectSystemLanguage());
		const render = { language, daemonLive };
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
		description: "Set and manage persistent alarms: one-shot timers, REMOTE Docker container watches over SSH (watch_container probes Docker on the configured remote host, not locally), batch barriers over many containers (watch_container_group — ONE summary wake when any/all/required members are terminal), completion-file conditions (watch_condition — a remote result file exists/contains a marker/reaches a size), bounded container log tails in exit wakes (logTailLines), an explicit outbox (list_wakes, drop_wake, purge_wakes, ack), and a display-language switch for the status bar / widget (set_language: auto/en/zh). Wakes that fire while ALL sessions are closed are delivered by the pi-wake daemon (auto-started; heartbeat in .pi/wake-alarm.daemon.json). Deterministic polling never wakes the model unless a configured event occurs.",
		promptSnippet: "Set named timers, container/group barriers, and completion-file conditions; manage wakes with ack/drop_wake",
		promptGuidelines: [
			"Use wake_alarm as a scheduling/event primitive. Prefer one watch_container_group for multi-container batches instead of many individual alarms: it emits a single summary wake. Alarm names are short labels, not plans or continuation instructions. Use ack/drop_wake to clear undelivered wakes you have already acted on.",
			"Wake delivery when all sessions are closed depends on the pi-wake daemon; if the tool result notes no live daemon, start one or expect wakes to wait in the outbox until the next session (list_wakes shows them).",
			"Reuse one alarm id for retries of the same watch (reset) instead of remove+recreate — ids are cheap identities, not one-shot handles. Keep logTailLines small (5-20); evidence is capped at 2000 chars on disk.",
			"watch_condition 'exists' cannot tell a stale marker from a fresh one: have drivers write run-specific markers inside the run directory that gets cleaned, prefer contains with a run-specific value, or set ignoreBefore so files older than the watch are ignored.",
		],
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
			await refreshPresence();
		}
		runtime = new WakeAlarmRuntime({
			cwd: ctx.cwd,
			emit: sessionEmit,
			deferDeliveryCompletion: true,
			execFn: (file, args, options) => pi.exec(file, args, options),
			schedulingEnabled: !passive,
			owns: (alarm) => alarm.ownerSessionFile === undefined ? isLeader : alarm.ownerSessionFile === ownerSessionFile,
			claimantId: `session:${instanceId}`,
		});
		try {
			await runtime.start({ flushPending: !passive });
			if (passive) return;
			startHeartbeat();
			void refreshAlarmWidget();
			if (ctx.hasUI && !isLeader) ctx.ui.notify("Wake alarm: another live session leads legacy alarms; this session schedules its own.", "info");
			if (ctx.hasUI && runtime.retiredLegacy) ctx.ui.notify("Wake alarm retired the incompatible v1 periodic watcher state; no alarm was migrated.", "info");
			if (ctx.hasUI && runtime.alarmCount) ctx.ui.notify(`Wake alarm restored ${runtime.alarmCount} alarm(s).`, "info");
		} catch (error) {
			const failed = runtime;
			runtime = undefined;
			if (failed) await failed.stop().catch(() => undefined);
			if (presenceDir) await releasePresence(presenceDir, instanceId);
			ctx.ui.notify(`Wake alarm disabled: ${(error as Error).message}`, "error");
		}
	});

	pi.on("session_shutdown", async () => {
		// Stop accepting and finish in-flight scheduler work first; only then
		// withdraw live presence, so the daemon cannot interleave mid-shutdown.
		stopHeartbeat();
		uiSet?.setStatus(STATUS_KEY, undefined);
		uiSet?.setWidget(WIDGET_KEY, undefined);
		uiSet = undefined;
		uiNotify = undefined;
		const current = runtime;
		runtime = undefined;
		if (current) await current.stop();
		// Closing the (possibly last) session: leave a live daemon behind so
		// pending wakes still fire. Best effort — spawnDaemon:false users manage it.
		await ensureDaemon().catch(() => "");
		if (presenceDir) { await releasePresence(presenceDir, instanceId); presenceDir = undefined; }
	});
}
