import { randomUUID } from "node:crypto";
import path from "node:path";
import { StringEnum, Type } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { OutboxEntry } from "./core.ts";
import {
	PRESENCE_DIR_NAME,
	leaderInstanceId,
	listLivePresences,
	registerPresence,
	releasePresence,
} from "./presence.ts";
import {
	ACTION_ENUM,
	WakeAlarmRuntime,
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
	events: Type.Optional(Type.Array(StringEnum(["exit", "abnormal", "missing", "replaced", "log-error", "log-match", "deadline", "connection-failure"] as const), { minItems: 1, maxItems: 8, description: "OR-combined container events" })),
	policy: Type.Optional(StringEnum(["pause", "keep"] as const, { description: "pause after a trigger (default), or keep monitoring with dedupe" })),
	logPath: Type.Optional(Type.String({ description: "Authoritative absolute remote application log path" })),
	logPattern: Type.Optional(Type.String({ description: "Literal (not regex) required with log-match" })),
	deadline: Type.Optional(Type.String({ description: "Relative container-watch deadline required with the deadline event" })),
	statusPoll: Type.Optional(Type.String({ description: "Deterministic model-free condition polling interval, e.g. 60s" })),
	eventId: Type.Optional(Type.String({ description: "Outbox wake eventId; required for drop_wake" })),
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

	function sessionEmit(entry: OutboxEntry): boolean {
		// The outbox entry carries a bounded message snapshot built at fire time with
		// the configured evidence policy; delivery never recomputes it from the
		// current alarm state, so a pause/reset in the meantime cannot change it.
		pi.sendMessage({ customType: "wake-alarm", content: entry.message, display: true, details: { alarmId: entry.alarmId, eventId: entry.eventId, events: entry.events.map((event) => event.kind) } }, { triggerTurn: true, deliverAs: "followUp" });
		return true;
	}

	async function runAction(params: ToolParams): Promise<string> {
		if (!runtime) throw new Error("wake alarm has not received session_start");
		return runtime.runAction(params, { ownerSessionFile });
	}

	pi.registerTool({
		name: "wake_alarm",
		label: "Wake Alarm",
		description: "Set and manage persistent one-shot time alarms and condition-based remote container alarms. Deterministic polling never wakes the model unless a configured event occurs.",
		promptSnippet: "Set named timers or container-event alarms and manage their lifecycle",
		promptGuidelines: ["Use wake_alarm as a scheduling/event primitive. Alarm names are short labels, not plans or continuation instructions."],
		parameters: TOOL_PARAMETERS,
		async execute(_toolCallId, params) {
			const details: { action: string; error?: boolean } = { action: params.action as string };
			try { const text = await runAction(params as ToolParams); return { content: [{ type: "text" as const, text }], details }; }
			catch (error) { return { content: [{ type: "text" as const, text: `Wake alarm error: ${(error as Error).message}` }], details: { ...details, error: true } }; }
		},
	});

	pi.registerCommand("wake-alarm", {
		description: "Manage wake alarms with lifecycle arguments or a JSON wake_alarm request",
		handler: async (args, ctx) => {
			const trimmed = args.trim();
			try {
				let params: ToolParams;
				if (trimmed.startsWith("{")) params = JSON.parse(trimmed) as ToolParams;
				else {
					const [action, id] = trimmed.split(/\s+/);
					if (!(action === "list" || action === "check" || action === "pause" || action === "resume" || action === "remove")) throw new Error("usage: /wake-alarm <JSON request> | list | check [id] | pause|resume|remove <id>");
					params = { action, id: id || undefined } as ToolParams;
				}
				ctx.ui.notify(await runAction(params), "info");
			} catch (error) { ctx.ui.notify((error as Error).message, "error"); }
		},
	});

	pi.on("session_start", async (_event, ctx: ExtensionContext) => {
		passive = process.env.WAKE_ALARM_PASSIVE === "1";
		ownerSessionFile = ctx.sessionManager.getSessionFile() ?? undefined;
		isLeader = false;
		presenceDir = undefined;
		presenceFailures = 0;
		presenceWarned = false;
		uiNotify = ctx.hasUI ? (message) => ctx.ui.notify(message, "warning") : undefined;
		// Presence is established BEFORE any scheduling or wake flushing: each live
		// session owns one registry file, so this never contends with other sessions.
		if (!passive) {
			presenceDir = path.join(ctx.cwd, ".pi", PRESENCE_DIR_NAME);
			await refreshPresence();
		}
		runtime = new WakeAlarmRuntime({
			cwd: ctx.cwd,
			emit: sessionEmit,
			execFn: (file, args, options) => pi.exec(file, args, options),
			schedulingEnabled: !passive,
			owns: (alarm) => alarm.ownerSessionFile === undefined ? isLeader : alarm.ownerSessionFile === ownerSessionFile,
			claimantId: `session:${instanceId}`,
		});
		try {
			await runtime.start({ flushPending: !passive });
			if (passive) return;
			startHeartbeat();
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
		uiNotify = undefined;
		const current = runtime;
		runtime = undefined;
		if (current) await current.stop();
		if (presenceDir) { await releasePresence(presenceDir, instanceId); presenceDir = undefined; }
	});
}
