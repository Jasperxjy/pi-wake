import { randomUUID } from "node:crypto";
import path from "node:path";
import { StringEnum, Type } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AlarmState, FiredEvent } from "./core.ts";
import {
	cleanStaleLeaseTemps,
	heartbeatLease,
	releaseLease,
	tryAcquireLease,
	type LeaseHandle,
} from "./lease.ts";
import {
	ACTION_ENUM,
	WakeAlarmRuntime,
	wakeMessage,
	type ToolParams,
} from "./runtime.ts";

const LEASE_NAME = "wake-alarm.lock.json";
const LEASE_HEARTBEAT_MS = 15_000;
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
});

export default function wakeAlarmExtension(pi: ExtensionAPI) {
	const instanceId = randomUUID();
	let runtime: WakeAlarmRuntime | undefined;
	let ownerSessionFile: string | undefined;
	let lease: LeaseHandle | undefined;
	let leaseHeartbeat: ReturnType<typeof setInterval> | undefined;
	let leaseHolder = false;
	let passive = false;

	function stopHeartbeat(): void {
		if (leaseHeartbeat) clearInterval(leaseHeartbeat);
		leaseHeartbeat = undefined;
	}

	function startHeartbeat(ctx: ExtensionContext): void {
		stopHeartbeat();
		leaseHeartbeat = setInterval(() => {
			const current = lease;
			if (!current) return;
			void heartbeatLease(current, ownerSessionFile).then((stillHolder) => {
				if (stillHolder) return;
				// Lost the fencing token: another session owns ownerless alarms from now on.
				lease = undefined;
				leaseHolder = false;
				stopHeartbeat();
				if (ctx.hasUI) ctx.ui.notify("Wake alarm lease moved to another session; scheduling only own alarms now.", "info");
			}).catch(() => undefined);
		}, LEASE_HEARTBEAT_MS);
		if (typeof leaseHeartbeat.unref === "function") leaseHeartbeat.unref();
	}

	function sessionEmit(alarm: AlarmState, events: FiredEvent[], now: number): boolean {
		let maxEvidenceChars = 1000;
		try { maxEvidenceChars = runtime?.runtimeConfig.maxEvidenceChars ?? maxEvidenceChars; } catch { /* config not ready */ }
		pi.sendMessage({ customType: "wake-alarm", content: wakeMessage(alarm, events, now, maxEvidenceChars), display: true, details: { alarmId: alarm.id, events: events.map((event) => event.kind) } }, { triggerTurn: true, deliverAs: "followUp" });
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
		lease = undefined;
		leaseHolder = false;
		// Live-presence is established BEFORE any scheduling or wake flushing, so a
		// standing daemon observes the lease before this session can start firing.
		if (!passive) {
			const candidate = path.join(ctx.cwd, ".pi", LEASE_NAME);
			await cleanStaleLeaseTemps(candidate);
			lease = await tryAcquireLease(candidate, instanceId, ownerSessionFile).catch(() => undefined);
			leaseHolder = lease !== undefined;
		}
		// Every live session schedules the alarms it owns (ownerSessionFile match).
		// The single lease holder additionally schedules ownerless (legacy) alarms,
		// which keeps multi-session hosts such as pi-web from double-firing them.
		runtime = new WakeAlarmRuntime({
			cwd: ctx.cwd,
			emit: sessionEmit,
			execFn: (file, args, options) => pi.exec(file, args, options),
			schedulingEnabled: !passive,
			owns: (alarm) => alarm.ownerSessionFile === undefined ? leaseHolder : alarm.ownerSessionFile === ownerSessionFile,
		});
		try {
			await runtime.start({ flushPending: !passive });
			if (passive) return;
			if (lease) startHeartbeat(ctx);
			else if (ctx.hasUI) ctx.ui.notify("Another session holds the wake-alarm lease; this session schedules only alarms it owns.", "info");
			if (ctx.hasUI && runtime.retiredLegacy) ctx.ui.notify("Wake alarm retired the incompatible v1 periodic watcher state; no alarm was migrated.", "info");
			if (ctx.hasUI && runtime.alarmCount) ctx.ui.notify(`Wake alarm restored ${runtime.alarmCount} alarm(s).`, "info");
		} catch (error) {
			const failed = runtime;
			runtime = undefined;
			if (failed) await failed.stop().catch(() => undefined);
			if (lease) { await releaseLease(lease); lease = undefined; leaseHolder = false; }
			ctx.ui.notify(`Wake alarm disabled: ${(error as Error).message}`, "error");
		}
	});

	pi.on("session_shutdown", async () => {
		stopHeartbeat();
		if (lease) { await releaseLease(lease); lease = undefined; leaseHolder = false; }
		const current = runtime;
		runtime = undefined;
		if (current) await current.stop();
	});
}
