import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { StringEnum, Type } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { leaseIsAlive, type AlarmState, type FiredEvent } from "./core.ts";
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
	let leasePath: string | undefined;
	let leaseHeartbeat: ReturnType<typeof setInterval> | undefined;
	let leaseWrite: Promise<void> = Promise.resolve();
	let leaseHolder = false;
	let passive = false;

	function pidAlive(pid: number): boolean {
		try { process.kill(pid, 0); return true; }
		catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
	}

	async function readLease(file: string): Promise<{ pid: number; instanceId?: string; heartbeatAt: number } | undefined> {
		try {
			const raw = JSON.parse(await fs.readFile(file, "utf8")) as { pid?: unknown; instanceId?: unknown; heartbeatAt?: unknown };
			if (typeof raw.pid !== "number" || typeof raw.heartbeatAt !== "number") return undefined;
			return { pid: raw.pid, instanceId: typeof raw.instanceId === "string" ? raw.instanceId : undefined, heartbeatAt: raw.heartbeatAt };
		} catch { return undefined; }
	}

	async function cleanStaleLeaseTemps(own: string): Promise<void> {
		try {
			const dir = path.dirname(own);
			const prefix = `${path.basename(own)}.tmp-`;
			for (const entry of await fs.readdir(dir)) {
				if (!entry.startsWith(prefix)) continue;
				const full = path.join(dir, entry);
				const stat = await fs.stat(full).catch(() => undefined);
				if (stat && Date.now() - stat.mtimeMs > 10 * 60_000) await fs.rm(full, { force: true }).catch(() => undefined);
			}
		} catch { /* best effort */ }
	}

	async function writeLease(): Promise<void> {
		if (!leasePath) return;
		const temp = `${leasePath}.tmp-${process.pid}`;
		const lease = { version: 1, pid: process.pid, instanceId, role: "session", sessionFile: ownerSessionFile, heartbeatAt: Date.now() };
		await fs.writeFile(temp, `${JSON.stringify(lease)}\n`, { encoding: "utf8" });
		try { await fs.rename(temp, leasePath); }
		catch (error) { await fs.rm(temp, { force: true }); throw error; }
	}

	function queueLeaseWrite(): void {
		leaseWrite = leaseWrite.then(writeLease, writeLease).catch(() => undefined);
	}

	async function acquireLease(cwd: string): Promise<void> {
		leasePath = path.join(cwd, ".pi", LEASE_NAME);
		await cleanStaleLeaseTemps(leasePath);
		await writeLease().catch(() => undefined);
		leaseHeartbeat = setInterval(queueLeaseWrite, LEASE_HEARTBEAT_MS);
		if (typeof leaseHeartbeat.unref === "function") leaseHeartbeat.unref();
	}

	async function releaseLease(): Promise<void> {
		if (leaseHeartbeat) clearInterval(leaseHeartbeat);
		leaseHeartbeat = undefined;
		const own = leasePath;
		leasePath = undefined;
		await leaseWrite;
		if (!own) return;
		await fs.rm(`${own}.tmp-${process.pid}`, { force: true }).catch(() => undefined);
		try {
			const raw = JSON.parse(await fs.readFile(own, "utf8")) as { instanceId?: unknown };
			if (raw.instanceId === instanceId) await fs.rm(own, { force: true });
		} catch { /* A missing or replaced lease is not ours to remove. */ }
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
			try { const text = await runAction(params as ToolParams); return { content: [{ type: "text", text }], details: { action: params.action } }; }
			catch (error) { return { content: [{ type: "text", text: `Wake alarm error: ${(error as Error).message}` }], details: { action: params.action, error: true } }; }
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
		const candidateLease = path.join(ctx.cwd, ".pi", LEASE_NAME);
		let heldElsewhere = false;
		if (!passive) {
			const existing = await readLease(candidateLease);
			heldElsewhere = Boolean(existing && existing.instanceId !== instanceId && leaseIsAlive(existing, Date.now(), pidAlive));
		}
		leaseHolder = !passive && !heldElsewhere;
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
			if (leaseHolder) await acquireLease(ctx.cwd);
			else if (ctx.hasUI) ctx.ui.notify("Another session holds the wake-alarm lease; this session schedules only alarms it owns.", "info");
			if (ctx.hasUI && runtime.retiredLegacy) ctx.ui.notify("Wake alarm retired the incompatible v1 periodic watcher state; no alarm was migrated.", "info");
			if (ctx.hasUI && runtime.alarmCount) ctx.ui.notify(`Wake alarm restored ${runtime.alarmCount} alarm(s).`, "info");
		} catch (error) {
			runtime = undefined;
			ctx.ui.notify(`Wake alarm disabled: ${(error as Error).message}`, "error");
		}
	});

	pi.on("session_shutdown", async () => {
		await releaseLease();
		const current = runtime;
		runtime = undefined;
		if (current) await current.stop();
	});
}
