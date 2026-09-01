/**
 * Display-layer i18n for the status bar and widget (zh / en).
 *
 * The runtime's alarmDigest() emits language-neutral facts; this module owns
 * every human word. Layout is width-aware: CJK characters are double-width in
 * terminals and in the web client's monospace <pre>, so padding uses DISPLAY
 * width, not string length — Chinese alarm names keep the table aligned.
 */
import { promises as fs } from "node:fs";
import path from "node:path";
import { formatDelay, type AlarmDigest, type AlarmDigestEntry } from "./runtime.ts";

export type UiLanguage = "en" | "zh";
export type LanguagePreference = "auto" | UiLanguage;

const PREFS_NAME = "wake-alarm.prefs.json";

export interface UiPrefs {
	version: 1;
	language: LanguagePreference;
}

export function prefsPath(cwd: string): string {
	return path.join(cwd, ".pi", PREFS_NAME);
}

export async function readPrefs(cwd: string): Promise<UiPrefs | undefined> {
	try {
		const raw = JSON.parse((await fs.readFile(prefsPath(cwd), "utf8")).trim()) as Partial<UiPrefs>;
		if (raw.language === "auto" || raw.language === "en" || raw.language === "zh") return { version: 1, language: raw.language };
	} catch { /* absent or unreadable: fall through */ }
	return undefined;
}

export async function writePrefs(cwd: string, prefs: UiPrefs): Promise<void> {
	const target = prefsPath(cwd);
	const tmp = `${target}.tmp-${process.pid}`;
	await fs.mkdir(path.dirname(target), { recursive: true, mode: 0o700 }).catch(() => undefined);
	await fs.writeFile(tmp, `${JSON.stringify(prefs, null, 1)}\n`, { mode: 0o600 });
	await fs.rename(tmp, target);
}

/** The system locale, as far as Node can tell (ICU default follows LANG/LC_* and the OS locale). */
export function detectSystemLanguage(): UiLanguage {
	const locale = Intl.NumberFormat().resolvedOptions().locale;
	return locale.toLowerCase().startsWith("zh") ? "zh" : "en";
}

/**
 * Precedence: the tool-level preference (set_language, persisted per project)
 * wins, then the config file's uiLanguage, then the system locale.
 */
export function resolveLanguage(preference: LanguagePreference | undefined, config: LanguagePreference | undefined, detected: UiLanguage): UiLanguage {
	if (preference === "en" || preference === "zh") return preference;
	if (config === "en" || config === "zh") return config;
	return preference === "auto" || config === "auto" ? detected : detected;
}

// ---- display width (terminal semantics: CJK/fullwidth = 2 columns) ----
const WIDE = /[\u2E80-\u9FFF\uF900-\uFAFF\uFF00-\uFF60\uFFE0-\uFFE6\u3000-\u303F]/;

export function displayWidth(text: string): number {
	let width = 0;
	for (const char of text) width += WIDE.test(char) ? 2 : 1;
	return width;
}

export function padDisplay(text: string, width: number): string {
	const padding = width - displayWidth(text);
	return padding > 0 ? text + " ".repeat(padding) : text;
}

export function truncateDisplay(text: string, width: number): string {
	let current = 0;
	let out = "";
	for (const char of text) {
		const w = WIDE.test(char) ? 2 : 1;
		if (current + w > width - 1) return `${out}…`;
		out += char;
		current += w;
	}
	return out;
}

// ---- dictionaries ----
const TYPE_WORDS = {
	en: { timer: "timer", container: "docker", group: "group", condition: "file" } as Record<AlarmDigestEntry["kind"], string>,
	zh: { timer: "定时", container: "容器", group: "编组", condition: "文件" } as Record<AlarmDigestEntry["kind"], string>,
};
const CONTAINER_STATUS = {
	en: (status: string) => status,
	zh: (status: string) => ({ running: "运行中", exited: "已退出", created: "已创建", restarting: "重启中", paused: "已暂停", missing: "已消失", unchecked: "未检查" } as Record<string, string>)[status] ?? status,
};
const TEXT = {
	en: {
		wakePrefix: "wake",
		active: (n: number) => `${n} active`,
		paused: (n: number) => `${n} paused`,
		daemon: (live: boolean) => live ? "daemon live" : "daemon offline",
		pending: (n: number) => `!${n} pending`,
		more: (n: number) => `  … +${n} more`,
		next: (name: string, inMs: number) => `next ${name} ${inMs >= 0 ? "in" : "overdue"} ${formatDelay(Math.abs(inMs))}`,
		watching: "watching",
		timerDetail: (inMs: number) => (inMs >= 0 ? `in ${formatDelay(inMs)}` : `overdue ${formatDelay(-inMs)}`),
		containerDetail: (status: string, failures: number) => `${status}${failures > 0 ? ` · fail ${failures}` : ""}`,
		conditionDetail: (satisfied: boolean, size: number | undefined) => (satisfied ? "satisfied" : `waiting · ${size ?? "?"}B`),
	},
	zh: {
		wakePrefix: "唤醒",
		active: (n: number) => `${n} 活跃`,
		paused: (n: number) => `${n} 暂停`,
		daemon: (live: boolean) => live ? "守护在线" : "守护离线",
		pending: (n: number) => `!${n} 待送达`,
		more: (n: number) => `  … 另有 ${n} 个`,
		next: (name: string, inMs: number) => `下一个 ${name} ${inMs >= 0 ? formatDelay(inMs) + "后" : "超时 " + formatDelay(-inMs)}`,
		watching: "监视中",
		timerDetail: (inMs: number) => (inMs >= 0 ? `${formatDelay(inMs)}后` : `超时 ${formatDelay(-inMs)}`),
		containerDetail: (status: string, failures: number) => `${CONTAINER_STATUS.zh(status)}${failures > 0 ? ` · 失败 ${failures}` : ""}`,
		conditionDetail: (satisfied: boolean, size: number | undefined) => (satisfied ? "已满足" : `等待 · ${size ?? "?"}B`),
	},
};

/** Translate the core group summary ("<t>/<n> terminal; <e> exit 0; …"), our own fixed format. */
export function translateGroupSummary(summary: string | undefined, lang: UiLanguage): string {
	if (!summary) return lang === "zh" ? "尚未评估" : "not yet evaluated";
	const match = /^(\d+)\/(\d+) terminal; (\d+) exit 0; (\d+) abnormal; (\d+) missing; (\d+) replaced$/.exec(summary);
	if (!match) return summary;
	const [, terminal, total, exit0, abnormal, missing, replaced] = match;
	if (lang === "zh") return `${terminal}/${total} 结束; ${exit0} 正常退出; ${abnormal} 异常; ${missing} 失踪; ${replaced} 被替换`;
	return summary;
}

function entryDetail(entry: AlarmDigestEntry, lang: UiLanguage): string {
	const t = TEXT[lang];
	if (entry.kind === "timer" && entry.dueInMs !== undefined) return t.timerDetail(entry.dueInMs);
	if (entry.kind === "container") return t.containerDetail(entry.containerStatus ?? "unchecked", entry.failures ?? 0);
	if (entry.kind === "condition") return t.conditionDetail(entry.conditionSatisfied ?? false, entry.conditionSize);
	return entry.detail; // group: translateGroupSummary applied by the caller at row build
}

export interface WidgetRenderOptions {
	language: UiLanguage;
	daemonLive: boolean;
	maxEntries?: number;
	nameWidthClamp?: number;
}

export function formatWidgetLines(digest: AlarmDigest, options: WidgetRenderOptions): string[] {
	const lang = options.language;
	const t = TEXT[lang];
	const max = options.maxEntries ?? 5;
	const visible = digest.entries.slice(0, max);
	// Width-aware name column: CJK names occupy two columns per character.
	const nameWidth = Math.min(options.nameWidthClamp ?? 48, Math.max(6, ...visible.map((entry) => displayWidth(entry.name))));
	const typeWidth = Math.max(...Object.values(TYPE_WORDS[lang]).map(displayWidth));
	const lines = [
		`${t.wakePrefix}: ${t.active(digest.active)}${digest.paused > 0 ? `, ${t.paused(digest.paused)}` : ""} · ${t.daemon(options.daemonLive)}${digest.pendingWakes > 0 ? ` · ${t.pending(digest.pendingWakes)}` : ""}`,
	];
	for (const entry of visible) {
		const type = padDisplay(TYPE_WORDS[lang][entry.kind], typeWidth);
		const name = padDisplay(displayWidth(entry.name) > nameWidth ? truncateDisplay(entry.name, nameWidth) : entry.name, nameWidth);
		const detail = entry.kind === "group" ? translateGroupSummary(entry.detail, lang) : entryDetail(entry, lang);
		lines.push(`${type} ${name}  ${detail}`);
	}
	if (digest.entries.length > max) lines.push(t.more(digest.entries.length - max));
	return lines;
}

export function formatFooterStatus(digest: AlarmDigest, options: WidgetRenderOptions): string {
	const t = TEXT[options.language];
	const next = digest.nextDue ? t.next(digest.nextDue.name, digest.nextDue.inMs) : t.watching;
	return `${t.wakePrefix}: ${digest.active} · ${next} · ${t.daemon(options.daemonLive)}`;
}
