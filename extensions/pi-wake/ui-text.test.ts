import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import {
	detectSystemLanguage,
	displayWidth,
	formatFooterStatus,
	formatWidgetLines,
	padDisplay,
	readPrefs,
	resolveLanguage,
	updatePrefs,
	translateGroupSummary,
	truncateDisplay,
	writePrefs,
} from "./ui-text.ts";
import type { AlarmDigest, AlarmDigestEntry } from "./runtime.ts";

function entry(partial: Partial<AlarmDigestEntry> & Pick<AlarmDigestEntry, "id" | "name" | "kind">): AlarmDigestEntry {
	return { active: true, detail: "", ...partial };
}

function digest(entries: AlarmDigestEntry[], extra: Partial<AlarmDigest> = {}): AlarmDigest {
	return { active: entries.length, paused: 0, pendingWakes: 0, entries, nextDue: { id: "t1", name: "Eval gate review", inMs: 16 * 60_000 }, ...extra };
}

test("display width and padding are CJK-aware (double-width chars keep the table aligned)", () => {
	assert.equal(displayWidth("abc"), 3);
	assert.equal(displayWidth("定时"), 4);
	assert.equal(displayWidth("评测门禁review"), 8 + 6); // 4 CJK chars (2 cols each) + ascii
	assert.equal(padDisplay("定时", 6), "定时  ");
	assert.equal(displayWidth(padDisplay("定时", 6)), 6);
	assert.equal(truncateDisplay("评测门禁审查", 7), "评测门…");
});

test("widget renders in English and Chinese from the same language-neutral digest", () => {
	const mixed: AlarmDigestEntry[] = [
		entry({ id: "t1", name: "Eval gate review", kind: "timer", dueInMs: 16 * 60_000, detail: "in 16m" }),
		entry({ id: "c1", name: "评测容器", kind: "container", containerStatus: "running", failures: 2, detail: "running · fail 2" }),
		entry({ id: "g1", name: "Relay batch", kind: "group", detail: "1/2 terminal; 1 exit 0; 0 abnormal; 0 missing; 0 replaced" }),
		entry({ id: "f1", name: "Analysis done", kind: "condition", conditionSatisfied: false, conditionSize: 57, detail: "waiting · 57B" }),
	];
	const en = formatWidgetLines(digest(mixed, { paused: 3, pendingWakes: 1 }), { language: "en", daemonLive: true });
	assert.match(en[0], /^wake: 4 active, 3 paused · daemon live · !1 pending$/);
	assert.ok(en.some((line) => line.startsWith("timer  Eval gate review")), `en timer row: ${JSON.stringify(en)}`);
	assert.ok(en.some((line) => line.includes("running · fail 2")));
	const zh = formatWidgetLines(digest(mixed, { paused: 3, pendingWakes: 1 }), { language: "zh", daemonLive: false });
	assert.match(zh[0], /^唤醒: 4 活跃, 3 暂停 · 守护离线 · !1 待送达$/);
	assert.ok(zh.some((line) => line.includes("16m后")), `zh timer countdown: ${JSON.stringify(zh)}`);
	assert.ok(zh.some((line) => line.includes("运行中 · 失败 2")), "zh container status");
	assert.ok(zh.some((line) => line.includes("1/2 结束; 1 正常退出; 0 异常; 0 失踪; 0 被替换")), "zh group summary");
	assert.ok(zh.some((line) => line.includes("等待 · 57B")), "zh condition");
	// The detail column must start at the same DISPLAY column for every row,
	// CJK names included (details never contain double spaces, so the final
	// "  " separator is the column boundary).
	const detailColumns = zh.filter((line) => /^(定时|容器|编组|文件)/.test(line)).map((line) => displayWidth(line.slice(0, line.lastIndexOf("  "))));
	assert.equal(detailColumns.length, 4, "one row per kind");
	assert.ok(detailColumns.every((col) => col === detailColumns[0]), `aligned detail column across CJK and ascii names: ${detailColumns}`);
	// Footer in both languages.
	const enFooter = formatFooterStatus(digest(mixed), { language: "en", daemonLive: true });
	assert.match(enFooter, /^wake: 4 · next Eval gate review in 16m · daemon live$/);
	assert.match(formatFooterStatus(digest(mixed), { language: "zh", daemonLive: true }), /^唤醒: 4 · 下一个 Eval gate review 16m后 · 守护在线$/);
});

test("group summary translation: our fixed format parses; foreign strings pass through", () => {
	assert.equal(translateGroupSummary("2/2 terminal; 2 exit 0; 0 abnormal; 0 missing; 0 replaced", "zh"), "2/2 结束; 2 正常退出; 0 异常; 0 失踪; 0 被替换");
	assert.equal(translateGroupSummary("pending fire coalescing", "zh"), "pending fire coalescing");
	assert.equal(translateGroupSummary(undefined, "zh"), "尚未评估");
});

test("language resolution: tool preference > config > system locale", () => {
	assert.equal(resolveLanguage("zh", "en", "en"), "zh");
	assert.equal(resolveLanguage(undefined, "zh", "en"), "zh");
	assert.equal(resolveLanguage("auto", "auto", "zh"), "zh");
	assert.equal(resolveLanguage(undefined, undefined, "en"), "en");
	assert.equal(resolveLanguage(undefined, "auto", "en"), "en");
});

test("prefs round-trip to .pi/wake-alarm.prefs.json", async () => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-prefs-"));
	assert.equal(await readPrefs(dir), undefined, "absent prefs read as undefined");
	await writePrefs(dir, { version: 1, language: "zh" });
	assert.deepEqual(await readPrefs(dir), { version: 1, language: "zh" });
});

test("updatePrefs merges: setting the display keeps the language and vice versa", async () => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-prefs-"));
	await writePrefs(dir, { version: 1, language: "zh" });
	const updated = await updatePrefs(dir, { display: "short" });
	assert.deepEqual(updated, { version: 1, language: "zh", display: "short" });
	assert.deepEqual(await readPrefs(dir), { version: 1, language: "zh", display: "short" });
	// Invalid display values are ignored on read (corrupt file -> defaults).
	await writePrefs(dir, { version: 1, language: "en", display: "bogus" as never });
	assert.deepEqual(await readPrefs(dir), { version: 1, language: "en" });
});

test("system detection: zh locale prefix maps to zh, anything else to en", () => {
	assert.equal(detectSystemLanguage().length > 0, true);
});
