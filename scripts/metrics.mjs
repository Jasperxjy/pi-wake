#!/usr/bin/env node
/**
 * Regenerate .github/metrics/downloads.{json,svg} from the npm registry
 * download API. Runs dependency-free under plain Node (>= 18, global fetch).
 *
 *   node scripts/metrics.mjs
 *
 * Data policy: refetch the ENTIRE range from first publish to today on every
 * run (the npm /range endpoint allows 18 months and the history is tiny), so
 * the stored JSON is always a full, self-correcting snapshot — no merging, no
 * drift if a day was missed.
 */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const PACKAGE = "pi-wake";
const METRICS_DIR = path.join(process.cwd(), ".github", "metrics");
const JSON_PATH = path.join(METRICS_DIR, "downloads.json");
const SVG_PATH = path.join(METRICS_DIR, "downloads.svg");
const FIRST_PUBLISH = "2026-08-26";
const WINDOW_DAYS = 60;

const utcDay = (date) => date.toISOString().slice(0, 10);
const today = () => utcDay(new Date());

async function existingFirstDay() {
	try {
		const saved = JSON.parse(await readFile(JSON_PATH, "utf8"));
		if (Array.isArray(saved.days) && saved.days.length) return saved.days[0].day;
	} catch { /* absent on first run */ }
	return FIRST_PUBLISH;
}

const start = await existingFirstDay();
const end = today();
if (end < start) throw new Error(`clock skew: today ${end} < first day ${start}`);
const response = await fetch(`https://api.npmjs.org/downloads/range/${start}:${end}/${PACKAGE}`);
if (!response.ok) throw new Error(`npm downloads API responded ${response.status}`);
const payload = (await response.json());
const days = (payload.downloads ?? []).filter((entry) => typeof entry.downloads === "number");
if (!days.length) throw new Error("npm downloads API returned no daily data");

await mkdir(METRICS_DIR, { recursive: true });
await writeFile(JSON_PATH, `${JSON.stringify({ package: PACKAGE, generatedAt: new Date().toISOString(), days }, null, 1)}\n`);

// ---- SVG chart: daily bars over the last WINDOW_DAYS ----
const window = days.slice(-WINDOW_DAYS);
const total = window.reduce((sum, entry) => sum + entry.downloads, 0);
const max = Math.max(...window.map((entry) => entry.downloads), 1);
const width = 760;
const height = 150;
const padX = 10;
const top = 30;
const bottom = 24;
const chartHeight = height - top - bottom;
const slot = (width - padX * 2) / window.length;
const barWidth = Math.max(1, slot * 0.68);
const barX = (index) => padX + slot * index + (slot - barWidth) / 2;
const barHeight = (count) => Math.round((count / max) * chartHeight);
const barY = (count) => top + (chartHeight - barHeight(count));

const bars = window.map((entry, index) => {
	const x = barX(index).toFixed(1);
	const y = barY(entry.downloads);
	const h = Math.max(entry.downloads > 0 ? 2 : 0, barHeight(entry.downloads));
	const date = entry.day;
	const fill = entry.downloads > 0 ? "#4c8bf5" : "#30363d";
	return `  <rect x="${x}" y="${y.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${h.toFixed(1)}" rx="1" fill="${fill}"><title>${date}: ${entry.downloads} download(s)</title></rect>`;
}).join("\n");

const first = window[0].day;
const last = window[window.length - 1];
const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="npm downloads per day">
 <title>${PACKAGE}: npm downloads per day</title>
 <rect width="${width}" height="${height}" fill="transparent"/>
 <text x="${padX}" y="16" font-family="-apple-system,Segoe UI,Helvetica,Arial,sans-serif" font-size="12" fill="#8b949e">npm downloads / day — ${window.length}d window · total ${total.toLocaleString("en-US")} · max ${max.toLocaleString("en-US")}/day</text>
 <line x1="${padX}" y1="${top}" x2="${width - padX}" y2="${top}" stroke="#30363d" stroke-width="1"/>
${bars}
 <text x="${padX}" y="${height - 8}" font-family="-apple-system,Segoe UI,Helvetica,Arial,sans-serif" font-size="11" fill="#8b949e">${first}</text>
 <text x="${width - padX}" y="${height - 8}" text-anchor="end" font-family="-apple-system,Segoe UI,Helvetica,Arial,sans-serif" font-size="11" fill="#8b949e">${last} (daily refresh)</text>
</svg>
`;
await writeFile(SVG_PATH, svg);
console.log(`metrics: ${window.length} days, total ${total}, wrote ${path.relative(process.cwd(), SVG_PATH)}`);
