#!/usr/bin/env node
/**
 * Regenerate .github/metrics/downloads.{json,svg} from the npm registry
 * download API. Runs dependency-free under plain Node (>= 18, global fetch).
 *
 *   node scripts/metrics.mjs               # fetch the full range, then render
 *   node scripts/metrics.mjs --from-cache  # render the SVG from the saved JSON (no network)
 *
 * The chart plots the SUBSCRIPTION TOTAL per WEEK: the daily registry rows are
 * bucketed into weeks (Monday–Sunday, UTC), each week's downloads are summed,
 * and the buckets are accumulated into a running total from the first week to
 * today. The x axis is therefore one point per week, not per day. The daily
 * rows stay in the JSON snapshot, so the weekly table can always be recomputed
 * from raw data.
 *
 * Data policy: refetch the ENTIRE range from first publish to today on every
 * run (the npm /range endpoint allows 18 months and the history is tiny), so
 * the stored JSON is always a full, self-correcting snapshot — no merging, no
 * drift if a day was missed. The workflow runs weekly (Fridays); because every
 * run rebuilds the entire curve, a missed Friday costs nothing.
 */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const PACKAGE = "pi-wake";
const METRICS_DIR = path.join(process.cwd(), ".github", "metrics");
const JSON_PATH = path.join(METRICS_DIR, "downloads.json");
const SVG_PATH = path.join(METRICS_DIR, "downloads.svg");
const FIRST_PUBLISH = "2026-08-26";
const FROM_CACHE = process.argv.includes("--from-cache");

const utcDay = (date) => date.toISOString().slice(0, 10);
const today = () => utcDay(new Date());

async function savedSnapshot() {
	try {
		const saved = JSON.parse(await readFile(JSON_PATH, "utf8"));
		return Array.isArray(saved?.days) ? saved : null;
	} catch { /* absent on first run */ }
	return null;
}

const saved = await savedSnapshot();
const start = saved?.days.length ? saved.days[0].day : FIRST_PUBLISH;

let days;
if (FROM_CACHE) {
	days = (saved?.days ?? []).filter((entry) => typeof entry?.downloads === "number");
	if (!days.length) throw new Error(`--from-cache found no saved days in ${path.relative(process.cwd(), JSON_PATH)}`);
} else {
	const end = today();
	if (end < start) throw new Error(`clock skew: today ${end} < first day ${start}`);
	const response = await fetch(`https://api.npmjs.org/downloads/range/${start}:${end}/${PACKAGE}`);
	if (!response.ok) throw new Error(`npm downloads API responded ${response.status}`);
	const payload = (await response.json());
	days = (payload.downloads ?? []).filter((entry) => typeof entry.downloads === "number");
	if (!days.length) throw new Error("npm downloads API returned no daily data");
	await mkdir(METRICS_DIR, { recursive: true });
	await writeFile(JSON_PATH, `${JSON.stringify({ package: PACKAGE, generatedAt: new Date().toISOString(), days }, null, 1)}\n`);
}

// ---- Weekly buckets (Mon–Sun, UTC): the historical daily rows re-tallied by
// ---- date, then accumulated into the subscription total at each week ----
const weekStartOf = (day) => {
	const date = new Date(`${day}T00:00:00Z`);
	date.setUTCDate(date.getUTCDate() - ((date.getUTCDay() + 6) % 7)); // Monday
	return date.toISOString().slice(0, 10);
};
const buckets = new Map();
for (const entry of days) {
	const week = weekStartOf(entry.day);
	const bucket = buckets.get(week) ?? { week, downloads: 0, through: entry.day };
	bucket.downloads += entry.downloads;
	bucket.through = entry.day;
	buckets.set(week, bucket);
}
let running = 0;
const series = [...buckets.values()].map((bucket) => ({ ...bucket, total: (running += bucket.downloads) }));
const total = running;
const max = Math.max(total, 1);

// ---- Hand-drawn (xkcd-style) line chart, one point per week ----
// Deterministic PRNG seeded from the data: the same numbers always redraw the
// same wobble, so the weekly commit diff stays byte-stable for unchanged weeks.
function mulberry32(seed) {
	let a = seed >>> 0;
	return () => {
		a |= 0; a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}
function hashData(text) {
	let h = 2166136261;
	for (let i = 0; i < text.length; i++) { h ^= text.charCodeAt(i); h = Math.imul(h, 16777619); }
	return h >>> 0;
}
/** Polyline with hand tremor: subdivide each segment and jitter perpendicular. */
function sketchPath(points, rng, wobble) {
	const path = [];
	for (let i = 0; i < points.length - 1; i++) {
		const [x1, y1] = points[i];
		const [x2, y2] = points[i + 1];
		const dx = x2 - x1;
		const dy = y2 - y1;
		const len = Math.hypot(dx, dy) || 1;
		const nx = -dy / len;
		const ny = dx / len;
		const segs = Math.max(2, Math.round(len / 26));
		path.push(i === 0 ? `M ${x1.toFixed(1)} ${y1.toFixed(1)}` : "");
		for (let s = 1; s <= segs; s++) {
			const t = s / segs;
			const w = s === segs ? 0 : (rng() * 2 - 1) * wobble;
			path.push(`L ${(x1 + dx * t + nx * w).toFixed(1)} ${(y1 + dy * t + ny * w).toFixed(1)}`);
		}
	}
	return path.filter(Boolean).join(" ");
}
/** A wobbly closed ellipse loop (two passes, xkcd-style encirclement). */
function sketchEllipse(cx, cy, rx, ry, rng) {
	const pass = (phase) => {
		const pts = [];
		for (let a = 0; a <= Math.PI * 2 + 0.001; a += Math.PI / 10) {
			const wob = 1 + (rng() * 2 - 1) * 0.09;
			pts.push([cx + Math.cos(a + phase) * rx * wob, cy + Math.sin(a + phase) * ry * wob]);
		}
		return pts;
	};
	return sketchPath(pass(0), rng, 1.5) + " " + sketchPath(pass(0.35), rng, 1.5);
}

const seed = hashData(series.map((week) => `${week.week}:${week.downloads}:${week.total}`).join("|"));
const rng = mulberry32(seed);
const rng2 = mulberry32(seed ^ 0x9e3779b9);

const width = 900;
const height = 252;
const padL = 46;
const padR = 34; // room for the peak-day encirclement at the right edge
const top = 46;
const bottom = 36;
const chartW = width - padL - padR;
const chartH = height - top - bottom;
const yOf = (count) => top + chartH - (count / max) * chartH;
const xOf = (index) => padL + (series.length === 1 ? chartW / 2 : (index / (series.length - 1)) * chartW);
const points = series.map((week, index) => [xOf(index), yOf(week.total)]);

const line = sketchPath(points, rng, 2.1);
const lineGhost = sketchPath(points, rng2, 2.6);
const area = `${line} L ${points[points.length - 1][0].toFixed(1)} ${top + chartH} L ${points[0][0].toFixed(1)} ${top + chartH} Z`;
const baseline = sketchPath([[padL - 6, top + chartH], [padL + chartW, top + chartH]], rng, 1.4);
const leftAxis = sketchPath([[padL, top - 8], [padL, top + chartH + 5]], rng, 1.4);
// One wavy dashed gridline at half height with its label.
const gridY = top + chartH / 2;
const grid = sketchPath([[padL, gridY], [padL + chartW, gridY]], rng2, 1.1);

const markers = series
	.map((week, index) => week.total > 0 ? `<circle cx="${xOf(index).toFixed(1)}" cy="${yOf(week.total).toFixed(1)}" r="2.6" fill="#316dca"><title>week of ${week.week}: +${week.downloads} (${week.total} total, through ${week.through})</title></circle>` : "")
	.filter(Boolean).join("\n");
const FONT = "'Comic Sans MS','Segoe Print','Chalkboard SE','Comic Neue',cursive";
// Sketchy encirclement of the newest week that holds the maximum running
// total, with a hand-labelled callout (ties resolve to the newest week, so a
// flat tail still points at the current total).
const peakIndex = series.reduce((best, week, index) => (week.total >= series[best].total ? index : best), 0);
const peak = series[peakIndex];
const peakX = xOf(peakIndex);
const peakY = yOf(peak.total);
const peakLabel = `${peak.total.toLocaleString("en-US")} total · week of ${peak.week.slice(5)}`;
// Callout side follows the peak's position so the label never leaves the canvas.
const calloutLeft = peakX > 300;
const [cArrow, cTextX, cAnchor] = calloutLeft
	? [`M ${peakX.toFixed(1)} ${(peakY - 18).toFixed(1)} C ${(peakX - 30).toFixed(1)} ${(peakY - 42).toFixed(1)}, ${(peakX - 46).toFixed(1)} ${(peakY - 40).toFixed(1)}, ${(peakX - 52).toFixed(1)} ${(peakY - 30).toFixed(1)}`, peakX - 56, "end"]
	: [`M ${peakX.toFixed(1)} ${(peakY - 18).toFixed(1)} C ${(peakX + 30).toFixed(1)} ${(peakY - 42).toFixed(1)}, ${(peakX + 46).toFixed(1)} ${(peakY - 40).toFixed(1)}, ${(peakX + 52).toFixed(1)} ${(peakY - 30).toFixed(1)}`, peakX + 56, "start"];
const callout = peak.total > 0
	? `<path d="${sketchEllipse(peakX, peakY, 26, 16, rng)}" fill="none" stroke="#e78100" stroke-width="1.6" stroke-linecap="round"/>
 <path d="${cArrow}" fill="none" stroke="#e78100" stroke-width="1.4" stroke-linecap="round"/>
 <text x="${cTextX.toFixed(1)}" y="${(peakY - 26).toFixed(1)}" text-anchor="${cAnchor}" font-family="${FONT}" font-size="12.5" fill="#e78100">${peakLabel}</text>`
	: "";

// ---- Weekly x axis: one tick per week (Monday date), thinned when dense ----
const tickEvery = Math.max(1, Math.ceil(series.length / 12));
const ticks = series
	.map((week, index) => {
		if (index % tickEvery !== 0 && index !== series.length - 1) return "";
		const anchor = index === 0 ? "start" : index === series.length - 1 ? "end" : "middle";
		return `<text x="${xOf(index).toFixed(1)}" y="${height - 22}" text-anchor="${anchor}" font-family="${FONT}" font-size="10.5" fill="#8b949e">${week.week.slice(5)}</text>`;
	})
	.filter(Boolean).join("\n");
const last = series[series.length - 1];
const caption = `week of (Mon) · data through ${last.through} · refreshed weekly (Fri)`;

const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="npm downloads by week, cumulative subscription total, hand-drawn line chart">
 <title>${PACKAGE}: npm downloads — cumulative subscription total by week</title>
 <rect width="${width}" height="${height}" fill="transparent"/>
 <text x="${padL}" y="24" font-family="${FONT}" font-size="14.5" fill="#3b4249">npm downloads · cumulative total (weekly)</text>
 <path d="${grid}" fill="none" stroke="#d0d7de" stroke-width="1.1" stroke-dasharray="5 6"/>
 <text x="${padL - 6}" y="${gridY + 4}" text-anchor="end" font-family="${FONT}" font-size="10.5" fill="#8b949e">${Math.round(max / 2).toLocaleString("en-US")}</text>
 <text x="${padL - 6}" y="${top + 4}" text-anchor="end" font-family="${FONT}" font-size="10.5" fill="#8b949e">${max.toLocaleString("en-US")}</text>
 <path d="${area}" fill="#4c8bf5" fill-opacity="0.10" stroke="none"/>
 <path d="${lineGhost}" fill="none" stroke="#4c8bf5" stroke-width="1.3" stroke-opacity="0.45" stroke-linecap="round" stroke-linejoin="round"/>
 <path d="${line}" fill="none" stroke="#316dca" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
 <path d="${leftAxis}" fill="none" stroke="#8b949e" stroke-width="1.6" stroke-linecap="round"/>
 <path d="${baseline}" fill="none" stroke="#8b949e" stroke-width="1.6" stroke-linecap="round"/>
${markers}
${callout}
${ticks}
 <text x="${padL}" y="${height - 6}" font-family="${FONT}" font-size="10" fill="#8b949e">${caption}</text>
</svg>
`;
await writeFile(SVG_PATH, svg);
console.log(`metrics: ${series.length} weeks (${days.length} days), subscription total ${total.toLocaleString("en-US")}, hand-drawn cumulative chart -> ${path.relative(process.cwd(), SVG_PATH)}`);
