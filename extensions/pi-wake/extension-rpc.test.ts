import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

/**
 * Full-stack UI test: a REAL pi process (the peer-dependency install) loads the
 * extension from source in RPC mode with a seeded alarm state, and the harness
 * asserts the extension_ui_requests pi emits on stdout — the same protocol the
 * TUI footer and the pi-web client render. This is the automated replacement
 * for "launch another pi and look at the screen by hand".
 */
// new URL(".", import.meta.url) is already the test file's directory (with trailing slash);
// no path.dirname here — dirname would eat "pi-wake" off the trailing-slash form.
const repoRoot = path.resolve(fileURLToPath(new URL(".", import.meta.url)), "..", "..");
const piCli = path.join(repoRoot, "node_modules", "@earendil-works", "pi-coding-agent", "dist", "cli.js");
const extensionEntry = path.join(repoRoot, "extensions", "pi-wake", "index.ts");

interface UiRequest {
	method: string;
	statusKey?: string;
	statusText?: string;
	widgetKey?: string;
	widgetLines?: string[];
}

function parseUiRequests(stdout: string): UiRequest[] {
	const found: UiRequest[] = [];
	for (const line of stdout.split(/\r?\n/)) {
		if (!line.trim().startsWith("{")) continue;
		try {
			const parsed = JSON.parse(line) as Record<string, unknown>;
			if (parsed.type === "extension_ui_request") found.push(parsed as unknown as UiRequest);
		} catch { /* partial JSON line while streaming */ }
	}
	return found;
}

test("the extension surfaces its status bar + widget through a real pi RPC session", { timeout: 90_000 }, async (t) => {
	try { await fs.access(piCli); }
	catch { t.skip("pi peer package not installed (run npm install)"); return; }

	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-rpc-"));
	const statePath = path.join(dir, ".pi", "wake-alarm.state.json");
	await fs.mkdir(path.dirname(statePath), { recursive: true });
	await fs.writeFile(statePath, `${JSON.stringify({
		version: 3,
		alarms: [{ id: "t1", name: "RPC probe", kind: "timer", active: true, createdAt: Date.now() - 1000, dueAt: Date.now() + 90_000, revision: 1 }],
		outbox: [],
	}, null, 2)}\n`);

	// --no-extensions: skip ALL auto-discovery (user packages, project .pi/extensions)
// so a locally installed pi-wake cannot collide with the source under test.
const child = spawn(process.execPath, [piCli, "--mode", "rpc", "--offline", "--approve", "--no-session", "--no-extensions", "--extension", extensionEntry], {
		cwd: dir,
		env: { ...process.env, WAKE_ALARM_NO_AUTOSPAWN: "1" },
		stdio: ["pipe", "pipe", "pipe"],
		windowsHide: true,
	});
	let stdout = "";
	let stderr = "";
	child.stdout.on("data", (chunk) => { stdout += chunk; });
	child.stderr.on("data", (chunk) => { stderr += chunk; });
	const closed = new Promise<void>((resolve) => child.on("close", () => resolve(undefined)));
	try {
		child.stdin.write(`${JSON.stringify({ id: "probe", type: "command", command: "get_state" })}\n`);
		const deadline = Date.now() + 60_000;
		let widget: UiRequest | undefined;
		let status: UiRequest | undefined;
		while (Date.now() < deadline) {
			const requests = parseUiRequests(stdout);
			widget = requests.find((request) => request.method === "setWidget" && request.widgetKey === "wake-alarms" && request.widgetLines !== undefined);
			status = requests.find((request) => request.method === "setStatus" && request.statusKey === "wake" && request.statusText !== undefined);
			if (widget && status) break;
			await new Promise((resolve) => setTimeout(resolve, 250));
		}
		assert.ok(widget, `no setWidget request; stdout tail: ${stdout.slice(-800)}; stderr tail: ${stderr.slice(-400)}`);
		assert.ok(status, `no setStatus request; stdout tail: ${stdout.slice(-800)}`);
		assert.match(widget.widgetLines![0], /^wake: 1 active · daemon offline$/, `widget header: ${widget.widgetLines![0]}`);
		assert.ok(widget.widgetLines!.some((line) => /T RPC probe\s+due in \d+[sm]/.test(line)), `timer table row present: ${JSON.stringify(widget.widgetLines)}`);
		assert.match(widget.widgetLines!.at(-1) ?? "", /^T timer  C container  G group  F condition$/, "legend line explains the symbols");
		assert.ok(widget.widgetLines!.every((line) => !/[\u001b-\u001f]/.test(line)), "widget lines are plain text over RPC");
		assert.match(status.statusText!, /^wake: 1 · next RPC probe in \d+[sm] · daemon offline$/, `footer status: ${status.statusText}`);
	} finally {
		child.kill();
		await closed;
	}
});
