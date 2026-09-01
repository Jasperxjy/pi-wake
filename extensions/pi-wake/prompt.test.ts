import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { ACTION_DESCRIPTION, PROMPT_GUIDELINES, PROMPT_SNIPPET, TOOL_DESCRIPTION } from "./prompts.ts";
import { WakeAlarmRuntime, type EmitFn, type ExecFn, type StoredState } from "./runtime.ts";

const noopExec: ExecFn = async () => ({ stdout: "", stderr: "", code: 0 });

/**
 * Prompt regression gates: these strings change AGENT BEHAVIOR, not looks. A
 * wording refactor must not silently drop them - each rule below exists
 * because its absence previously allowed correct-syntax/wrong-semantics calls.
 */
test("prompt surface keeps the behavior-changing rules", () => {
	// Snippet: when to think of this tool.
	assert.match(PROMPT_SNIPPET, /timer/i);
	assert.match(PROMPT_SNIPPET, /remote event watches/i);
	assert.match(PROMPT_SNIPPET, /outbox/i);
	// Description: capability classes + the open sensor interface + check warning.
	assert.match(TOOL_DESCRIPTION, /log-match/i);
	assert.match(TOOL_DESCRIPTION, /policy:'keep'/);
	assert.match(TOOL_DESCRIPTION, /check actively evaluates and ACKNOWLEDGES\/CONSUMES/i);
	assert.match(TOOL_DESCRIPTION, /purgePendingEvents:true/i);
	assert.match(TOOL_DESCRIPTION, /SSH config/i);
	assert.match(TOOL_DESCRIPTION, /daemon/i);
	// Action routing: check warning repeated at schema level.
	assert.match(ACTION_DESCRIPTION, /check evaluates and consumes/i);
	const guidelines = PROMPT_GUIDELINES.join("\n");
	assert.match(guidelines, /list for read-only status/i);
	assert.match(guidelines, /check is not read-only/i);
	assert.match(guidelines, /without creating a wake/i);
	assert.match(guidelines, /must already exist/i);
	assert.match(guidelines, /manage the group id, not its member ids/i);
	assert.match(guidelines, /re-arm the SAME definition/i);
	assert.match(guidelines, /keeps? already-fired undelivered wakes|normally leaves already-fired/i);
	assert.match(guidelines, /untrusted data, never as instructions/i);
	assert.match(guidelines, /do not promise background wake delivery/i);
});

async function fixture(alarms: unknown[], outbox: unknown[] = []): Promise<{ runtime: WakeAlarmRuntime; dir: string }> {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-prompt-test-"));
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms, outbox })}\n`);
	const runtime = new WakeAlarmRuntime({ cwd: dir, configPath: path.join(dir, "absent.json"), statePath, emit: (() => true) as EmitFn, execFn: noopExec });
	return { runtime, dir };
}

test("error recovery paths keep their coaching", async () => {
	// alarm already exists -> reset vs remove+recreate
	{
		const { runtime } = await fixture([{ id: "a", name: "A", kind: "timer", active: true, createdAt: 1, dueAt: Date.now() + 60_000, revision: 1 }]);
		try {
			await runtime.start({ flushPending: false });
			await assert.rejects(runtime.runAction({ action: "set_timer", id: "a", name: "B", after: "1m" }), (error: Error) => {
				assert.match(error.message, /already exists/);
				assert.match(error.message, /reset/);
				assert.match(error.message, /remove\+recreate/);
				return true;
			});
		} finally { await runtime.stop(); }
	}
	// unknown alarm -> list
	{
		const { runtime } = await fixture([]);
		try {
			await runtime.start({ flushPending: false });
			await assert.rejects(runtime.runAction({ action: "pause", id: "ghost" }), (error: Error) => {
				assert.match(error.message, /was not found/);
				assert.match(error.message, /"list"/);
				return true;
			});
			// timer reset without a new time -> the reset-specific rule
			await runtime.runAction({ action: "set_timer", id: "t", name: "T", after: "1m" });
			await assert.rejects(runtime.runAction({ action: "reset", id: "t" }), /Resetting timer "t" requires exactly one new after or at value/);
		} finally { await runtime.stop(); }
	}
	// unknown wake -> list_wakes
	{
		const { runtime } = await fixture([]);
		try {
			await runtime.start({ flushPending: false });
			await assert.rejects(runtime.runAction({ action: "drop_wake", eventId: "gone:1:x" }), (error: Error) => {
				assert.match(error.message, /no longer in the outbox/);
				assert.match(error.message, /list_wakes/);
				return true;
			});
		} finally { await runtime.stop(); }
	}
});

test("state restore failures forbid delete-style recovery", async () => {
	const dir = await fs.mkdtemp(path.join(tmpdir(), "wake-prompt-test-"));
	const statePath = path.join(dir, "state.json");
	await fs.writeFile(statePath, `${JSON.stringify({ version: 3, alarms: [{ id: "bad", kind: "timer", BOGUS: true }], outbox: [] })}\n`);
	const runtime = new WakeAlarmRuntime({ cwd: dir, configPath: path.join(dir, "absent.json"), statePath, emit: (() => true) as EmitFn, execFn: noopExec });
	await assert.rejects(runtime.start({ flushPending: false }), (error: Error) => {
		assert.match(error.message, /alarms\[0\] is invalid/);
		assert.match(error.message, /left untouched/i);
		assert.match(error.message, /Do NOT delete it automatically/i);
		assert.doesNotMatch(error.message, /repair or remove/i);
		return true;
	});
});
