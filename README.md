# pi-wake

Programmable event subscriptions for the [Pi coding agent](https://github.com/earendil-works/pi). The agent names a condition — a point in time, a container exit, a line in a remote log — and pi-wake wakes **the same session** when it happens:

- **While the session is open**, the wake is inserted into the running agent loop like a notification (queued behind any in-flight turn, never interrupting it).
- **When no session is open**, a small daemon resumes the alarm's owner session headlessly (`pi --session <file> --print <facts>`), so the agent continues with its full original context, handles the event, and exits.

Polling and timing are deterministic and model-free — no tokens are spent until an event actually fires. Wake messages contain facts only (event kind, status, exit code, bounded log evidence). Timestamps in wake messages, `list`/`list_wakes`/`check` output are shown in the **local timezone of the machine running Pi** (with the zone name, e.g. `GMT+8`), not UTC. There is no hidden prompt: the woken agent decides what to do next. It can set new alarms, which makes multi-stage workflows with unpredictable waits (train → evaluate → recertify → …) cheap to run.

## Features

- One-shot timers (`after` / `at`), fired once, then paused.
- Remote Docker container watches over SSH: `exit`, `abnormal`, `missing`, `replaced`, `log-error`, `log-match`, `deadline`, `connection-failure` (OR-combined, fingerprint-deduped).
- Same-session delivery in both process states (live loop insertion / headless resume).
- Durable at-least-once outbox: a fired event is persisted before delivery, so a crash cannot lose it.
- Multi-session safe (including pi-web hosting several sessions in one process): per-session presence registry, owner-scoped scheduling with a deterministic leader, a cross-process state transaction lock with revision CAS, and atomic wake-delivery claims prevent double-firing, starvation, and clobbering.
- Zero-config for timers. SSH config is needed only for `watch_container`.

## Install

```bash
pi install npm:pi-wake
```

Or from source: `pi install git:github.com/Jasperxjy/pi-wake`. Reload Pi afterwards.

Requires Node ≥ 22.19 (aligned with Pi's own baseline; native TypeScript support for the daemon). The in-session extension runs wherever Pi runs.

## Quick start

Talk to your agent, or use the tool/command directly:

```text
Set a timer to check the deployment in 30 minutes
```

```text
Watch the training container and wake me when it exits or errors
```

Tool JSON (the agent calls `wake_alarm`; you can also use the slash command):

```json
{"action": "set_timer", "id": "deploy-check", "name": "Check deployment", "after": "30m"}
```

```json
{
  "action": "watch_container",
  "id": "train-run",
  "name": "Training ended or failed",
  "container": "train_job_7",
  "events": ["exit", "abnormal", "log-error", "deadline"],
  "logPath": "/home/you/experiments/train_job_7.log",
  "deadline": "8h",
  "statusPoll": "60s"
}
```

Lifecycle: `{"action":"list"}`, `{"action":"check","id":"…"}`, `pause`, `resume`, `reset`, `remove`, and `evidence` (opt-in historical log excerpts, see below). Outbox (undelivered wakes) is managed separately: `list_wakes`, `drop_wake` (by `eventId`), `purge_wakes` (by alarm `id`), and `ack` (drop every undelivered wake of an alarm, including all members of a group). `remove` accepts `purgePendingEvents: true` to also clear the alarm's undelivered wakes. Slash form: `/wake-alarm list`, `/wake-alarm check train-run`, …

### Container events

| Event | Fires when |
|---|---|
| `exit` | container stopped cleanly (exit code 0, not OOM) |
| `abnormal` | nonzero exit, OOM, `dead`, or `restarting` |
| `missing` | the container name/ID no longer resolves |
| `replaced` | the name now resolves to a different container ID |
| `log-error` | newly appended `Traceback` or `*Error` |
| `log-match` | newly appended literal `logPattern` (not regex) |
| `deadline` | the relative `deadline` elapses (scheduled independently of polling) |
| `connection-failure` | consecutive SSH/probe failures reach the configured threshold |

Creation establishes a log baseline, so historical log content never fires. An explicit `logPath` is authoritative and must stay under `remote.allowedRemoteLogRoots`; it never silently falls back to Docker output. `policy: "pause"` (default) fires once then pauses for the agent to re-arm; `policy: "keep"` stays active and dedupes stable terminal states.

### Batch barriers: `watch_container_group`

For multi-container batches, create one group instead of N independent alarms:

```json
{
  "action": "watch_container_group",
  "id": "interp-local",
  "name": "Interp local",
  "containers": ["ctx-a", "ctx-b", "ctx-c", "ctx-d", "ctx-e", "ctx-f"],
  "condition": "all_terminal",
  "statusPoll": "30s"
}
```

Conditions: `any_terminal` (first terminal member), `all_terminal` (every member terminal; default), `any_abnormal` (any abnormal exit/OOM), `n_of_m_terminal` (with `required`). Members are created automatically (events exit/abnormal/missing/replaced, keep policy, `logTailLines` optional) and produce **no individual wakes** — the group emits exactly **one summary wake** when the condition is met:

```text
[Wake alarm] Interp local (interp-local)
Group condition met: all_terminal
3/6 terminal; 3 exit 0; 0 abnormal; 0 missing; 0 replaced
Members:
  interp-local-1 (ctx-a): exited, code 0
  ...
```

Optional `coalesceWindow` (e.g. `30s`) delays a partial-condition fire so the summary can include stragglers; all-terminal always fires immediately. If the outbox has no capacity when the condition is met, the occurrence is **frozen** (members pause, summary kept) and the group only retries the slot — a restarted container or removed result file can never erase an already-happened event. Once fired, the group and its members pause together; **group lifecycle controls members** — `pause`/`resume` apply to the group AND its members, and `reset` **rebaselines every member** (fresh probes, cleared fingerprints), so a new run can never re-fire the previous run's terminal state. `remove` on a group removes its members in the same transaction; `ack` on a group drops every member's undelivered wakes. Deleting a member alarm directly is detected as a group integrity failure (the group pauses with a diagnostic instead of counting it as terminal).

### Completion files: `watch_condition`

When an experiment's true completion is a result file rather than a container state:

```json
{
  "action": "watch_condition",
  "id": "analysis-done",
  "name": "Analysis done",
  "path": "/data/results/analysis.json",
  "condition": "contains",
  "value": "\"pass\": true",
  "statusPoll": "30s"
}
```

Conditions: `exists`, `contains` (literal substring in the file tail), `min_size` (byte threshold with `minSize`). Fires once when satisfied, with a bounded tail excerpt as evidence; `reset` re-arms.

**Stale markers**: `exists` cannot tell a fresh marker from one left behind by a previous run. Have drivers write run-specific markers into a directory that gets cleaned, prefer `contains` with a run-specific value, or set `ignoreBefore` (absolute ISO timestamp or relative like `"5m"`) — files last modified before the cutoff never satisfy the condition. Condition evidence carries both clocks: when the probe detected it, and `(file mtime: …)` when it actually happened.

### Wake result summaries

`logTailLines` (1-200) on `watch_container` / `watch_container_group` attaches the last N container log lines to exit/abnormal wake evidence, so a wake can answer "what did it print at the end" without an extra SSH round-trip. Evidence stays sanitized, length-bounded, and labeled untrusted.

## How waking works

```
session open   →  in-process scheduler → sendMessage(triggerTurn, followUp)  →  wake appears in the live loop
session closed →  daemon (owner offline)  → pi --session <owner> --print …   →  same session continues headlessly
```

Every alarm created in a session records that session's file as its owner. Coordination uses two simple primitives instead of a global lock lease:

- **Presence registry** (`.pi/wake-alarm.sessions/`): each live session owns exactly one heartbeat file, so registration never contends. A live session schedules only alarms it owns; ownerless (pre-0.1) alarms belong to the deterministic leader (smallest live instance id). The daemon schedules alarms whose owner session is **offline** — so one open session never starves another session's alarms — and ownerless alarms only when no session is live.
- **Outbox is independent of alarm state.** A fired event becomes a durable outbox entry — a fact that HAPPENED — with its own bounded message snapshot. Entries survive pause/reset of the alarm, and **remove stops future events only**: undelivered wakes from a removed alarm stay durable and are still delivered (the scheduler tracks the outbox independently of alarm existence). One event kind may occur in many entries (keep policy re-fires), and delivering an entry requires winning a claim token written under the state transaction lock. Session and daemon use the identical claim transaction, so routing overlaps cannot double-deliver, and a crashed claimant's claim simply expires. Outbox lifecycle is explicit: `list_wakes` shows undelivered wakes, `drop_wake`/`purge_wakes` remove them — removing an alarm never implicitly discards its wakes, and no wake is ever dropped automatically.
- **Disk state is the source of truth.** The runtime keeps only a cache; every action, every scheduler tick, and the daemon's 5-second poll re-reads the state file and merges it into the cache (`reconcile`). Alarms created by another session after this process started are adopted automatically, removed alarms disappear, and any alarm whose revision advanced is replaced wholesale.
- All state mutations run under a cross-process transaction lock (`.pi/wake-alarm.state.json.lock`): the stale-takeover path is designed and stress-tested (including WSL/DrvFS and Windows concurrent-rename interleavings) to prevent a crash-recovery contender from clobbering a live successor's lock — it renames the lock it judged stale to a victim, confirms the victim by BOTH content and inode before deleting it, restores (never drops) any lock it displaced by mistake, and never force-recovers a lock whose owner PID is alive. Per-alarm revision CAS is layered on top: concurrent creates of the same id fail cleanly (`alarm already exists`), and scheduler decisions are recomputed from the freshest persisted state — a stale scheduler cannot fire a timer that was paused or reset in the meantime.
- Runs spawned by the daemon get `WAKE_ALARM_PASSIVE=1`: their extension instance serves the tool but never schedules. While a wake run is active the daemon pauses scheduling, then reloads the state file — alarms the woken agent created or changed are picked up.
- A wake run that exits 0 clears the outbox record; otherwise it is retried with capped linear backoff. Alarms without an owner session (ephemeral `--no-session`) are never spawned; their wakes wait in the outbox for the next interactive session.

### Running the daemon

**Single instance.** Daemons guard against duplicates at startup: a daemon that finds a fresh foreign heartbeat steps down immediately; on a cold-start race, each claims the heartbeat file with its pid and re-verifies after a 1–2.5 s stagger, so at most one survives.

**Auto-start.** Sessions watch the daemon heartbeat (`.pi/wake-alarm.daemon.json`, rewritten every 5 s with pid + the last 30 log lines for post-mortem). When you create an alarm and no live daemon exists, the session starts one automatically (detached, `WAKE_ALARM_CWD` set to the project), and again on the way out when the last session closes. Set `"spawnDaemon": false` in `.pi/wake-alarm.json` (or `WAKE_ALARM_NO_AUTOSPAWN=1`) to manage the daemon yourself. Without a daemon, wakes that fire while all sessions are closed simply wait in the outbox until the next session starts.

```bash
pi-wake-daemon            # from the project directory; installed as a bin by the package
```

Or keep it alive with your service manager, e.g.:

```powershell
# Windows
schtasks /create /tn "pi-wake" /sc onlogon /tr "pi-wake-daemon"
```

```ini
# systemd --user
[Service]
ExecStart=pi-wake-daemon
WorkingDirectory=/path/to/project
Restart=on-failure
```

Daemon environment overrides:

| Variable | Effect |
|---|---|
| `WAKE_ALARM_CWD` | Project directory (default: process cwd) |
| `WAKE_ALARM_CONFIG_PATH` / `WAKE_ALARM_STATE_PATH` | Override config/state locations |
| `WAKE_ALARM_PI_COMMAND` | pi command override; a `.js` path is run with the current Node |
| `WAKE_ALARM_SPAWN=0` | Record wakes to the outbox only; never resume sessions |
| `WAKE_ALARM_SPAWN_DRY_RUN=1` | Log the resume command instead of running it |

On Windows the daemon unwraps the npm `pi.cmd` shim and runs the CLI script with the current Node (modern Node refuses to spawn `.cmd` without a shell).

## Configuration

`.pi/wake-alarm.json` in the project directory — entirely optional unless you use `watch_container`:

```json
{
  "remote": {
    "host": "192.168.1.10",
    "user": "you",
    "port": 22,
    "identityFile": "../keys/id_ed25519",
    "allowedRemoteLogRoots": ["/home/you/experiments/"],
    "sshAttempts": 3,
    "sshBackoffMs": 1000,
    "connectTimeoutSeconds": 10,
    "maxConsecutiveFailures": 3
  },
  "statusPoll": "60s",
  "maxLogBytes": 65536,
  "maxEvidenceChars": 1000,
  "maxOutboxEntries": 1000,
  "maxOutboxEntriesPerAlarm": 100,
  "piCommand": null,
  "spawnOnWake": true,
  "spawnDaemon": true,
  "runTimeout": "30m",
  "headlessTrust": "saved",
  "includeWakeEvidence": true
}
```

- `identityFile` is resolved relative to the `.pi/` directory that holds the config (write `"../keys/id_ed25519"` for a key stored outside `.pi/`); private key **paths only** — `password`/`passphrase`/`privateKey` fields are rejected. The tool error for a missing `remote` section embeds this minimal shape, so the schema is discoverable without the README.
- `allowedRemoteLogRoots` constrains which remote log files may be read (realpath-checked remotely).
- A headless wake run that exceeds `runTimeout` is terminated in two phases: a graceful termination request, a grace period, then a force-kill (process tree on Windows via `taskkill /T /F`). The delivery slot is only released after the woken Pi has actually exited (or the force-kill fallback deadline fires), so a retry cannot start a second Pi on the same session file while the old one still lives.
- `headlessTrust` controls project trust for headless wake runs: `"saved"` (default) passes no approval flag, so a woken run only loads project resources when a saved Pi trust decision or `defaultProjectTrust` allows it; `"always"` adds `--approve`, trusting project resources on every wake — convenient for full automation, weaker for unattended security.
- `includeWakeEvidence`: when `false`, wake messages contain no remote log excerpts (only the factual event fields), keeping untrusted log text out of the prompt entirely; the woken agent can still fetch the stored evidence on demand with `{"action":"evidence","id":"…"}` — evidence is returned only on that explicit request, capped at 8000 characters and labeled as untrusted remote data.
- `maxOutboxEntries` / `maxOutboxEntriesPerAlarm` bound the undelivered-wake backlog (defaults 1000 / 100; if only `maxOutboxEntries` is set, the per-alarm default follows it). When a new occurrence would exceed a cap, the producing alarm is **paused with an explicit `outbox overflow` reason, and the overflow-causing occurrence is NOT consumed**: its fingerprint/cursor stay unadvanced, so once capacity is freed with `drop_wake`/`purge_wakes` and the alarm is `resume`d, the very same event fires again and produces its wake (at-least-once). Existing entries are never discarded automatically.
- `check` evaluates and **acknowledges** the current condition: a due timer is marked fired and a newly-detected container event is consumed, and **no wake is generated** — it is a management action (available from any session) for explicitly observing/acknowledging state, not a status probe. Use `evidence` to retrieve log text afterwards; the owner gets no automatic wake.
- `statusPoll` (and the `statusPoll` request field) has a hard 1-second minimum, so an agent cannot configure a per-millisecond SSH probe loop.
- Unknown fields are rejected, so typos fail loudly instead of being silently ignored.

## Security notes

- Headless wake runs respect Pi's project-trust model by default (`headlessTrust: "saved"`, no `--approve`). A wake that fires without a saved trust decision runs without project extensions/skills; the wake message itself is still delivered and the agent can act with built-in tools. Set `"headlessTrust": "always"` only if you accept re-granting project trust on every unattended wake. `spawnOnWake: false` / `WAKE_ALARM_SPAWN=0` turns spawning off entirely.
- Log evidence in wake messages is untrusted remote content; it is sanitized, length-bounded, and labeled `untrusted data`. Treat it as data, not instructions.
- Runtime state lives in `.pi/wake-alarm.state.json` (git-ignore it). Do not point two daemons at the same project.

## Development

```bash
npm test          # unit + multi-session runtime + multi-process integration tests
npm run typecheck # tsc --noEmit (strict, erasableSyntaxOnly)
npm run build     # emit dist/ (compiled daemon bin; prepack runs this automatically)
```

Layout: `core.ts` (pure alarm/event logic) · `runtime.ts` (config, SSH probe, scheduler, state transaction lock + CAS, wake claims) · `presence.ts` (presence registry / leadership) · `index.ts` (Pi extension shell) · `daemon.ts` (standalone scheduler/resume host) · `scripts/build.mjs` (emits `dist/` — the daemon bin runs under plain Node, which refuses type stripping for `node_modules`, so it ships as compiled ESM; the extension itself is loaded by Pi via jiti and ships as TypeScript).

## Honest limitations

- The daemon is auto-started by sessions but not supervised: an OS reboot still needs a service manager (schtasks/systemd) to bring it back before the next session opens. Roadmap: one global daemon serving multiple projects.
- **Fired alarms are never garbage-collected.** A triggered one-shot timer or satisfied condition stays in the state file until `remove`d — deliberate while the project is young (post-mortem inspection beats auto-deletion during debugging). Planned: opt-in GC for "fired + all wakes delivered + older than N days" once usage stabilizes.
- Wake evidence is remote-produced text and is included in wake messages by default (bounded, labeled untrusted). This is fine for experiment environments you control; point `includeWakeEvidence: false` at it before pointing pi-wake at hosts you do not trust.
- The daemon delivers one wake at a time (deliberate, to keep state writes single-writer during a wake run); multiple alarms firing together are delivered sequentially.
- A crashed session's presence record takes up to 60 s to expire, so daemon takeover for its alarms lags by at most that window; clean exits are immediate.
- Alarms are project-global objects: **delivery** is owner-scoped, but **management** (`list`/`pause`/`reset`/`remove`) is available from any session — treat them like cron entries, not private session data. Alarm state is operational project state, not session-history state: rewinding or forking a Pi conversation does not roll back alarms that were already created.
- Tested against Pi 0.83.x on Node 22.19+ / 24.x (see CI). Requires Node >= 22.19 to match Pi's own baseline.
- One-shot timers only (no recurring cron) — recurring schedules are deliberately out of scope; see pi-loop / pi-scheduler for in-session recurrence.
- The headless resume path depends on Pi's `--session` / `--print` CLI surface (and `--approve` only with `headlessTrust: "always"`); track upstream changes when upgrading Pi.
- Container watching is read-only over SSH (docker inspect + bounded log reads); it never mutates the remote host.

## License

MIT
