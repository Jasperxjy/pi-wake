# pi-wake

Programmable event subscriptions for the [Pi coding agent](https://github.com/earendil-works/pi). The agent names a condition — a point in time, a container exit, a line in a remote log — and pi-wake wakes **the same session** when it happens:

- **While the session is open**, the wake is inserted into the running agent loop like a notification (queued behind any in-flight turn, never interrupting it).
- **When no session is open**, a small daemon resumes the alarm's owner session headlessly (`pi --session <file> --print <facts>`), so the agent continues with its full original context, handles the event, and exits.

Polling and timing are deterministic and model-free — no tokens are spent until an event actually fires. Wake messages contain facts only (event kind, status, exit code, bounded log evidence). There is no hidden prompt: the woken agent decides what to do next. It can set new alarms, which makes multi-stage workflows with unpredictable waits (train → evaluate → recertify → …) cheap to run.

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

Lifecycle: `{"action":"list"}`, `{"action":"check","id":"…"}`, `pause`, `resume`, `reset`, `remove`, and `evidence` (opt-in historical log excerpts, see below). Outbox (undelivered wakes) is managed separately: `list_wakes`, `drop_wake` (by `eventId`), `purge_wakes` (by alarm `id`). Slash form: `/wake-alarm list`, `/wake-alarm check train-run`, …

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

```bash
pi-wake-daemon            # from the project directory; installed as a bin by the package
```

Run it from the project directory (or set `WAKE_ALARM_CWD`). Keep it alive with your service manager, e.g.:

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
  "runTimeout": "30m",
  "headlessTrust": "saved",
  "includeWakeEvidence": true
}
```

- `identityFile` is resolved relative to the config file; private key **paths only** — `password`/`passphrase`/`privateKey` fields are rejected.
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

- The daemon is only as reliable as whatever keeps it alive; use a service manager.
- The daemon delivers one wake at a time (deliberate, to keep state writes single-writer during a wake run); multiple alarms firing together are delivered sequentially.
- A crashed session's presence record takes up to 60 s to expire, so daemon takeover for its alarms lags by at most that window; clean exits are immediate.
- Alarms are project-global objects: **delivery** is owner-scoped, but **management** (`list`/`pause`/`reset`/`remove`) is available from any session — treat them like cron entries, not private session data. Alarm state is operational project state, not session-history state: rewinding or forking a Pi conversation does not roll back alarms that were already created.
- Tested against Pi 0.83.x on Node 22.19+ / 24.x (see CI). Requires Node >= 22.19 to match Pi's own baseline.
- One-shot timers only (no recurring cron) — recurring schedules are deliberately out of scope; see pi-loop / pi-scheduler for in-session recurrence.
- The headless resume path depends on Pi's `--session` / `--print` CLI surface (and `--approve` only with `headlessTrust: "always"`); track upstream changes when upgrading Pi.
- Container watching is read-only over SSH (docker inspect + bounded log reads); it never mutates the remote host.

## License

MIT
