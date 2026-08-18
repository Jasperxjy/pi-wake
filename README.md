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
- Multi-session safe (including pi-web hosting several sessions in one process): per-instance lease, owner-scoped scheduling, and merge-save state writes prevent double-firing and clobbering.
- Zero-config for timers. SSH config is needed only for `watch_container`.

## Install

```bash
pi install npm:pi-wake
```

Or from source: `pi install git:github.com/<you>/pi-wake`. Reload Pi afterwards.

Requires Node ≥ 22.18 for the standalone daemon (native TypeScript support). The in-session extension runs wherever Pi runs.

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

Lifecycle: `{"action":"list"}`, `{"action":"check","id":"…"}`, `pause`, `resume`, `reset`, `remove`. Slash form: `/wake-alarm list`, `/wake-alarm check train-run`, …

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
session closed →  daemon (holds no lease) → pi --session <owner> --print …   →  same session continues headlessly
```

Every alarm created in a session records that session's file as its owner. Ownership routing:

- A live session schedules only alarms it owns. The single lease holder additionally schedules ownerless (pre-0.1) alarms.
- The daemon stands down whenever a live session lease (`.pi/wake-alarm.lock.json`, PID + heartbeat) exists, and takes over when it lapses. It never double-fires with a session.
- State writes are merge-saves: each runtime only writes alarms it actually changed, so concurrent sessions (e.g. pi-web tabs) do not clobber each other.
- Runs spawned by the daemon get `WAKE_ALARM_PASSIVE=1`: their extension instance serves the tool but never schedules, so the daemon stays the single scheduler. While a wake run is active the daemon pauses scheduling, then reloads the state file — alarms the woken agent created or changed are picked up.
- A wake run that exits 0 clears the outbox record; otherwise it is retried with linear backoff (60 s × attempts, max 5 per daemon activation). Alarms without an owner session (ephemeral `--no-session`) are never spawned; their wakes wait in the outbox for the next interactive session.

### Running the daemon

```bash
node ~/.pi/agent/npm/node_modules/pi-wake/extensions/pi-wake/daemon.ts
```

Run it from the project directory (or set `WAKE_ALARM_CWD`). Keep it alive with your service manager, e.g.:

```powershell
# Windows
schtasks /create /tn "pi-wake" /sc onlogon /tr "node %USERPROFILE%\.pi\agent\npm\node_modules\pi-wake\extensions\pi-wake\daemon.ts"
```

```ini
# systemd --user
[Service]
ExecStart=node %h/.pi/agent/npm/node_modules/pi-wake/extensions/pi-wake/daemon.ts
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
  "piCommand": null,
  "spawnOnWake": true,
  "runTimeout": "30m"
}
```

- `identityFile` is resolved relative to the config file; private key **paths only** — `password`/`passphrase`/`privateKey` fields are rejected.
- `allowedRemoteLogRoots` constrains which remote log files may be read (realpath-checked remotely).
- `runTimeout` bounds every headless wake run; the run is terminated after it.

## Security notes

- The daemon resumes sessions with `pi --approve`, which trusts project resources non-interactively. Set `"spawnOnWake": false` or `WAKE_ALARM_SPAWN=0` if you only want outbox delivery.
- Log evidence in wake messages is untrusted remote content; it is sanitized, length-bounded, and labeled `untrusted data`. Treat it as data, not instructions.
- Runtime state lives in `.pi/wake-alarm.state.json` (git-ignore it). Do not point two daemons at the same project.

## Development

```bash
npm test          # node --test (24 tests: pure logic + multi-session runtime)
```

Layout: `core.ts` (pure alarm/event logic) · `runtime.ts` (config, SSH probe, scheduler, merge-save) · `index.ts` (Pi extension shell) · `daemon.ts` (standalone scheduler/resume host).

## Honest limitations

- The daemon is only as reliable as whatever keeps it alive; use a service manager.
- One-shot timers only (no recurring cron) — recurring schedules are deliberately out of scope; see pi-loop / pi-scheduler for in-session recurrence.
- The headless resume path depends on Pi's `--session` / `--print` / `--approve` CLI surface; track upstream changes when upgrading Pi.
- Container watching is read-only over SSH (docker inspect + bounded log reads); it never mutates the remote host.

## License

MIT
