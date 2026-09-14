# pi-wake v0.2.5 — daemon lifecycle: upgrades can no longer strand a project on stale daemon code

**GitHub Release draft for tag `v0.2.5`. Paste the section below into the release editor; the full incident narrative lives in [CHANGELOG.md](../CHANGELOG.md).**

---

## Who this matters to

If you upgraded pi-wake while a daemon was already running (which, before this release, was *every* upgrade — daemons never exited on their own), the old daemon kept running your project with the old code. From 0.2.4 onwards, session code writes an `ignoreBeforeSpec` field that pre-0.2.4 daemon code rejects, and the failed state reload was silently swallowed. Symptoms you might have seen:

- **Wakes stopped firing** for alarms created in new sessions while all sessions were closed.
- This line in `.pi/wake-alarm.daemon.json` → `logTail`:
  `state reload after wake run failed: ... unknown alarm field(s): ignoreBeforeSpec` (or `reconcile failed: ...`).
- **Zombie daemon processes** surviving `pi remove` / package uninstalls — a machine audit during development found 16 of them, 8 running code from a deleted directory.
- Worst case: the stale daemon wrote its old in-memory state back over the file, **dropping alarm definitions** created by newer code.

This is a lifecycle/data-integrity bug, not a security vulnerability: no attacker, no privilege boundary — just long-lived processes and evolving state.

## What 0.2.5 does

- **Version-aware takeover.** The daemon heartbeat now carries the running code's version. A newer daemon takes the role over from an older one, and any live 0.2.5 session replaces a healthy-but-older daemon within one 15s presence tick (rate-limited to one attempt per 5 minutes).
- **Established daemons yield.** Before every 5s heartbeat write the daemon re-reads the file and yields to a newer version, to any replacement once degraded, or to a larger pid at the same version. This also closes a single-instance guard hole where a challenger could slip between write ticks and coexist forever.
- **Degraded instead of wedged.** An unreadable state file (activation, reconcile, or post-wake reload) now *stops scheduling* and marks the heartbeat `degraded` — it never writes stale memory over newer state. Sessions show `daemon degraded` / `守护降级` in the footer and spawn a replacement automatically.
- **Idle exit.** After 6 hours with zero active alarms and zero live sessions, the daemon exits gracefully; the next alarm-creating session restarts it.
- **Project-gone exit.** A daemon whose project directory was deleted (or replaced) exits — detected by directory identity, because the daemon's own heartbeat writer can resurrect a deleted `.pi/`.

Net effect: **you should never need to hunt daemon processes by hand again**, and upgrades are self-healing.

## Upgrading

1. **Upgrade straight to 0.2.5** — do not stay on 0.2.4 if any daemon predates it:
   `pi install npm:pi-wake@0.2.5`
2. **Restart your pi sessions.** The first 0.2.5 session in each project replaces the old daemon within ~15s (watch the footer: a notice appears when a replacement starts).
3. **If you saw the symptoms above:** check `wake_alarm` → `list`. Alarm definitions dropped by the stale-write bug cannot be auto-recovered — recreate them. Undelivered wakes that are still in the outbox are delivered normally after the upgrade.
4. Pre-0.2.5 daemons for projects you never open again will not exit on their own (they are idle and harmless); feel free to kill them (`tasklist` / `ps`, command line contains `pi-wake/dist/daemon.js`).

## Verification

- New `daemon-lifecycle` test suite: takeover, degraded replacement, idle exit, project-gone (this last test is why the directory-identity check exists — the heartbeat writer resurrecting deleted directories kept the naive existence check green).
- End-to-end: a daemon tagged 0.2.2 is detected and replaced by a real pi session running 0.2.5 within one presence tick.
- Full suite: 114 tests (111 pass + 3 POSIX skips on Windows; 114/114 on WSL).
