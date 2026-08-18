# Changelog

## 0.1.0 (unreleased)

Initial public release.

- One-shot timers and remote Docker container watches (read-only SSH probes).
- Same-session wake in both process states: live agent-loop insertion (`sendMessage` with `triggerTurn`, queued as follow-up) and headless daemon resume of the alarm's owner session.
- State model v3: alarms (the current world) and an independent durable **outbox** (facts that happened, one entry per occurrence, with a bounded message snapshot). An outbox entry survives pause/reset/remove of its alarm — remove stops future events only — and one event kind may occur in many entries under `keep` policy. Outbox delivery is scheduled independently of alarm existence.
- Disk state is the source of truth: every action, scheduler tick, and the daemon's 5-second poll reconciles the runtime cache with the state file, so the daemon discovers alarms created by other sessions after it started, and stale caches can never fire a timer that was paused or reset in the meantime.
- Concurrency protocol: per-session presence registry (deterministic leader for ownerless alarms), cross-process state transaction lock with inode-verified rename-based stale takeover (a lock whose owner PID is alive is never force-recovered, closing the release-after-takeover window), per-alarm revision CAS, and atomic per-entry wake-delivery claims (at-least-once).
- Scheduler fire decisions are recomputed from the freshest persisted state under the transaction lock; container probes run outside the lock and are committed only after a revision/active re-validation.
- Daemon re-checks live presence between claiming a wake and spawning Pi: if the owner session came back online, the wake is left in the outbox for the session instead of starting a second Pi on the same session file. The check fails closed — an unreadable presence registry is never interpreted as "nobody is live". Sessions surface consecutive presence-heartbeat failures as UI warnings.
- Resource bounds: `statusPoll` has a hard 1-second minimum, and `maxOutboxEntries` / `maxOutboxEntriesPerAlarm` (defaults 1000/100) pause a producing alarm with an explicit `outbox overflow` reason instead of silently dropping or growing the backlog forever. Failed state saves roll back all mutation bookkeeping so reconciliation is never blocked.
- Safe-by-default headless trust (`headlessTrust: "saved"`); `includeWakeEvidence: false` hardening with an explicit opt-in `evidence` action (total response capped at 8000 chars, labeled untrusted).
- `pi-wake-daemon` bin is compiled to `dist/` (plain ESM), because Node refuses type stripping under `node_modules`; the extension still ships as TypeScript and is loaded by Pi via jiti. CI installs the actual publish tarball and smoke-tests the installed daemon bin.
- 50 tests: pure logic, multi-session runtime, and multi-process integration (state race, id-collision, presence election, stale-snapshot CAS, claim rivalry, crash redelivery, daemon-first new alarm, stale-vs-pause/reset, outbox keep re-fire, deadline-crossing-during-probe, v2->v3 migration single- and dual-process, remove-keeps-wakes, outbox overflow cap, dead-lock takeover race, presence re-check + fail-closed spawn, evidence opt-in). CI on Ubuntu + Windows, Node 22.19 / 24.
