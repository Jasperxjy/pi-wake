# Changelog

## 0.1.0 (unreleased)

Initial public release.

- One-shot timers and remote Docker container watches (read-only SSH probes).
- Same-session wake in both process states: live agent-loop insertion (`sendMessage` with `triggerTurn`, queued as follow-up) and headless daemon resume of the alarm's owner session.
- State model v3: alarms (the current world) and an independent durable **outbox** (facts that happened, one entry per occurrence, with a bounded message snapshot). An outbox entry survives pause/reset/remove of its alarm, and one event kind may occur in many entries under `keep` policy.
- Disk state is the source of truth: every action, scheduler tick, and the daemon's 5-second poll reconciles the runtime cache with the state file, so the daemon discovers alarms created by other sessions after it started, and stale caches can never fire a timer that was paused or reset in the meantime.
- Concurrency protocol: per-session presence registry (deterministic leader for ownerless alarms), cross-process state transaction lock with inode-verified rename-based stale takeover (no stat-rm-acquire TOCTOU), per-alarm revision CAS, and atomic per-entry wake-delivery claims (at-least-once).
- Scheduler fire decisions are recomputed from the freshest persisted state under the transaction lock; container probes run outside the lock and are committed only after a revision/active re-validation.
- Daemon re-checks live presence between claiming a wake and spawning Pi: if the owner session came back online, the wake is left in the outbox for the session instead of starting a second Pi on the same session file.
- Safe-by-default headless trust (`headlessTrust: "saved"`); `includeWakeEvidence: false` hardening with explicit opt-in `evidence` action.
- `pi-wake-daemon` bin for service managers; zero-config timers.
- 46 tests: pure logic, multi-session runtime, and multi-process integration (state race, id-collision, presence election, stale-snapshot CAS, claim rivalry, crash redelivery, daemon-first new alarm, stale-vs-pause/reset, outbox keep re-fire, deadline-crossing-during-probe, dead-lock takeover race, presence re-check before spawn, evidence opt-in). CI on Ubuntu + Windows, Node 22.19 / 24.
