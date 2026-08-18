# Changelog

## 0.1.0 (unreleased)

Initial public release.

- One-shot timers and remote Docker container watches (read-only SSH probes).
- Same-session wake in both process states: live agent-loop insertion (`sendMessage` with `triggerTurn`, queued as follow-up) and headless daemon resume of the alarm's owner session.
- Concurrency protocol: per-session presence registry (deterministic leader for legacy alarms), cross-process state transaction lock, per-alarm revision CAS, and atomic wake-delivery claims (at-least-once outbox).
- Safe-by-default headless trust (`headlessTrust: "saved"`); optional `includeWakeEvidence: false` hardening.
- `pi-wake-daemon` bin for service managers; zero-config timers.
- 34 tests: pure logic, multi-session runtime, and multi-process integration (state race, id-collision, presence election, stale-snapshot CAS, claim rivalry, crash redelivery). CI on Ubuntu + Windows, Node 22.19 / 24.
