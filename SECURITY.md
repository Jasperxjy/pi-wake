# Security Policy

## Scope

pi-wake is an agent-automation extension: it schedules work, probes remote hosts over SSH, and can resume Pi sessions headlessly. Like any Pi package, it runs with the full permissions of the user who starts Pi. Please evaluate it with that in mind.

## Trust model

- **Headless wake runs respect Pi project trust by default.** `headlessTrust: "saved"` (the default) never passes `--approve`; a woken run only loads project extensions/skills when a saved Pi trust decision or `defaultProjectTrust` allows it. `"always"` re-grants project trust on every unattended wake — enable it only if you accept that trade-off.
- **Project trust is not a sandbox.** A woken session runs with your OS permissions. For high-risk unattended automation, run the daemon and Pi inside a container/VM, and consider `spawnOnWake: false` (outbox-only mode).
- **Wake evidence is untrusted remote content.** Log excerpts are sanitized, length-bounded, and labeled `untrusted data`, but no sanitization can fully rule out prompt injection. Set `"includeWakeEvidence": false` to keep remote log content out of wake messages entirely; the agent can then query evidence explicitly via `check`.
- **SSH is private-key-path only** (`identityFile`), `BatchMode=yes`, bounded output; password/key material fields in config are rejected. Remote log reads are confined to `remote.allowedRemoteLogRoots` with a remote realpath check.

## Reporting a vulnerability

Please report security issues privately via GitHub's "Report a vulnerability" flow on the repository page (Security tab → Advisories). Do not open a public issue for vulnerabilities.

We aim to acknowledge within a few days and will credit reporters in the release notes unless you prefer otherwise.
