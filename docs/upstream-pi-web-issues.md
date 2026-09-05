# pi-web upstream issue drafts (github.com/agegr/pi-web, tested on 0.8.11)

Submit as two separate issues. Both use pi-wake as the repro vehicle, but the
gaps are generic (any extension / any external session writer).

---

## Issue 1 — Open idle session view does not update when the session file is written externally

**Environment**: pi-web 0.8.11, Windows; session written by an external headless
`pi -p` process (same machine).

**Behavior**: when an idle session is open in pi-web and another pi process
(headless run, RPC writer, scheduler/wake tool) appends to the session `.jsonl`,
the open view never updates. The only way to see new content is a full page
reload. The session *list* shows running/unread badges, so changes are
observable — the open view just never refetches.

**What the client does today** (0.8.11 bundle): the only periodic poll is a 15s
`fetch("/api/agent/running")` while the tab is visible, plus per-agent SSE for
*running* agents. There is no mtime/size check for the open session's file and
no server-side watcher for idle sessions.

**Ask**: live-update the open idle session view. Minimal version: piggyback the
open session file's `mtime`/`size` onto an existing cheap endpoint (e.g. the
`/api/agent/running` poll) and soft-refetch `/api/sessions/[id]/context` when it
changes while the tab is visible. `fs.watch` push would be even better.

**Why it matters**: headless/background workflows are invisible until a manual
reload — which in turn discards other client state (see issue 2).

---

## Issue 2 — Extension widgets/status lost after page reload, even though /state still serves them

**Environment**: pi-web 0.8.11; extension using `ui.setWidget`/`ui.setStatus`
(pi-wake 0.2.4: pushes a footer status + widget on `session_start` and every
15s afterwards).

**Behavior**: after any page reload (F5), extension status bar and widgets are
gone for the reopened session — including sessions pi-web itself served minutes
earlier — until the user interacts again.

**Evidence that the data is available server-side** (all measured with curl
against a live pi-web 0.8.11):

- `POST /api/agent/new {type:"ensure_session"}` creates the agent, the extension
  loads, and `POST /api/agent/[id] {type:"get_state"}` returns correct
  `extensionStatuses` + `extensionWidgets`.
- With **no SSE connection at all**, `GET /api/sessions/[id]/state` kept
  returning the widgets for at least **4 minutes** after the session went idle
  (stopped measuring there) — i.e. the server-side state survives an F5 easily.
- The client bundle even contains the hydration
  (`state.extensionWidgets !== undefined && setWidgets(...)`), but that code path
  runs only on the client-side session-switch effect (third argument true) and
  apparently is not reached on the reload/mount path for a reopened session.
- User-verified in the browser: F5 → wait → widget/status do not return; the
  first interaction brings them back within ~1s.

**Additionally**: `ensure_session` always returns a NEW session id — even when
`sessionId` names a session whose agent object is still alive server-side — so
"auto-attach on open" is not a workaround (it forks the session).

**Ask**: on opening an idle session after reload, fetch
`/api/sessions/[id]/state` (or fold `extensionStatuses`/`extensionWidgets` into
the initial `/api/sessions/[id]` response) and hydrate them, the same way the
session-switch path already does. Longer-term, persist the last extension UI per
session to disk so it survives pi-web restarts, and consider a read-only
"preview" agent (`--no-session` equivalent, no session writes) so externally
written sessions can show extension UI without forking.

**Note**: for sessions written entirely by EXTERNAL pi processes, /state has
`extensionWidgets: null` (no pi-web agent ever ran) — the disk-persistence or
preview-agent ideas above are what would cover that case.

