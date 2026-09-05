# pi-web upstream issue drafts (github.com/agegr/pi-web, tested on 0.8.11)

Submit as two separate issues. Both use pi-wake as the repro vehicle, but the
gaps are generic (any extension / any external session writer).

---

## Issue 1 — Open idle session view does not update when the session file is written externally

**Environment**: pi-web 0.8.11, Windows; session written by an external headless
`pi -p` process (same machine).

**Behavior**: when an idle session is open in pi-web and another pi process
(headless run, RPC writer, scheduler/wake tool) appends to the session `.jsonl`,
the open view never updates. The session file is updated correctly and the new
content becomes visible immediately after a reload — but the reload has to be
manual.

**What the client does today** (0.8.11): per-session streaming SSE for running
agents, plus visible-tab polling of `/api/agent/running` for the running-session
list (SessionSidebar). Note that `/api/agent/running` reflects only sessions
pi-web's own rpc-manager knows about — an independent external `pi -p` writer is
invisible to it — and the open idle conversation itself has no external-file
change detection at all.

**Ask**: live-update the open idle session view. Minimal version: piggyback the
open session file's `mtime`/`size` onto an existing cheap endpoint and
soft-refetch the session context when it changes while the tab is visible.
`fs.watch`-based push would be even better.

**Why it matters**: headless/background workflows are invisible until a manual
reload — which in turn discards other client state (see issue 2).

---

## Issue 2 — Extension widget panel does not reappear after page reload even though /state retains it

**Environment**: pi-web 0.8.11; extension using `ui.setWidget`/`ui.setStatus`
(pi-wake 0.2.4: pushes a footer status + a widget on `session_start` and every
15s afterwards).

**Behavior**: after any page reload (F5), the extension widget panel is gone for
the reopened session — including sessions pi-web itself served minutes earlier —
until the user interacts again (the first message/command brings it back in
~1s). User-verified in the browser.

**Facts established**:

1. Server-side data lifetime is NOT the problem. `GET /api/sessions/[id]/state`
   kept returning correct `extensionWidgets` in all of these states (verified
   via curl):
   - idle with no SSE connection ever attached: 4+ minutes;
   - after a real SSE connect→disconnect cycle: 30+ seconds;
   - after a completed agent turn followed by SSE disconnect: 2+ minutes.
   A reload window sits comfortably inside these lifetimes.
2. The mount path appears to hydrate: the mount effect calls
   `loadSession(session.id, true, true)`; with `includeState=true` it fetches
   `/api/sessions/[id]/state` and calls `setExtensionStatuses(...)` /
   `setExtensionWidgets(...)`.
3. Sending a message to an existing session reuses `session.id`
   (`ensureEventsConnected(session.id)` + `sendAgentCommand(session.id,
   {type:"prompt"})`), so the recovery on first interaction is not explained by
   a session id change.

So the open question is: why is the panel absent even though the server retains
the data and the mount path reads it? A single browser DevTools capture of the
actual `/api/sessions/<id>/state` response during a reload would discriminate
between: (a) the request never being made on the restore path, (b) a transient
empty response during initialization (not reproducible via curl before/after),
(c) hydration landing and then being overwritten by a later client state update,
or (d) a render-side selection issue. We could not distinguish these from
outside the browser; happy to provide the exact repro project.

**Independent observations** (real per source, but not claimed as this issue's
cause):

- `ExtensionWidgets` captures the expanded widget key once, in a lazy
  `useState(() => getDefaultExpandedWidgetKey(widgets))`. Later `widgets`
  updates never re-derive the default selection — e.g. a widget that grows
  beyond the cutoff while mounted stays expanded (selection persists), while a
  fresh mount of the same state would not select it.
- `DEFAULT_EXPANDED_WIDGET_LINES = 3`: only widgets with 2–3 lines are
  auto-expanded on mount; 4+ line widgets render as a trigger chip only, and
  1-line widgets are not auto-selected either. Extensions with variable-size
  widgets (pi-wake: header + one line per active alarm) will silently flip
  between panel and chip depending on alarm count.

**Additional ask (longer-term)**: persist the last extension UI per session to
disk so it survives pi-web restarts, and consider a read-only "preview" agent
(`--no-session` equivalent, no session writes) so externally-written sessions —
for which `/state` has `extensionWidgets: null` because no pi-web agent ever
ran — can show extension UI without forking.
