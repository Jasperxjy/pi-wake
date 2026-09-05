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
reload. The session *list* does show running/unread badges (SessionSidebar's
visible-tab polling of `/api/agent/running`, 2.5s), so changes are observable —
the open view itself just never refetches.

**What the client does today** (0.8.11): per-session streaming SSE for running
agents; visible-tab polling for the running-session list; no mtime/size check
for the open session's file and no server-side watcher for idle sessions.

**Ask**: live-update the open idle session view. Minimal version: piggyback the
open session file's `mtime`/`size` onto an existing cheap endpoint and
soft-refetch the session context when it changes while the tab is visible.
`fs.watch`-based push would be even better.

**Why it matters**: headless/background workflows are invisible until a manual
reload — which in turn discards other client state (see issue 2).

---

## Issue 2 — Extension widget panel does not reappear after page reload; suspect one-shot selected-widget state captured before /state hydration

**Environment**: pi-web 0.8.11; extension using `ui.setWidget`/`ui.setStatus`
(pi-wake 0.2.4: pushes a footer status + a 2-line widget on `session_start` and
every 15s afterwards).

**Behavior**: after any page reload (F5), the extension widget panel is gone for
the reopened session — including sessions pi-web itself served minutes earlier —
until the user interacts (first message/command brings it back in ~1s).
User-verified in the browser.

**What we verified is NOT the cause**:

- Server state lifetime: `GET /api/sessions/[id]/state` kept returning correct
  `extensionWidgets` for 4+ minutes after the session went idle with no SSE
  connection, and for 30+ seconds after a real SSE connect/disconnect cycle.
  The reload window is well inside that lifetime (all verified via curl).
- Mount hydration: the mount effect calls `loadSession(session.id, true, true)`;
  `includeState=true` fetches `/api/sessions/[id]/state` and writes
  `extensionStatuses`/`extensionWidgets` into React state. From reading the
  source, the data reaches state — the loss happens after that.

**Suspicion (from the minified bundle; please confirm in source)**: the
component that renders `.extension-widget-panel` captures the *selected widget
key* once, in a lazy `useState` initializer — roughly:

```js
const [selected, setSelected] = useState(
  () => widgets.find(w => w.lines.length > 1 && w.lines.length <= 3)?.key ?? null
);
const visible = widgets.find(w => w.key === selected && w.lines.length > 0);
return visible && <section className="extension-widget-panel">…
```

On reload the component mounts while `widgets` is still `[]` (the `/state`
fetch is async), so `selected` freezes to `null`; when hydration lands nothing
matches `key === null` and the panel never renders. The first interaction
creates a new session (ensure_session resume semantics → new session id), which
remounts the view component with `widgets` already populated in React state, so
the initializer finally picks a key and the panel appears — consistent with the
observed "any first message brings it back".

If that reading is right, the fix is to re-derive the selection when widgets
arrive after mount (e.g. `useEffect` fallback: if `selected == null`, pick the
default candidate; or initialize from the hydrated value).

**Secondary observation**: the initializer only auto-selects widgets with 2–3
lines. A widget with 4+ lines (pi-wake shows one header line + one line per
active alarm, up to 5) is never auto-selected even on a clean mount — it only
appears behind its trigger chip. Worth considering whether that cutoff is
intended for all widget shapes.

**Additional ask (longer-term)**: persist the last extension UI per session to
disk so it survives pi-web restarts, and consider a read-only "preview" agent
(`--no-session` equivalent, no session writes) so externally-written sessions —
for which `/state` has `extensionWidgets: null` because no pi-web agent ever
ran — can show extension UI without forking.
