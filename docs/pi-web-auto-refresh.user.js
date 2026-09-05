// ==UserScript==
// @name         pi-wake companion (auto-refresh + widget restore)
// @namespace    pi-wake
// @version      2.0.0
// @description  Two interim patches for pi-web gaps, until they are fixed upstream:
//               1) auto-refresh the open idle session when its file changes externally
//                  (headless runs / wake-daemon deliveries) — automates the manual F5;
//               2) restore the pi-wake status bar + widget after a page reload by
//                  reading /api/sessions/<id>/state (the server keeps extension UI for
//                  minutes; the browser just doesn't fetch it after F5).
//               Skips reload while you are typing. Localhost origins only.
// @match        http://127.0.0.1/*
// @match        http://localhost/*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

(function () {
	"use strict";

	const CONTENT_POLL_MS = 5000;   // session-file change detection
	const WIDGET_POLL_MS = 15000;   // extension UI refresh (same cadence as pi-wake)
	const RELOAD_COOLDOWN_MS = 4000;

	// ---- part 1: remember the open session + its last content signature --------
	let openSessionId = null;
	let openContextUrl = null;
	let lastSignature = null;
	let lastReloadAt = 0;

	const nativeFetch = window.fetch.bind(window);
	window.fetch = function (input, init) {
		const url = typeof input === "string" ? input : input?.url ?? "";
		const m = url.match(/\/api\/sessions\/([0-9a-f-]{8,})/);
		if (m) openSessionId = m[1];
		const promise = nativeFetch(input, init);
		if (m && /\/context/.test(url) && (!init || init.method === undefined || init.method === "GET")) {
			promise.then((res) => {
				if (!res.ok) return;
				openContextUrl = url;
				res.clone().text().then((text) => { lastSignature = signature(text); }).catch(() => {});
			}).catch(() => {});
		}
		return promise;
	};

	function signature(text) {
		let h = 0;
		const step = Math.max(1, Math.floor(text.length / 20000));
		for (let i = 0; i < text.length; i += step) h = (h * 31 + text.charCodeAt(i)) | 0;
		return text.length + ":" + h;
	}

	function userIsComposing() {
		for (const el of document.querySelectorAll("textarea, input[type=text], input[type=search], [contenteditable=true]")) {
			if ((el.value && el.value.length) || (el.isContentEditable && el.textContent.trim())) return true;
		}
		return false;
	}

	setInterval(async () => {
		if (!openContextUrl || document.visibilityState !== "visible" || userIsComposing()) return;
		if (Date.now() - lastReloadAt < RELOAD_COOLDOWN_MS) return;
		try {
			const res = await nativeFetch(openContextUrl);
			if (!res.ok) return;
			const sig = signature(await res.text());
			if (lastSignature !== null && sig !== lastSignature) {
				lastReloadAt = Date.now();
				location.reload();
			} else {
				lastSignature = sig;
			}
		} catch { /* server restarting */ }
	}, CONTENT_POLL_MS);

	// ---- part 2: overlay the wake status + widget from /state ------------------
	// Reuses pi-web's own CSS classes (extension-widget-panel / -content) so the
	// overlay inherits native styling. Node is marked data-wake-overlay and is
	// hidden automatically whenever pi-web renders the REAL widget again.
	const OVERLAY = "data-wake-overlay";

	function realWidgetVisible() {
		return !!document.querySelector(`section.extension-widget-panel:not([${OVERLAY}])`);
	}

	function editorAnchor() {
		const ta = document.querySelector("textarea");
		return (ta && (ta.closest("form") || ta.parentElement)) || document.querySelector("main") || document.body;
	}

	function renderOverlay(widgets, statuses) {
		const existing = document.querySelector(`[${OVERLAY}]`);
		if (realWidgetVisible()) { if (existing) existing.remove(); return; }
		const lines = [];
		for (const w of widgets || []) {
			lines.push((w.lines || []).join("\n"));
		}
		if (!lines.length && !(statuses || []).length) { if (existing) existing.remove(); return; }

		let panel = existing;
		if (!panel) {
			panel = document.createElement("section");
			panel.className = "extension-widget-panel";
			panel.setAttribute(OVERLAY, "1");
			panel.setAttribute("aria-label", "wake alarms (restored)");
			const anchor = editorAnchor();
			if (anchor && anchor.parentElement) anchor.parentElement.insertBefore(panel, anchor);
			else return;
		}
		const html = [];
		for (const w of widgets || []) {
			html.push(`<div class="extension-widget-panel-heading">${escapeHtml(w.key ?? "wake-alarms")}</div>`);
			html.push(`<pre class="extension-widget-content">${escapeHtml((w.lines || []).join("\n"))}</pre>`);
		}
		for (const s of statuses || []) {
			html.push(`<pre class="extension-widget-content" style="opacity:.75">${escapeHtml(s.text ?? "")}</pre>`);
		}
		panel.innerHTML = html.join("");
	}

	function escapeHtml(s) {
		return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
	}

	async function refreshOverlay() {
		if (!openSessionId || document.visibilityState !== "visible") return;
		try {
			const res = await nativeFetch(`/api/sessions/${encodeURIComponent(openSessionId)}/state`);
			if (!res.ok) return;
			const body = await res.json();
			const st = body.state || {};
			renderOverlay(st.extensionWidgets, st.extensionStatuses);
		} catch { /* ignore */ }
	}

	setInterval(refreshOverlay, WIDGET_POLL_MS);
	window.addEventListener("online", refreshOverlay);
	document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") refreshOverlay(); });
	refreshOverlay();
})();
