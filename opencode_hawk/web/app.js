const $ = (id) => document.getElementById(id);
const badge = $("badge");

function fmtTime(ms) {
  if (!ms) return "";
  const d = new Date(ms);
  const p = (n) => String(n).padStart(2, "0");
  return d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate())
       + " " + p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds());
}

function setBadge(modes, cls) { badge.textContent = modes.replaceAll("_", " "); badge.className = "badge " + cls; }

function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g,
    (c) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
}

async function fetchJSON(url, opts) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(url + " " + r.status);
  return r.json();
}

let stateSelected = "";
// Set once the user manually picks a session; suppresses auto-select so a
// deliberate choice is not yanked back by the 2s refresh loop.
let userPickedSession = false;
// Timeline "All sessions" view: the Event list aggregates every session
// instead of only the focused one. Default ON (unless the user previously
// picked a specific session) so hawk never opens on an empty Event list
// just because the focused session has no recent milestones/commits.
let timelineAll = (typeof localStorage !== "undefined"
  && localStorage.getItem("hawk_timeline_all") !== "0");
let showAllSessions = (typeof localStorage !== "undefined"
  && localStorage.getItem("hawk_showall") === "1");
let hideSubSessions = (typeof localStorage !== "undefined"
  && localStorage.getItem("hawk_hidesub") === "1");

// Telemetry chart state: the downsampled history series for the selected
// window, plus the pinned Y-axis totals (GB) reported by /api/llama.
// Default 24h: an ops view — 1h usually opens empty on an idle evening.
let teleWin = "24h";
let teleHist = [];
let tokenHist = [];
let vramTotalGB = 0, ramTotalGB = 0, ctxMaxTokens = 0;
// Shared time window: filters telemetry history, escalations, milestones/commits
// timeline, and the notification log. Ranges must match the range-btn data-win values.
const WIN_MS = { "1h": 3600000, "6h": 21600000, "24h": 86400000, "7d": 604800000, "30d": 2592000000 };
let timelineRaw = [];
function winCutoff() { return Date.now() - (WIN_MS[teleWin] || WIN_MS["1h"]); }
function renderState(s) {
  stateSelected = s.selected || stateSelected;
  const mode = (s.mode || "MONITOR OFFLINE").toLowerCase();
  setBadge(s.mode, mode.includes("paus") ? "paused"
                    : mode.includes("busy") ? "watch"
                    : mode.includes("offline") ? "offline" : "busy");
  const ts = s.task_stream || {};
  const tse = $("task-stream");
  if (tse) {
    tse.textContent = ts.ok ? "ok" : "degraded(" + (ts.cause || "?") + ")";
    tse.className = ts.ok ? "ok" : "bad";
    tse.title = !ts.hint ? "" : ts.hint;
  }
  const trigger_s = (s.idle_minutes || 10) * 60;
  function idleBadge(id, idle_s, active) {
    const el = $(id);
    if (!el) return;
    const label = idle_s < 0 ? "n/a"
        : (idle_s < 60 ? "<1m" : Math.floor(idle_s / 60) + "m");
    el.textContent = (id === "badge-worker" ? "W: " : "L: ") + label;
    el.className = "badge " + (active ? "busy"
        : (idle_s < 60 ? "watch"
            : (idle_s < trigger_s ? "busy" : "paused")));
  }
  idleBadge("badge-worker", s.worker_idle_s, false);
  const ll = s.llama || {};
  idleBadge("badge-llama", ll.active ? 0 : (ll.idle_for_s || 0), ll.active);
  $("btn-pause").textContent = s.paused ? "Resume" : "Pause";
  const picker = $("session-picker");
  // In the All-sessions view the picker shows "all"; never stomp that back
  // to the focused session on the 2s refresh tick.
  if (picker && !timelineAll && picker.value !== stateSelected) picker.value = stateSelected;
}

// Agent-type bubble for subagent detail rows (same color as the subagent
// indicator tag). Renders `<name>` instead of the "subagent <name>:" prefix.
const SA_TITLE = /^subagent\s+(.+?):\s*/;
function agentChip(name) {
  const c = document.createElement("span");
  c.className = "achip";
  c.textContent = name;
  c.title = "subagent " + name;
  return c;
}

function renderTimeline(evs) {
  const ul = $("timeline-list");
  ul.innerHTML = "";
  const cutoff = winCutoff();
  let evsF = (evs || []).filter(e => e.time >= cutoff);
  if (!evsF.length) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = "no events in this window";
    ul.appendChild(li);
    return;
  }
  const kinds = NOTIFY_TAG_KINDS;
  for (const e of evsF) {
    const li = document.createElement("li");
    const tag = document.createElement("span");
    const kind = e.kind === "milestone" ? (e.ok ? "milestone-ok" : "milestone-bad")
        : kinds[e.kind] || "commit";
    tag.className = "tag " + kind;
    tag.textContent = e.kind;
    // State cell: one fixed grid column on every row, so subagent state chips
    // (design 2026-09-23) no longer push the time/session/detail columns right.
    // Provider-truth state; error counts as finished and dims the row.
    const st = document.createElement("span");
    st.className = "st";
    if (e.kind === "subagent") {
      const stv = e.tag || "started";
      st.classList.add("tag", "st-" + stv);
      st.textContent = stv === "error" ? "✗ error" : stv === "finished" ? "✓ finished" : "▶ started";
      if (stv === "finished" || stv === "error") li.classList.add("done");
    }
    const tm = document.createElement("span");
    tm.className = "time";
    tm.textContent = fmtTime(e.time);
    const ss = document.createElement("span");
    ss.className = "session" + (e.sess ? "" : " none");
    ss.textContent = e.sess || "—";
    if (e.sid) ss.title = e.sid;   // full session id on hover
    const dt = document.createElement("span");
    dt.className = "detail";
    if (e.kind === "subagent") {
      // The agent type gets the indicator-colored bubble; the "subagent <name>:"
      // prefix of the title is replaced by the chip.
      const m = SA_TITLE.exec(e.title || "");
      const name = e.atype || (m ? m[1] : "");
      if (name && m) {
        dt.appendChild(agentChip(name));
        dt.appendChild(document.createTextNode(e.title.slice(m[0].length)));
      } else {
        dt.textContent = e.title || "";
      }
    } else {
      dt.textContent = e.title || "";
    }
    dt.appendChild(document.createTextNode(
      e.tests != null ? "  ·  " + e.tests + " tests"
      : e.tokens != null ? "  ·  " + e.tokens + " tok" : ""));
    dt.title = e.detail || e.title || "";
    li.append(tag, st, tm, ss, dt);
    ul.appendChild(li);
  }
}

const chartMeta = {};
const tt = document.createElement("div");
tt.id = "chart-tt";

const CPAD = { l: 48, r: 10, t: 10, b: 14 };

function fmtNum(v) {
  if (!isFinite(v)) return "";
  if (Math.abs(v) >= 1e6) {
    const m = v / 1e6;
    return (Number.isInteger(m) ? m : m.toFixed(1)) + "M";
  }
  if (Math.abs(v) >= 1000) {
    const k = v / 1000;
    return (Number.isInteger(k) ? k : k.toFixed(1)) + "k";
  }
  return Number.isInteger(v) ? String(v) : v.toFixed(1);
}

function niceNum(range, round) {
  if (!(range > 0)) return 1;
  const exp = Math.floor(Math.log10(range));
  const f = range / Math.pow(10, exp);
  let nf;
  if (round) nf = f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10;
  else nf = f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10;
  return nf * Math.pow(10, exp);
}

function niceScale(min, max, maxTicks) {
  if (!(max > min)) { min = 0; max = 1; }
  const step = niceNum(niceNum(max - min, false) / (maxTicks - 1), true);
  const lo = Math.floor(min / step) * step;
  const hi = Math.ceil(max / step) * step;
  const n = Math.round((hi - lo) / step);
  const ticks = [];
  for (let i = 0; i <= n; i++) ticks.push(+(lo + i * step).toFixed(6));
  return { lo, hi, step, ticks };
}

function yScale(raw, opts) {
  const dmin = Math.min(...raw), dmax = Math.max(...raw);
  if (opts.max != null) {
    const lo = opts.min != null ? opts.min : 0;
    const sc = niceScale(Math.min(lo, dmin), Math.max(opts.max, dmax), opts.ticks || 5);
    return { lo, hi: opts.max, ticks: sc.ticks.filter((v) => v >= lo - 1e-9 && v <= opts.max + 1e-9) };
  }
  let lo = dmin, hi = dmax;
  if (opts.min != null) lo = Math.max(lo, opts.min);
  if (hi - lo < 1e-9) {
    if (hi === 0) { lo = 0; hi = 1; }
    else { const pad = Math.abs(hi) * 0.15; lo = Math.max(lo - pad, 0); hi = hi + pad; }
  }
  return niceScale(lo, hi, opts.ticks || 5);
}

function drawChart(canvasId, series, yOf, opts = {}) {
  const canvas = $(canvasId);
  if (!canvas) return;
  chartMeta[canvasId] = { series, yOf, opts };
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 300, h = canvas.clientHeight || 110;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);
  if (!canvas.dataset.hb) { canvas.dataset.hb = "1"; bindChart(canvas, canvasId); }
  if (!series.length) return;
  // The x-axis always spans the selected window, so a series that only has
  // data for part of it is drawn where it belongs in time.
  const minT = Math.min(winCutoff(), series[0].t);
  const maxT = Math.max(Date.now(), series[series.length - 1].t);
  const range = (maxT - minT) || 1;
  chartMeta[canvasId].minT = minT;
  chartMeta[canvasId].maxT = maxT;
  const lines = opts.lines || [{ yOf, color: opts.color, fmt: opts.fmt }];
  const allVals = [];
  for (const ln of lines) for (const p of series) allVals.push(ln.yOf(p));
  const b = yScale(allVals, opts);
  const X = (t) => CPAD.l + (t - minT) / range * (w - CPAD.l - CPAD.r);
  const Y = (v) => CPAD.t + (1 - (v - b.lo) / ((b.hi - b.lo) || 1)) * (h - CPAD.t - CPAD.b);
  const fmt = opts.fmtTick || (v => fmtNum(v));
  ctx.font = "10px ui-monospace, monospace";
  ctx.textBaseline = "middle";
  ctx.textAlign = "right";
  const n = b.ticks.length;
  for (let i = 0; i < n; i++) {
    const v = b.ticks[i], yy = Y(v);
    ctx.strokeStyle = "rgba(139,148,158,.16)";
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(CPAD.l, yy); ctx.lineTo(w - CPAD.r, yy); ctx.stroke();
    ctx.fillStyle = "#8b949e";
    ctx.fillText(fmt(v) + (i === n - 1 && opts.unit ? " " + opts.unit : ""), CPAD.l - 5, yy);
  }
  if (b.lo < 0 && b.hi > 0) {
    ctx.strokeStyle = "rgba(139,148,158,.4)";
    ctx.setLineDash([3, 3]);
    ctx.beginPath(); ctx.moveTo(CPAD.l, Y(0)); ctx.lineTo(w - CPAD.r, Y(0)); ctx.stroke();
    ctx.setLineDash([]);
  }
  for (const ln of lines) {
    ctx.strokeStyle = ln.color || opts.color || "#58a6ff";
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    series.forEach((p, i) => {
      const x = X(p.t), y = Y(ln.yOf(p));
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }
  // x-axis time labels
  const xN = 5;
  ctx.textAlign = "center";
  ctx.fillStyle = "#8b949e";
  for (let i = 0; i <= xN; i++) {
    const tt = minT + (i / xN) * range;
    const xx = X(tt);
    const d = new Date(tt);
    const label = range < 172800000
      ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false })
      : d.toLocaleDateString([], { month: "short", day: "numeric" });
    ctx.fillText(label, xx, h - CPAD.b / 2);
  }
  const hov = chartMeta[canvasId].hover;
  if (hov) {
    ctx.strokeStyle = "rgba(139,148,158,.5)";
    ctx.setLineDash([2, 3]);
    ctx.beginPath(); ctx.moveTo(hov.x, CPAD.t); ctx.lineTo(hov.x, h - CPAD.b); ctx.stroke();
    ctx.setLineDash([]);
    for (const ln of lines) {
      ctx.fillStyle = ln.color || opts.color || "#58a6ff";
      ctx.beginPath(); ctx.arc(hov.x, Y(ln.yOf(hov.p)), 3, 0, 7); ctx.fill();
    }
  }
}

function bindChart(canvas, canvasId) {
  canvas.addEventListener("mousemove", (ev) => {
    const m = chartMeta[canvasId];
    if (!m || !m.series.length) return;
    const r = canvas.getBoundingClientRect();
    const x = ev.clientX - r.left;
    const minT = m.minT, maxT = m.maxT, range = (maxT - minT) || 1;
    const t = minT + (x - CPAD.l) / (r.width - CPAD.l - CPAD.r) * range;
    let best = null, bestD = Infinity;
    for (const p of m.series) {
      const d = Math.abs(p.t - t);
      if (d < bestD) { bestD = d; best = p; }
    }
    if (!best) return;
    const X = (tt2) => CPAD.l + (tt2 - minT) / range * (r.width - CPAD.l - CPAD.r);
    chartMeta[canvasId].hover = { x: X(best.t), p: best };
    drawChart(canvasId, m.series, m.yOf, m.opts);
    if (!tt.parentNode) document.body.appendChild(tt);
    const tl = m.opts.lines || [{ fmt: m.opts.fmt }];
    let html = "";
    for (const ln of tl) {
      const val = ln.fmt ? ln.fmt(best) : (ln.yOf ? ln.yOf(best) : "");
      const col = ln.color ? " style=\"color:" + ln.color + "\"" : "";
      html += "<span class=\"v\"" + col + ">" + val + "</span>";
    }
    html += "<span class=\"tm\">" + fmtTime(best.t) + "</span>";
    tt.innerHTML = html;
    tt.style.display = "block";
    let left = ev.clientX + 12, top = ev.clientY + 10;
    if (left + tt.offsetWidth > innerWidth) left = ev.clientX - tt.offsetWidth - 12;
    if (top + tt.offsetHeight > innerHeight) top = ev.clientY - tt.offsetHeight - 10;
    tt.style.left = left + "px"; tt.style.top = top + "px";
  });
  canvas.addEventListener("mouseleave", () => {
    if (chartMeta[canvasId]) chartMeta[canvasId].hover = null;
    tt.style.display = "none";
    drawChart(canvasId, chartMeta[canvasId].series, chartMeta[canvasId].yOf, chartMeta[canvasId].opts);
  });
}

function fmtMB(v) {
  if (v == null) return "—";
  return v >= 1024 ? (v / 1024).toFixed(1) + " GB" : Math.round(v) + " MB";
}

function renderTelemetry(t) {
  if (t.vram_total_mb > 0) vramTotalGB = t.vram_total_mb / 1024;
  if (t.ram_total_mb > 0) ramTotalGB = t.ram_total_mb / 1024;
  if (t.n_ctx > 0) ctxMaxTokens = t.n_ctx;
  $("t-ram").textContent = t.ram_mb != null
      ? fmtMB(t.ram_mb) + (ramTotalGB ? " / " + fmtMB(t.ram_total_mb) : "")
      : "—";
  $("t-vram").textContent = t.vram_mb != null
      ? fmtMB(t.vram_mb) + (vramTotalGB ? " / " + fmtMB(t.vram_total_mb) : "")
      : "—";
  $("t-ctx").textContent = t.pid && t.context_tokens != null
      ? t.context_tokens.toLocaleString() + " / " + (t.n_ctx || "?") : "—";
  var spdLabel = $("t-speed-label");
  var spdVal = $("t-tps");
  if (!t.pid) {
    spdLabel.textContent = "Speed";
    spdVal.textContent = "—";
  } else if (!t.processing) {
    spdLabel.textContent = "Speed";
    spdVal.textContent = "idle";
  } else if (t.ptps > 0) {
    spdLabel.textContent = "Input speed";
    spdVal.textContent = t.ptps.toFixed(1) + " t/s";
  } else if (t.tps > 0) {
    spdLabel.textContent = "Output speed";
    spdVal.textContent = t.tps.toFixed(1) + " t/s";
  } else {
    spdLabel.textContent = "Speed";
    spdVal.textContent = "generating";
  }
  // Hardware icons: each widget draws ONE solid color, chosen from the worst
  // (maximum) usage it displays — never per-zone / per-part colors. The fill
  // height reveals the actual usage amount. RAM draws one stick glyph per
  // installed stick; Disk one glyph per physical disk filled to that disk's
  // REAL-TIME activity %; GPU one glyph per adapter with dedicated VRAM filled
  // to that GPU's own VRAM usage. Colors are still decided by the worst.
  setHW("cpu", t.cpu_pct, "CPU usage");
  renderDisk(t);
  renderRam(t);
  renderGpu(t);
}

const HW_OK = "#3fb950", HW_WARN = "#d29922", HW_BAD = "#f85149"; // CSS --ok/--warn/--bad
function hwColor(pct) { return pct > 85 ? HW_BAD : pct > 60 ? HW_WARN : HW_OK; }
function hwPct(v) { return Math.max(0, Math.min(100, Number(v) || 0)); }
const SNS = "http://www.w3.org/2000/svg";
function svgEl(tag, attrs, parent) {
  const e = document.createElementNS(SNS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  parent.appendChild(e);
  return e;
}

function setHW(key, pct, title) {
  const p = hwPct(pct);
  const r = $("hw-" + key + "-use-r");
  if (r) { r.setAttribute("y", (48 * (1 - p / 100)).toFixed(2));
           r.setAttribute("height", (48 * p / 100).toFixed(2)); }
  const f = $("hw-" + key + "-fill");
  if (f) f.setAttribute("fill", hwColor(p));
  const w = $("hw-" + key + "-w");
  if (w) w.title = title + ": " + (Number.isFinite(Number(pct)) ? Math.round(p) + "%" : "—");
}

function renderRam(t) {
  const el = $("hw-ram-svg");
  if (!el) return;
  const sticks = Math.max(1, Math.round(Number(t.hw_sticks) || 1));
  const p = hwPct(t.ram_pct);
  const fill = hwColor(p);
  // Real per-stick capacities when WMI answered; stick count is the fallback.
  const info = (Array.isArray(t.hw_ram) ? t.hw_ram : [])
      .filter(s => s && Number(s.gb) > 0);
  const totalGB = info.reduce((m, s) => m + Number(s.gb), 0);
  el.innerHTML = "";
  // Full-height DIMMs: each stick fills the symbol rack (top 4 .. bottom 44 of
  // the 48-high viewBox) so sticks and the CPU/GPU icons read at the same size.
  const H = 48, sw = 11, gap = 4.5, top = 4, bot = 44;
  const W = sticks * sw + (sticks - 1) * gap + 12;
  const x0 = 6;
  const svg = svgEl("svg", { viewBox: "0 0 " + W + " " + H, "aria-hidden": "true" }, el);
  for (let i = 0; i < sticks; i++) {
    const x = x0 + i * (sw + gap);
    const d = "M" + (x + 1) + "," + bot + " L" + (x + 1) + "," + top + " L" + (x + sw - 1) + "," + top
      + " L" + (x + sw - 1) + "," + bot + " L" + (x + sw - 3) + "," + bot + " L" + (x + sw - 3)
      + "," + (bot - 4) + " L" + (x + 3) + "," + (bot - 4) + " L" + (x + 3) + "," + bot + " Z";
    const clip = svgEl("clipPath", { id: "hw-ram-sil-" + i }, svg);
    svgEl("path", { d: d }, clip);
    const g = svgEl("g", { "clip-path": "url(#hw-ram-sil-" + i + ")" }, svg);
    svgEl("rect", { x: x, y: (H * (1 - p / 100)).toFixed(2), width: sw,
                    height: (H * p / 100).toFixed(2), fill: fill }, g);
    svgEl("path", { d: d, fill: "none", stroke: "#fff", "stroke-width": 2 }, svg);
    // chip bars on the front face (two per stick)
    svgEl("rect", { x: x + 2, y: 10, width: sw - 4, height: 9, fill: "none",
                    stroke: "#fff", "stroke-width": 1.5 }, svg);
    svgEl("rect", { x: x + 2, y: 24, width: sw - 4, height: 9, fill: "none",
                    stroke: "#fff", "stroke-width": 1.5 }, svg);
  }
  const w = $("hw-ram-w");
  if (w) {
    const cap = info.length === sticks && totalGB > 0
        ? sticks + " × " + (totalGB / info.length) + " GB sticks"
        : sticks + (sticks > 1 ? " sticks" : " stick");
    w.title = "RAM · " + cap + " · " + Math.round(p) + "% in use";
  }
}

function renderDisk(t) {
  const el = $("hw-disk-svg");
  if (!el) return;
  // hw_disks = per-physical-disk REAL-TIME activity % (100 - %Idle Time), NOT
  // storage-used % — the user is looking at how busy each drive is right now.
  const drives = (Array.isArray(t.hw_disks) ? t.hw_disks : [])
      .filter(d => d && typeof d.pct === "number");
  // Full-size platters: r=19 makes the disk diameter (38) match the CPU/GPU
  // symbol height; the row is allowed to grow wider to fit every drive.
  const H = 48, r = 19, cy = 21, per = 2 * r + 8;
  const n = drives.length || 1;
  const W = 14 + (n - 1) * per + 2 * r;
  const maxPct = hwPct(drives.reduce((m, d) => Math.max(m, d.pct), 0));
  const fill = hwColor(maxPct);
  const x0 = 7 + r;
  el.innerHTML = "";
  const svg = svgEl("svg", { viewBox: "0 0 " + W + " " + H, "aria-hidden": "true" }, el);
  for (let i = 0; i < n; i++) {
    const cx = x0 + i * per;
    const d = drives[i] || { label: "", pct: 0 };
    const p = hwPct(d.pct);
    const clip = svgEl("clipPath", { id: "hw-disk-sil-" + i }, svg);
    svgEl("circle", { cx: cx, cy: cy, r: r }, clip);
    const g = svgEl("g", { "clip-path": "url(#hw-disk-sil-" + i + ")" }, svg);
    svgEl("rect", { x: cx - r, y: (H * (1 - p / 100)).toFixed(2), width: 2 * r,
                    height: (H * p / 100).toFixed(2), fill: fill }, g);
    svgEl("circle", { cx: cx, cy: cy, r: r, fill: "none", stroke: "#fff",
                      "stroke-width": 2 }, svg);
    svgEl("circle", { cx: cx, cy: cy, r: r * 0.45, fill: "none", stroke: "#fff",
                      "stroke-width": 1.5 }, svg);
    svgEl("circle", { cx: cx, cy: cy, r: r * 0.14, fill: "#fff" }, svg);
    if (d.label) svgEl("text", { x: cx, y: 45.5, "text-anchor": "middle",
                       "font-size": 7, fill: "#8b949e" }, svg).textContent = d.label;
  }
  const w = $("hw-disk-w");
  if (w) w.title = drives.length
      ? "Disk activity (real time) · " + drives.map(x => x.label + " " + Math.round(x.pct) + "%").join(", ")
        + " · worst " + Math.round(maxPct) + "%"
      : "Disk — unavailable";
}

function renderGpu(t) {
  const el = $("hw-gpu-svg");
  if (!el) return;
  // hw_gpus = one entry per GPU with REAL dedicated VRAM (registry
  // qwMemorySize > 0; pseudo adapters — iGPU/UMA, virtual display adapters —
  // are excluded upstream and defensively here too, so no card is drawn for
  // them). pct is that GPU's PROCESSING (engine) utilisation % (its busiest
  // engine, all processes) — NOT VRAM used %. Fill each card with its own
  // GPU's engine %, ONE color by the worst of them; VRAM facts ride along in
  // the tooltip.
  const gpus = (Array.isArray(t.hw_gpus) ? t.hw_gpus : [])
      .filter(g => g && typeof g.pct === "number" && g.total_mb > 0);
  // Full-height cards: 40 of the 48-high viewBox, matching CPU/RAM/disk size.
  const H = 48, gh = 40, gw = 40, gy = (H - gh) / 2, per = gw + 8;
  const n = gpus.length || 1;
  const W = 10 + (n - 1) * per + gw;
  const maxPct = hwPct(gpus.reduce((m, g) => Math.max(m, g.pct), 0));
  const fill = hwColor(maxPct);
  const x0 = 5;
  const cy = H / 2, fr = 8.5, fdx = 9;
  el.innerHTML = "";
  const svg = svgEl("svg", { viewBox: "0 0 " + W + " " + H, "aria-hidden": "true" }, el);
  for (let i = 0; i < n; i++) {
    const cx = x0 + gw / 2 + i * per;
    const g = gpus[i] || { name: "", pct: 0 };
    const p = hwPct(g.pct);
    const clip = svgEl("clipPath", { id: "hw-gpu-sil-" + i }, svg);
    svgEl("rect", { x: cx - gw / 2, y: gy, width: gw, height: gh, rx: 5 }, clip);
    const gi = svgEl("g", { "clip-path": "url(#hw-gpu-sil-" + i + ")" }, svg);
    svgEl("rect", { x: cx - gw / 2, y: (H * (1 - p / 100)).toFixed(2), width: gw,
                    height: (H * p / 100).toFixed(2), fill: fill }, gi);
    svgEl("rect", { x: cx - gw / 2, y: gy, width: gw, height: gh, rx: 5,
                    fill: "none", stroke: "#fff", "stroke-width": 2 }, svg);
    // twin cooling fans on the card face
    svgEl("circle", { cx: cx - fdx, cy: cy, r: fr, fill: "none", stroke: "#fff",
                      "stroke-width": 1.5 }, svg);
    svgEl("circle", { cx: cx + fdx, cy: cy, r: fr, fill: "none", stroke: "#fff",
                      "stroke-width": 1.5 }, svg);
    svgEl("circle", { cx: cx - fdx, cy: cy, r: 2, fill: "#fff" }, svg);
    svgEl("circle", { cx: cx + fdx, cy: cy, r: 2, fill: "#fff" }, svg);
  }
  const w = $("hw-gpu-w");
  if (w) {
    // processing % per GPU, plus dedicated-VRAM facts where the adapter has
    // real VRAM (UMA/virtual adapters report total 0 and are skipped)
    const parts = gpus.map(x => {
      let s = x.name + " " + Math.round(x.pct) + "%";
      if (x.total_mb > 0) {
        s += " (" + fmtMB(x.used_mb) + "/" + fmtMB(x.total_mb)
            + ", " + Math.round(100 * x.used_mb / x.total_mb) + "%)";
      }
      return s;
    });
    w.title = gpus.length
        ? "GPU processing · " + parts.join(", ") + " · worst " + Math.round(maxPct) + "%"
        : "GPU — unavailable";
  }
}

function renderCharts() {
  const s = teleHist;
  const cOpts = { color: "#58a6ff",
    fmt: p => (p.tokens || 0).toLocaleString() + " tokens" };
  if (ctxMaxTokens > 0) { cOpts.min = 0; cOpts.max = ctxMaxTokens; }
  drawChart("ctx-chart", s, p => p.tokens, cOpts);
  drawChart("input-chart", tokenHist, p => p.tokens_input,
    { color: "#58a6ff", min: 0, fmt: p => p.tokens_input.toLocaleString() + " in" });
  drawChart("output-chart", tokenHist, p => p.tokens_output,
    { color: "#3fb950", min: 0, fmt: p => p.tokens_output.toLocaleString() + " out" });
  drawChart("decode-chart", s, p => p.tps, { color: "#3fb950", unit: "t/s",
    fmt: p => (p.tps || 0).toFixed(1) + " t/s" });
  drawChart("prefill-chart", s, p => p.ptps, { color: "#f778ba", unit: "t/s",
    fmt: p => (p.ptps || 0).toFixed(1) + " t/s" });
  drawChart("cpu-gpu-chart", s, p => p.cpu_pct || 0, {
    min: 0, max: 100, fmtTick: v => v + "%",
    lines: [
      { yOf: p => p.cpu_pct || 0, color: "#f0883e", fmt: p => (p.cpu_pct || 0) + "% CPU" },
      { yOf: p => p.gpu_pct || 0, color: "#bc8cff", fmt: p => Math.round(p.gpu_pct || 0) + "% GPU" },
    ],
  });
  const vOpts = { color: "#39c5cf", unit: "GB", fmt: p => fmtMB(p.vram_mb) };
  if (vramTotalGB > 0) { vOpts.min = 0; vOpts.max = vramTotalGB; }
  drawChart("vram-chart", s, p => (p.vram_mb || 0) / 1024, vOpts);
  const rOpts = { color: "#e3b341", unit: "GB", fmt: p => fmtMB(p.ram_mb) };
  if (ramTotalGB > 0) { rOpts.min = 0; rOpts.max = ramTotalGB; }
  drawChart("ram-chart", s, p => (p.ram_mb || 0) / 1024, rOpts);
}

// Canvases are sized from their CSS box at draw time; redraw after a resize
// so they don't stay stretched at the old size.
let chartResizeTimer = 0;
window.addEventListener("resize", () => {
  clearTimeout(chartResizeTimer);
  chartResizeTimer = setTimeout(renderCharts, 150);
});

async function loadHistory() {
  // All charts use global (server-wide) telemetry data.
  const gh = await fetchJSON("/api/llama/history?window=" + encodeURIComponent(teleWin));
  teleHist = gh.points || [];
  // Token tiles and the two token charts share ONE series: tokens processed
  // in the window (opencode's per-message accounting), for the picked session
  // incl. its subagents, or for all sessions. The chart's last point is the
  // tile value.
  const sid = (picker && picker.value && picker.value !== "all") ? picker.value : "all";
  try {
    const sh = await fetchJSON("/api/session/token-history?session_id=" +
      encodeURIComponent(sid) + "&window=" + encodeURIComponent(teleWin));
    tokenHist = sh.points || [];
    $("t-input").textContent = (sh.total_input || 0).toLocaleString();
    $("t-output").textContent = (sh.total_output || 0).toLocaleString();
  } catch (_) {
    tokenHist = [];
    $("t-input").textContent = "—";
    $("t-output").textContent = "—";
  }
  renderCharts();
}

async function saveSession(sid, patch) {
  try {
    await fetchJSON("/api/session", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(Object.assign({ session_id: sid }, patch)) });
  } catch (e) { console.error("save session", e); }
}

function renderSessions(list) {
  const cap = $("session-cap");
  const visible = hideSubSessions
      ? (list || []).filter(s => !s.is_subworker)
      : (list || []);
  if (cap) cap.textContent = "#" + visible.length;
  const ul = $("session-list");
  ul.innerHTML = "";
  const picker = $("session-picker");
  picker.innerHTML = "";
  const allOpt = document.createElement("option");
  allOpt.value = "all";
  allOpt.textContent = "◇ All sessions";
  picker.appendChild(allOpt);
  for (const s of visible) {
    const sub = !!s.is_subworker;
    // Server groups children under their main session (depth 0 = parent).
    // Shift subsessions right by 26px per nesting level so the parent/child
    // tree reads at a glance; the ↳ marker stays with the title.
    const depth = Math.max(0, parseInt(s.depth, 10) || (sub ? 1 : 0));
    const indent = sub ? "↳ ".repeat(depth) : "";
    const li = document.createElement("li");
    li.className = "sess-row" + (s.settings.enabled ? " on" : "")
        + (s.live ? " live" : "") + (sub ? " sub" : "")
        + (sub && s.sub_state === "finished" ? " done" : "");
    if (sub) li.style.paddingLeft = (26 * depth) + "px";
    // Subagent sessions are view-only: hawk follows its parent's active leaf,
    // so a per-subagent monitor toggle would be meaningless.
    // The title names BOTH effects on purpose: one flag drives steering
    // (enabled/auto_continue) and the permission auto-accept sweep
    // (auto_accept), so a tooltip saying only "monitor" would understate it.
    const mon = sub ? "" :
        '<input type="checkbox" data-k="enabled"'
        + ' title="Monitor this session; auto-approve its file-location permission asks"'
        + (s.settings.enabled ? " checked" : "") + "> ";
    li.innerHTML =
        '<label class="sess-mon"><span class="live-dot"></span>' + mon + "<b>" +
        esc(indent + (s.title || s.id)) + "</b>" +
        ' <span class="dim">' + esc(s.provider === "llama.cpp" ? "llama" : s.provider) +
        "</span>" +
        ' <span class="dim mono">' + esc(s.dir) + "</span>" +
        (sub ? '<span class="tag st-' + (s.sub_tag || "started") + '">'
              + (s.sub_tag || "started") + "</span>" : "") + "</label>" +
        '<button type="button" class="sess-del" title="Delete session" aria-label="Delete session">' +
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" ' +
        'stroke="currentColor" stroke-width="2" stroke-linecap="round" ' +
        'stroke-linejoin="round" aria-hidden="true">' +
        '<polyline points="3 6 5 6 21 6"/>' +
        '<path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6' +
        'm3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>' +
        '<line x1="10" y1="11" x2="10" y2="17"/>' +
        '<line x1="14" y1="11" x2="14" y2="17"/></svg></button>';
    if (!sub) {
      li.querySelector("input[data-k=enabled]").onchange =
          (e) => {
            const on = e.target.checked;
            saveSession(s.id, {enabled: on, auto_accept: on, auto_continue: on});
            renderSessions(list);
          };
    }
    const delBtn = li.querySelector(".sess-del");
    delBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      const hasSubs = s.is_subworker || (s.depth === 0);
      const msg = hasSubs
          ? "This will delete the session with all subsessions. Continue?"
          : "Delete this session?";
      if (!confirm(msg)) return;
      delBtn.disabled = true;
      fetchJSON("/api/session/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: s.id }),
      }).then(() => {
        userPickedSession = false;
        refreshAll();
      }).catch(() => { delBtn.disabled = false; });
    });
    ul.appendChild(li);
    const opt = document.createElement("option");
    opt.value = s.id;
    opt.textContent = (sub ? "↳ " : (s.settings.enabled ? "● " : "○ ")) +
        (s.title || s.id).slice(0, 40);
    picker.appendChild(opt);
  }
  // Hawk found exactly one active session: select it automatically (and
  // persist, so the monitor's cfg.session_id follows it), unless the user
  // already picked one manually.
  const actives = (list || []).filter((s) => s.live);
  if (!userPickedSession && !timelineAll && actives.length === 1 &&
      (stateSelected || "") !== actives[0].id) {
    stateSelected = actives[0].id;
    saveSession(actives[0].id, {selected: actives[0].id});
  }
  picker.value = timelineAll ? "all" : (stateSelected || "");
}

// ---- Hawk notifications ----
// Unified event-kind → tag-class map (timeline + notification list).
const NOTIFY_TAG_KINDS = {
  commit: "commit", milestone: "milestone-ok", escalation: "escalation",
  continue: "continue", nudge: "nudge", plan_done: "plan_done",
  confirm_done: "confirm_done", permission: "permission",
  permission_accept: "permission", subagent: "subagent", test: "test",
  reply: "reply", llama: "llama", routing: "reply", alert: "commit",
};
// ntfy clear-all pacing: one DELETE per message (ntfy.sh rate-limits
// bursts); failed ones get one slower retry pass.
const NTFY_CLEAR_SPACING_MS = 2500;
const NTFY_CLEAR_RETRY_SPACING_MS = 6000;

let ntfyCache = { log: [] };

// Message ids the user expanded; kept across the auto-refresh rebuilds so an
// open message stays open when the log re-renders.
const expandedNtfy = new Set();

function devMsg(s) {
  const m = $("dev-msg");
  if (m) m.textContent = s;
}

const NTFY_SVG_EXP =
    '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" '
    + 'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    + 'stroke-linejoin="round" aria-hidden="true">'
    + '<polyline points="6 9 12 15 18 9"/></svg>';
const NTFY_SVG_DEL =
    '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" '
    + 'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    + 'stroke-linejoin="round" aria-hidden="true">'
    + '<polyline points="3 6 5 6 21 6"/>'
    + '<path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6'
    + 'm3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>'
    + '<line x1="10" y1="11" x2="10" y2="17"/>'
    + '<line x1="14" y1="11" x2="14" y2="17"/></svg>';

function ntfyExpandButton(li, en, bodyText, truncated) {
  const exp = document.createElement("button");
  exp.className = "notif-exp";
  exp.type = "button";
  exp.title = "show the full message";
  exp.setAttribute("aria-label", "show the full message");
  exp.setAttribute("aria-expanded", "false");
  exp.innerHTML = NTFY_SVG_EXP;
  const openPanel = () => {
    li.classList.add("open");
    exp.setAttribute("aria-expanded", "true");
    const p = document.createElement("div");
    p.className = "notif-body";
    p.textContent = bodyText || "";
    if (truncated) {
      const legacy = document.createElement("div");
      legacy.className = "legacy";
      legacy.textContent =
          "legacy entry: only the first " + String(bodyText.length)
          + " characters were stored (not on the ntfy cache anymore)";
      p.appendChild(legacy);
    }
    li.appendChild(p);
  };
  if (en.id && expandedNtfy.has(en.id)) openPanel();
  exp.addEventListener("click", () => {
    if (li.classList.contains("open")) {
      li.classList.remove("open");
      exp.setAttribute("aria-expanded", "false");
      const p = li.querySelector(".notif-body");
      if (p) p.remove();
      if (en.id) expandedNtfy.delete(en.id);
    } else {
      openPanel();
      if (en.id) expandedNtfy.add(en.id);
    }
  });
  return exp;
}

function ntfyDeleteButton(en) {
  const del = document.createElement("button");
  del.className = "notif-del";
  del.type = "button";
  del.title = "delete from the channel and all devices";
  del.setAttribute("aria-label", "delete notification");
  del.innerHTML = NTFY_SVG_DEL;
  del.addEventListener("click", () => deleteNtfyMessage(en.seq || en.id, del));
  return del;
}

// Detail cell for a hawk push title: "[hawk]" prefix dropped, and a
// "subagent <name>: ..." title gets the agent chip like the Events list.
function ntfyTitleCell(title) {
  const dt = document.createElement("span");
  dt.className = "detail";
  const t = String(title || "").replace(/^\[hawk\]\s*/i, "");
  const m = SA_TITLE.exec(t);
  if (m) {
    dt.appendChild(agentChip(m[1]));
    dt.appendChild(document.createTextNode(t.slice(m[0].length)));
  } else {
    dt.textContent = t;
  }
  return dt;
}

// One row per message an ntfy client currently shows for the channel
// (deleted messages are already gone): kind | time | title | body | delete.
function renderChannelRow(en, ul) {
  const li = document.createElement("li");
  const isReply = en.kind === "reply";
  li.className = (isReply ? "notif-recv" : "notif-sent")
      + (en.priority >= 4 ? " notif-urgent" : "");
  li.dataset.seq = en.seq || en.id;
  const tag = document.createElement("span");
  tag.className = "tag " + (NOTIFY_TAG_KINDS[en.kind] || "commit");
  tag.textContent = isReply ? "you" : (en.kind || "alert");
  const tm = document.createElement("span");
  tm.className = "time";
  tm.textContent = fmtTime(en.ts);
  const body = String(en.message || "");
  let dt;
  if (isReply) {
    dt = document.createElement("span");
    dt.className = "detail";
    dt.textContent = "← " + body;
  } else {
    dt = ntfyTitleCell(en.title);
  }
  li.append(tag, tm, dt);
  const titleText = String(en.title || "").replace(/^\[hawk\]\s*/i, "");
  if (!isReply && body && body !== titleText) {
    li.appendChild(ntfyExpandButton(li, en, body, false));
  } else {
    li.appendChild(document.createElement("span"));   // keep the grid column
  }
  li.appendChild(ntfyDeleteButton(en));
  ul.appendChild(li);
}

// One row for a dashboard log entry that is no longer on ntfy (expired from
// its 12 h cache) -- shown only under the "older" toggle.
function renderLocalRow(en, ul) {
  const isSent = en.dir === "sent";
  const li = document.createElement("li");
  li.className = "notif-old " + (isSent ? "notif-sent" : "notif-recv");
  const tag = document.createElement("span");
  let cls, label;
  if (isSent) {
    label = en.kind || "alert";
    cls = label === "milestone"
        ? (en.meta && en.meta.state === "BLOCKED" ? "milestone-bad" : "milestone-ok")
        : (NOTIFY_TAG_KINDS[label] || "commit");
  } else if (en.kind === "reply") {
    label = "you"; cls = "reply";
  } else {
    label = en.kind || "reply"; cls = "reply-unrouted";
  }
  tag.className = "tag " + cls;
  tag.textContent = label;
  const tm = document.createElement("span");
  tm.className = "time";
  tm.textContent = fmtTime(en.ts);
  let dt;
  if (isSent) {
    dt = ntfyTitleCell(en.title);
  } else {
    dt = document.createElement("span");
    dt.className = "detail";
    dt.textContent = "← " + (en.text || en.choice || "")
        + (en.session ? "  · " + en.session : "")
        + (en.note ? " · " + en.note : "");
  }
  li.append(tag, tm, dt);
  const bodyText = isSent ? String(en.body || en.excerpt || "") : "";
  if (bodyText) {
    const truncated = !en.body && bodyText.length >= 160;
    li.appendChild(ntfyExpandButton(li, en, bodyText, truncated));
  } else {
    li.appendChild(document.createElement("span"));
  }
  li.appendChild(document.createElement("span"));
  ul.appendChild(li);
}

// Whether the "older, expired from ntfy" dashboard history is unfolded.
let ntfyShowOlder = false;

// The ntfy section mirrors the channel exactly as the ntfy app shows it;
// the dashboard's own older history (past ntfy's 12 h cache) is folded
// behind a toggle row so it can't be mistaken for channel contents.
function renderNtfyList(d, ul) {
  const cutoff = winCutoff();
  const channel = (d.channel || []).filter((e) => e.ts >= cutoff);
  const channelIds = new Set(channel.map((c) => String(c.id)));
  const oldestLive = channel.length ? channel[channel.length - 1].ts : Infinity;
  // Local rows only count as "older" when they predate everything ntfy still
  // holds; newer local rows not on the channel were deleted, not expired.
  const local = (d.log || [])
      .filter((e) => e.ts >= cutoff && e.ts < oldestLive
          && !(e.id && channelIds.has(String(e.id))))
      .slice().reverse();
  ul.innerHTML = "";
  for (const en of channel) renderChannelRow(en, ul);
  if (!channel.length) {
    const e = document.createElement("li");
    e.className = "empty";
    e.textContent = "no messages on the channel";
    ul.appendChild(e);
  }
  if (local.length) {
    const h = document.createElement("li");
    h.className = "ntfy-older-head";
    const b = document.createElement("button");
    b.type = "button";
    b.className = "linkbtn";
    b.textContent = (ntfyShowOlder ? "▾ hide " : "▸ show ") + local.length
        + " older message" + (local.length === 1 ? "" : "s")
        + " (expired from ntfy's 12 h cache · dashboard history)";
    b.onclick = () => { ntfyShowOlder = !ntfyShowOlder; renderNtfyList(ntfyCache, ul); };
    h.appendChild(b);
    ul.appendChild(h);
    if (ntfyShowOlder) for (const en of local.slice(0, 100)) renderLocalRow(en, ul);
  }
}

function renderNtfy(d) {
  const inp = $("ntfy-topic");
  if (inp && document.activeElement !== inp) inp.value = d.topic || "";
  const sub = $("ntfy-sub");
  if (sub) sub.textContent = (d.topic && d.server) ? d.server + "/" + d.topic : "ntfy not configured";
  const logUl = $("notify-log");
  if (logUl) renderNtfyList(d, logUl);
  const clr = $("btn-clear-all");
  if (clr) clr.hidden = !(d.channel || []).length;
}

async function saveTopic() {
  const inp = $("ntfy-topic");
  const topic = inp ? inp.value.trim() : "";
  devMsg("");
  if (!topic) { devMsg("channel name required"); return; }
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(topic)) { devMsg("1-64 chars: letters, digits, - or _"); return; }
  try {
    await fetchJSON("/api/ntfy", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topic }),
    });
    ntfyCache.lastKey = "";
    devMsg("channel saved");
    loadNtfy();
  } catch (e) { devMsg("save failed"); }
}

async function loadNtfy() {
  try {
    const d = await fetchJSON("/api/ntfy");
    ntfyCache = d;
    const key = JSON.stringify([d.topic, d.log, d.channel]);
    if (key !== ntfyCache.lastKey) {
      ntfyCache.lastKey = key;
      renderNtfy(d);
    }
  } catch (e) { /* keep stale view */ }
}

async function testNtfy() {
  devMsg("testing push…");
  try {
    const r = await fetchJSON("/api/ntfy/test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    devMsg(r.sent ? "test push sent — check your phone" : "test failed (topic empty?)");
    setTimeout(loadNtfy, 1500);
  } catch (e) { devMsg("test failed"); }
}

// Drop a message's row right away (the server cache already forgot it).
function dropNtfyRow(seq) {
  ntfyCache.channel = (ntfyCache.channel || []).filter((m) => (m.seq || m.id) !== seq);
  ntfyCache.lastKey = "";
  renderNtfy(ntfyCache);
}

async function deleteNtfyMessage(id, btn) {
  if (btn) btn.disabled = true;
  try {
    await fetchJSON("/api/ntfy/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id }),
    });
    dropNtfyRow(id);
    devMsg("deleted");
  } catch (e) {
    devMsg("delete failed — check server log");
    if (btn) btn.disabled = false;
  }
}

async function clearAllNtfy() {
  let ids = (ntfyCache.channel || []).map((m) => String(m.seq || m.id));
  const total = ids.length;
  const topic = (ntfyCache && ntfyCache.topic) || "";
  if (!confirm("Delete all " + total + " message(s) on ntfy channel '"
      + topic + "'?\n\nThey disappear from the ntfy app on every device and "
      + "from this list.")) return;
  const btn = $("btn-clear-all");
  if (btn) btn.disabled = true;
  let done = 0;
  let failed = [];
  try {
    // One delete per message (ntfy has no topic-wide delete); each row
    // disappears as soon as its delete lands, so progress is visible.
    // Failures get one slower retry pass.
    for (let pass = 0; pass < 2 && ids.length; pass++) {
      failed = [];
      for (const id of ids) {
        try {
          await fetchJSON("/api/ntfy/delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ id }),
          });
          done++;
          dropNtfyRow(id);
        } catch (e) { failed.push(id); }
        devMsg("deleting… " + done + " / " + total
            + (failed.length ? " (" + failed.length + " failed)" : ""));
        await sleep(pass ? NTFY_CLEAR_RETRY_SPACING_MS : NTFY_CLEAR_SPACING_MS);
      }
      ids = failed;
    }
    try {
      await fetchJSON("/api/ntfy/prune-local", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ all: true }),
      });
    } catch (e) { /* not fatal */ }
    ntfyCache.lastKey = "";
    await loadNtfy();
    devMsg(failed.length
        ? "deleted " + done + " of " + total + "; " + failed.length + " failed — try again"
        : "deleted " + done + " message(s) — channel is empty");
  } finally {
    if (btn) btn.disabled = false;
  }
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ---- Push-kind checkboxes (gate phone push only; web shows all) ----
const kindsState = { all: [], enabled: [], lastKey: "" };

async function loadNotifyKinds() {
  try {
    const k = await fetchJSON("/api/notify/kinds");
    const key = JSON.stringify([k.all, k.enabled]);
    if (key !== kindsState.lastKey) {
      kindsState.all = k.all || [];
      kindsState.enabled = k.enabled || [];
      kindsState.lastKey = key;
      renderNotifyKinds();
    }
  } catch (e) { /* keep stale view */ }
}

function renderNotifyKinds() {
  const box = $("notify-kinds-box");
  if (!box) return;
  box.innerHTML = "";
  for (const kind of kindsState.all) {
    const lab = document.createElement("label");
    lab.className = "chk";
    const c = document.createElement("input");
    c.type = "checkbox";
    c.checked = kindsState.enabled.includes(kind);
    const t = document.createElement("span");
    t.className = "tag " + (NOTIFY_TAG_KINDS[kind] || "commit");
    t.textContent = kind;
    c.onchange = () => saveNotifyKinds();
    lab.append(c, t);
    box.appendChild(lab);
  }
}

async function saveNotifyKinds() {
  const box = $("notify-kinds-box");
  const enabled = [...box.querySelectorAll("input:checked")]
      .map((i) => i.parentElement.querySelector(".tag").textContent);
  try {
    const r = await fetchJSON("/api/notify/kinds", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled }),
    });
    kindsState.enabled = r.enabled || enabled;
    kindsState.lastKey = "";
    devMsg(r.ok ? "push kinds saved" : "save failed");
    setTimeout(loadNotifyKinds, 300);
  } catch (e) { devMsg("save kinds failed"); }
}

// ---- Pending reply-routing prompt (multi-session) ----
async function loadNotifyPending() {
  const box = $("notify-pending");
  if (!box) return;
  try {
    const p = await fetchJSON("/api/notify/pending");
    if (!p.pending) { box.innerHTML = ""; return; }
    box.innerHTML = "";
    const wrap = document.createElement("div");
    wrap.className = "pending-route";
    const head = document.createElement("div");
    head.className = "pending-head";
    head.textContent = "Reply needs routing — send the number on your phone, or route here:";
    const txt = document.createElement("div");
    txt.className = "pending-text";
    txt.textContent = p.text || "";
    wrap.append(head, txt);
    for (let i = 0; i < (p.candidates || []).length; i++) {
      const c = (p.candidates || [])[i];
      if (!c || !c.sid) continue;
      const row = document.createElement("div");
      row.className = "pending-cand";
      const num = document.createElement("span");
      num.className = "pending-num";
      num.textContent = i + 1;
      const info = document.createElement("span");
      info.className = "pending-info";
      info.textContent = (c.project_dir || c.sid).slice(-48);
      info.title = c.sid + (c.project_dir ? " · " + c.project_dir : "");
      const btn = document.createElement("button");
      btn.className = "btn"; btn.type = "button";
      btn.textContent = "Route #" + (i + 1);
      btn.onclick = () => routePending(i + 1);
      row.append(num, info, btn);
      wrap.appendChild(row);
    }
    box.appendChild(wrap);
  } catch (e) { box.innerHTML = ""; }
}

async function routePending(index) {
  try {
    const r = await fetchJSON("/api/notify/route", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ index }),
    });
    devMsg(r.ok ? "routed to " + String(r.session || "").slice(-8)
                : (r.error || "route failed"));
    setTimeout(() => { loadNotifyPending(); loadNtfy(); }, 400);
  } catch (e) { devMsg("route failed"); }
}

const btnTestNtfy = $("btn-test-ntfy");
if (btnTestNtfy) btnTestNtfy.onclick = () => testNtfy();
const btnSaveTopic = $("btn-save-topic");
if (btnSaveTopic) btnSaveTopic.onclick = () => saveTopic();
const btnClearAll = $("btn-clear-all");
if (btnClearAll) btnClearAll.onclick = () => clearAllNtfy();

let refreshBusy = false;
async function refreshAll() {
  // The full refresh gathers ~6 endpoints; while one pass is still in flight
  // the 2 s interval would stack overlapping passes (each re-scanning the DB
  // and queuing behind the slowest request), which reads as 20-30 s lag after
  // window/session changes. Drop ticks that arrive mid-refresh; the next tick
  // covers them.
  if (refreshBusy) return;
  refreshBusy = true;
  try {
    await refreshAllInner();
  } finally {
    refreshBusy = false;
  }
}

async function refreshAllInner() {
  try { renderState(await fetchJSON("/api/state")); } catch (e) { setBadge("MONITOR OFFLINE", "offline"); }
  try { timelineRaw = await fetchJSON("/api/timeline?session=" +
    (timelineAll ? "all" : encodeURIComponent(picker.value || ""))); renderTimeline(timelineRaw); } catch (e) {}
  const sa = $("session-showall");
  if (sa && sa.checked !== showAllSessions) sa.checked = showAllSessions;
  try { renderSessions(await fetchJSON("/api/sessions?all=" + (showAllSessions ? "1" : "0"))); } catch (e) {}
  try { loadNtfy(); } catch (e) {}
  try { loadNotifyKinds(); } catch (e) {}
  try { loadNotifyPending(); } catch (e) {}
  try { renderTelemetry(await fetchJSON("/api/llama")); } catch (e) {}
  try { await loadHistory(); } catch (e) {}
}

async function control(intent) {
  try { await fetchJSON("/api/control", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ intent }) }); }
  catch (e) { console.error(e); }
  setTimeout(refreshAll, 500);
}

$("btn-pause").onclick = () => control($("btn-pause").textContent === "Pause" ? "pause" : "resume");
$("btn-poll").onclick = () => control("poll_now");

const showallEl = $("session-showall");
if (showallEl) {
  showallEl.checked = showAllSessions;
  showallEl.onchange = () => {
    showAllSessions = showallEl.checked;
    try { localStorage.setItem("hawk_showall", showAllSessions ? "1" : "0"); } catch (_) {}
    setTimeout(refreshAll, 100);
  };
}
const hidesubEl = $("session-hidesub");
if (hidesubEl) {
  hidesubEl.checked = hideSubSessions;
  hidesubEl.onchange = () => {
    hideSubSessions = hidesubEl.checked;
    try { localStorage.setItem("hawk_hidesub", hideSubSessions ? "1" : "0"); } catch (_) {}
    setTimeout(refreshAll, 100);
  };
}

const rangeBtns = document.querySelectorAll("#range-btns .range-btn");
function setWin(w) {
  teleWin = w;
  for (const x of rangeBtns) x.classList.toggle("active", x.dataset.win === w);
  loadHistory().catch(() => {});
  renderTimeline(timelineRaw);
  const nlog = $("notify-log");
  if (nlog) renderNotifyLog(ntfyCache.log, nlog);
}
for (const b of rangeBtns) b.onclick = () => setWin(b.dataset.win);



const settingsMsg = $("settings-msg");
const settingsFields = $("settings-fields");
const killFields = $("kill-settings-fields");
async function loadSettings() {
  try {
    const cfg = await fetchJSON("/api/settings");
    const specs = cfg.spec || {};
    const vals = cfg.values || {};
    const keep = {};
    for (const [k, s] of Object.entries(specs)) {
      // The llama kill switch and its thresholds share one card.
      const host = k.startsWith("llama_kill") ? killFields : settingsFields;
      if (!host) continue;
      keep[host.id + "/" + k] = true;
      let lab = host.querySelector(`label[data-k="${k}"]`);
      if (!lab) {
        lab = document.createElement("label");
        lab.dataset.k = k;
        lab.appendChild(document.createTextNode(s.label + " "));
        const inp = document.createElement("input");
        inp.dataset.k = k;
        if (s.type === "bool") {
          inp.type = "checkbox";
          lab.appendChild(inp);
        } else {
          inp.type = "number";
          inp.min = s.min;
          inp.max = s.max;
          if (s.type === "float") inp.dataset.f = "1";
          lab.appendChild(inp);
        }
        host.appendChild(lab);
      }
      const inp = lab.querySelector("input");
      if (inp.type === "checkbox") {
        const v = vals[k] === true || vals[k] === "true" || vals[k] === 1;
        if (inp.checked !== v) inp.checked = v;
      } else {
        inp.min = s.min;
        inp.max = s.max;
        const num = (inp.dataset.f ? parseFloat(vals[k]) : parseInt(vals[k], 10));
        const v = Math.max(parseFloat(s.min), Math.min(parseFloat(s.max), num || parseFloat(s.min)));
        if (inp.value !== String(v)) inp.value = v;
      }
    }
    for (const host of [settingsFields, killFields]) {
      if (!host) continue;
      host.querySelectorAll("label[data-k]").forEach((lab) => {
        const key = host.id + "/" + lab.dataset.k;
        if (!keep[key]) lab.remove();
      });
    }
  } catch (_) {}
}
$("btn-save-settings").onclick = async () => {
  settingsMsg.textContent = "";
  try {
    const body = {};
    [settingsFields, killFields].forEach((host) => {
      if (!host) return;
      host.querySelectorAll("input[data-k]").forEach((inp) => {
        if (inp.type === "checkbox") body[inp.dataset.k] = inp.checked;
        else if (inp.dataset.f) body[inp.dataset.k] = parseFloat(inp.value);
        else body[inp.dataset.k] = parseInt(inp.value, 10);
      });
    });
    const r = await fetch("/api/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (!r.ok) { settingsMsg.textContent = d.error || "error"; return; }
    settingsMsg.textContent = "saved";
    loadSettings();
    setTimeout(refreshAll, 300);
  } catch (e) { settingsMsg.textContent = "save failed"; }
};
const gearBtn = $("btn-gear");
const settingsPanel = $("settings-panel");
function toggleSettings() {
  if (!settingsPanel) return;
  const open = settingsPanel.hidden;
  settingsPanel.hidden = !open;
  if (gearBtn) gearBtn.classList.toggle("active", open);
  if (open) loadSettings();
}
if (gearBtn) gearBtn.onclick = toggleSettings;
const btnCloseSettings = $("btn-close-settings");
if (btnCloseSettings) btnCloseSettings.onclick = toggleSettings;

const picker = $("session-picker");
picker.onchange = () => {
  if (picker.value === "all") {
    timelineAll = true;
    userPickedSession = false;
    try { localStorage.setItem("hawk_timeline_all", "1"); } catch (_) {}
    refreshAll();
    return;
  }
  userPickedSession = true;
  timelineAll = false;
  try { localStorage.setItem("hawk_timeline_all", "0"); } catch (_) {}
  saveSession(picker.value, {selected: picker.value}).then(refreshAll);
};

loadSettings();
refreshAll();
setInterval(refreshAll, 2000);
