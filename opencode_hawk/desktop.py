"""OpenCode Desktop integration: find the Desktop's sidecar server and its credentials, auto-accept external-directory permission asks and auto-answer question-tool asks for monitored sessions.

Split out of coordinator.py and still part of its namespace: every coordinator-level name is used as
`core.<name>` and coordinator re-binds the names defined here, so a patch on the coordinator module
reaches this code. Import it through coordinator, not directly."""
from __future__ import annotations
import json
import os
import threading
import time
import urllib.parse
import urllib.request
from . import hawk_linux

from . import coordinator as core  # shared namespace: see the module docstring


# ── Desktop permission auto-accept ───────────────────────────────────────────
_PERM_REPLIED = {}  # requestID -> timestamp of last reply (dedupe window 15s)


def record_permission_accept(rid, ver, session_id, target, kind="permission_accept"):
    """Append one JSONL line to home_dir()/permission_events.jsonl for the
    dashboard's event timeline. Kept separate from the session DB so the fed
    transcript (and thus the LLM context) is never touched by sidecar actions.
    Never raises — the event feed is optional."""
    try:
        ev = {"t": int(time.time() * 1000), "kind": kind,
              "id": rid, "ver": ver, "session": session_id or "",
              "target": (target or "")[:240]}
        with open(core.home_dir() / "permission_events.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    except Exception:
        pass


def desktop_sidecar():
    """Discover the opencode Desktop sidecar as (base_url, creds).

    The Desktop bundles its opencode server behind HTTP Basic auth with a
    per-launch generated username/password exposed only in the OpenCode.exe
    process environment, so read them fresh at call time; never persist them.
    """
    try:
        import psutil
    except ImportError:
        core.log_debug("desktop_sidecar: psutil not installed")
        return None
    if os.name != "nt":
        return hawk_linux.desktop_sidecar()
    sidecar = core.find_desktop_server()
    if not sidecar:
        return None
    user = pwd = None
    for proc in psutil.process_iter(["name"]):
        if proc.info.get("name") != "OpenCode.exe":
            continue
        try:
            env = proc.environ()
        except Exception:
            continue
        u = env.get("OPENCODE_SERVER_USERNAME")
        p = env.get("OPENCODE_SERVER_PASSWORD")
        if u and p:
            user, pwd = u, p
            break
    if not (user and pwd):
        core.log_debug("desktop_sidecar: no OpenCode.exe server credentials in env")
        return None
    return sidecar, {"username": user, "password": pwd}


def _perm_request(base, creds, path, method="GET", body=None, timeout=8):
    import base64
    import urllib.request
    auth = "Basic " + base64.b64encode(
        ("%s:%s" % (creds["username"], creds["password"])).encode("utf-8")).decode("ascii")
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={
        "Authorization": auth,
        "Content-Type": "application/json" if data is not None else "text/plain"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        return -1, (str(e)).encode("utf-8", "replace")


PERM_AUTO_ACTIONS = ("external_directory",)
# Auto-accept scope bounds (plan §4a). Depth matches active_leaf's, so both
# walkers agree on what "the session tree" means.
PERM_LINEAGE_MAX_DEPTH = 3   # parent hops walked up session.parent_id
PERM_SUBDIR_FANOUT = 16      # children read per lineage node
PERM_SUBDIR_NODES = 400      # lineage nodes discovered per sweep
PERM_DIR_CAP = 128           # directories returned by permission_directories


def _perm_action(req: dict) -> str:
    """Name of the thing being asked for. v2 calls it 'action', v1 'permission'."""
    if not isinstance(req, dict):
        return ""
    return str(req.get("action") or req.get("permission") or "")


def _perm_json(base, creds, path):
    """GET a sidecar route and parse JSON, or None when the route is absent.

    Unknown /api/* paths are proxied to app.opencode.ai and come back as 403
    HTML rather than 404, so a non-200 or non-JSON body means "this build does
    not serve that route" -- never "the queue is empty"."""
    st, raw = core._perm_request(base, creds, path)
    if st != 200:
        core.log_debug("permission GET %s -> HTTP %s" % (path, st))
        return None
    try:
        return json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        core.log_debug("permission GET %s -> non-JSON body %r" % (path, raw[:80]))
        return None


def _open_session_db():
    """One best-effort read-only handle on the opencode session DB, or None.

    None is a normal answer, not an error. The exists() guard keeps a missing DB
    free (connect_db retries 5x with a 0.5*(i+1)s backoff), and a DB that exists
    but will not open -- locked past busy_timeout, mid-recovery -- is swallowed
    too, so one unreadable DB can never take a whole sweep down. Callers must
    read None as "unprovable" and fail closed."""
    try:
        sdb = core.db_path()
        return core.connect_db(sdb) if sdb.exists() else None
    except Exception as e:
        core.log_debug("permission: open session DB: %s" % e)
        return None


def perm_root_of(con, sid: str, max_depth: int = PERM_LINEAGE_MAX_DEPTH) -> str | None:
    """Walk `sid` up session.parent_id to its root session id.

    Returns None -- never raises -- when the id is empty, the row is unknown,
    the chain is longer than max_depth, a cycle is seen, or the query fails.
    None means "unprovable", and every caller must read that as "do not
    auto-accept". The only DB call is wrapped, and every other operation is on
    a value already narrowed to str/int, so the sole caller (which passes the
    default bound) cannot be handed an exception to propagate. A dedicated
    one-column PK walk: session_row (:536) does not select parent_id and must not
    grow one (4 callers, 4 fake tables)."""
    cur = sid.strip() if isinstance(sid, str) else ""
    seen = set()
    for _ in range(max_depth + 1):  # +1 so a 3-deep chain still reaches its root
        if not cur or cur in seen:
            return None  # nameless ask, or a parent_id cycle (R6)
        seen.add(cur)
        try:
            row = con.execute("SELECT parent_id FROM session WHERE id=?",
                              (cur,)).fetchone()
        except Exception as e:
            core.log_debug("perm_root_of(%s): %s" % (cur, e))
            return None
        if row is None:
            return None  # no such row: lineage unprovable (AC5)
        parent = row[0].strip() if isinstance(row[0], str) else ""
        if not parent:
            return cur  # root reached
        cur = parent
    return None  # deeper than the bound (R7): fails closed, ask stays hanging


def perm_session_allowed(cfg, sid: str, con=None) -> bool:
    """True iff `sid` is, or descends from, a `monitored_sessions` key whose
    auto_accept is on. `enabled` is hawk's steering toggle and is NOT consulted.

    Fails closed, and "provable" admits no exceptions: the grant set is a
    function of the session table, never of whether the DB happened to open this
    cycle. `con=None` is a HARD fail-closed sentinel -- it never means "open your
    own", and no code path does -- so it rejects every sid, registered or not,
    and says why under COORD_DEBUG. A registered key is judged by its own entry
    only when the DB PROVES it is a root (parent_id NULL), never because the DB
    could not be read: 32 of the 46 registered keys on this machine are
    subagents, so a missing handle that judged one by its own stale auto_accept
    would re-admit exactly the sessions a root's revocation exists to keep
    rejected, and would do it with durable grants (AC4(d), AC5(i))."""
    if not isinstance(sid, str) or not sid.strip():
        return False  # must not accept a nameless ask
    ms = cfg.get("monitored_sessions") or {}
    if not isinstance(ms, dict):
        return False
    if con is None:
        core.log_debug("perm_session_allowed(%s): no session DB handle this cycle; "
                  "rejecting, lineage unprovable" % sid)
        return False  # DB unavailability must never widen the grant set
    # perm_root_of never raises (its docstring's contract: the one DB call is
    # wrapped, everything else operates on a value already narrowed to str),
    # so there is nothing to catch here -- only a result to distrust.
    root = core.perm_root_of(con, sid)
    # Unprovable lineage -- no row, a cycle, deeper than the bound, or a
    # transient DB error -- comes back None and REJECTS (AC5, R6, R7). Never
    # read a missing row as "its own root": a transient error would then
    # turn into a durable "always" grant with zero lineage proof.
    entry = ms.get(root) if root else None
    return isinstance(entry, dict) and bool(entry.get("auto_accept", True))


def permission_directories(cfg, base, creds, con=None) -> list[str]:
    """Directories to sweep, in order: the `/project` worktrees, cfg["project_dir"],
    each monitored session's own project_dir, then each monitored session's own
    session directory and its sub-agents' (root-first, capped at PERM_DIR_CAP).

    Both pending-permission list routes are scoped to one directory and default
    to the server's own project, so asks raised in another project (e.g.
    demo_project) are only visible when the directory is passed explicitly --
    and a session (or a sub-agent spawned under it) can work somewhere that is
    neither a /project worktree nor in the config, so its directory has to be
    discovered too or its asks sit unanswered. `enabled` is deliberately NOT
    consulted: a session the user registered is monitored whether or not steering
    happens to be on for it, and gating discovery on it left live asks unswept.

    Pass 1 (each root's own directory) is uncapped in practice, so a tight
    PERM_DIR_CAP degrades by dropping sub-agent directories first (R3) -- but
    only while pass 1 fits: 2 slots per distinct root directory, so >64 distinct
    root directories breaches the 128 cap (46 keys today => <=92).

    `con` is a shared read-only handle from the caller; None means "open one here
    and close it before returning", so a sweep passes its own single handle down
    instead of paying connect_db's retry storm twice per cycle. Note the
    asymmetry with perm_session_allowed, on purpose: here None means "get one
    myself" because discovery is best-effort and a DB-less directory list is
    still useful, whereas there None means "nothing is provable" and the grant
    decision must fail closed."""
    dirs = []

    def _add(d):
        # "." is what os.path.normpath("") returns, and an empty/NULL
        # `directory` must not smuggle it into the swept set (a bogus
        # "GET /permission?directory=." every 15s).
        d = d.strip() if isinstance(d, str) else ""
        if d and d not in ("/", "\\", ".") and d not in dirs:
            dirs.append(d)

    def _add_dir(d):
        """One directory in both observed spellings: the DB stores '/', the
        live route uses that form, asks record '\\'."""
        d = d.strip() if isinstance(d, str) else ""
        if not d:
            return
        _add(d)
        _add(os.path.normpath(d))

    for p in core._perm_json(base, creds, "/project") or []:
        if isinstance(p, dict):
            _add(p.get("worktree"))
    _add(cfg.get("project_dir"))
    ms_all = cfg.get("monitored_sessions") or {}
    if not isinstance(ms_all, dict):
        ms_all = {}
    for ms in ms_all.values():
        if isinstance(ms, dict):
            _add(ms.get("project_dir"))
    roots = [sid for sid, ms in ms_all.items() if isinstance(ms, dict)]
    own_con = con is None
    con = core._open_session_db() if own_con else con
    try:
        if con is not None:
            # Pass 1: one PK seek per monitored root. Cannot be starved by the
            # cap, and a sub-agent normally shares its root's directory -- so
            # this alone already covers the primary case.
            for sid in roots:
                _add_dir((core.session_row(con, sid) or {}).get("directory"))
            # Pass 2: bounded walk DOWN parent_id, for a sub-agent that works
            # somewhere else. active_leaf's proven per-node seek against
            # session.parent_id, kept as a bounded BFS rather than a recursive
            # CTE so the node budget can be enforced here.
            seen, queue, budget = set(roots), list(roots), core.PERM_SUBDIR_NODES
            while queue and budget > 0:
                for cid, cdir in con.execute(
                        "SELECT id, directory FROM session WHERE parent_id=? "
                        "ORDER BY time_updated DESC LIMIT ?",
                        (queue.pop(0), core.PERM_SUBDIR_FANOUT)).fetchall():
                    if cid in seen:
                        continue  # a mislinked row can name a node twice
                    seen.add(cid)
                    _add_dir(cdir)
                    budget -= 1
                    if budget > 0:
                        queue.append(cid)
            if budget <= 0:
                core.log_debug("permission_directories: lineage node budget %d hit"
                          % core.PERM_SUBDIR_NODES)
    except Exception as e:
        core.log_debug("permission_directories: %s" % e)
    finally:
        if own_con and con is not None:
            try:
                con.close()
            except Exception:
                pass
    if len(dirs) > core.PERM_DIR_CAP:
        core.log_debug("permission_directories: %d dirs, capped at %d"
                  % (len(dirs), core.PERM_DIR_CAP))
        dirs = dirs[:core.PERM_DIR_CAP]
    return dirs


def desktop_permission_sweep(cfg, base=None, creds=None) -> int:
    """Reply "always" to every pending external_directory permission request
    raised by a monitored session on the Desktop sidecar (the same call the GUI
    "accept" button makes).

    The sidecar serves two permission APIs and the GUI's file-location asks can
    land on either, so both are swept per project directory:

      v1  GET  /permission?directory=<dir>          -> [ {id, sessionID, permission, patterns} ]
          POST /permission/<rid>/reply?directory=<dir>
      v2  GET  /api/permission/request?location[directory]=<dir>
                                                    -> {"data": [ {id, sessionID, action, resources} ]}
          POST /api/session/<sid>/permission/<rid>/reply

    Returns how many requests were answered.

    Scope: an ask is answered only when its `sessionID` resolves through the DB
    parent/child lineage to a `monitored_sessions` root whose `auto_accept` is
    on (`enabled` is never consulted, and a session the user never registered is
    left hanging on purpose). Both the v1 and the v2 loop funnel through
    `_claim`, so that decision cannot drift between them."""
    if not cfg.get("auto_accept_external_dirs", True):
        return 0
    if base is None or creds is None:
        found = core.desktop_sidecar()
        if not found:
            return 0
        base, creds = found
    replied = 0
    now = time.time()
    # One best-effort read-only handle for the whole sweep, shared with
    # permission_directories, so neither a per-ask lineage walk nor discovery
    # pays connect_db's 5-retry 0.5*(i+1)s backoff of its own. Sharing it is
    # what makes con=None LOAD-BEARING: it is a real state, not a hypothetical,
    # because a missing/locked DB yields None and every ask then has to fail
    # closed -- so perm_session_allowed treats it as a hard reject, and never
    # opens a connection behind the sweep's back (R5, AC5(i)).
    con = core._open_session_db()
    memo = {}  # ask sessionID -> allowed; both polarities, for this sweep only

    def _claim(req) -> str | None:
        """Request id, if this is an ask we auto-accept and have not just answered.

        Reads the sweep's own `con` from the enclosing scope -- both call sites
        passed that same object, and desktop_question_sweep's _claim below has
        the parameterless shape."""
        if core._perm_action(req) not in core.PERM_AUTO_ACTIONS:
            return None  # cheapest test first; a bash ask opens no DB
        rid = req.get("id")
        if not rid:
            return None  # malformed ask; no DB
        sid = req.get("sessionID") or ""
        ok = memo.get(sid)
        if ok is None:
            ok = core.perm_session_allowed(cfg, sid, con)
            memo[sid] = ok
        if not ok:
            # Ahead of the dedupe write on purpose: a rejected ask must NOT
            # start a 15s timer, or ticking the dashboard checkbox would make
            # the grant wait one out (AC8).
            return None
        if core._PERM_REPLIED.get(rid, 0.0) > now - 15:
            return None
        core._PERM_REPLIED[rid] = now
        return rid

    def _accept(path, rid, ver, req, resources):
        st, raw = core._perm_request(base, creds, path, method="POST", body={"reply": "always"})
        if st not in (200, 201, 202, 204):
            core.log_debug("permission reply %s -> HTTP %s %s" % (rid, st, raw[:120]))
            return 0
        target = (req.get("metadata") or {}).get("filepath") or ",".join(resources or [])
        core.log("permission auto-accept: %s %s session=%s resources=%s" %
            (rid, ver, req.get("sessionID"), target))
        core.record_permission_accept(rid, ver, req.get("sessionID"), target)
        return 1

    try:
        for d in core.permission_directories(cfg, base, creds, con):
            q = urllib.parse.quote(d, safe="")
            for req in core._perm_json(base, creds, "/permission?directory=" + q) or []:
                rid = _claim(req)
                if rid:
                    replied += _accept("/permission/%s/reply?directory=%s" % (rid, q),
                                       rid, "v1", req, req.get("patterns"))
            body = core._perm_json(base, creds, "/api/permission/request?location%5Bdirectory%5D=" + q)
            for req in (body.get("data") if isinstance(body, dict) else None) or []:
                rid = _claim(req)
                sid = req.get("sessionID")
                if rid and sid:
                    replied += _accept("/api/session/%s/permission/%s/reply" % (sid, rid),
                                       rid, "v2", req, req.get("resources"))
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    return replied


# ── Desktop question auto-answer ─────────────────────────────────────────────
_QUESTION_REPLIED = {}  # requestID -> timestamp of last reply (dedupe window 15s)


def _pick_answer(question: dict) -> list[str] | None:
    """Labels to auto-select for one question: the option whose label carries
    '(Recommended)' (case-insensitive), else the first option. None when the
    question has no options (free-text asks are left for a human)."""
    opts = [o for o in (question.get("options") or []) if isinstance(o, dict)]
    if not opts:
        return None
    for o in opts:
        if "recommended" in (o.get("label") or "").lower():
            return [o.get("label")]
    return [opts[0].get("label")]


def desktop_question_sweep(cfg, base=None, creds=None) -> int:
    """Answer pending question-tool requests on the Desktop sidecar with the
    recommended option (label containing 'Recommended', else the first option)
    -- the same call the GUI option buttons make.

    A `question` tool call blocks the worker's turn until answered, so an
    unanswered ask pins the session in 'busy' forever (the permission sweep's
    blind spot: self-check injections cannot unblock it either). The pending
    list routes are directory-scoped, so sweep per project directory:

      v1  GET  /question?directory=<dir>              -> [ {id, sessionID, questions, tool} ]
          POST /question/<rid>/reply?directory=<dir>  {"answers": [[labels]]}
      v2  GET  /api/question/request?location[directory]=<dir>
                                                     -> {"data": [...]}
          POST /api/session/<sid>/question/<rid>/reply

    'answers' is one entry per question, each an array of selected labels.
    Returns how many requests were answered."""
    if not cfg.get("auto_answer_questions", True):
        return 0
    if base is None or creds is None:
        found = core.desktop_sidecar()
        if not found:
            return 0
        base, creds = found
    answered = 0
    now = time.time()

    def _claim(req) -> str | None:
        """Request id, if not just answered (dedupe window 15s)."""
        rid = req.get("id") if isinstance(req, dict) else None
        if not rid or core._QUESTION_REPLIED.get(rid, 0.0) > now - 15:
            return None
        core._QUESTION_REPLIED[rid] = now
        return rid

    def _answers(req):
        out = []
        for q in req.get("questions") or []:
            labels = core._pick_answer(q)
            if labels is None:
                return None
            out.append(labels)
        return out

    def _reply(path, rid, ver, req):
        body = {"answers": _answers(req)}
        if body["answers"] is None:
            core.log_debug("question %s has an option-less question; left for a human" % rid)
            return 0
        st, raw = core._perm_request(base, creds, path, method="POST", body=body)
        if st not in (200, 201, 202, 204):
            core.log_debug("question reply %s -> HTTP %s %s"
                      % (rid, st, raw[:120].decode("utf-8", "replace")
                         if isinstance(raw, bytes) else str(raw)[:120]))
            return 0
        core.log("question auto-answer: %s %s session=%s answers=%s"
            % (rid, ver, req.get("sessionID"),
               json.dumps(body["answers"], ensure_ascii=False)))
        core.record_permission_accept(rid, ver, req.get("sessionID"),
                                 json.dumps(body["answers"], ensure_ascii=False),
                                 kind="question_answer")
        return 1

    for d in core.permission_directories(cfg, base, creds):
        q = urllib.parse.quote(d, safe="")
        for req in core._perm_json(base, creds, "/question?directory=" + q) or []:
            rid = _claim(req)
            if rid:
                answered += _reply("/question/%s/reply?directory=%s" % (rid, q),
                                   rid, "v1", req)
        body = core._perm_json(base, creds, "/api/question/request?location%5Bdirectory%5D=" + q)
        for req in (body.get("data") if isinstance(body, dict) else None) or []:
            rid = _claim(req)
            sid = req.get("sessionID")
            if rid and sid:
                answered += _reply("/api/session/%s/question/%s/reply" % (sid, rid),
                                   rid, "v2", req)
    return answered


def permission_event_watcher(stop: threading.Event):
    """Watch the Desktop /event stream and the pending-permission lists.

    Answers external_directory asks immediately on SSE "permission" events
    (v1 emits permission.asked, v2 permission.v2.asked) AND on a ~15s backstop
    sweep, and answers pending question-tool asks the same way (a `question` tool
    call blocks the worker turn until answered). The backstop runs on its own
    thread because the SSE read blocks: a quiet stream would otherwise pin the
    sweep to the socket timeout.
    Re-discovers port + credentials when the Desktop restarts."""
    import base64
    import urllib.request
    cfg_holder = {"cfg": {}}  # load once per sweep instead of per line
    sweep_lock = threading.Lock()
    while not stop.is_set():
        found = core.desktop_sidecar()
        if not found:
            if stop.wait(10):
                return
            continue
        base, creds = found
        auth = "Basic " + base64.b64encode(
            ("%s:%s" % (creds["username"], creds["password"])).encode("utf-8")).decode("ascii")
        stream_done = threading.Event()

        def _sweep_now():
            with sweep_lock:
                try:
                    cfg_holder["cfg"] = core.load_config()
                    core.desktop_permission_sweep(cfg_holder["cfg"], base=base, creds=creds)
                    core.desktop_question_sweep(cfg_holder["cfg"], base=base, creds=creds)
                except Exception as e:
                    core.log_debug("permission sweep error: %s" % e)

        def _backstop():
            while not (stop.is_set() or stream_done.is_set()):
                _sweep_now()
                if stream_done.wait(15):
                    return

        backstop = threading.Thread(target=_backstop, name="perm-sweep", daemon=True)
        backstop.start()
        try:
            req = urllib.request.Request(base + "/event", headers={"Authorization": auth})
            with urllib.request.urlopen(req, timeout=55) as r:
                core.log("permission watcher connected: %s/event" % base)
                while not stop.is_set():
                    line = r.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", "replace").strip()
                    if not text.startswith("data:"):
                        continue
                    try:
                        ev = json.loads(text[5:].strip())
                    except Exception:
                        continue
                    etype = str(ev.get("type", "")).lower()
                    if "permission" in etype or "question" in etype:
                        _sweep_now()
        except Exception as e:
            core.log_debug("permission watcher dropped: %s" % e)
        finally:
            stream_done.set()
            backstop.join(timeout=5)
        stop.wait(5)
