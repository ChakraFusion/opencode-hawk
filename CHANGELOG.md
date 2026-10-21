# Changelog

All notable changes to Hawk are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). `0.2.1` is the current version in
`opencode_hawk/__init__.py` (the single source of truth — `pyproject.toml` reads
it dynamically); entries are added under **Unreleased** until a version is tagged.

## [Unreleased]

## [0.2.1] - 2026-10-21


### Added
- Log levels (`HAWK_LOG_LEVEL`) and size-capped rotation
  (`HAWK_LOG_MAX_BYTES`, default 2 MB, `0` disables), plus one structured
  decision line per poll (`sid`, `action`, `reason`, `rule_hits`).
- Config and state schema versioning: `load_config` deep-merges
  `CONFIG_DEFAULTS` on upgrade; `migrate_state` fills per-session defaults and
  stamps `version` (non-destructive — legacy top-level fields are preserved).
- Dev tooling in CI: `pytest`, `ruff`, `mypy` (pure helpers) and `coverage`,
  pinned in `requirements-dev.txt`; `tests/test_selftests.py` runs the existing
  self-test suites in-process under pytest.
- This changelog; the package version is single-sourced from
  `opencode_hawk/__init__.py`.
- Screenshot attachments on phone alerts: with `notify.ntfy.attach_screenshots`
  (default on) the newest recent image a workflow left in the session's
  project dir (root, `.playwright-mcp/`, `docs/screenshots/`, `screenshots/`,
  `.opencode/tmp/`) is uploaded and attached to its escalation / plan_done
  push; best-effort, falling back to a text alert.

### Changed
- `/api/llama` no longer ships the ~600-point chart series and compaction
  markers with every tile poll (~36 KB -> ~1 KB; the charts read the dedicated
  `/api/llama/history` snapshot instead; every field the tiles display is
  unchanged).
- Hidden browser tab stops polling entirely (`visibilitychange` gate, one full
  catch-up pass on return instead of a stack of missed ticks); timeline and
  session-list renders short-circuit when the payload is unchanged (the same
  `lastKey` filter pattern the ntfy panel already used).
- `git_branch` cache TTL raised 15 s -> 30 s (the `/api/state` coldest outlier).
- `apply_action` is split into an action-handler registry (`_ACTION_HANDLERS`);
  `evaluate` documents its first-match rule order as a numbered catalog.
- `poll_once` and `poll_multisession` share one engine (`poll_ctx`) and an
  explicit `Ctx`; `analyze()` is computed once per poll and shared.
- ntfy reply routing is multi-session aware: a reply goes to the sessions
  awaiting an answer, else to every enabled monitored session; with more than
  one candidate ntfy now asks which session (numbered prompt with a readable
  label) instead of silently routing to the most recent escalation.

### Fixed
- **Stale watched-session in the web UI**: timers froze on the legacy config
  `session_id` whenever all monitor roots were enabled — the dashboard now
  derives the watched session as: UI picker override, then the newest enabled
  monitor root, then the legacy `session_id` fallback (`watched_session_id()`).
- Web UI panels fetch independently on their own cadence (timeline 4 s,
  sessions 4 s, ntfy 10 s, kinds 30 s, telemetry 2 s, history 30 s) with a
  first-fill strobe and staggered retries for failed panels — a slow endpoint
  can no longer hold the other panels hostage, and window switches read
  instantly.
- Session picker selection survives picker re-renders; the override is
  decoupled from `/api/state` (`userPickedSession`) and no longer stomped
  between renders.
- Per-session tracking is keyed by the evaluated leaf session, not the
  monitored root — a continued sub-session no longer suppresses the root
  session's re-stall handling.
- The ntfy publish path retries with backoff on HTTP 429 (rate limiting).
- `evaluate` stands down on provider/context-overflow errors and on user
  interrupts ("unterbrochen", `MessageAbortedError`): a deliberate stop is no
  longer mistaken for a stalled worker, and a new message releases the latch.
