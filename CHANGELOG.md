# Changelog

All notable changes to Hawk are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). `0.2.0` is the current version in
`opencode_hawk/__init__.py` (the single source of truth — `pyproject.toml` reads
it dynamically); entries are added under **Unreleased** until a version is tagged.

## [Unreleased]

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

### Changed
- `apply_action` is split into an action-handler registry (`_ACTION_HANDLERS`);
  `evaluate` documents its first-match rule order as a numbered catalog.
- `poll_once` and `poll_multisession` share one engine (`poll_ctx`) and an
  explicit `Ctx`; `analyze()` is computed once per poll and shared.

### Fixed
- Per-session tracking is keyed by the evaluated leaf session, not the
  monitored root — a continued sub-session no longer suppresses the root
  session's re-stall handling.
- The ntfy publish path retries with backoff on HTTP 429 (rate limiting).
- `evaluate` stands down on provider/context-overflow errors and on user
  interrupts ("unterbrochen", `MessageAbortedError`): a deliberate stop is no
  longer mistaken for a stalled worker, and a new message releases the latch.
