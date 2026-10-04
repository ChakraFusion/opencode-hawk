# Hawk

[![tests](https://github.com/ChakraFusion/opencode-hawk/actions/workflows/tests.yml/badge.svg)](https://github.com/ChakraFusion/opencode-hawk/actions/workflows/tests.yml)

**A coordinator that keeps long-running, multi-agent [OpenCode](https://opencode.ai) workflows moving while you're away.**

Hawk is built for **multi-agent OpenCode workflows**: a lead agent works through
a multi-milestone build plan and dispatches subagents (developer, researcher,
reviewer, and so on) for the individual steps. On a local model served by
llama.cpp, such a run takes hours, and it often stops along the way: the lead
finishes a step, waits for a permission prompt, asks a question, or the model
server degrades. Hawk watches the whole session tree (lead and subagents) and
nudges it along. It also pings your phone when a human is actually needed.

Built and tested with **OpenCode v1** (1.18.x).

![Hawk dashboard, Modern skin](docs/screenshots/dashboard-modern.png)

<details>
<summary>Classic skin</summary>

![Hawk dashboard, Classic skin](docs/screenshots/dashboard-classic.png)

</details>

## What it does

- **Auto-continue.** When the worker has stopped and the model is idle, Hawk
  injects a self-check prompt telling the agent to re-check the plan and keep
  going. It never interrupts a turn that is still generating, and sends at
  most one prompt per `re_stall_minutes`.
- **Confirmed finish.** The build counts as done only when the agent says
  `STOP: DONE` twice, on two separate messages. Hawk asks once to confirm.
- **Permissions and questions.** It auto-accepts external-directory permission
  prompts and answers question-tool prompts (choosing the "(Recommended)"
  option) for sessions you mark as monitored.
- **llama-server watchdog.** It detects a dead or degraded `llama-server`
  (low decode speed at deep context) and restarts it.
- **Phone alerts via [ntfy](https://ntfy.sh).** Escalations and plan-complete
  events go to your phone, and replies from the ntfy app are routed back into
  the session.
- **Subagent tracking.** It follows the session tree, so a lead that is waiting on
  a running subagent counts as busy, and every subagent start and finish shows
  up in the timeline.
- **Dashboard.** A local web UI (`http://127.0.0.1:8765`) with sessions, events,
  token speed, GPU/VRAM charts, the ntfy channel and settings.

## Requirements

- **Windows 10/11 or Linux.** On Linux, GPU stats come from the amdgpu driver
  (AMD) or `nvidia-smi` (NVIDIA); see [Platform notes](#platform-notes).
- **Python 3.10+** and `psutil` (`pip install -r requirements.txt`)
- **OpenCode v1** (tested with 1.18.x), either the Desktop app or the CLI. Hawk reads its session database
  (default `~/.local/share/opencode/opencode.db`, override with `OPENCODE_DB`).
- **Optional:** a local `llama-server` (llama.cpp) for the idle and degradation
  probes and the watchdog. Defaults to port 1234.
- **Optional:** the [ntfy app](https://ntfy.sh) on your phone.

## Quick start

Install with [pipx](https://pipx.pypa.io) (Windows and Linux):

```sh
pipx install git+https://github.com/ChakraFusion/opencode-hawk.git
hawk init
```

`hawk init` creates `config.json` and generates a **private ntfy topic** for this
install (`hawk-` followed by 12 random digits). Then:

1. Open the `config.json` it printed and set `project_dir` to the repository your
   agent works in.
2. Subscribe to the printed topic in the ntfy app. Treat the topic name like a
   password: anyone who knows it can read and send messages on it.
3. Start Hawk with `hawk run`: the monitor and the dashboard, with the dashboard
   opened in your browser. Ctrl+C stops both.

Update with `pipx upgrade opencode-hawk`.

### Commands

| Command | What it does |
|---|---|
| `hawk init` | Create `config.json` and the ntfy topic (keeps an existing config) |
| `hawk run` | Monitor + dashboard (`--interval N` minutes, `--port P`, `--no-open`) |
| `hawk monitor` | The poll loop only (`--interval`, `--dry-run`, `--session-id`) |
| `hawk poll` | One pass, e.g. from Task Scheduler or cron (`--dry-run`) |
| `hawk dashboard` | The web dashboard only (`--port`, `--open`) |
| `hawk install-task` | Print the `schtasks` (Windows) or crontab (Linux) entry for `hawk poll` |
| `hawk where` | Show the folders Hawk uses |

### From a source checkout

```sh
git clone https://github.com/ChakraFusion/opencode-hawk.git
cd opencode-hawk
pip install -r requirements.txt
python -m opencode_hawk init
python -m opencode_hawk run
```

Or use `quickstart.bat` / `./quickstart.sh`, which open the monitor and the
dashboard in their own windows; `quick_kill.bat` / `./quick_kill.sh` stops both.

## Configuration

Everything lives in `config.json`. Most timing settings can also be changed
from the dashboard.

| Key | Default | Meaning |
|---|---|---|
| `project_dir` | `""` | Repository the agent works in (used for git and commit checks) |
| `session_id` | `""` | Session to watch; empty = newest llama.cpp session |
| `monitored_sessions` | `{}` | Per-session switches: `enabled`, `auto_accept`, `auto_continue`, `project_dir` |
| `idle_minutes` | `8` | Quiet time before a session counts as stopped |
| `poll_minutes` | `10` | Monitor interval (overridden by `--interval`) |
| `stalled_turn_minutes` | `15` | An unfinished turn silent this long counts as stopped |
| `re_stall_minutes` | `45` | Minimum gap between self-check prompts for one session |
| `continue_message` / `done_confirm_message` | built-in | The prompts Hawk injects |
| `auto_accept_external_dirs` | `true` | Auto-accept external-directory permission prompts (monitored sessions only) |
| `auto_answer_questions` | `true` | Auto-answer question-tool prompts |
| `continue_via_attach` | `true` | Inject through the Desktop sidecar so the GUI streams live |
| `llama_api_port` | `1234` | llama-server API port |
| `llama_bat_path` | `""` | Script that starts llama-server (`.bat` on Windows, shell script on Linux); the watchdog uses it to respawn a dead server. Empty = no respawn |
| `llama_pid` | `0` | 0 = find llama-server by process name |
| `gpu_vram_total_mb` | `0` | VRAM shown in charts; 0 = auto-detect |
| `notify.ntfy.topic` | generated | Your private ntfy topic |
| `notify.ntfy.server` | `https://ntfy.sh` | ntfy server (self-hosted works too) |
| `notify.ntfy.kinds` | `escalation, plan_done, test` | Which events are pushed to the phone |

Where `config.json`, state and logs live (`hawk where` prints it): `COORD_HOME`
when set; in a source checkout the repository folder; otherwise
`%APPDATA%\opencode-hawk` on Windows and `~/.config/opencode-hawk` on Linux.
Other environment variables: `OPENCODE_DB`, `OPENCODE_BIN`.

## Platform notes

| | Windows | Linux |
|---|---|---|
| GPU load and VRAM | Performance counters (any GPU) | AMD: amdgpu sysfs, no extra tools. NVIDIA: `nvidia-smi` |
| VRAM used by llama-server | Per-process GPU counters | AMD: the process's DRM fdinfo. NVIDIA: `nvidia-smi` |
| Disk activity | Performance counters | `psutil` per-disk busy time |
| RAM stick inventory | WMI | Not shown (needs root) |
| OpenCode Desktop sidecar | `OpenCode.exe` | Any `opencode*` process that exposes the server credentials |

Linux support is new. Bug reports from real Linux setups are welcome.

## Agent markers

Hawk's injected prompts tell the agent which markers to use, so normally no
extra setup is needed:

| Marker | Meaning |
|---|---|
| `STOP: DONE` | The whole plan is complete (Hawk asks once to confirm) |
| `STOP: NEEDS_DECISION ...` | A human decision is needed |
| `STOP: BLOCKED ...` | A problem the agent cannot solve alone |

## Privacy

Hawk runs locally. The dashboard binds to `127.0.0.1`. The only outbound
traffic is to your ntfy server, and only if a topic is configured.

## Tests

```bat
python tests/selftest_coordinator.py
python tests/selftest_dashboard.py
python tests/selftest_notify.py
python tests/selftest_linux.py
```

## License

[MIT](LICENSE) © ChakraFusion
