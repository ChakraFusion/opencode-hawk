import type { Plugin } from "@opencode-ai/plugin"
import { promises as fs } from "node:fs"
import { homedir } from "node:os"
import { join } from "node:path"

/**
 * Hawk's llama-restart plugin for OpenCode (installed with `hawk install-plugin`).
 *
 * Coordinates with the Hawk dashboard to restart the local llama-server at the
 * least disruptive moment, and only after compaction:
 *
 *   1. When a compacted event fires we ask the dashboard whether a restart is
 *      due (3.5 h uptime cadence or slow-deep degradation backstop).
 *   2. We arm the restart but DO NOT fire immediately: we wait for a session
 *      idle event (the compaction just happened -> the point where the context
 *      is smallest and the server is not decoding).
 *   3. Only then do we POST /api/llama/restart; the dashboard side additionally
 *      refuses while slot 0 is busy, and gracefully /shutdowns + respawns the
 *      server with the identical live cmdline.
 *
 * The dashboard status gate (slots_idle, locked) prevents restarting during an
 * in-flight request. A restart also self-heals dropped connections: after the
 * new server is healthy the next user request re-prefills and proceeds.
 */

// The Hawk dashboard; without one running this plugin stays inert.
const DASHBOARD_BASE = (process.env.HAWK_DASHBOARD_URL ?? "http://127.0.0.1:8765").replace(/\/+$/, "")
// Runtime-neutral (Node sidecar or Bun CLI) and outside the git-tracked config dir.
const STATE_FILE = join(homedir(), ".local", "share", "opencode", "llama-restart-state.json")

// Wait this long after arming (i.e. after a compaction with no idle window yet)
// before firing on the next idle. Compaction itself briefly counts as activity,
// so idle is usually only a few seconds later.
const STALE_WAIT_MS = 30 * 60 * 1000
const ATTEMPT_COOLDOWN_MS = 5 * 60 * 1000
// An arm older than this is dropped (the due condition it was based on is stale).
const MAX_ARMED_MS = 6 * 60 * 60 * 1000

type RestartState = {
  armed: boolean
  armedAtMs: number
  lastAttemptMs: number
  lastRestartMs: number
}

function defaultState(): RestartState {
  return { armed: false, armedAtMs: 0, lastAttemptMs: 0, lastRestartMs: 0 }
}

async function loadState(): Promise<RestartState> {
  try {
    const raw = JSON.parse(await fs.readFile(STATE_FILE, "utf8"))
    return { ...defaultState(), ...(raw ?? {}) }
  } catch {
    return defaultState() // missing/corrupt state -> start fresh
  }
}

async function saveState(s: RestartState) {
  try {
    await fs.writeFile(STATE_FILE, JSON.stringify(s, null, 2))
  } catch (e) {
    console.warn("[llama-restart] save state failed:", e)
  }
}

async function dashboardStatus(): Promise<Record<string, unknown> | null> {
  try {
    const res = await fetch(`${DASHBOARD_BASE}/api/llama/restart/status`, {
      signal: AbortSignal.timeout(5_000),
    })
    if (!res.ok) return null
    return (await res.json()) as Record<string, unknown>
  } catch {
    return null // dashboard down -> do nothing
  }
}

async function requestRestart(): Promise<Record<string, unknown> | null> {
  try {
    const res = await fetch(`${DASHBOARD_BASE}/api/llama/restart`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason: "plugin" }),
      signal: AbortSignal.timeout(10_000),
    })
    if (!res.ok) return null
    return (await res.json()) as Record<string, unknown>
  } catch {
    return null
  }
}

export const LlamaRestartPlugin: Plugin = ({ client, directory }) => {
  let state: RestartState = defaultState()
  // Only machines running the Hawk dashboard get restarts (and the compaction hint).
  let dashboardSeen = false

  const disarm = async (): Promise<void> => {
    if (!state.armed) return
    state.armed = false
    state.armedAtMs = 0
    await saveState(state)
  }

  const arm = async (): Promise<void> => {
    const st = await dashboardStatus()
    if (!st || st.error) return
    dashboardSeen = true
    const due = st.restart_due === true
    const idle = st.slots_idle !== false // unknown -> attempt anyway, server side re-gates
    const locked = st.locked === true
    if (due && idle && !locked) {
      if (!state.armed) {
        state.armed = true
        state.armedAtMs = Date.now()
        await saveState(state)
      }
    } else if (!due) {
      await disarm()
    }
  }

  const maybeFire = async (): Promise<void> => {
    if (!state.armed) return
    const now = Date.now()
    if (now - state.armedAtMs > MAX_ARMED_MS) return disarm()
    if (now - state.lastAttemptMs < ATTEMPT_COOLDOWN_MS) return
    const st = await dashboardStatus()
    if (!st || st.error) return // dashboard down -> stay quiet, keep the arm until it expires
    if (st.restart_due !== true) return disarm()
    if (st.locked === true) return // another restart in flight
    state.lastAttemptMs = now
    await saveState(state)
    const result = await requestRestart()
    if (result && result.ok === true) {
      state.armed = false
      state.lastRestartMs = Date.now()
      console.warn(
        `[llama-restart] server restarted (pid ${result.pid_old} -> ${result.pid_new}, healthy=${result.healthy})`,
      )
      if (client?.tui?.showToast) {
        await client.tui.showToast({
          body: {
            title: "llama-server restart",
            message: `restarted (${String(result.reason ?? "plugin")})`,
            variant: "success",
          },
          query: { directory },
        })
      }
    } else {
      console.warn("[llama-restart] restart rejected:", result?.reason ?? "no reply")
    }
    await saveState(state)
  }

  void loadState().then(s => {
    state = s
    // If we were armed in a previous life, re-arm once the dashboard is
    // reachable so the restart is not lost on OpenCode restart.
    void arm()
  })

  // Only ROOT sessions may arm/fire: subagent sessions share the slot and
  // their idle events fire mid-task (between parent steps).
  const childSessions = new Set<string>()

  return {
    event: async ({ event }) => {
      const props = (event as any).properties ?? {}
      if (event.type === "session.created" || event.type === "session.updated") {
        const info = props.info ?? {}
        if (info.id && info.parentID) childSessions.add(info.id)
        return
      }
      const sid: string | undefined = props.sessionID
      if (sid && childSessions.has(sid)) return
      if (event.type === "session.compacted") {
        await arm()
      } else if (event.type === "session.idle") {
        // Stay quiet until the post-compaction wait elapses, then fire at the
        // next root-session idle window (the least disruptive moment).
        const waitedEnough =
          state.armedAtMs > 0 && Date.now() - state.armedAtMs >= STALE_WAIT_MS
        if (waitedEnough) await maybeFire()
      }
    },
    // Hint for the post-compaction context: a restart may follow and a single
    // connection error is expected and self-heals.
    "experimental.session.compacting": async (_input, output) => {
      if (!dashboardSeen) return
      output.context.push(
        "Note: the llama.cpp backend may be restarted right after this compaction to recover decode speed. If one request fails with a connection error, retry it once.",
      )
    },
  }
}
