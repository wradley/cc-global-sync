local fmt = require("ui.fmt")
local summaryModel = require("ui.summary_model")

---Coordinator top-level health/config pages.
---@class CoordinatorView
local M = {}

local function writeLine(text)
  print(text)
end

local function drawHealth(state)
  local summary = summaryModel.buildGlobalSummary(state)
  local cycle = state.execution_cycle
  local schedule = state.schedule
  local now = os.epoch("utc")
  local remainingSeconds = schedule and schedule:remainingSeconds(now)

  term.clear()
  term.setCursorPos(1, 1)
  writeLine("Coordinator Health")
  writeLine(state.config.coordinator.display_name or state.config.coordinator.id)
  writeLine("")
  writeLine("Known: " .. tostring(summary.known_warehouses))
  writeLine("Accepted: " .. tostring(summary.accepted_warehouses))
  writeLine("Online: " .. tostring(summary.online_warehouses))
  writeLine("Pending: " .. tostring(summary.pending_warehouses))
  writeLine("Last msg: " .. fmt.ageFromEpoch(state.last_message_at))
  writeLine("Last plan: " .. fmt.ageFromEpoch(state.last_plan_refresh_at))
  if schedule and schedule.paused then
    writeLine("Schedule: paused")
  else
    writeLine("Next sync: " .. (remainingSeconds and fmt.formatElapsed(remainingSeconds) or "unknown"))
  end
  if cycle and cycle.active then
    writeLine("Cycle: active")
    writeLine("Progress: " .. tostring(cycle.completed_warehouses or 0) .. "/" .. tostring(cycle.total_warehouses or 0))
    writeLine("Released: " .. fmt.ageFromEpoch(cycle.released_at))
    writeLine("Done at: " .. fmt.ageFromEpoch(cycle.completed_at))
  else
    writeLine("Cycle: idle")
    writeLine("Last done: " .. fmt.ageFromEpoch(cycle and cycle.completed_at or nil))
  end
  writeLine("")
  writeLine("[m] main  [g] config")
end

local function drawConfig(state)
  local config = state.config
  local timing = config.timing or {}

  term.clear()
  term.setCursorPos(1, 1)
  writeLine("Coordinator Config")
  writeLine(config.coordinator.display_name or config.coordinator.id)
  writeLine("")
  writeLine("Id: " .. tostring(config.coordinator.id))
  writeLine("Protocol: " .. tostring(config.network.protocol))
  writeLine("Ender: " .. tostring(config.network.ender_modem))
  writeLine("Heartbeat: " .. tostring(config.network.heartbeat_timeout_seconds) .. "s")
  writeLine("Poll: " .. tostring(timing.snapshot_poll_seconds) .. "s")
  writeLine("Plan: " .. tostring(timing.plan_refresh_seconds) .. "s")
  writeLine("Sync: " .. tostring(timing.sync_interval_seconds) .. "s")
  writeLine("Persist: " .. tostring(timing.persist_seconds) .. "s")
  writeLine("Depart req: " .. tostring(config.execution.departures_required_per_warehouse))
  writeLine("Log: " .. tostring(config.logging.output.level))
  writeLine("")
  writeLine("[m] main  [h] health")
end

function M.draw(state)
  if state.ui.view == "config" then
    drawConfig(state)
    return
  end

  drawHealth(state)
end

return M
