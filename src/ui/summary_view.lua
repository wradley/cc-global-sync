local fmt = require("ui.fmt")
local summaryModel = require("ui.summary_model")

---Summary screen renderer for the coordinator UI.
---@class SummaryView
local M = {}

---Write one warehouse summary line in the overview list.
---@param state CoordinatorState
---@param warehouseId string
---@param warehouseState WarehouseState
---@param globalUsedTotal integer|nil
---@return nil
local function writeStatusLine(state, warehouseId, warehouseState)
  local snapshot = warehouseState.snapshot
  local statusText = fmt.stateLabel(state, warehouseState)
  local snapshotAge = state.warehouse_registry:snapshotAgeSeconds(warehouseState)
  local capacityText = "no snap"

  if snapshot then
    local usedPercent = fmt.usedCapacityPercent(snapshot)
    if usedPercent ~= nil then
      capacityText = tostring(usedPercent) .. "% used"
    else
      capacityText = "cap ?"
    end
  end

  term.write(warehouseId .. " [")

  local originalColor
  local color = fmt.statusColor(state, warehouseState)
  if color and term.getTextColor then
    originalColor = term.getTextColor()
    term.setTextColor(color)
  end

  term.write(statusText)

  if originalColor and term.setTextColor then
    term.setTextColor(originalColor)
  end

  term.write("] ")
  term.write(capacityText)
  if snapshotAge ~= nil then
    term.write(" snap " .. fmt.formatElapsed(snapshotAge))
  end

  local _, y = term.getCursorPos()
  term.setCursorPos(1, y + 1)
end

---Draw the summary/overview screen.
---@param state CoordinatorState
---@return nil
function M.draw(state)
  local summary = summaryModel.buildGlobalSummary(state)
  local cycle = state.execution_cycle
  local schedule = state.schedule
  local now = os.epoch("utc")
  local remainingSeconds = schedule and schedule:remainingSeconds(now)

  term.clear()
  term.setCursorPos(1, 1)
  print(state.config.coordinator.display_name or state.config.coordinator.id)
  print("Warehouses " .. tostring(summary.online_warehouses) .. "/" .. tostring(summary.accepted_warehouses)
    .. " online, " .. tostring(summary.pending_warehouses) .. " pending")
  print("Items " .. tostring(summary.global_item_types)
    .. "  Used " .. (summary.global_used_percent and (tostring(summary.global_used_percent) .. "%") or "?"))
  if schedule and schedule.paused then
    print("Schedule: paused")
  else
    print("Next sync: " .. (remainingSeconds and fmt.formatElapsed(remainingSeconds) or "unknown"))
  end
  if cycle and cycle.active then
    print("Cycle: active " .. tostring(cycle.completed_warehouses or 0) .. "/" .. tostring(cycle.total_warehouses or 0))
  else
    print("Cycle: idle")
  end
  print("Last msg: " .. fmt.ageFromEpoch(state.last_message_at))
  print("Last plan: " .. fmt.ageFromEpoch(state.last_plan_refresh_at))
  print("[x] sync [p] pause [c] clear")
  print("[h] health [g] config [1-9] wh")
  print("")

  local warehouseIds = state.warehouse_registry:listedIds()
  local lineCount = 9
  for index, warehouseId in ipairs(warehouseIds) do
    if lineCount > 18 or index > 9 then
      break
    end

    local warehouseState = state.warehouses[warehouseId]
    term.write(tostring(index) .. ". ")
    writeStatusLine(state, warehouseId, warehouseState)
    lineCount = lineCount + 1
  end
end

return M
