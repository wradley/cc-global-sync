local coordinatorView = require("ui.coordinator_view")
local detailView = require("ui.detail_view")
local summaryView = require("ui.summary_view")

---Terminal UI rendering and input handling for the coordinator computer.
---@class CoordinatorUi
local M = {}

---Draw the summary/overview screen.
---@param state CoordinatorState
---@return nil
function M.drawSummaryView(state)
  summaryView.draw(state)
end

---Draw the detail screen for one warehouse, or fall back to summary if missing.
---@param state CoordinatorState
---@param warehouseId string
---@return nil
function M.drawWarehouseDetailView(state, warehouseId)
  if detailView.draw(state, warehouseId) then
    return
  end

  state.ui.view = "summary"
  state.ui.selected_warehouse_id = nil
  state.ui.warehouse_page = "overview"
  M.drawSummaryView(state)
end

---Draw the currently selected UI view.
---@param state CoordinatorState
---@return nil
function M.draw(state)
  if state.ui.view == "warehouse" and state.ui.selected_warehouse_id then
    M.drawWarehouseDetailView(state, state.ui.selected_warehouse_id)
    return
  end

  if state.ui.view == "health" or state.ui.view == "config" then
    coordinatorView.draw(state)
    return
  end

  M.drawSummaryView(state)
end

local function openWarehouse(state, warehouseId)
  state.ui.view = "warehouse"
  state.ui.selected_warehouse_id = warehouseId
  state.ui.warehouse_page = "overview"
end

local function handleGlobalNavigation(state, char)
  if char == "m" then
    state.ui.view = "summary"
    state.ui.selected_warehouse_id = nil
    state.ui.warehouse_page = "overview"
    return true
  end

  if char == "h" then
    state.ui.view = "health"
    state.ui.selected_warehouse_id = nil
    state.ui.warehouse_page = "overview"
    return true
  end

  if char == "g" then
    state.ui.view = "config"
    state.ui.selected_warehouse_id = nil
    state.ui.warehouse_page = "overview"
    return true
  end

  local index = tonumber(char)
  if index then
    local warehouseIds = state.warehouse_registry:listedIds()
    local warehouseId = warehouseIds[index]
    if warehouseId then
      openWarehouse(state, warehouseId)
      return true
    end
  end

  return false
end

---Handle one character of operator input.
---@param state CoordinatorState
---@param stateLib WarehouseRuntime
---@param char string
---@return nil
function M.handleInput(state, stateLib, char)
  if handleGlobalNavigation(state, char) then
    return
  end

  if state.ui.view == "summary" then
    if char == "x" then
      if not state.execution_cycle.active then
        state.ui.release_requested = "manual"
      end
      return
    end

    if char == "p" then
      if state.schedule.paused then
        state.schedule:resumeFromNow(os.epoch("utc"))
      else
        state.schedule:pause()
      end
      state.state_dirty = true
      return
    end

    if char == "c" then
      state.execution_cycle:clear()
      state.state_dirty = true
      state.ui.release_requested = nil
      return
    end
    return
  end

  if state.ui.view == "warehouse" then
    local warehouseId = state.ui.selected_warehouse_id
    if char == "b" then
      state.ui.view = "summary"
      state.ui.selected_warehouse_id = nil
      state.ui.warehouse_page = "overview"
    elseif char == "o" then
      state.ui.warehouse_page = "overview"
    elseif char == "e" then
      state.ui.warehouse_page = "execution"
    elseif char == "n" then
      state.ui.warehouse_page = "network"
    elseif char == "a" then
      stateLib.acceptWarehouse(state, warehouseId)
    elseif char == "r" then
      stateLib.removeWarehouse(state, warehouseId)
      state.ui.view = "summary"
      state.ui.selected_warehouse_id = nil
      state.ui.warehouse_page = "overview"
    end
  end
end

return M
