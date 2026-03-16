--- Compose coordinator runtime state and expose the warehouse-facing mutations
--- used by the main loop and UI.
---@class CoordinatorUiState
---@field view '"summary"'|'"warehouse"'|'"health"'|'"config"'|string
---@field selected_warehouse_id string|nil
---@field warehouse_page '"overview"'|'"execution"'|'"network"'|string
---@field release_requested '"manual"'|nil|string

---Top-level coordinator runtime state shared across loops, persistence, and UI.
---@class CoordinatorState
---@field config Config
---@field warehouses table<string, WarehouseState>
---@field warehouse_registry WarehouseRegistry
---@field state_dirty boolean
---@field last_message_at number|nil
---@field last_plan_refresh_at number|nil
---@field latest_plan Plan|nil
---@field latest_transfer_queue TransferQueue|nil
---@field execution_cycle Cycle
---@field schedule Schedule
---@field ui CoordinatorUiState

---Coordinator runtime composition and warehouse-facing mutations.
---@class WarehouseRuntime
local M = {}
local Cycle = require("model.cycle")
local Schedule = require("model.schedule")
local WarehouseRegistry = require("model.warehouse_registry")

---Normalize a restored warehouse record before it is bound into the registry.
---@param warehouseState WarehouseState
---@return nil
function M.ensureWarehouseState(warehouseState)
  WarehouseRegistry.normalizeWarehouseState(warehouseState)
end

---Create the top-level runtime state for the coordinator process.
---@param config Config
---@return CoordinatorState
function M.new(config)
  local warehouses = {}

  return {
    config = config,
    warehouses = warehouses,
    warehouse_registry = WarehouseRegistry:new(config, warehouses),
    state_dirty = false,
    last_message_at = nil,
    execution_cycle = Cycle:new(),
    schedule = Schedule:new(config),
    ui = {
      view = "summary",
      selected_warehouse_id = nil,
      warehouse_page = "overview",
      release_requested = nil,
    },
  }
end

---Apply one inbound warehouse message to the registry and cycle state.
---@param state CoordinatorState
---@param senderId integer
---@param message table
---@param protocol string
---@return nil
function M.handleMessage(state, senderId, message, protocol)
  if state.warehouse_registry:handleMessage(senderId, message, protocol, state.execution_cycle) then
    state.last_message_at = os.epoch("utc")
    state.state_dirty = true
  end
end

---Ask accepted warehouses for fresh snapshots over rednet.
---@param state CoordinatorState
---@return nil
function M.pollSnapshots(state)
  state.warehouse_registry:pollSnapshots()
end

---Accept a pending warehouse into the active coordinator set.
---@param state CoordinatorState
---@param warehouseId string
---@return nil
function M.acceptWarehouse(state, warehouseId)
  if state.warehouse_registry:accept(warehouseId) then
    state.state_dirty = true
  end
end

---Remove a warehouse back to pending and clear its accepted snapshot state.
---@param state CoordinatorState
---@param warehouseId string
---@return nil
function M.removeWarehouse(state, warehouseId)
  if state.warehouse_registry:remove(warehouseId) then
    state.state_dirty = true
  end
end

-- Freeze the current queue as the executable wave so ongoing planning refreshes
-- cannot mutate the work for an active cycle.
---@param state CoordinatorState
---@param queue? TransferQueue
---@return boolean started
function M.beginExecutionCycle(state, queue)
  if not state.execution_cycle:begin(state, queue, state.warehouse_registry) then
    return false
  end

  state.state_dirty = true
  return true
end

---Record which batch identifier was sent for one warehouse in the active cycle.
---@param state CoordinatorState
---@param warehouseId string
---@param batchId string
---@return nil
function M.markCycleBatchSent(state, warehouseId, batchId)
  local cycle = state.execution_cycle
  if not cycle then
    return
  end

  cycle:markBatchSent(warehouseId, batchId)
  state.state_dirty = true
end

return M
