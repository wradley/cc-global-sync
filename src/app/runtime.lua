--- Compose coordinator runtime state and expose the warehouse-facing mutations
--- used by the main loop and UI.
local contracts = require("rednet_contracts")
local log = require("log")

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
local discoveryService = contracts.discovery_v1
local warehouseService = contracts.warehouse_v1

local function isDiscoveryVersionMismatch(err)
  local path = err and err.details and err.details.path or nil
  return path == "message.discovery_version"
end

local function coordinatorOwnerParams(state)
  return {
    coordinator_id = state.config.coordinator.id,
    coordinator_address = state.config.coordinator.id,
    claimed_at = os.epoch("utc"),
  }
end

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

---Receive and apply one discovery heartbeat from rednet.
---@param state CoordinatorState
---@return nil
function M.handleDiscoveryHeartbeat(state)
  local message, senderId, err = discoveryService.receive()
  if not message then
    if err and err.code ~= "timeout" then
      if isDiscoveryVersionMismatch(err) then
        log.warn("Rejected discovery heartbeat from sender=%s due to discovery version mismatch: %s", tostring(senderId), tostring(err.message))
      else
        log.warn("Ignored invalid discovery heartbeat from sender=%s: %s", tostring(senderId), tostring(err.message))
      end
    end
    return
  end

  if state.warehouse_registry:handleDiscoveryHeartbeat(senderId, message) then
    state.last_message_at = os.epoch("utc")
    state.state_dirty = true
  end
end

---Poll one accepted warehouse for its latest snapshot and active transfer status.
---@param state CoordinatorState
---@param warehouseId string
---@param activeTransferRequestId string|nil
---@return nil
function M.pollWarehouse(state, warehouseId, activeTransferRequestId)
  local warehouseState = state.warehouses[warehouseId]
  if not warehouseState or warehouseState.state ~= "accepted" or not warehouseState.sender_id then
    return
  end

  local snapshot, snapshotErr = warehouseService.getSnapshot(warehouseState.sender_id)
  if snapshot then
    state.warehouse_registry:observeSnapshot(warehouseState.sender_id, snapshot)
    state.last_message_at = os.epoch("utc")
    state.state_dirty = true
  else
    log.warn("Snapshot poll failed for warehouse=%s sender=%s: %s", warehouseId, tostring(warehouseState.sender_id), tostring(snapshotErr and snapshotErr.message))
  end

  if not activeTransferRequestId then
    return
  end

  local status, statusErr = warehouseService.getTransferRequestStatus(warehouseState.sender_id, {
    transfer_request_id = activeTransferRequestId,
  })
  if status then
    state.warehouse_registry:observeTransferRequestStatus(warehouseState.sender_id, status, state.execution_cycle)
    state.last_message_at = os.epoch("utc")
    state.state_dirty = true
    return
  end

  if statusErr and statusErr.code ~= "unknown_transfer_request" then
    log.warn(
      "Transfer status poll failed for warehouse=%s transfer_request=%s: %s",
      warehouseId,
      tostring(activeTransferRequestId),
      tostring(statusErr.message)
    )
  end
end

---Accept a pending warehouse into the active coordinator set.
---@param state CoordinatorState
---@param warehouseId string
---@return nil
function M.acceptWarehouse(state, warehouseId)
  local warehouseState = state.warehouses[warehouseId]
  if not warehouseState or warehouseState.state ~= "pending" or not warehouseState.sender_id then
    return
  end

  local ownerResult, ownerErr = warehouseService.getOwner(warehouseState.sender_id)
  if not ownerResult then
    log.warn("Failed to query owner for warehouse=%s: %s", warehouseId, tostring(ownerErr and ownerErr.message))
    return
  end

  warehouseState.warehouse_address = ownerResult.warehouse_address or warehouseState.warehouse_address
  state.warehouses[warehouseId] = warehouseState

  local desiredOwner = coordinatorOwnerParams(state)
  local owner = ownerResult.owner
  if owner ~= nil then
    if owner.coordinator_id ~= desiredOwner.coordinator_id or owner.coordinator_address ~= desiredOwner.coordinator_address then
      log.warn(
        "Refused to accept warehouse=%s because it is already owned by coordinator=%s",
        warehouseId,
        tostring(owner.coordinator_id)
      )
      return
    end
  else
    local setOwnerResult, setOwnerErr = warehouseService.setOwner(warehouseState.sender_id, desiredOwner)
    if not setOwnerResult then
      log.warn("Failed to claim warehouse=%s: %s", warehouseId, tostring(setOwnerErr and setOwnerErr.message))
      return
    end

    if not setOwnerResult.accepted then
      log.warn("Warehouse=%s did not accept coordinator ownership claim", warehouseId)
      return
    end
  end

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
