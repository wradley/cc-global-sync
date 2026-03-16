---Planning, release, and assignment dispatch helpers for the coordinator.
---@class ReleaseService
local M = {}
local log = require("deps.log")
local persistence = require("infra.persistence")
local Plan = require("model.plan")
local TransferQueue = require("model.transfer_queue")

---Recompute and persist the latest plan and transfer queue.
---@param state CoordinatorState
---@return nil
function M.refreshPlan(state)
  state.latest_plan = Plan.fromWarehouseSnapshots(state.warehouse_registry:plannableWarehouses())
  state.latest_transfer_queue = TransferQueue.fromPlan(state.latest_plan)
  state.last_plan_refresh_at = os.epoch("utc")
  persistence.savePlan(state.latest_plan, state.last_plan_refresh_at)
  persistence.saveTransferQueue(state.latest_transfer_queue, state.last_plan_refresh_at)
end

---Build a deterministic batch id from one source warehouse assignment payload.
---@param warehouseId string
---@param sourceEntry TransferQueueSourceEntry
---@return string
local function assignmentBatchId(warehouseId, sourceEntry)
  local serialized = textutils.serialize({
    warehouse_id = warehouseId,
    total_assignments = sourceEntry.total_assignments or 0,
    total_items = sourceEntry.total_items or 0,
    assignments = sourceEntry.assignments or {},
  })

  local checksum = 0
  for index = 1, #serialized do
    checksum = (checksum + (string.byte(serialized, index) * index)) % 2147483647
  end

  return string.format("%s:%d:%d:%d", warehouseId, sourceEntry.total_assignments or 0, sourceEntry.total_items or 0, checksum)
end

---Return the currently active released batch id for one warehouse, if any.
---@param state CoordinatorState
---@param warehouseId string
---@return string|nil
function M.currentBatchIdForWarehouse(state, warehouseId)
  local cycle = state.execution_cycle
  if not cycle or not cycle.active then
    return nil
  end

  local queue = cycle.released_queue
  if type(queue) ~= "table" or type(queue.assignments_by_source) ~= "table" then
    return nil
  end

  local sourceEntry = queue.assignments_by_source[warehouseId]
  if type(sourceEntry) ~= "table" then
    return nil
  end

  return assignmentBatchId(warehouseId, sourceEntry)
end

---Report whether a warehouse should receive this assignment batch now.
---@param state CoordinatorState
---@param warehouseState WarehouseState
---@param batchId string
---@param dispatchedAt number
---@return boolean
local function shouldDispatchAssignment(state, warehouseState, batchId, dispatchedAt)
  local cycle = state.execution_cycle
  if not cycle or not cycle.active then
    return false
  end

  if warehouseState.last_assignment_execution_batch_id == batchId then
    return false
  end

  if warehouseState.last_assignment_sent_batch_id ~= batchId then
    return true
  end

  if not warehouseState.last_assignment_sent_at then
    return true
  end

  local ageSeconds = math.floor((dispatchedAt - warehouseState.last_assignment_sent_at) / 1000)
  return ageSeconds >= state.config.network.heartbeat_timeout_seconds
end

---Dispatch the active cycle's assignment batches to eligible warehouses.
---@param state CoordinatorState
---@param warehouseRuntime WarehouseRuntime
---@return nil
function M.dispatchAssignments(state, warehouseRuntime)
  local cycle = state.execution_cycle
  if not cycle or not cycle.active then
    log.warn("Dispatch skipped because no active cycle exists")
    return
  end

  local queue = cycle.released_queue
  if type(queue) ~= "table" then
    log.warn("Dispatch skipped because the active cycle has no released queue")
    return
  end

  local dispatchedAt = os.epoch("utc")
  local dispatchedWarehouses = 0
  for _, warehouseId in ipairs(state.warehouse_registry:sortedIds()) do
    local warehouseState = state.warehouses[warehouseId]
    if warehouseState and warehouseState.state == "accepted" and warehouseState.sender_id and state.warehouse_registry:isOnline(warehouseState) then
      local sourceEntry = queue.assignments_by_source[warehouseId] or {
        assignments = {},
        total_assignments = 0,
        total_items = 0,
      }
      local assignments = {}
      for _, assignment in ipairs(sourceEntry.assignments or {}) do
        local destinationState = state.warehouses[assignment.destination]
        assignments[#assignments + 1] = {
          assignment_id = assignment.assignment_id,
          source = assignment.source,
          destination = assignment.destination,
          destination_address = destinationState and destinationState.warehouse_address or nil,
          reason = assignment.reason,
          status = assignment.status,
          items = assignment.items,
          total_items = assignment.total_items,
          line_count = assignment.line_count,
        }
      end

      local batchId = assignmentBatchId(warehouseId, sourceEntry)
      if shouldDispatchAssignment(state, warehouseState, batchId, dispatchedAt) then
        rednet.send(warehouseState.sender_id, {
          type = "assignment_batch",
          protocol_version = 1,
          coordinator_id = state.config.coordinator.id,
          warehouse_id = warehouseId,
          batch_id = batchId,
          plan_refreshed_at = cycle.plan_refreshed_at,
          sent_at = dispatchedAt,
          assignments = assignments,
          total_assignments = sourceEntry.total_assignments or 0,
          total_items = sourceEntry.total_items or 0,
        }, state.config.network.protocol)

        warehouseState.last_assignment_sent_at = dispatchedAt
        warehouseState.last_assignment_sent_batch_id = batchId
        warehouseState.last_assignment_count = sourceEntry.total_assignments or 0
        warehouseState.last_assignment_item_count = sourceEntry.total_items or 0
        state.warehouses[warehouseId] = warehouseState
        warehouseRuntime.markCycleBatchSent(state, warehouseId, batchId)
        state.state_dirty = true
        dispatchedWarehouses = dispatchedWarehouses + 1
      end
    end
  end

  log.info("Dispatched assignments for %d warehouse(s)", dispatchedWarehouses)
end

---Open a new execution cycle for the current plan and dispatch assignments.
---@param state CoordinatorState
---@param warehouseRuntime WarehouseRuntime
---@param kind string
---@return boolean released
function M.releaseCurrentPlan(state, warehouseRuntime, kind)
  if not warehouseRuntime.beginExecutionCycle(state, state.latest_transfer_queue) then
    log.warn("Release skipped for kind=%s because no eligible cycle could be opened", kind)
    return false
  end

  state.schedule:recordRelease(kind, os.epoch("utc"))
  state.state_dirty = true
  log.info("Opened %s release cycle", kind)
  M.dispatchAssignments(state, warehouseRuntime)
  return true
end

return M
