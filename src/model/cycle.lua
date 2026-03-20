---@class CycleWarehouseEntry
---@field batch_id string|nil Deterministic batch identifier sent to this warehouse for the active wave.
---@field completed boolean True once execution and required package evidence have both been observed.
---@field execution_reported boolean True after the warehouse reports assignment execution for the active batch.
---@field execution_reported_at number|nil Epoch milliseconds when execution was reported.
---@field reported_items_queued integer Items the warehouse reported as queued for the active batch.
---@field package_ids_in string[] Package ids observed inbound for the active batch.
---@field package_ids_out string[] Package ids observed outbound for the active batch.
---@field unmatched_outgoing integer Count of outbound package ids not yet seen inbound anywhere in the cycle.
---@field total_assignments integer Number of outbound assignments included for this warehouse.
---@field total_items integer Number of outbound items included for this warehouse.
---@field status any Last reported execution status payload for this warehouse.

---@class CycleReleasedAssignmentItem
---@field name string
---@field count integer
---@field transfer_id string

---@class CycleReleasedAssignment
---@field assignment_id string
---@field source string
---@field destination string
---@field reason string
---@field status string
---@field items CycleReleasedAssignmentItem[]
---@field total_items integer
---@field line_count integer

---@class CycleReleasedSourceEntry
---@field source string
---@field assignments CycleReleasedAssignment[]
---@field total_items integer
---@field total_assignments integer

---Coordinator execution cycle state and transitions for one released wave.
---@class Cycle
---@field active boolean Whether a released wave is currently blocking the next release.
---@field released_at number|nil Epoch milliseconds when the current wave was opened.
---@field plan_refreshed_at number|nil Epoch milliseconds of the plan snapshot frozen into this wave.
---@field completed_warehouses integer Count of participating warehouses that have completed this wave.
---@field total_warehouses integer Count of participating warehouses in this wave.
---@field warehouses table<string, CycleWarehouseEntry> Per-warehouse progress keyed by warehouse id.
---@field released_queue TransferQueue|nil Deep-copied queue snapshot frozen at release time.
---@field completed_at number|nil Epoch milliseconds when the wave became complete.
local Cycle = {}
Cycle.__index = Cycle

local function deepCopy(value)
  if type(value) ~= "table" then
    return value
  end

  return textutils.unserialize(textutils.serialize(value))
end

local function copyArray(values)
  local copied = {}
  for index, value in ipairs(values or {}) do
    copied[index] = value
  end
  return copied
end

---Create a cycle object from persisted data or fresh defaults.
---@param data? Cycle
---@return Cycle
function Cycle:new(data)
  local instance = data or {
    active = false,
    released_at = nil,
    plan_refreshed_at = nil,
    completed_warehouses = 0,
    total_warehouses = 0,
    warehouses = {},
    released_queue = nil,
    completed_at = nil,
  }

  if type(instance.warehouses) ~= "table" then
    instance.warehouses = {}
  end

  return setmetatable(instance, self)
end

---Recompute per-warehouse and overall cycle completion from current entries.
---@param now number Epoch milliseconds
---@return nil
function Cycle:refreshProgress(now)
  if not self.warehouses then
    return
  end

  local inboundPackages = {}
  for _, entry in pairs(self.warehouses) do
    for _, packageId in ipairs(entry.package_ids_in or {}) do
      inboundPackages[packageId] = true
    end
  end

  local completed = 0
  local total = 0
  for _, entry in pairs(self.warehouses) do
    total = total + 1
    local unmatched = 0
    for _, packageId in ipairs(entry.package_ids_out or {}) do
      if not inboundPackages[packageId] then
        unmatched = unmatched + 1
      end
    end
    entry.unmatched_outgoing = unmatched

    if not entry.execution_reported then
      entry.completed = false
    elseif (entry.total_items or 0) <= 0 then
      entry.completed = true
    elseif (entry.reported_items_queued or 0) <= 0 then
      entry.completed = false
    elseif #(entry.package_ids_out or {}) == 0 then
      entry.completed = false
    else
      entry.completed = unmatched == 0
    end

    if entry.completed then
      completed = completed + 1
    end
  end

  self.total_warehouses = total
  self.completed_warehouses = completed
  if total > 0 and completed >= total then
    self.completed_at = now or os.epoch("utc")
    self.active = false
  end
end

---Freeze the current queue so later planning refreshes do not mutate this wave.
---@param state CoordinatorState Coordinator runtime fields used to initialize the wave.
---@param queue? TransferQueue Latest transfer queue to freeze into the cycle.
---@param warehouseRegistry WarehouseRegistry
---@return boolean started True when a new cycle was opened.
function Cycle:begin(state, queue, warehouseRegistry)
  if self.active then
    return false
  end

  local releasedAt = os.epoch("utc")
  self.active = true
  self.released_at = releasedAt
  self.plan_refreshed_at = state.last_plan_refresh_at
  self.completed_warehouses = 0
  self.total_warehouses = 0
  self.warehouses = {}
  self.released_queue = deepCopy(queue or {})
  self.completed_at = nil

  for _, warehouseId in ipairs(warehouseRegistry:sortedIds()) do
    local warehouseState = warehouseRegistry.warehouses[warehouseId]
    if warehouseState and warehouseState.state == "accepted" and warehouseState.sender_id and warehouseRegistry:isOnline(warehouseState) then
      local sourceEntry = queue and queue.assignments_by_source and queue.assignments_by_source[warehouseId] or nil
      local hasOutboundWork = sourceEntry ~= nil and (sourceEntry.total_assignments or 0) > 0
      self.warehouses[warehouseId] = {
        batch_id = nil,
        completed = false,
        -- Empty outbound waves may be de-duplicated at dispatch time, so they
        -- cannot rely on a fresh execution echo to unblock the cycle.
        execution_reported = not hasOutboundWork,
        execution_reported_at = hasOutboundWork and nil or releasedAt,
        reported_items_queued = 0,
        package_ids_in = {},
        package_ids_out = {},
        unmatched_outgoing = 0,
        total_assignments = sourceEntry and sourceEntry.total_assignments or 0,
        total_items = sourceEntry and sourceEntry.total_items or 0,
      }
      self.total_warehouses = self.total_warehouses + 1
    end
  end

  if self.total_warehouses == 0 then
    self:clear()
    return false
  end

  self:refreshProgress(releasedAt)
  return true
end

---Reset the cycle back to an inactive state.
---@return nil
function Cycle:clear()
  self.active = false
  self.released_at = nil
  self.plan_refreshed_at = nil
  self.completed_warehouses = 0
  self.total_warehouses = 0
  self.warehouses = {}
  self.released_queue = nil
  self.completed_at = nil
end

---Record the dispatched batch identifier for one warehouse in the active cycle.
---@param warehouseId string Unique identifier for warehouse.
---@param batchId string Deterministic batch identifier.
---@return nil
function Cycle:markBatchSent(warehouseId, batchId)
  if not self.active then
    return
  end

  local entry = self.warehouses and self.warehouses[warehouseId]
  if not entry then
    return
  end

  entry.batch_id = batchId
  self.warehouses[warehouseId] = entry
end

---Record an execution report from a warehouse for a released batch and refresh completion.
---@param warehouseId string Unique warehouse identifier.
---@param batchId string Deterministic batch identifier.
---@param status any
---@param reportedAt number Epoch milliseconds.
---@return nil
function Cycle:recordExecution(warehouseId, batchId, status, reportedAt)
  if not self.active then
    return
  end

  local entry = self.warehouses and self.warehouses[warehouseId]
  if not entry or entry.batch_id ~= batchId then
    return
  end

  local statusText = status
  local queuedItems = entry.reported_items_queued or 0
  local packagesIn = entry.package_ids_in or {}
  local packagesOut = entry.package_ids_out or {}
  if type(status) == "table" then
    statusText = status.status
    queuedItems = status.total_items_queued or 0
    packagesIn = copyArray(status.packages and status.packages["in"] or {})
    packagesOut = copyArray(status.packages and status.packages["out"] or {})
  end

  entry.execution_reported = true
  entry.status = statusText
  entry.execution_reported_at = reportedAt or os.epoch("utc")
  entry.reported_items_queued = queuedItems
  entry.package_ids_in = packagesIn
  entry.package_ids_out = packagesOut
  self.warehouses[warehouseId] = entry
  self:refreshProgress(reportedAt)
end

return Cycle
