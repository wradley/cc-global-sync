---@class CycleWarehouseEntry
---@field batch_id string|nil Deterministic batch identifier sent to this warehouse for the active wave.
---@field completed boolean True once execution and required departures have both been observed.
---@field execution_reported boolean True after the warehouse reports assignment execution for the active batch.
---@field execution_reported_at number|nil Epoch milliseconds when execution was reported.
---@field departures_seen integer Qualifying departures observed after execution.
---@field required_departures integer Departures required before this warehouse counts as complete.
---@field last_train_name string|nil Most recent qualifying train name seen for this warehouse.
---@field last_departure_at number|nil Epoch milliseconds of the most recent qualifying departure.
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

  local completed = 0
  local total = 0
  for _, entry in pairs(self.warehouses) do
    total = total + 1
    local requiredDepartures = entry.required_departures or 0
    local departuresSeen = entry.departures_seen or 0
    entry.completed = entry.execution_reported and departuresSeen >= requiredDepartures
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
        departures_seen = 0,
        required_departures = hasOutboundWork and state.config.execution.departures_required_per_warehouse or 0,
        last_train_name = nil,
        last_departure_at = nil,
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

  entry.execution_reported = true
  entry.status = status
  entry.execution_reported_at = reportedAt or os.epoch("utc")
  self.warehouses[warehouseId] = entry
  self:refreshProgress(reportedAt)
end

---Count a qualifying train departure from a warehouse after execution and refresh completion.
---@param warehouseId string Unique warehouse identifier.
---@param departureAt? number Epoch milliseconds when the departure was observed.
---@param trainName? string Human-readable train name.
---@return nil
function Cycle:recordDeparture(warehouseId, departureAt, trainName)
  if not self.active then
    return
  end

  local entry = self.warehouses and self.warehouses[warehouseId]
  if not entry or not entry.execution_reported then
    return
  end

  local effectiveDepartureAt = departureAt or os.epoch("utc")
  local executionAt = entry.execution_reported_at or 0
  if effectiveDepartureAt < executionAt then
    return
  end

  local required = entry.required_departures or 0
  local current = entry.departures_seen or 0
  if current >= required then
    return
  end

  entry.departures_seen = current + 1
  entry.last_train_name = trainName
  entry.last_departure_at = effectiveDepartureAt
  self.warehouses[warehouseId] = entry
  self:refreshProgress(effectiveDepartureAt)
end

return Cycle
