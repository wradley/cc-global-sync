---@class WarehouseRegistrySnapshotCapacity
---@field slot_capacity_used integer|nil
---@field slot_capacity_free integer|nil
---@field slot_capacity_total integer|nil
---@field storages_with_unknown_capacity integer|nil

---@class WarehouseRegistrySnapshot
---@field warehouse_id string|nil
---@field warehouse_address string|nil
---@field inventory table<string, integer>|nil
---@field capacity WarehouseRegistrySnapshotCapacity|nil
---@field observed_at number|nil
---@field sent_at number|nil

---@class WarehouseState
---@field state string
---@field sender_id integer|nil
---@field warehouse_id string|nil
---@field warehouse_address string|nil
---@field last_heartbeat_at number|nil
---@field last_snapshot_at number|nil
---@field snapshot WarehouseRegistrySnapshot|nil
---@field last_assignment_ack_at number|nil
---@field last_assignment_ack_batch_id string|nil
---@field last_assignment_ack table|nil
---@field last_assignment_execution_at number|nil
---@field last_assignment_execution_batch_id string|nil
---@field last_assignment_execution table|nil
---@field last_train_departure_at number|nil
---@field last_train_departure table|nil
---@field last_assignment_sent_at number|nil
---@field last_assignment_sent_batch_id string|nil
---@field last_assignment_count integer|nil
---@field last_assignment_item_count integer|nil

---@class WarehouseRegistryPlannableWarehouse
---@field warehouse_id string
---@field snapshot WarehouseRegistrySnapshot

---Coordinator warehouse registry state and warehouse-facing transitions.
---@class WarehouseRegistry
---@field config Config
---@field warehouses table<string, WarehouseState>
local WarehouseRegistry = {}
WarehouseRegistry.__index = WarehouseRegistry

---Normalize a warehouse record so required default state exists after load or first contact.
---@param warehouseState WarehouseState
---@return nil
function WarehouseRegistry.normalizeWarehouseState(warehouseState)
  if warehouseState.state == nil then
    warehouseState.state = "pending"
  end
end

---Create a registry bound to the coordinator config and warehouse state table.
---@param config Config
---@param warehouses? table<string, WarehouseState>
---@return WarehouseRegistry
function WarehouseRegistry:new(config, warehouses)
  local instance = {
    config = config,
    warehouses = warehouses or {},
  }

  return setmetatable(instance, self)
end

---Rebind the registry to a replacement warehouse table after persistence load.
---@param warehouses? table<string, WarehouseState>
---@return nil
function WarehouseRegistry:bind(warehouses)
  self.warehouses = warehouses or {}
end

---Return warehouse ids sorted for deterministic UI and dispatch order.
---@return string[]
function WarehouseRegistry:sortedIds()
  local ids = {}
  for warehouseId in pairs(self.warehouses) do
    ids[#ids + 1] = warehouseId
  end
  table.sort(ids)
  return ids
end

---Return a sorted array copy of the known warehouse ids.
---@return string[]
function WarehouseRegistry:listedIds()
  local ids = {}
  for _, warehouseId in ipairs(self:sortedIds()) do
    ids[#ids + 1] = warehouseId
  end
  return ids
end

---Return seconds since the warehouse's most recent heartbeat.
---@param warehouseState WarehouseState
---@return integer|nil
function WarehouseRegistry:heartbeatAgeSeconds(warehouseState)
  if not warehouseState.last_heartbeat_at then
    return nil
  end

  return math.floor((os.epoch("utc") - warehouseState.last_heartbeat_at) / 1000)
end

---Return seconds since the warehouse's most recent snapshot.
---@param warehouseState WarehouseState
---@return integer|nil
function WarehouseRegistry:snapshotAgeSeconds(warehouseState)
  if not warehouseState.last_snapshot_at then
    return nil
  end

  return math.floor((os.epoch("utc") - warehouseState.last_snapshot_at) / 1000)
end

---Return seconds since the warehouse last acknowledged an assignment batch.
---@param warehouseState WarehouseState
---@return integer|nil
function WarehouseRegistry:assignmentAckAgeSeconds(warehouseState)
  if not warehouseState.last_assignment_ack_at then
    return nil
  end

  return math.floor((os.epoch("utc") - warehouseState.last_assignment_ack_at) / 1000)
end

---Return seconds since the warehouse last reported assignment execution.
---@param warehouseState WarehouseState
---@return integer|nil
function WarehouseRegistry:assignmentExecutionAgeSeconds(warehouseState)
  if not warehouseState.last_assignment_execution_at then
    return nil
  end

  return math.floor((os.epoch("utc") - warehouseState.last_assignment_execution_at) / 1000)
end

---Return seconds since the warehouse last reported a train departure.
---@param warehouseState WarehouseState
---@return integer|nil
function WarehouseRegistry:trainDepartureAgeSeconds(warehouseState)
  if not warehouseState.last_train_departure_at then
    return nil
  end

  return math.floor((os.epoch("utc") - warehouseState.last_train_departure_at) / 1000)
end

---Report whether the warehouse is considered online by heartbeat freshness.
---@param warehouseState WarehouseState
---@return boolean
function WarehouseRegistry:isOnline(warehouseState)
  local age = self:heartbeatAgeSeconds(warehouseState)
  return age and age <= self.config.network.heartbeat_timeout_seconds or false
end

---Accept a pending warehouse into the active coordinator set.
---@param warehouseId string
---@return boolean changed True when the warehouse was transitioned to accepted.
function WarehouseRegistry:accept(warehouseId)
  local warehouseState = self.warehouses[warehouseId]
  if not warehouseState or warehouseState.state ~= "pending" then
    return false
  end

  warehouseState.state = "accepted"
  self.warehouses[warehouseId] = warehouseState
  return true
end

---Remove a warehouse back to pending and clear its latest accepted snapshot.
---@param warehouseId string
---@return boolean changed True when a warehouse record was updated.
function WarehouseRegistry:remove(warehouseId)
  local warehouseState = self.warehouses[warehouseId]
  if not warehouseState then
    return false
  end

  warehouseState.state = "pending"
  warehouseState.snapshot = nil
  warehouseState.last_snapshot_at = nil
  self.warehouses[warehouseId] = warehouseState
  return true
end

---Return accepted warehouses with valid capacity snapshots for planning.
---@return WarehouseRegistryPlannableWarehouse[]
function WarehouseRegistry:plannableWarehouses()
  local warehouses = {}

  for _, warehouseId in ipairs(self:sortedIds()) do
    local warehouseState = self.warehouses[warehouseId]
    local snapshot = warehouseState and warehouseState.snapshot
    local capacityTotal = snapshot and snapshot.capacity and snapshot.capacity.slot_capacity_total

    if warehouseState and warehouseState.state == "accepted" and snapshot and type(capacityTotal) == "number" and capacityTotal > 0 then
      warehouses[#warehouses + 1] = {
        warehouse_id = warehouseId,
        snapshot = snapshot,
      }
    end
  end

  return warehouses
end

local function applyHeartbeat(self, senderId, message, observedAt)
  local warehouseState = self.warehouses[message.warehouse_id] or {}
  WarehouseRegistry.normalizeWarehouseState(warehouseState)
  warehouseState.sender_id = senderId
  warehouseState.warehouse_id = message.warehouse_id
  warehouseState.warehouse_address = message.warehouse_address
  warehouseState.last_heartbeat_at = message.sent_at or observedAt
  self.warehouses[message.warehouse_id] = warehouseState
end

local function applySnapshot(self, senderId, message, observedAt)
  local warehouseState = self.warehouses[message.warehouse_id] or {}
  WarehouseRegistry.normalizeWarehouseState(warehouseState)
  warehouseState.sender_id = senderId
  warehouseState.warehouse_id = message.warehouse_id
  warehouseState.warehouse_address = message.warehouse_address
  warehouseState.last_snapshot_at = observedAt
  warehouseState.snapshot = message
  self.warehouses[message.warehouse_id] = warehouseState
end

local function applyAssignmentAck(self, senderId, message, observedAt)
  local warehouseState = self.warehouses[message.warehouse_id] or {}
  WarehouseRegistry.normalizeWarehouseState(warehouseState)
  warehouseState.sender_id = senderId
  warehouseState.warehouse_id = message.warehouse_id
  warehouseState.last_assignment_ack_at = observedAt
  warehouseState.last_assignment_ack_batch_id = message.batch_id
  warehouseState.last_assignment_ack = message
  self.warehouses[message.warehouse_id] = warehouseState
end

local function applyAssignmentExecution(self, senderId, message, cycle, observedAt)
  local warehouseState = self.warehouses[message.warehouse_id] or {}
  WarehouseRegistry.normalizeWarehouseState(warehouseState)
  warehouseState.sender_id = senderId
  warehouseState.warehouse_id = message.warehouse_id
  warehouseState.last_assignment_execution_at = observedAt
  warehouseState.last_assignment_execution_batch_id = message.batch_id
  warehouseState.last_assignment_execution = message
  self.warehouses[message.warehouse_id] = warehouseState

  if cycle then
    cycle:recordExecution(message.warehouse_id, message.batch_id, message.status, observedAt)
  end
end

local function applyTrainDeparture(self, senderId, message, cycle, observedAt)
  local warehouseState = self.warehouses[message.warehouse_id] or {}
  WarehouseRegistry.normalizeWarehouseState(warehouseState)
  warehouseState.sender_id = senderId
  warehouseState.warehouse_id = message.warehouse_id
  warehouseState.last_train_departure_at = observedAt
  warehouseState.last_train_departure = message
  self.warehouses[message.warehouse_id] = warehouseState

  if cycle then
    cycle:recordDeparture(message.warehouse_id, message.sent_at or observedAt, message.train_name)
  end
end

---Apply a coordinator-network message to warehouse state and the active cycle when relevant.
---@param senderId integer
---@param message table
---@param protocol string
---@param cycle? Cycle
---@return boolean handled True when the message matched the configured protocol and known message types.
function WarehouseRegistry:handleMessage(senderId, message, protocol, cycle)
  if protocol ~= self.config.network.protocol or type(message) ~= "table" then
    return false
  end

  local observedAt = os.epoch("utc")
  if message.type == "heartbeat" then
    applyHeartbeat(self, senderId, message, observedAt)
    return true
  end

  if message.type == "snapshot" then
    applySnapshot(self, senderId, message, observedAt)
    return true
  end

  if message.type == "assignment_ack" then
    applyAssignmentAck(self, senderId, message, observedAt)
    return true
  end

  if message.type == "assignment_execution" then
    applyAssignmentExecution(self, senderId, message, cycle, observedAt)
    return true
  end

  if message.type == "train_departure_notice" then
    applyTrainDeparture(self, senderId, message, cycle, observedAt)
    return true
  end

  return false
end

---Request fresh snapshots from all accepted warehouses with a known sender id.
---@return nil
function WarehouseRegistry:pollSnapshots()
  for _, warehouseId in ipairs(self:sortedIds()) do
    local warehouseState = self.warehouses[warehouseId]
    if warehouseState and warehouseState.state == "accepted" and warehouseState.sender_id then
      rednet.send(warehouseState.sender_id, { type = "get_snapshot" }, self.config.network.protocol)
    end
  end
end

return WarehouseRegistry
