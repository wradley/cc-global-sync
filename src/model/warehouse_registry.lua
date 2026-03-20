local function supportsWarehouseService(message)
  for _, protocol in ipairs(message.protocols or {}) do
    if protocol.name == "warehouse" and protocol.version == 1 and protocol.role == "server" then
      return true
    end
  end

  return false
end

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
  local warehouseState = self.warehouses[message.device_id] or {}
  WarehouseRegistry.normalizeWarehouseState(warehouseState)
  warehouseState.sender_id = senderId
  warehouseState.warehouse_id = message.device_id
  warehouseState.last_heartbeat_at = message.sent_at or observedAt
  self.warehouses[message.device_id] = warehouseState
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
  warehouseState.warehouse_address = message.warehouse_address
  warehouseState.last_assignment_ack_at = observedAt
  warehouseState.last_assignment_ack_batch_id = message.transfer_request_id
  warehouseState.last_assignment_ack = {
    warehouse_id = message.warehouse_id,
    warehouse_address = message.warehouse_address,
    batch_id = message.transfer_request_id,
    transfer_request_id = message.transfer_request_id,
    assignment_count = message.assignment_count,
    item_count = message.item_count,
    accepted = message.accepted,
    sent_at = message.sent_at,
  }
  self.warehouses[message.warehouse_id] = warehouseState
end

local function applyAssignmentExecution(self, senderId, message, cycle, observedAt)
  local warehouseState = self.warehouses[message.warehouse_id] or {}
  WarehouseRegistry.normalizeWarehouseState(warehouseState)
  warehouseState.sender_id = senderId
  warehouseState.warehouse_id = message.warehouse_id
  warehouseState.warehouse_address = message.warehouse_address
  warehouseState.last_assignment_execution_at = observedAt
  warehouseState.last_assignment_execution_batch_id = message.transfer_request_id
  warehouseState.last_assignment_execution = {
    warehouse_id = message.warehouse_id,
    warehouse_address = message.warehouse_address,
    batch_id = message.transfer_request_id,
    transfer_request_id = message.transfer_request_id,
    status = message.status,
    executed_at = message.executed_at,
    total_assignments = message.total_assignments,
    total_items_requested = message.total_items_requested,
    total_items_queued = message.total_items_queued,
    assignments = message.assignments,
    packages = message.packages,
    sent_at = message.sent_at,
  }
  self.warehouses[message.warehouse_id] = warehouseState

  if cycle then
    cycle:recordExecution(message.warehouse_id, message.transfer_request_id, message, observedAt)
  end
end

---Apply a discovery heartbeat to warehouse state.
---@param senderId integer
---@param message table
---@return boolean handled
function WarehouseRegistry:handleDiscoveryHeartbeat(senderId, message)
  if type(message) ~= "table" then
    return false
  end

  if message.type ~= "device_discovery_heartbeat" or message.device_type ~= "warehouse_controller" then
    return false
  end

  if not supportsWarehouseService(message) then
    return false
  end

  applyHeartbeat(self, senderId, message, os.epoch("utc"))
  return true
end

---Apply one snapshot result returned from `warehouse_v1.get_snapshot()`.
---@param senderId integer
---@param message WarehouseRegistrySnapshot
---@return nil
function WarehouseRegistry:observeSnapshot(senderId, message)
  applySnapshot(self, senderId, message, os.epoch("utc"))
end

---Apply one ack result returned from `warehouse_v1.assign_transfer_request()`.
---@param senderId integer
---@param message table
---@return nil
function WarehouseRegistry:observeAssignmentAck(senderId, message)
  applyAssignmentAck(self, senderId, message, os.epoch("utc"))
end

---Apply one transfer status result returned from `warehouse_v1.get_transfer_request_status()`.
---@param senderId integer
---@param message table
---@param cycle? Cycle
---@return nil
function WarehouseRegistry:observeTransferRequestStatus(senderId, message, cycle)
  applyAssignmentExecution(self, senderId, message, cycle, os.epoch("utc"))
end

return WarehouseRegistry
