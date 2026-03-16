---Terminal formatting helpers shared across coordinator UI screens.
---@class UiFmt
local M = {}

---Compute used capacity as a whole-number percentage for one snapshot.
---@param snapshot WarehouseRegistrySnapshot
---@return integer|nil
function M.usedCapacityPercent(snapshot)
  local total = snapshot.capacity.slot_capacity_total
  local used = snapshot.capacity.slot_capacity_used

  if type(total) ~= "number" or total <= 0 or type(used) ~= "number" then
    return nil
  end

  return math.floor((used / total) * 100 + 0.5)
end

---Format elapsed seconds for compact terminal display.
---@param seconds integer|nil
---@return string
function M.formatElapsed(seconds)
  if seconds == nil then
    return "unknown"
  end
  if seconds < 60 then
    return tostring(seconds) .. "s"
  end

  local minutes = math.floor(seconds / 60)
  if minutes < 60 then
    return tostring(minutes) .. "m"
  end

  local hours = math.floor(minutes / 60)
  return tostring(hours) .. "h"
end

---Format an epoch-millisecond timestamp as a compact age for UI output.
---@param timestamp number|nil
---@return string
function M.ageFromEpoch(timestamp)
  if timestamp == nil then
    return "never"
  end

  return M.formatElapsed(math.floor((os.epoch("utc") - timestamp) / 1000)) .. " ago"
end

---Shorten a namespaced item id for terminal display.
---@param itemName any
---@return string
function M.shortItemName(itemName)
  if type(itemName) ~= "string" then
    return tostring(itemName)
  end

  local _, endIndex = string.find(itemName, ":", 1, true)
  if endIndex then
    return string.sub(itemName, endIndex + 1)
  end

  return itemName
end

---Return the human-readable warehouse state label shown in the UI.
---@param state CoordinatorState
---@param warehouseState WarehouseState
---@return string
function M.stateLabel(state, warehouseState)
  if warehouseState.state == "accepted" then
    if state.warehouse_registry:isOnline(warehouseState) then
      return "online"
    end
    return "stale " .. M.formatElapsed(state.warehouse_registry:heartbeatAgeSeconds(warehouseState))
  end

  return warehouseState.state or "pending"
end

---Return the coarse warehouse state bucket used for color selection.
---@param state CoordinatorState
---@param warehouseState WarehouseState
---@return string
local function stateKind(state, warehouseState)
  if warehouseState.state == "accepted" then
    if state.warehouse_registry:isOnline(warehouseState) then
      return "online"
    end
    return "stale"
  end

  return warehouseState.state or "pending"
end

---Return the terminal color for a warehouse status when colors are available.
---@param state CoordinatorState
---@param warehouseState WarehouseState
---@return integer|nil
function M.statusColor(state, warehouseState)
  if not term.isColor or not term.isColor() then
    return nil
  end

  local status = stateKind(state, warehouseState)
  if status == "online" then
    return colors.green
  end
  if status == "pending" then
    return colors.yellow
  end
  if status == "stale" then
    return colors.orange
  end

  return colors.white
end

---Format slot-capacity details for the warehouse detail view.
---@param snapshot WarehouseRegistrySnapshot|nil
---@return string
function M.formatSlotsLine(snapshot)
  if not snapshot or not snapshot.capacity then
    return "Slots: unknown"
  end

  local capacity = snapshot.capacity
  local used = capacity.slot_capacity_used or 0
  local total = capacity.slot_capacity_total
  local free = capacity.slot_capacity_free
  local usedPercent = M.usedCapacityPercent(snapshot)

  if type(total) ~= "number" then
    return "Slots: " .. tostring(used) .. "/? used"
  end

  return tostring(used) .. "/" .. tostring(total) .. " used, "
    .. tostring(free or "?") .. " free"
    .. (usedPercent and (" (" .. tostring(usedPercent) .. "%)") or "")
end

return M
