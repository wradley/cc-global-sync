---@class GlobalWarehouseSummary
---@field known_warehouses integer
---@field online_warehouses integer
---@field accepted_warehouses integer
---@field pending_warehouses integer
---@field global_item_types integer
---@field global_slot_capacity_used integer
---@field global_slot_capacity_total integer
---@field global_used_percent integer|nil

---Read-only warehouse summary helpers used by the coordinator UI.
---@class SummaryModel
local M = {}

local function countTableKeys(value)
  local count = 0
  for _ in pairs(value) do
    count = count + 1
  end
  return count
end

---Build a global summary view model from the current coordinator state.
---@param state CoordinatorState
---@return GlobalWarehouseSummary
function M.buildGlobalSummary(state)
  local warehouseIds = state.warehouse_registry:listedIds()
  local onlineWarehouses = 0
  local acceptedWarehouses = 0
  local pendingWarehouses = 0
  local inventoryTotals = {}
  local slotCapacityUsed = 0
  local slotCapacityTotal = 0
  local capacityKnownForAll = true

  for _, warehouseId in ipairs(warehouseIds) do
    local warehouseState = state.warehouses[warehouseId]
    if warehouseState.state == "accepted" then
      acceptedWarehouses = acceptedWarehouses + 1
    elseif warehouseState.state == "pending" then
      pendingWarehouses = pendingWarehouses + 1
    end

    if warehouseState.state == "accepted" and state.warehouse_registry:isOnline(warehouseState) then
      onlineWarehouses = onlineWarehouses + 1
    end

    if warehouseState.state == "accepted" then
      local snapshot = warehouseState.snapshot
      if snapshot then
        slotCapacityUsed = slotCapacityUsed + (snapshot.capacity.slot_capacity_used or 0)

        if type(snapshot.capacity.slot_capacity_total) == "number" and (snapshot.capacity.storages_with_unknown_capacity or 0) == 0 then
          slotCapacityTotal = slotCapacityTotal + snapshot.capacity.slot_capacity_total
        else
          capacityKnownForAll = false
        end

        for itemName, count in pairs(snapshot.inventory or {}) do
          inventoryTotals[itemName] = (inventoryTotals[itemName] or 0) + count
        end
      else
        capacityKnownForAll = false
      end
    end
  end

  local usedPercent
  if capacityKnownForAll and slotCapacityTotal > 0 then
    usedPercent = math.floor((slotCapacityUsed / slotCapacityTotal) * 100 + 0.5)
  end

  return {
    known_warehouses = #warehouseIds,
    online_warehouses = onlineWarehouses,
    accepted_warehouses = acceptedWarehouses,
    pending_warehouses = pendingWarehouses,
    global_item_types = countTableKeys(inventoryTotals),
    global_slot_capacity_used = slotCapacityUsed,
    global_slot_capacity_total = slotCapacityTotal,
    global_used_percent = usedPercent,
  }
end

return M
