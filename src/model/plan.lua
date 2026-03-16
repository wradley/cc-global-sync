---@class PlanItemCount
---@field name string Item identifier.
---@field count integer Item count.

---@class PlanWarehouseInput
---@field warehouse_id string
---@field snapshot { inventory: table<string, integer>|nil, capacity: { slot_capacity_total: integer|nil }|nil }

---@class PlanWarehouse
---@field warehouse_id string
---@field capacity_total integer
---@field current_inventory table<string, integer>
---@field target_inventory table<string, integer>
---@field diffs table<string, integer>
---@field deficit_items PlanItemCount[]
---@field surplus_items PlanItemCount[]
---@field planned_receive_count integer
---@field planned_send_count integer
---@field target_share_percent number|nil

---Coordinator distribution plan state and plan-building logic.
---@class Plan
---@field warehouses table<string, PlanWarehouse> Planned state keyed by warehouse id.
---@field total_capacity integer Sum of accepted warehouse capacities used for planning.
---@field total_item_types integer Number of distinct item ids seen across accepted warehouses.
local Plan = {}
Plan.__index = Plan

local function countTableKeys(value)
  local count = 0
  for _ in pairs(value) do
    count = count + 1
  end
  return count
end

---Create a plan object from persisted data or fresh defaults.
---@param data? Plan
---@return Plan
function Plan:new(data)
  local instance = data or {
    warehouses = {},
    total_capacity = 0,
    total_item_types = 0,
  }

  if type(instance.warehouses) ~= "table" then
    instance.warehouses = {}
  end

  return setmetatable(instance, self)
end

---Build a fresh plan from the current accepted warehouse snapshots.
---@param warehouses? PlanWarehouseInput[] Accepted warehouse snapshots eligible for planning.
---@return Plan
function Plan.fromWarehouseSnapshots(warehouses)
  local plan = Plan:new()
  local globalTotals = {}

  for _, warehouse in ipairs(warehouses or {}) do
    local warehouseId = warehouse.warehouse_id
    local snapshot = warehouse.snapshot
    local capacityTotal = snapshot and snapshot.capacity and snapshot.capacity.slot_capacity_total

    if snapshot and type(capacityTotal) == "number" and capacityTotal > 0 then
      plan.total_capacity = plan.total_capacity + capacityTotal
      plan.warehouses[warehouseId] = {
        warehouse_id = warehouseId,
        capacity_total = capacityTotal,
        current_inventory = snapshot.inventory or {},
        target_inventory = {},
        diffs = {},
        deficit_items = {},
        surplus_items = {},
        planned_receive_count = 0,
        planned_send_count = 0,
      }

      for itemName, count in pairs(snapshot.inventory or {}) do
        globalTotals[itemName] = (globalTotals[itemName] or 0) + count
      end
    end
  end

  plan.total_item_types = countTableKeys(globalTotals)

  if plan.total_capacity <= 0 then
    return plan
  end

  for _, warehouse in ipairs(warehouses or {}) do
    local warehouseId = warehouse.warehouse_id
    local warehousePlan = plan.warehouses[warehouseId]
    if warehousePlan then
      warehousePlan.target_share_percent = math.floor((warehousePlan.capacity_total / plan.total_capacity) * 1000 + 0.5) / 10
    end
  end

  for itemName, globalCount in pairs(globalTotals) do
    local allocations = {}
    local allocated = 0

    for _, warehouse in ipairs(warehouses or {}) do
      local warehouseId = warehouse.warehouse_id
      local warehousePlan = plan.warehouses[warehouseId]
      if warehousePlan then
        local exact = (globalCount * warehousePlan.capacity_total) / plan.total_capacity
        local base = math.floor(exact)
        allocations[#allocations + 1] = {
          warehouse_id = warehouseId,
          target = base,
          remainder = exact - base,
        }
        allocated = allocated + base
      end
    end

    table.sort(allocations, function(left, right)
      if left.remainder == right.remainder then
        return left.warehouse_id < right.warehouse_id
      end
      return left.remainder > right.remainder
    end)

    local remaining = globalCount - allocated
    local allocationIndex = 1
    while remaining > 0 do
      allocations[allocationIndex].target = allocations[allocationIndex].target + 1
      remaining = remaining - 1
      allocationIndex = allocationIndex + 1
      if allocationIndex > #allocations then
        allocationIndex = 1
      end
    end

    for _, allocation in ipairs(allocations) do
      local warehousePlan = plan.warehouses[allocation.warehouse_id]
      local current = warehousePlan.current_inventory[itemName] or 0
      local diff = allocation.target - current
      warehousePlan.target_inventory[itemName] = allocation.target
      if diff ~= 0 then
        warehousePlan.diffs[itemName] = diff
        if diff > 0 then
          warehousePlan.planned_receive_count = warehousePlan.planned_receive_count + diff
          warehousePlan.deficit_items[#warehousePlan.deficit_items + 1] = {
            name = itemName,
            count = diff,
          }
        else
          warehousePlan.planned_send_count = warehousePlan.planned_send_count + (-diff)
          warehousePlan.surplus_items[#warehousePlan.surplus_items + 1] = {
            name = itemName,
            count = -diff,
          }
        end
      end
    end
  end

  for _, warehouse in ipairs(warehouses or {}) do
    local warehousePlan = plan.warehouses[warehouse.warehouse_id]
    if warehousePlan then
      table.sort(warehousePlan.deficit_items, function(left, right)
        if left.count == right.count then
          return left.name < right.name
        end
        return left.count > right.count
      end)
      table.sort(warehousePlan.surplus_items, function(left, right)
        if left.count == right.count then
          return left.name < right.name
        end
        return left.count > right.count
      end)
    end
  end

  return plan
end

return Plan
