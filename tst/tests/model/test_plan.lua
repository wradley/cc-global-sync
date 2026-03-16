local lu = require("deps.luaunit")
local Plan = require("model.plan")

local M = {}

function M.testBuildsCapacityWeightedTargets()
  local plan = Plan.fromWarehouseSnapshots({
    {
      warehouse_id = "alpha",
      snapshot = {
        inventory = {
          ["minecraft:stone"] = 10,
        },
        capacity = {
          slot_capacity_total = 10,
        },
      },
    },
    {
      warehouse_id = "beta",
      snapshot = {
        inventory = {},
        capacity = {
          slot_capacity_total = 30,
        },
      },
    },
  })

  lu.assertEquals(plan.total_capacity, 40)
  lu.assertEquals(plan.total_item_types, 1)
  lu.assertEquals(plan.warehouses.alpha.target_inventory["minecraft:stone"], 3)
  lu.assertEquals(plan.warehouses.beta.target_inventory["minecraft:stone"], 7)
  lu.assertEquals(plan.warehouses.alpha.planned_send_count, 7)
  lu.assertEquals(plan.warehouses.beta.planned_receive_count, 7)
end

return M
