local lu = require("deps.luaunit")
local ccEnv = require("support.cc_test_env")
local Config = require("model.config")
local Cycle = require("model.cycle")
local WarehouseRegistry = require("model.warehouse_registry")

local M = {}

function M:setUp()
  ccEnv.install({ epoch = 1000 })
end

function M:tearDown()
  ccEnv.restore()
end

function M:testCompletesAfterExecutionAndDeparture()
  local config = Config.default()
  config.execution.departures_required_per_warehouse = 1
  local registry = WarehouseRegistry:new(config, {
    alpha = {
      state = "accepted",
      sender_id = 42,
      last_heartbeat_at = 1000,
    },
  })
  local cycle = Cycle:new()
  local queue = {
    assignments_by_source = {
      alpha = {
        source = "alpha",
        assignments = {
          {
            assignment_id = "assign-1",
            source = "alpha",
            destination = "beta",
            reason = "rebalance",
            status = "planned",
            items = {
              { name = "minecraft:stone", count = 3, transfer_id = "xfer-1" },
            },
            total_items = 3,
            line_count = 1,
          },
        },
        total_items = 3,
        total_assignments = 1,
      },
    },
  }

  lu.assertTrue(cycle:begin({
    last_plan_refresh_at = 999,
    config = config,
  }, queue, registry))

  cycle:markBatchSent("alpha", "batch-1")
  cycle:recordExecution("alpha", "batch-1", "ok", 1100)
  lu.assertTrue(cycle.active)

  cycle:recordDeparture("alpha", 1200, "Train A")

  lu.assertFalse(cycle.active)
  lu.assertEquals(cycle.completed_warehouses, 1)
  lu.assertEquals(cycle.warehouses.alpha.last_train_name, "Train A")
end

return M
