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

function M:testCompletesAfterExecutionAndMatchingPackageEvidence()
  local config = Config.default()
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
  cycle:recordExecution("alpha", "batch-1", {
    status = "queued",
    total_items_queued = 3,
    packages = {
      ["in"] = {},
      ["out"] = {
        "123-1-1",
      },
    },
  }, 1100)
  lu.assertTrue(cycle.active)

  cycle.warehouses.beta = {
    batch_id = nil,
    completed = true,
    execution_reported = true,
    execution_reported_at = 1000,
    reported_items_queued = 0,
    package_ids_in = {
      "123-1-1",
    },
    package_ids_out = {},
    unmatched_outgoing = 0,
    total_assignments = 0,
    total_items = 0,
  }
  cycle:refreshProgress(1200)

  lu.assertFalse(cycle.active)
  lu.assertEquals(cycle.completed_warehouses, 2)
  lu.assertEquals(cycle.warehouses.alpha.unmatched_outgoing, 0)
end

return M
