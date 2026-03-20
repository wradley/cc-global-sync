local lu = require("deps.luaunit")
local ccEnv = require("support.cc_test_env")
local Config = require("model.config")
local WarehouseRegistry = require("model.warehouse_registry")
local contracts = require("rednet_contracts")

local M = {}

function M:setUp()
  ccEnv.install({ epoch = 5000 })
end

function M:tearDown()
  ccEnv.restore()
end

function M:testHandleDiscoveryHeartbeat()
  local config = Config.default()
  local registry = WarehouseRegistry:new(config, {})

  lu.assertTrue(registry:handleDiscoveryHeartbeat(17, {
    type = "device_discovery_heartbeat",
    discovery_version = 1,
    device_id = "alpha",
    device_type = "warehouse_controller",
    sent_at = 4900,
    protocols = {
      {
        name = contracts.warehouse_v1.NAME,
        version = contracts.warehouse_v1.VERSION,
        role = "server",
      },
    },
  }))

  lu.assertTrue(registry:accept("alpha"))

  lu.assertEquals(registry.warehouses.alpha.sender_id, 17)
  lu.assertEquals(registry.warehouses.alpha.state, "accepted")
  lu.assertEquals(registry.warehouses.alpha.last_heartbeat_at, 4900)
end

function M:testObserveSnapshotAckAndTransferStatus()
  local config = Config.default()
  local registry = WarehouseRegistry:new(config, {})
  local cycleCalls = {}
  local cycle = {
    recordExecution = function(_, warehouseId, transferRequestId, status, reportedAt)
      cycleCalls[#cycleCalls + 1] = {
        warehouse_id = warehouseId,
        transfer_request_id = transferRequestId,
        status = status,
        reported_at = reportedAt,
      }
    end,
  }

  registry:observeSnapshot(17, {
    warehouse_id = "alpha",
    warehouse_address = "A1",
    observed_at = 4900,
    inventory = {
      ["minecraft:stone"] = 4,
    },
    capacity = {
      slot_capacity_total = 10,
      slot_capacity_used = 2,
    },
  })
  registry:observeAssignmentAck(17, {
    warehouse_id = "alpha",
    warehouse_address = "A1",
    transfer_request_id = "tr-1",
    assignment_count = 1,
    item_count = 4,
    accepted = true,
    sent_at = 4910,
  })
  registry:observeTransferRequestStatus(17, {
    warehouse_id = "alpha",
    warehouse_address = "A1",
    transfer_request_id = "tr-1",
    status = "queued",
    executed_at = 4920,
    total_assignments = 1,
    total_items_requested = 4,
    total_items_queued = 4,
    assignments = {},
    packages = {
      ["in"] = {},
      ["out"] = {
        "123-1-1",
      },
    },
    sent_at = 4925,
  }, cycle)

  lu.assertEquals(registry.warehouses.alpha.snapshot.inventory["minecraft:stone"], 4)
  lu.assertEquals(registry.warehouses.alpha.last_assignment_ack_batch_id, "tr-1")
  lu.assertEquals(registry.warehouses.alpha.last_assignment_execution_batch_id, "tr-1")
  lu.assertEquals(registry.warehouses.alpha.last_assignment_execution.batch_id, "tr-1")
  lu.assertEquals(registry.warehouses.alpha.last_assignment_execution.packages["out"][1], "123-1-1")
  lu.assertEquals(#cycleCalls, 1)
  lu.assertEquals(cycleCalls[1].transfer_request_id, "tr-1")
end

return M
