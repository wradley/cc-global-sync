local lu = require("deps.luaunit")
local ccEnv = require("support.cc_test_env")
local Config = require("model.config")
local WarehouseRegistry = require("model.warehouse_registry")

local M = {}

function M:setUp()
  ccEnv.install({ epoch = 5000 })
end

function M:tearDown()
  ccEnv.restore()
end

function M:testHandleHeartbeatAndPollSnapshots()
  local config = Config.default()
  local registry = WarehouseRegistry:new(config, {})

  lu.assertTrue(registry:handleMessage(17, {
    type = "heartbeat",
    warehouse_id = "alpha",
    warehouse_address = "A1",
    sent_at = 4900,
  }, config.network.protocol))

  lu.assertTrue(registry:accept("alpha"))
  registry:pollSnapshots()

  local sent = ccEnv.getSentMessages()
  lu.assertEquals(registry.warehouses.alpha.sender_id, 17)
  lu.assertEquals(registry.warehouses.alpha.state, "accepted")
  lu.assertEquals(#sent, 1)
  lu.assertEquals(sent[1].target_id, 17)
  lu.assertEquals(sent[1].message.type, "get_snapshot")
end

return M
