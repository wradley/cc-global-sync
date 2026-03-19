local lu = require("deps.luaunit")
local ccEnv = require("support.cc_test_env")
local Config = require("model.config")

local M = {}

local function resetModule(name)
  if package and package.loaded then
    package.loaded[name] = nil
  end
end

local function resetRednetContracts()
  resetModule("rednet_contracts")
  resetModule("rednet_contracts.init")
  resetModule("rednet_contracts.errors")
  resetModule("rednet_contracts.schema_validation")
  resetModule("rednet_contracts.mrpc_v1")
  resetModule("rednet_contracts.services.warehouse_v1")
end

function M:setUp()
  ccEnv.install({ epoch = 5000 })
  resetRednetContracts()
  resetModule("app.runtime")
end

function M:tearDown()
  ccEnv.restore()
end

function M:testAcceptWarehouseClaimsUnownedWarehouse()
  local runtime = require("app.runtime")
  local config = Config.default()
  local state = runtime.new(config)
  state.warehouses.alpha = {
    state = "pending",
    sender_id = 17,
    warehouse_id = "alpha",
  }

  ccEnv.queueRednetReceive(17, {
    type = "response",
    protocol = {
      name = "warehouse",
      version = 1,
    },
    request_id = "req-5000-1",
    ok = true,
    result = {
      warehouse_id = "alpha",
      warehouse_address = "A1",
      owner = nil,
      observed_at = 4990,
    },
    sent_at = 4990,
  }, "rc.mrpc_v1")
  ccEnv.queueRednetReceive(17, {
    type = "response",
    protocol = {
      name = "warehouse",
      version = 1,
    },
    request_id = "req-5000-2",
    ok = true,
    result = {
      warehouse_id = "alpha",
      warehouse_address = "A1",
      accepted = true,
      owner = {
        coordinator_id = "central",
        coordinator_address = "central",
        claimed_at = 5000,
      },
      sent_at = 5000,
    },
    sent_at = 5000,
  }, "rc.mrpc_v1")

  runtime.acceptWarehouse(state, "alpha")

  local sent = ccEnv.getSentMessages()
  lu.assertEquals(state.warehouses.alpha.state, "accepted")
  lu.assertEquals(state.warehouses.alpha.warehouse_address, "A1")
  lu.assertTrue(state.state_dirty)
  lu.assertEquals(#sent, 2)
  lu.assertEquals(sent[1].message.method, "get_owner")
  lu.assertEquals(sent[2].message.method, "set_owner")
end

return M
