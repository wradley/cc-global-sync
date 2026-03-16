local lu = require("deps.luaunit")
local Config = require("model.config")

local M = {}

function M.testDefaultConfigValidates()
  local config = Config.fromDeserialized(Config.default())

  lu.assertEquals(config.version, 1)
  lu.assertEquals(config.coordinator.id, "central")
  lu.assertEquals(config.logging.output.file, "/var/inventory-coordinator/coordinator.log")
end

function M.testMissingLoggingUsesDefaults()
  local config = Config.default()
  config.logging = nil

  local normalized = Config.fromDeserialized(config)

  lu.assertEquals(normalized.logging.output.level, "info")
  lu.assertEquals(normalized.logging.retention.mode, "truncate")
end

return M
