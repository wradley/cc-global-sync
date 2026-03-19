local M = {
  VERSION = "0.1.0",
}

M.errors = require("rednet_contracts.errors")
M.discovery_v1 = require("rednet_contracts.discovery_v1")
M.warehouse_v1 = require("rednet_contracts.services.warehouse_v1")
M.global_inventory_v1 = require("rednet_contracts.services.global_inventory_v1")

return M
