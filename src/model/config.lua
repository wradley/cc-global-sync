---@class ConfigCoordinator
---@field id string Stable coordinator identity included in outbound messages.
---@field display_name string Human-readable name shown in the coordinator UI.

---@class ConfigNetwork
---@field ender_modem string Ender modem side used for warehouse-to-coordinator messaging.
---@field protocol string Rednet protocol name shared by coordinator and warehouse controllers.
---@field heartbeat_timeout_seconds number How long a warehouse can go without a heartbeat before it is considered stale.

---@class ConfigTiming
---@field display_refresh_seconds number How often to redraw the coordinator terminal UI.
---@field snapshot_poll_seconds number How often to ask warehouses for fresh snapshots of their inventory.
---@field plan_refresh_seconds number How often to rebuild the global plan and transfer queue.
---@field sync_interval_seconds number How often the coordinator may release a new sync wave when not paused.
---@field persist_seconds number How often coordinator state is persisted to disk when dirty.

---@class ConfigExecution
---@field departures_required_per_warehouse number Train departures required after execution before a warehouse counts as cycle-complete.

---@class ConfigLoggingOutput
---@field file string
---@field level '"info"'|'"warn"'|'"error"'|'"panic"'|string
---@field mirror_to_term boolean
---@field timestamp '"utc"'|'"epoch"'|string

---@class ConfigLoggingRetention
---@field mode '"none"'|'"truncate"'|string
---@field max_lines integer

---@class ConfigLogging
---@field output ConfigLoggingOutput
---@field retention ConfigLoggingRetention

---Coordinator runtime configuration and validation.
---@class Config
---@field version integer
---@field coordinator ConfigCoordinator
---@field network ConfigNetwork
---@field timing ConfigTiming
---@field execution ConfigExecution
---@field logging ConfigLogging
local Config = {}

local function validatePositiveNumber(value, name)
  if type(value) ~= "number" or value <= 0 then
    error(name .. " must be a positive number", 0)
  end
end

local function validateOneOf(value, name, allowed)
  if type(value) ~= "string" or not allowed[value] then
    error(name .. " is invalid: " .. tostring(value), 0)
  end
end

local function loadConfigModule(path)
  if type(path) ~= "string" or path == "" then
    error("config path is required", 0)
  end

  if not fs.exists(path) then
    error("missing config file: " .. path, 0)
  end

  return dofile(path)
end

---Build the default coordinator config table.
---@return Config
function Config.default()
  return {
    version = 1,
    coordinator = {
      id = "central",
      display_name = "Central Coordinator",
    },
    network = {
      ender_modem = "top",
      protocol = "warehouse_sync_v1",
      heartbeat_timeout_seconds = 30,
    },
    timing = {
      display_refresh_seconds = 1,
      snapshot_poll_seconds = 10,
      plan_refresh_seconds = 10,
      sync_interval_seconds = 10 * 60,
      persist_seconds = 5,
    },
    execution = {
      departures_required_per_warehouse = 2,
    },
    logging = {
      output = {
        file = "/var/inventory-coordinator/coordinator.log",
        level = "info",
        mirror_to_term = false,
        timestamp = "utc",
      },
      retention = {
        mode = "truncate",
        max_lines = 2000,
      },
    },
  }
end

---Validate and normalize a deserialized config table.
---@param data? table
---@return Config
function Config.fromDeserialized(data)
  local config = data or Config.default()

  if type(config) ~= "table" then
    error("config module must return a table", 0)
  end

  if config.version ~= 1 then
    error("unsupported config version: " .. tostring(config.version), 0)
  end

  if type(config.coordinator) ~= "table" then
    error("config.coordinator is required", 0)
  end

  if type(config.network) ~= "table" then
    error("config.network is required", 0)
  end

  if type(config.timing) ~= "table" then
    error("config.timing is required", 0)
  end

  if type(config.execution) ~= "table" then
    error("config.execution is required", 0)
  end
  if config.logging == nil then
    config.logging = {}
  end
  if type(config.logging) ~= "table" then
    error("config.logging must be a table when provided", 0)
  end
  if config.logging.output == nil then
    config.logging.output = {}
  end
  if type(config.logging.output) ~= "table" then
    error("config.logging.output must be a table when provided", 0)
  end
  if config.logging.retention == nil then
    config.logging.retention = {}
  end
  if type(config.logging.retention) ~= "table" then
    error("config.logging.retention must be a table when provided", 0)
  end

  if type(config.coordinator.id) ~= "string" or config.coordinator.id == "" then
    error("config.coordinator.id is required", 0)
  end

  if type(config.coordinator.display_name) ~= "string" or config.coordinator.display_name == "" then
    error("config.coordinator.display_name is required", 0)
  end

  if type(config.network.ender_modem) ~= "string" or config.network.ender_modem == "" then
    error("config.network.ender_modem is required", 0)
  end

  if type(config.network.protocol) ~= "string" or config.network.protocol == "" then
    error("config.network.protocol is required", 0)
  end

  if config.logging.output.file == nil then
    config.logging.output.file = "/var/inventory-coordinator/coordinator.log"
  end
  if config.logging.output.level == nil then
    config.logging.output.level = "info"
  end
  if config.logging.output.mirror_to_term == nil then
    config.logging.output.mirror_to_term = false
  end
  if config.logging.output.timestamp == nil then
    config.logging.output.timestamp = "utc"
  end
  if config.logging.retention.mode == nil then
    config.logging.retention.mode = "truncate"
  end
  if config.logging.retention.max_lines == nil then
    config.logging.retention.max_lines = 2000
  end

  validatePositiveNumber(config.network.heartbeat_timeout_seconds, "config.network.heartbeat_timeout_seconds")
  validatePositiveNumber(config.timing.display_refresh_seconds, "config.timing.display_refresh_seconds")
  validatePositiveNumber(config.timing.snapshot_poll_seconds, "config.timing.snapshot_poll_seconds")
  validatePositiveNumber(config.timing.plan_refresh_seconds, "config.timing.plan_refresh_seconds")
  validatePositiveNumber(config.timing.sync_interval_seconds, "config.timing.sync_interval_seconds")
  validatePositiveNumber(config.timing.persist_seconds, "config.timing.persist_seconds")

  if type(config.execution.departures_required_per_warehouse) ~= "number"
    or config.execution.departures_required_per_warehouse < 0 then
    error("config.execution.departures_required_per_warehouse must be a non-negative number", 0)
  end

  if type(config.logging.output.file) ~= "string" or config.logging.output.file == "" then
    error("config.logging.output.file must be a non-empty string", 0)
  end
  if type(config.logging.output.mirror_to_term) ~= "boolean" then
    error("config.logging.output.mirror_to_term must be a boolean", 0)
  end
  validateOneOf(config.logging.output.level, "config.logging.output.level", {
    info = true,
    warn = true,
    error = true,
    panic = true,
  })
  validateOneOf(config.logging.output.timestamp, "config.logging.output.timestamp", {
    utc = true,
    epoch = true,
  })
  validateOneOf(config.logging.retention.mode, "config.logging.retention.mode", {
    none = true,
    truncate = true,
  })
  if type(config.logging.retention.max_lines) ~= "number" or config.logging.retention.max_lines < 1 then
    error("config.logging.retention.max_lines must be a positive number", 0)
  end
  config.logging.retention.max_lines = math.floor(config.logging.retention.max_lines)

  return config
end

---Load coordinator config from a Lua file path or fall back to defaults.
---@param path string
---@return Config
function Config.load(path)
  return Config.fromDeserialized(loadConfigModule(path))
end

return Config
