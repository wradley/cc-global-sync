local errors = require("rednet_contracts.errors")
local schema = require("rednet_contracts.schema_validation")

---@class DiscoveryHeartbeatProtocol
---@field name string
---@field version integer
---@field role "client"|"server"

---@class DiscoveryHeartbeatFields
---@field device_id string
---@field device_type string
---@field sent_at integer
---@field protocols DiscoveryHeartbeatProtocol[]
---@field interval_seconds number|nil

---@class DiscoveryHeartbeat: DiscoveryHeartbeatFields
---@field type "device_discovery_heartbeat"
---@field discovery_version integer

---@class DiscoveryReceiveOptions
---@field rednet_protocol string|nil
---@field timeout number|nil

local M = {
  HEARTBEAT_TYPE = "device_discovery_heartbeat",
  REDNET_PROTOCOL = "rc.discovery_v1",
  DISCOVERY_VERSION = 1,
}

local function copyTable(source)
  local target = {}

  for key, value in pairs(source or {}) do
    target[key] = value
  end

  return target
end

local function validateProtocolEntry(entry, path)
  local ok, err = schema.requireTable(entry, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(entry.name, path .. ".name")
  if not ok then
    return false, err
  end

  ok, err = schema.requirePositiveInteger(entry.version, path .. ".version")
  if not ok then
    return false, err
  end

  ok, err = schema.requireOneOf(entry.role, path .. ".role", {
    client = true,
    server = true,
  })
  if not ok then
    return false, err
  end

  return true
end

local function validateHeartbeat(message)
  local ok, err = schema.requireTable(message, "message")
  if not ok then
    return false, err
  end

  if message.type ~= M.HEARTBEAT_TYPE then
    return schema.fail("invalid_value", "message.type", "message.type must be " .. M.HEARTBEAT_TYPE)
  end

  ok, err = schema.requirePositiveInteger(message.discovery_version, "message.discovery_version")
  if not ok then
    return false, err
  end

  if message.discovery_version ~= M.DISCOVERY_VERSION then
    return schema.fail("invalid_value", "message.discovery_version", "message.discovery_version did not match discovery_v1")
  end

  ok, err = schema.requireString(message.device_id, "message.device_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(message.device_type, "message.device_type")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(message.sent_at, "message.sent_at")
  if not ok then
    return false, err
  end

  ok, err = schema.requireArrayItems(message.protocols, "message.protocols", validateProtocolEntry)
  if not ok then
    return false, err
  end

  ok, err = schema.optionalNumber(message.interval_seconds, "message.interval_seconds")
  if not ok then
    return false, err
  end

  return true
end

local function buildHeartbeat(fields)
  local message = copyTable(fields)
  message.type = M.HEARTBEAT_TYPE
  message.discovery_version = M.DISCOVERY_VERSION

  local ok, err = validateHeartbeat(message)
  if not ok then
    errors.raise(err, 1)
  end

  return message
end

---Broadcast a validated discovery heartbeat over rednet.
---@param fields DiscoveryHeartbeatFields
---@param opts DiscoveryReceiveOptions|nil
---@return DiscoveryHeartbeat
function M.broadcast(fields, opts)
  local message = buildHeartbeat(fields)
  local protocol = opts and opts.rednet_protocol or M.REDNET_PROTOCOL

  rednet.broadcast(message, protocol)

  return message
end

---Receive and validate one discovery heartbeat from rednet.
---@param opts DiscoveryReceiveOptions|nil
---@return DiscoveryHeartbeat|nil, integer|nil, table|nil
---When `err` is `nil`, `senderId` is guaranteed to be a rednet id.
function M.receive(opts)
  local protocol = opts and opts.rednet_protocol or M.REDNET_PROTOCOL
  local timeout = opts and opts.timeout or nil
  local senderId, message = rednet.receive(protocol, timeout)

  if senderId == nil then
    return nil, nil, errors.new("timeout", "timed out waiting for discovery heartbeat", {
      path = "rednet.receive",
    })
  end

  local ok, err = validateHeartbeat(message)
  if not ok then
    return nil, senderId, err
  end

  ---@cast message DiscoveryHeartbeat
  return message, senderId, nil
end

return M
