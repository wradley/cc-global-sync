local errors = require("rednet_contracts.errors")
local mrpc = require("rednet_contracts.mrpc_v1")
local schema = require("rednet_contracts.schema_validation")

---@class GlobalInventoryServiceDefaults
---@field rednet_protocol string|nil
---@field timeout number|nil
---@field auto_reply_errors boolean|nil

---@class GlobalInventoryServiceCallOptions
---@field rednet_protocol string|nil
---@field timeout number|nil
---@field request_id string|nil
---@field auto_reply_errors boolean|nil
---@field details table|nil

---@enum GlobalInventoryV1Method
local METHODS = {
  GET_OVERVIEW = "get_overview",
  PAUSE_SYNC = "pause_sync",
  RESUME_SYNC = "resume_sync",
  SYNC_NOW = "sync_now",
}

---@class GlobalInventoryReceivedRequest
---@field request_id string
---@field method GlobalInventoryV1Method
---@field params table

local M = {
  NAME = "global_inventory",
  VERSION = 1,
  METHODS = METHODS,
}

M.SERVICE = {
  name = M.NAME,
  version = M.VERSION,
}

local config = {
  rednet_protocol = nil,
  timeout = nil,
  auto_reply_errors = true,
}

local function copyTable(source)
  local target = {}
  for key, value in pairs(source or {}) do
    target[key] = value
  end
  return target
end

local function mergeCallOptions(opts)
  local merged = copyTable(config)
  for key, value in pairs(opts or {}) do
    merged[key] = value
  end
  return merged
end

local function validateDefaults(options, path)
  local ok, err = schema.requireTable(options, path)
  if not ok then
    return false, err
  end

  ok, err = schema.optionalString(options.rednet_protocol, path .. ".rednet_protocol")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalNumber(options.timeout, path .. ".timeout")
  if not ok then
    return false, err
  end

  if options.auto_reply_errors ~= nil then
    ok, err = schema.requireBoolean(options.auto_reply_errors, path .. ".auto_reply_errors")
    if not ok then
      return false, err
    end
  end

  return true
end

local function ensureMethod(method)
  if method == M.METHODS.GET_OVERVIEW
    or method == M.METHODS.PAUSE_SYNC
    or method == M.METHODS.RESUME_SYNC
    or method == M.METHODS.SYNC_NOW
  then
    return true
  end

  return schema.fail("unknown_method", "message.method", "unknown global_inventory_v1 method: " .. tostring(method))
end

local function validateIssueList(value, path)
  return schema.requireTable(value, path)
end

local function validateGetOverviewParams(params)
  return schema.requireEmptyTable(params, "params")
end

local function validateSchedule(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireBoolean(value.paused, path .. ".paused")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.sync_interval_seconds, path .. ".sync_interval_seconds")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalInteger(value.next_sync_due_at, path .. ".next_sync_due_at")
  if not ok then
    return false, err
  end

  return true
end

local function validateTransferCycle(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireBoolean(value.active, path .. ".active")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalString(value.kind, path .. ".kind")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalInteger(value.started_at, path .. ".started_at")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.completed_warehouses, path .. ".completed_warehouses")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.total_warehouses, path .. ".total_warehouses")
  if not ok then
    return false, err
  end

  return true
end

local function validateWarehouseOverview(entry, path)
  local ok, err = schema.requireTable(entry, path)
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "warehouse_id", "warehouse_address" }) do
    ok, err = schema.requireString(entry[field], path .. "." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireOneOf(entry.state, path .. ".state", {
    accepted = true,
    pending = true,
  })
  if not ok then
    return false, err
  end

  ok, err = schema.requireBoolean(entry.online, path .. ".online")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalInteger(entry.last_heartbeat_at, path .. ".last_heartbeat_at")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalInteger(entry.last_snapshot_at, path .. ".last_snapshot_at")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalString(entry.last_transfer_request_id, path .. ".last_transfer_request_id")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalString(entry.last_transfer_request_status, path .. ".last_transfer_request_status")
  if not ok then
    return false, err
  end

  return true
end

local function validateInventorySummary(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.total_item_types, path .. ".total_item_types")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalInteger(value.total_item_count, path .. ".total_item_count")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.slot_capacity_used, path .. ".slot_capacity_used")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalInteger(value.slot_capacity_total, path .. ".slot_capacity_total")
  if not ok then
    return false, err
  end

  return true
end

local function validateGetOverviewResult(result)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(result.coordinator_id, "result.coordinator_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(result.observed_at, "result.observed_at")
  if not ok then
    return false, err
  end

  ok, err = validateSchedule(result.schedule, "result.schedule")
  if not ok then
    return false, err
  end

  ok, err = validateTransferCycle(result.transfer_cycle, "result.transfer_cycle")
  if not ok then
    return false, err
  end

  ok, err = schema.requireArrayItems(result.warehouses, "result.warehouses", validateWarehouseOverview)
  if not ok then
    return false, err
  end

  ok, err = validateInventorySummary(result.inventory_summary, "result.inventory_summary")
  if not ok then
    return false, err
  end

  ok, err = validateIssueList(result.recent_issues, "result.recent_issues")
  if not ok then
    return false, err
  end

  return true
end

local function validatePauseSyncParams(params)
  return schema.requireEmptyTable(params, "params")
end

local function validateResumeSyncParams(params)
  return schema.requireEmptyTable(params, "params")
end

local function validateSyncNowParams(params)
  return schema.requireEmptyTable(params, "params")
end

local function validatePausedResult(result, expectedPaused)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(result.coordinator_id, "result.coordinator_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireBoolean(result.paused, "result.paused")
  if not ok then
    return false, err
  end

  if result.paused ~= expectedPaused then
    return schema.fail("invalid_value", "result.paused", "result.paused does not match the method contract")
  end

  ok, err = schema.requireBoolean(result.changed, "result.changed")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(result.sent_at, "result.sent_at")
  if not ok then
    return false, err
  end

  return true
end

local function validatePauseSyncResult(result)
  return validatePausedResult(result, true)
end

local function validateResumeSyncResult(result)
  return validatePausedResult(result, false)
end

local function validateSyncNowResult(result)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(result.coordinator_id, "result.coordinator_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireBoolean(result.accepted, "result.accepted")
  if not ok then
    return false, err
  end

  ok, err = schema.optionalString(result.reason, "result.reason")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(result.sent_at, "result.sent_at")
  if not ok then
    return false, err
  end

  return true
end

local VALIDATORS = {
  [METHODS.GET_OVERVIEW] = {
    params = validateGetOverviewParams,
    result = validateGetOverviewResult,
  },
  [METHODS.PAUSE_SYNC] = {
    params = validatePauseSyncParams,
    result = validatePauseSyncResult,
  },
  [METHODS.RESUME_SYNC] = {
    params = validateResumeSyncParams,
    result = validateResumeSyncResult,
  },
  [METHODS.SYNC_NOW] = {
    params = validateSyncNowParams,
    result = validateSyncNowResult,
  },
}

local function validateRequest(message)
  local ok, err = mrpc.validateRequest(message)
  if not ok then
    return false, nil, err
  end

  if message.protocol.name ~= M.NAME or message.protocol.version ~= M.VERSION then
    return false, nil, errors.new("protocol_mismatch", "message.protocol does not match global_inventory_v1", {
      path = "message.protocol",
    })
  end

  ok, err = ensureMethod(message.method)
  if not ok then
    return false, nil, err
  end

  ok, err = VALIDATORS[message.method].params(message.params)
  if not ok then
    return false, nil, err
  end

  return true, message.method, nil
end

local function validateResponseForMethod(method, message)
  local ok, err = ensureMethod(method)
  if not ok then
    return false, err
  end

  ok, err = mrpc.validateResponse(message)
  if not ok then
    return false, err
  end

  if message.protocol.name ~= M.NAME or message.protocol.version ~= M.VERSION then
    return false, errors.new("protocol_mismatch", "message.protocol does not match global_inventory_v1", {
      path = "message.protocol",
    })
  end

  if not message.ok then
    return true, nil
  end

  return VALIDATORS[method].result(message.result)
end

local function toReceivedRequest(request)
  return {
    protocol = request.protocol,
    request_id = request.request_id,
    method = request.method,
    params = request.params,
  }
end

local function buildResponse(requestId, method, result, sentAt)
  local ok, err = ensureMethod(method)
  if not ok then
    errors.raise(err, 1)
  end

  ok, err = VALIDATORS[method].result(result or {})
  if not ok then
    errors.raise(err, 1)
  end

  return mrpc.buildResponse(M.SERVICE, requestId, result or {}, sentAt)
end

local function callMethod(rednetId, method, params, opts)
  local response, err = mrpc.call(rednetId, M.SERVICE, method, params or {}, mergeCallOptions(opts))
  if not response then
    return nil, err
  end

  local ok, validationErr = validateResponseForMethod(method, response)
  if not ok then
    return nil, validationErr
  end

  if not response.ok then
    return nil, response.error
  end

  return response.result, nil
end

---Return the current service defaults, or merge new defaults into them.
---@param options GlobalInventoryServiceDefaults|nil
---@return GlobalInventoryServiceDefaults
function M.config(options)
  if options == nil then
    return copyTable(config)
  end

  local ok, err = validateDefaults(options, "options")
  if not ok then
    errors.raise(err, 1)
  end

  for key, value in pairs(options) do
    config[key] = value
  end

  return copyTable(config)
end

---Call `global_inventory_v1.get_overview()`.
---@param rednetId integer
---@param opts GlobalInventoryServiceCallOptions|nil
---@return table|nil, table|nil
function M.getOverview(rednetId, opts)
  return callMethod(rednetId, M.METHODS.GET_OVERVIEW, {}, opts)
end

---Call `global_inventory_v1.pause_sync()`.
---@param rednetId integer
---@param opts GlobalInventoryServiceCallOptions|nil
---@return table|nil, table|nil
function M.pauseSync(rednetId, opts)
  return callMethod(rednetId, M.METHODS.PAUSE_SYNC, {}, opts)
end

---Call `global_inventory_v1.resume_sync()`.
---@param rednetId integer
---@param opts GlobalInventoryServiceCallOptions|nil
---@return table|nil, table|nil
function M.resumeSync(rednetId, opts)
  return callMethod(rednetId, M.METHODS.RESUME_SYNC, {}, opts)
end

---Call `global_inventory_v1.sync_now()`.
---@param rednetId integer
---@param opts GlobalInventoryServiceCallOptions|nil
---@return table|nil, table|nil
function M.syncNow(rednetId, opts)
  return callMethod(rednetId, M.METHODS.SYNC_NOW, {}, opts)
end

---Receive and validate one `global_inventory_v1` request.
---@param opts GlobalInventoryServiceCallOptions|nil
---@return integer|nil, GlobalInventoryReceivedRequest|nil, string|nil, table|nil
function M.receiveRequest(opts)
  local effective = mergeCallOptions(opts)
  local senderId, request, err = mrpc.receiveRequest(effective)
  if not request then
    return senderId, nil, nil, err
  end

  local ok, method, validationErr = validateRequest(request)
  if ok then
    return senderId, toReceivedRequest(request), method, nil
  end

  if effective.auto_reply_errors ~= false and senderId ~= nil and request.request_id ~= nil then
    mrpc.replyError(senderId, request, validationErr.code, validationErr.message, {
      rednet_protocol = effective.rednet_protocol,
      details = validationErr.details,
    })
  end

  return senderId, nil, nil, validationErr
end

---Reply to a validated `global_inventory_v1` request with a successful result.
---@param rednetId integer
---@param request GlobalInventoryReceivedRequest
---@param method string
---@param result table
---@param opts GlobalInventoryServiceCallOptions|nil
---@return table
function M.replySuccess(rednetId, request, method, result, opts)
  local response = buildResponse(request.request_id, method, result or {}, os.epoch("utc"))
  local effective = mergeCallOptions(opts)

  rednet.send(rednetId, response, effective.rednet_protocol or mrpc.REDNET_PROTOCOL)

  return response
end

---Reply to a validated `global_inventory_v1` request with a structured error.
---@param rednetId integer
---@param request GlobalInventoryReceivedRequest
---@param code string
---@param messageText string
---@param opts GlobalInventoryServiceCallOptions|nil
---@return table
function M.replyError(rednetId, request, code, messageText, opts)
  local effective = mergeCallOptions(opts)
  return mrpc.replyError(rednetId, request, code, messageText, {
    rednet_protocol = effective.rednet_protocol,
    details = effective.details,
  })
end

return M
