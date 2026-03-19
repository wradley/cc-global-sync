local errors = require("rednet_contracts.errors")
local mrpc = require("rednet_contracts.mrpc_v1")
local schema = require("rednet_contracts.schema_validation")

---@class WarehouseTransferItem
---@field name string
---@field count integer

---@class WarehouseAssignment
---@field assignment_id string
---@field source string
---@field destination string
---@field destination_address string
---@field reason string
---@field status string
---@field items WarehouseTransferItem[]
---@field total_items integer
---@field line_count integer

---@class WarehouseAssignTransferRequestParams
---@field coordinator_id string
---@field transfer_request_id string
---@field sent_at integer
---@field warehouse_id string
---@field assignments WarehouseAssignment[]
---@field total_assignments integer
---@field total_items integer

---@class WarehouseGetTransferRequestStatusParams
---@field transfer_request_id string

---@class WarehouseOwnerRecord
---@field coordinator_id string
---@field coordinator_address string
---@field claimed_at integer

---@class WarehouseSetOwnerParams
---@field coordinator_id string
---@field coordinator_address string
---@field claimed_at integer

---@class WarehouseServiceDefaults
---@field rednet_protocol string|nil
---@field timeout number|nil
---@field auto_reply_errors boolean|nil

---@class WarehouseServiceCallOptions
---@field rednet_protocol string|nil
---@field timeout number|nil
---@field request_id string|nil
---@field auto_reply_errors boolean|nil
---@field details table|nil

---@enum WarehouseV1Method
local METHODS = {
  GET_OWNER = "get_owner",
  GET_OVERVIEW = "get_overview",
  GET_SNAPSHOT = "get_snapshot",
  SET_OWNER = "set_owner",
  ASSIGN_TRANSFER_REQUEST = "assign_transfer_request",
  GET_TRANSFER_REQUEST_STATUS = "get_transfer_request_status",
}

---@class WarehouseReceivedRequest
---@field request_id string
---@field method WarehouseV1Method
---@field params table

local M = {
  NAME = "warehouse",
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
  if method == M.METHODS.GET_OWNER
    or method == M.METHODS.GET_OVERVIEW
    or method == M.METHODS.GET_SNAPSHOT
    or method == M.METHODS.SET_OWNER
    or method == M.METHODS.ASSIGN_TRANSFER_REQUEST
    or method == M.METHODS.GET_TRANSFER_REQUEST_STATUS
  then
    return true
  end

  return schema.fail("unknown_method", "message.method", "unknown warehouse_v1 method: " .. tostring(method))
end

local function validateIssueList(value, path)
  return schema.requireTable(value, path)
end

local function validateOwnerRecord(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(value.coordinator_id, path .. ".coordinator_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(value.coordinator_address, path .. ".coordinator_address")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.claimed_at, path .. ".claimed_at")
  if not ok then
    return false, err
  end

  return true
end

local function validateGetOwnerParams(params)
  return schema.requireEmptyTable(params, "params")
end

local function validateGetOwnerResult(result)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "warehouse_id", "warehouse_address" }) do
    ok, err = schema.requireString(result[field], "result." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireInteger(result.observed_at, "result.observed_at")
  if not ok then
    return false, err
  end

  if result.owner ~= nil then
    ok, err = validateOwnerRecord(result.owner, "result.owner")
    if not ok then
      return false, err
    end
  end

  return true
end

local function validateOverviewStatus(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireBoolean(value.online, path .. ".online")
  if not ok then
    return false, err
  end

  for _, field in ipairs({
    "storage_online",
    "storage_total",
    "slot_capacity_used",
    "slot_capacity_total",
    "storages_with_unknown_capacity",
  }) do
    ok, err = schema.requireInteger(value[field], path .. "." .. field)
    if not ok then
      return false, err
    end
  end

  return true
end

local function validateOverviewActiveTransfer(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(value.id, path .. ".id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.received_at, path .. ".received_at")
  if not ok then
    return false, err
  end

  return true
end

local function validateOverviewLastAck(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(value.transfer_request_id, path .. ".transfer_request_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.sent_at, path .. ".sent_at")
  if not ok then
    return false, err
  end

  return true
end

local function validateExecutionAssignmentSummary(entry, path)
  local ok, err = schema.requireTable(entry, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(entry.destination, path .. ".destination")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(entry.item_count, path .. ".item_count")
  if not ok then
    return false, err
  end

  return true
end

local function validateOverviewLastExecution(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(value.transfer_request_id, path .. ".transfer_request_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(value.status, path .. ".status")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.executed_at, path .. ".executed_at")
  if not ok then
    return false, err
  end

  ok, err = schema.requireTable(value.assignments, path .. ".assignments")
  if not ok then
    return false, err
  end

  for key, entry in pairs(value.assignments) do
    local entryOk, entryErr = validateExecutionAssignmentSummary(entry, string.format("%s.assignments[%s]", path, tostring(key)))
    if not entryOk then
      return false, entryErr
    end
  end

  ok, err = schema.requireInteger(value.total_items_requested, path .. ".total_items_requested")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(value.total_items_queued, path .. ".total_items_queued")
  if not ok then
    return false, err
  end

  return true
end

local function validateGetOverviewParams(params)
  return schema.requireEmptyTable(params, "params")
end

local function validateGetOverviewResult(result)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "warehouse_id", "warehouse_address" }) do
    ok, err = schema.requireString(result[field], "result." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireInteger(result.observed_at, "result.observed_at")
  if not ok then
    return false, err
  end

  ok, err = validateOverviewStatus(result.status, "result.status")
  if not ok then
    return false, err
  end

  if result.active_transfer_request ~= nil then
    ok, err = validateOverviewActiveTransfer(result.active_transfer_request, "result.active_transfer_request")
    if not ok then
      return false, err
    end
  end

  if result.last_ack ~= nil then
    ok, err = validateOverviewLastAck(result.last_ack, "result.last_ack")
    if not ok then
      return false, err
    end
  end

  if result.last_execution ~= nil then
    ok, err = validateOverviewLastExecution(result.last_execution, "result.last_execution")
    if not ok then
      return false, err
    end
  end

  ok, err = validateIssueList(result.recent_issues, "result.recent_issues")
  if not ok then
    return false, err
  end

  return true
end

local function validateGetSnapshotParams(params)
  return schema.requireEmptyTable(params, "params")
end

local function validateGetSnapshotResult(result)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "warehouse_id", "warehouse_address" }) do
    ok, err = schema.requireString(result[field], "result." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireInteger(result.observed_at, "result.observed_at")
  if not ok then
    return false, err
  end

  ok, err = schema.requireNamedIntegerMap(result.inventory, "result.inventory")
  if not ok then
    return false, err
  end

  ok, err = schema.requireTable(result.capacity, "result.capacity")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(result.capacity.slot_capacity_total, "result.capacity.slot_capacity_total")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(result.capacity.slot_capacity_used, "result.capacity.slot_capacity_used")
  if not ok then
    return false, err
  end

  return true
end

local function validateTransferItem(item, path)
  local ok, err = schema.requireTable(item, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(item.name, path .. ".name")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(item.count, path .. ".count")
  if not ok then
    return false, err
  end

  return true
end

local function validateAssignment(assignment, path)
  local ok, err = schema.requireTable(assignment, path)
  if not ok then
    return false, err
  end

  for _, field in ipairs({
    "assignment_id",
    "source",
    "destination",
    "destination_address",
    "reason",
    "status",
  }) do
    ok, err = schema.requireString(assignment[field], path .. "." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireArrayItems(assignment.items, path .. ".items", validateTransferItem)
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(assignment.total_items, path .. ".total_items")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(assignment.line_count, path .. ".line_count")
  if not ok then
    return false, err
  end

  return true
end

local function validateAssignTransferRequestParams(params)
  local ok, err = schema.requireTable(params, "params")
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "coordinator_id", "transfer_request_id", "warehouse_id" }) do
    ok, err = schema.requireString(params[field], "params." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireInteger(params.sent_at, "params.sent_at")
  if not ok then
    return false, err
  end

  ok, err = schema.requireArrayItems(params.assignments, "params.assignments", validateAssignment)
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(params.total_assignments, "params.total_assignments")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(params.total_items, "params.total_items")
  if not ok then
    return false, err
  end

  return true
end

local function validateSetOwnerParams(params)
  local ok, err = schema.requireTable(params, "params")
  if not ok then
    return false, err
  end

  return validateOwnerRecord(params, "params")
end

local function validateSetOwnerResult(result)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "warehouse_id", "warehouse_address" }) do
    ok, err = schema.requireString(result[field], "result." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireBoolean(result.accepted, "result.accepted")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(result.sent_at, "result.sent_at")
  if not ok then
    return false, err
  end

  if result.owner ~= nil then
    ok, err = validateOwnerRecord(result.owner, "result.owner")
    if not ok then
      return false, err
    end
  end

  return true
end

local function validateAssignTransferRequestResult(result)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "warehouse_id", "warehouse_address", "transfer_request_id" }) do
    ok, err = schema.requireString(result[field], "result." .. field)
    if not ok then
      return false, err
    end
  end

  for _, field in ipairs({ "assignment_count", "item_count", "sent_at" }) do
    ok, err = schema.requireInteger(result[field], "result." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireBoolean(result.accepted, "result.accepted")
  if not ok then
    return false, err
  end

  return true
end

local function validateGetTransferRequestStatusParams(params)
  local ok, err = schema.requireTable(params, "params")
  if not ok then
    return false, err
  end

  return schema.requireString(params.transfer_request_id, "params.transfer_request_id")
end

local function validateTransferStatusAssignment(entry, path)
  local ok, err = schema.requireTable(entry, path)
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "assignment_id", "destination", "destination_address", "status" }) do
    ok, err = schema.requireString(entry[field], path .. "." .. field)
    if not ok then
      return false, err
    end
  end

  for _, field in ipairs({ "line_count", "requested_items", "queued_items" }) do
    ok, err = schema.requireInteger(entry[field], path .. "." .. field)
    if not ok then
      return false, err
    end
  end

  return true
end

local function validateGetTransferRequestStatusResult(result)
  local ok, err = schema.requireTable(result, "result")
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "warehouse_id", "warehouse_address", "transfer_request_id", "status" }) do
    ok, err = schema.requireString(result[field], "result." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.optionalInteger(result.executed_at, "result.executed_at")
  if not ok then
    return false, err
  end

  for _, field in ipairs({ "total_assignments", "total_items_requested", "total_items_queued", "sent_at" }) do
    ok, err = schema.requireInteger(result[field], "result." .. field)
    if not ok then
      return false, err
    end
  end

  ok, err = schema.requireArrayItems(result.assignments, "result.assignments", validateTransferStatusAssignment)
  if not ok then
    return false, err
  end

  return true
end

local VALIDATORS = {
  [METHODS.GET_OWNER] = {
    params = validateGetOwnerParams,
    result = validateGetOwnerResult,
  },
  [METHODS.GET_OVERVIEW] = {
    params = validateGetOverviewParams,
    result = validateGetOverviewResult,
  },
  [METHODS.GET_SNAPSHOT] = {
    params = validateGetSnapshotParams,
    result = validateGetSnapshotResult,
  },
  [METHODS.SET_OWNER] = {
    params = validateSetOwnerParams,
    result = validateSetOwnerResult,
  },
  [METHODS.ASSIGN_TRANSFER_REQUEST] = {
    params = validateAssignTransferRequestParams,
    result = validateAssignTransferRequestResult,
  },
  [METHODS.GET_TRANSFER_REQUEST_STATUS] = {
    params = validateGetTransferRequestStatusParams,
    result = validateGetTransferRequestStatusResult,
  },
}

---Validate that an inbound MRPC request targets `warehouse_v1` and that its
---method-specific params match the service contract.
---@param message MrpcRequestEnvelope
---@return boolean, string|nil, RednetContractsError|nil
local function validateRequest(message)
  local ok, err = mrpc.validateRequest(message)
  if not ok then
    return false, nil, err
  end

  if message.protocol.name ~= M.NAME or message.protocol.version ~= M.VERSION then
    return false, nil, errors.new("protocol_mismatch", "message.protocol does not match warehouse_v1", {
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

---Validate that an MRPC response matches the requested `warehouse_v1` method
---and that a successful result uses the expected service-specific shape.
---@param method string
---@param message MrpcResponseEnvelope
---@return boolean, table|nil
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
    return false, errors.new("protocol_mismatch", "message.protocol does not match warehouse_v1", {
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
---@param options WarehouseServiceDefaults|nil
---@return WarehouseServiceDefaults
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

---Call `warehouse_v1.get_owner()`.
---@param rednetId integer
---@param opts WarehouseServiceCallOptions|nil
---@return table|nil, table|nil
function M.getOwner(rednetId, opts)
  return callMethod(rednetId, M.METHODS.GET_OWNER, {}, opts)
end

---Call `warehouse_v1.get_overview()`.
---@param rednetId integer
---@param opts WarehouseServiceCallOptions|nil
---@return table|nil, table|nil
function M.getOverview(rednetId, opts)
  return callMethod(rednetId, M.METHODS.GET_OVERVIEW, {}, opts)
end

---Call `warehouse_v1.get_snapshot()`.
---@param rednetId integer
---@param opts WarehouseServiceCallOptions|nil
---@return table|nil, table|nil
function M.getSnapshot(rednetId, opts)
  return callMethod(rednetId, M.METHODS.GET_SNAPSHOT, {}, opts)
end

---Call `warehouse_v1.set_owner()`.
---@param rednetId integer
---@param params WarehouseSetOwnerParams
---@param opts WarehouseServiceCallOptions|nil
---@return table|nil, table|nil
function M.setOwner(rednetId, params, opts)
  return callMethod(rednetId, M.METHODS.SET_OWNER, params, opts)
end

---Call `warehouse_v1.assign_transfer_request()`.
---@param rednetId integer
---@param params WarehouseAssignTransferRequestParams
---@param opts WarehouseServiceCallOptions|nil
---@return table|nil, table|nil
function M.assignTransferRequest(rednetId, params, opts)
  return callMethod(rednetId, M.METHODS.ASSIGN_TRANSFER_REQUEST, params, opts)
end

---Call `warehouse_v1.get_transfer_request_status()`.
---@param rednetId integer
---@param params WarehouseGetTransferRequestStatusParams
---@param opts WarehouseServiceCallOptions|nil
---@return table|nil, table|nil
function M.getTransferRequestStatus(rednetId, params, opts)
  return callMethod(rednetId, M.METHODS.GET_TRANSFER_REQUEST_STATUS, params, opts)
end

---Receive and validate one `warehouse_v1` request.
---@param opts WarehouseServiceCallOptions|nil
---@return integer|nil, WarehouseReceivedRequest|nil, string|nil, table|nil
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

---Reply to a validated `warehouse_v1` request with a successful result.
---@param rednetId integer
---@param request WarehouseReceivedRequest
---@param method string
---@param result table
---@param opts WarehouseServiceCallOptions|nil
---@return table
function M.replySuccess(rednetId, request, method, result, opts)
  local response = buildResponse(request.request_id, method, result or {}, os.epoch("utc"))
  local effective = mergeCallOptions(opts)

  rednet.send(rednetId, response, effective.rednet_protocol or mrpc.REDNET_PROTOCOL)

  return response
end

---Reply to a validated `warehouse_v1` request with a structured error.
---@param rednetId integer
---@param request WarehouseReceivedRequest
---@param code string
---@param messageText string
---@param opts WarehouseServiceCallOptions|nil
---@return table
function M.replyError(rednetId, request, code, messageText, opts)
  local effective = mergeCallOptions(opts)
  return mrpc.replyError(rednetId, request, code, messageText, {
    rednet_protocol = effective.rednet_protocol,
    details = effective.details,
  })
end

return M
