local errors = require("rednet_contracts.errors")
local schema = require("rednet_contracts.schema_validation")

---@class MrpcServiceIdentity
---@field name string
---@field version integer

---@class MrpcRequestEnvelope
---@field type "request"
---@field protocol MrpcServiceIdentity
---@field request_id string
---@field method string
---@field params table
---@field sent_at integer

---@class MrpcResponseEnvelope
---@field type "response"
---@field protocol MrpcServiceIdentity
---@field request_id string
---@field ok boolean
---@field result table|nil
---@field error table|nil
---@field sent_at integer

---@class MrpcOptions
---@field rednet_protocol string|nil
---@field timeout number|nil
---@field request_id string|nil
---@field details table|nil

local M = {
  REDNET_PROTOCOL = "rc.mrpc_v1",
}

local requestCounter = 0

local function copyTable(source)
  local target = {}

  for key, value in pairs(source or {}) do
    target[key] = value
  end

  return target
end

local function validateStructuredError(value, path)
  local ok, err = schema.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(value.code, path .. ".code")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(value.message, path .. ".message")
  if not ok then
    return false, err
  end

  if value.details ~= nil then
    ok, err = schema.requireTable(value.details, path .. ".details")
    if not ok then
      return false, err
    end
  end

  return true
end

---Create a request id suitable for one RPC call.
---@param prefix string|nil
---@return string
function M.newRequestId(prefix)
  requestCounter = requestCounter + 1
  local computerId = os.getComputerID and os.getComputerID() or nil
  if computerId ~= nil then
    return string.format("%sc%s-%d-%d", prefix or "req-", tostring(computerId), os.epoch("utc"), requestCounter)
  end

  return string.format("%s%d-%d", prefix or "req-", os.epoch("utc"), requestCounter)
end

---Validate a common RPC request envelope.
---@param message any
---@return boolean, table|nil
function M.validateRequest(message)
  local ok, err = schema.requireTable(message, "message")
  if not ok then
    return false, err
  end

  if message.type ~= "request" then
    return schema.fail("invalid_value", "message.type", "message.type must be request")
  end

  ok, err = schema.requireProtocolIdentity(message.protocol, "message.protocol")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(message.request_id, "message.request_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(message.method, "message.method")
  if not ok then
    return false, err
  end

  ok, err = schema.requireTable(message.params, "message.params")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(message.sent_at, "message.sent_at")
  if not ok then
    return false, err
  end

  return true
end

---Validate a common RPC response envelope.
---@param message any
---@return boolean, table|nil
function M.validateResponse(message)
  local ok, err = schema.requireTable(message, "message")
  if not ok then
    return false, err
  end

  if message.type ~= "response" then
    return schema.fail("invalid_value", "message.type", "message.type must be response")
  end

  ok, err = schema.requireProtocolIdentity(message.protocol, "message.protocol")
  if not ok then
    return false, err
  end

  ok, err = schema.requireString(message.request_id, "message.request_id")
  if not ok then
    return false, err
  end

  ok, err = schema.requireBoolean(message.ok, "message.ok")
  if not ok then
    return false, err
  end

  ok, err = schema.requireInteger(message.sent_at, "message.sent_at")
  if not ok then
    return false, err
  end

  if message.ok then
    ok, err = schema.requireTable(message.result, "message.result")
    if not ok then
      return false, err
    end
  else
    ok, err = validateStructuredError(message.error, "message.error")
    if not ok then
      return false, err
    end
  end

  return true
end

---Build a validated RPC request envelope.
---@param service MrpcServiceIdentity
---@param requestId string
---@param method string
---@param params table
---@param sentAt integer
---@return table
function M.buildRequest(service, requestId, method, params, sentAt)
  local message = {
    type = "request",
    protocol = copyTable(service),
    request_id = requestId,
    method = method,
    params = params or {},
    sent_at = sentAt,
  }

  local ok, err = M.validateRequest(message)
  if not ok then
    errors.raise(err, 1)
  end

  return message
end

---Build a validated successful RPC response envelope.
---@param service MrpcServiceIdentity
---@param requestId string
---@param result table
---@param sentAt integer
---@return table
function M.buildResponse(service, requestId, result, sentAt)
  local message = {
    type = "response",
    protocol = copyTable(service),
    request_id = requestId,
    ok = true,
    result = result or {},
    sent_at = sentAt,
  }

  local ok, err = M.validateResponse(message)
  if not ok then
    errors.raise(err, 1)
  end

  return message
end

---Build a validated non-success RPC response envelope.
---@param service MrpcServiceIdentity
---@param requestId string
---@param code string
---@param messageText string
---@param sentAt integer
---@param details table|nil
---@return table
function M.buildErrorResponse(service, requestId, code, messageText, sentAt, details)
  local message = {
    type = "response",
    protocol = copyTable(service),
    request_id = requestId,
    ok = false,
    error = errors.new(code, messageText, details),
    sent_at = sentAt,
  }

  local ok, err = M.validateResponse(message)
  if not ok then
    errors.raise(err, 1)
  end

  return message
end

---Receive and validate one RPC request envelope from rednet.
---@param opts MrpcOptions|nil
---@return integer|nil, table|nil, table|nil
function M.receiveRequest(opts)
  local protocol = opts and opts.rednet_protocol or M.REDNET_PROTOCOL
  local timeout = opts and opts.timeout or nil
  local senderId, message = rednet.receive(protocol, timeout)

  if senderId == nil then
    return nil, nil, errors.new("timeout", "timed out waiting for rpc request", {
      path = "rednet.receive",
    })
  end

  local ok, err = M.validateRequest(message)
  if not ok then
    return senderId, nil, err
  end

  ---@cast message MrpcRequestEnvelope
  return senderId, message, nil
end

---Send a validated successful RPC response.
---@param rednetId integer
---@param request MrpcRequestEnvelope
---@param result table
---@param opts MrpcOptions|nil
---@return table
function M.replySuccess(rednetId, request, result, opts)
  local protocol = opts and opts.rednet_protocol or M.REDNET_PROTOCOL
  local response = M.buildResponse(request.protocol, request.request_id, result, os.epoch("utc"))

  rednet.send(rednetId, response, protocol)

  return response
end

---Send a validated non-success RPC response.
---@param rednetId integer
---@param request MrpcRequestEnvelope
---@param code string
---@param messageText string
---@param opts MrpcOptions|nil
---@return table
function M.replyError(rednetId, request, code, messageText, opts)
  local protocol = opts and opts.rednet_protocol or M.REDNET_PROTOCOL
  local details = opts and opts.details or nil
  local response = M.buildErrorResponse(request.protocol, request.request_id, code, messageText, os.epoch("utc"), details)

  rednet.send(rednetId, response, protocol)

  return response
end

---Send one RPC request and wait for its matching response.
---@param rednetId integer
---@param service MrpcServiceIdentity
---@param method string
---@param params table
---@param opts MrpcOptions|nil
---@return table|nil, table|nil
function M.call(rednetId, service, method, params, opts)
  local protocol = opts and opts.rednet_protocol or M.REDNET_PROTOCOL
  local requestId = opts and opts.request_id or M.newRequestId()
  local request = M.buildRequest(service, requestId, method, params or {}, os.epoch("utc"))

  rednet.send(rednetId, request, protocol)

  local timeout = opts and opts.timeout or nil

  while true do
    local senderId, response = rednet.receive(protocol, timeout)
    if senderId == nil then
      return nil, errors.new("timeout", "timed out waiting for rpc response", {
        path = "rednet.receive",
      })
    end

    if senderId == rednetId then
      local ok, err = M.validateResponse(response)
      if not ok then
        return nil, err
      end
      ---@cast response MrpcResponseEnvelope

      if response.request_id ~= requestId then
        return nil, errors.new("request_id_mismatch", "rpc response request_id did not match the request", {
          path = "message.request_id",
        })
      end

      if response.protocol.name ~= service.name or response.protocol.version ~= service.version then
        return nil, errors.new("protocol_mismatch", "rpc response service did not match the request", {
          path = "message.protocol",
        })
      end

      return response, nil
    end
  end
end

return M
