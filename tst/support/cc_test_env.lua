---Minimal ComputerCraft global stubs for model tests.
---@class CcTestEnv
local M = {}

local original = {}
local currentEpoch = 0
local sentMessages = {}
local broadcastMessages = {}
local queuedReceives = {}

local function deepCopy(value, seen)
  if type(value) ~= "table" then
    return value
  end

  seen = seen or {}
  if seen[value] then
    return seen[value]
  end

  local copy = {}
  seen[value] = copy
  for key, innerValue in pairs(value) do
    copy[deepCopy(key, seen)] = deepCopy(innerValue, seen)
  end
  return copy
end

local function serializeValue(value)
  if type(value) == "table" then
    local pieces = { "{" }
    local first = true
    for key, innerValue in pairs(value) do
      if not first then
        pieces[#pieces + 1] = ","
      end
      first = false

      local renderedKey
      if type(key) == "string" and key:match("^[%a_][%w_]*$") then
        renderedKey = key
      else
        renderedKey = "[" .. serializeValue(key) .. "]"
      end

      pieces[#pieces + 1] = renderedKey .. "=" .. serializeValue(innerValue)
    end
    pieces[#pieces + 1] = "}"
    return table.concat(pieces)
  end

  if type(value) == "string" then
    return string.format("%q", value)
  end

  return tostring(value)
end

---Install test doubles for ComputerCraft globals used by the model layer.
---@param opts? { epoch: integer|nil, computer_id: integer|nil }
---@return nil
function M.install(opts)
  currentEpoch = opts and opts.epoch or 0
  sentMessages = {}
  broadcastMessages = {}
  queuedReceives = {}

  original.os = _G.os
  original.textutils = _G.textutils
  original.rednet = _G.rednet

  _G.os = setmetatable({
    epoch = function()
      return currentEpoch
    end,
    getComputerID = function()
      return opts and opts.computer_id or nil
    end,
  }, {
    __index = original.os,
  })

  _G.textutils = {
    serialize = function(value)
      return serializeValue(deepCopy(value))
    end,
    unserialize = function(value)
      if type(value) == "table" then
        return deepCopy(value)
      end

      local chunk = load("return " .. tostring(value))
      if not chunk then
        return nil
      end

      return chunk()
    end,
  }

  _G.rednet = {
    send = function(targetId, message, protocol)
      sentMessages[#sentMessages + 1] = {
        target_id = targetId,
        message = deepCopy(message),
        protocol = protocol,
      }
      return true
    end,
    broadcast = function(message, protocol)
      broadcastMessages[#broadcastMessages + 1] = {
        message = deepCopy(message),
        protocol = protocol,
      }
      return true
    end,
    receive = function(protocol)
      for index, entry in ipairs(queuedReceives) do
        if protocol == nil or entry.protocol == protocol then
          table.remove(queuedReceives, index)
          return entry.sender_id, deepCopy(entry.message), entry.protocol
        end
      end

      return nil, nil, protocol
    end,
  }
end

---Restore the original globals after a test.
---@return nil
function M.restore()
  _G.os = original.os
  _G.textutils = original.textutils
  _G.rednet = original.rednet
end

---Set the epoch milliseconds returned by `os.epoch("utc")`.
---@param epoch integer
---@return nil
function M.setEpoch(epoch)
  currentEpoch = epoch
end

---Return captured rednet sends.
---@return table[]
function M.getSentMessages()
  return sentMessages
end

---Return captured rednet broadcasts.
---@return table[]
function M.getBroadcastMessages()
  return broadcastMessages
end

---Queue one rednet receive result for later `rednet.receive(...)` calls.
---@param senderId integer
---@param message table
---@param protocol string|nil
---@return nil
function M.queueRednetReceive(senderId, message, protocol)
  queuedReceives[#queuedReceives + 1] = {
    sender_id = senderId,
    message = deepCopy(message),
    protocol = protocol,
  }
end

---Clear captured rednet sends and queued receives.
---@return nil
function M.clearSentMessages()
  sentMessages = {}
  broadcastMessages = {}
  queuedReceives = {}
end

return M
