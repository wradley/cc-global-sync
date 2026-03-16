---Minimal ComputerCraft global stubs for model tests.
---@class CcTestEnv
local M = {}

local original = {}
local currentEpoch = 0
local sentMessages = {}

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

---Install test doubles for ComputerCraft globals used by the model layer.
---@param opts? { epoch: integer|nil }
---@return nil
function M.install(opts)
  currentEpoch = opts and opts.epoch or 0
  sentMessages = {}

  original.os = _G.os
  original.textutils = _G.textutils
  original.rednet = _G.rednet

  _G.os = setmetatable({
    epoch = function()
      return currentEpoch
    end,
  }, {
    __index = original.os,
  })

  _G.textutils = {
    serialize = function(value)
      return deepCopy(value)
    end,
    unserialize = function(value)
      return deepCopy(value)
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

---Clear captured rednet sends.
---@return nil
function M.clearSentMessages()
  sentMessages = {}
end

return M
