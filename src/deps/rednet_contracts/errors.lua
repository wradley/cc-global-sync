local M = {}

---@class RednetContractsError
---@field code string
---@field message string
---@field details table|nil

local function copyTable(source)
  local target = {}

  for key, value in pairs(source or {}) do
    target[key] = value
  end

  return target
end

---Create a structured contract error value.
---@param code string
---@param message string
---@param details table|nil
---@return RednetContractsError
function M.new(code, message, details)
  return {
    code = code,
    message = message,
    details = details and copyTable(details) or nil,
  }
end

---Report whether a value looks like a structured contract error.
---@param value any
---@return boolean
function M.isError(value)
  return type(value) == "table"
    and type(value.code) == "string"
    and type(value.message) == "string"
end

---Format a structured contract error as a readable string.
---@param err any
---@return string
function M.format(err)
  if not M.isError(err) then
    return tostring(err)
  end

  local path = err.details and err.details.path
  if type(path) == "string" and path ~= "" then
    return string.format("%s (%s at %s)", err.message, err.code, path)
  end

  return string.format("%s (%s)", err.message, err.code)
end

---Raise a structured contract error using Lua's normal error mechanism.
---@param err any
---@param level integer|nil
---@return nil
function M.raise(err, level)
  error(M.format(err), (level or 1) + 1)
end

return M
