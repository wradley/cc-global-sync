local errors = require("rednet_contracts.errors")

local M = {}

local function fail(code, path, message)
  return false, errors.new(code, message, {
    path = path,
  })
end

local function isInteger(value)
  return type(value) == "number" and value % 1 == 0
end

local function isArray(value)
  if type(value) ~= "table" then
    return false
  end

  local count = 0
  for key in pairs(value) do
    if type(key) ~= "number" or key < 1 or key % 1 ~= 0 then
      return false
    end
    count = count + 1
  end

  for index = 1, count do
    if value[index] == nil then
      return false
    end
  end

  return true
end

---Construct a structured validation failure tuple.
---@param code string
---@param path string
---@param message string
---@return boolean, table
function M.fail(code, path, message)
  return fail(code, path, message)
end

---Require a table value.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requireTable(value, path)
  if type(value) ~= "table" then
    return fail("invalid_type", path, path .. " must be a table")
  end

  return true
end

---Require an array-style table value.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requireArray(value, path)
  if not isArray(value) then
    return fail("invalid_type", path, path .. " must be an array")
  end

  return true
end

---Require a non-empty string.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requireString(value, path)
  if type(value) ~= "string" or value == "" then
    return fail("invalid_type", path, path .. " must be a non-empty string")
  end

  return true
end

---Allow `nil` or require a non-empty string.
---@param value any
---@param path string
---@return boolean, table|nil
function M.optionalString(value, path)
  if value == nil then
    return true
  end

  return M.requireString(value, path)
end

---Require a boolean.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requireBoolean(value, path)
  if type(value) ~= "boolean" then
    return fail("invalid_type", path, path .. " must be a boolean")
  end

  return true
end

---Require an integer.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requireInteger(value, path)
  if not isInteger(value) then
    return fail("invalid_type", path, path .. " must be an integer")
  end

  return true
end

---Allow `nil` or require an integer.
---@param value any
---@param path string
---@return boolean, table|nil
function M.optionalInteger(value, path)
  if value == nil then
    return true
  end

  return M.requireInteger(value, path)
end

---Require an integer greater than zero.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requirePositiveInteger(value, path)
  local ok, err = M.requireInteger(value, path)
  if not ok then
    return false, err
  end

  if value <= 0 then
    return fail("invalid_value", path, path .. " must be greater than zero")
  end

  return true
end

---Allow `nil` or require a number.
---@param value any
---@param path string
---@return boolean, table|nil
function M.optionalNumber(value, path)
  if value == nil then
    return true
  end

  if type(value) ~= "number" then
    return fail("invalid_type", path, path .. " must be a number")
  end

  return true
end

---Require a string contained in the allowed set.
---@param value any
---@param path string
---@param allowed table<string, boolean>
---@return boolean, table|nil
function M.requireOneOf(value, path, allowed)
  local ok, err = M.requireString(value, path)
  if not ok then
    return false, err
  end

  if not allowed[value] then
    return fail("invalid_value", path, path .. " must be one of the allowed values")
  end

  return true
end

---Require an empty table.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requireEmptyTable(value, path)
  local ok, err = M.requireTable(value, path)
  if not ok then
    return false, err
  end

  for _ in pairs(value) do
    return fail("invalid_value", path, path .. " must be empty")
  end

  return true
end

---Require a string-keyed map and validate each value.
---@param value any
---@param path string
---@param valueValidator fun(value:any, path:string): boolean, table|nil
---@return boolean, table|nil
function M.requireMap(value, path, valueValidator)
  local ok, err = M.requireTable(value, path)
  if not ok then
    return false, err
  end

  for key, entry in pairs(value) do
    if type(key) ~= "string" or key == "" then
      return fail("invalid_type", path, path .. " keys must be non-empty strings")
    end

    local entryOk, entryErr = valueValidator(entry, path .. "." .. key)
    if not entryOk then
      return false, entryErr
    end
  end

  return true
end

---Require an array and validate each element.
---@param value any
---@param path string
---@param validator fun(value:any, path:string): boolean, table|nil
---@return boolean, table|nil
function M.requireArrayItems(value, path, validator)
  local ok, err = M.requireArray(value, path)
  if not ok then
    return false, err
  end

  for index, entry in ipairs(value) do
    local entryOk, entryErr = validator(entry, string.format("%s[%d]", path, index))
    if not entryOk then
      return false, entryErr
    end
  end

  return true
end

---Require a string-keyed map of integers.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requireNamedIntegerMap(value, path)
  return M.requireMap(value, path, function(entry, entryPath)
    return M.requireInteger(entry, entryPath)
  end)
end

---Require a `{ name, version }` service identity table.
---@param value any
---@param path string
---@return boolean, table|nil
function M.requireProtocolIdentity(value, path)
  local ok, err = M.requireTable(value, path)
  if not ok then
    return false, err
  end

  ok, err = M.requireString(value.name, path .. ".name")
  if not ok then
    return false, err
  end

  ok, err = M.requirePositiveInteger(value.version, path .. ".version")
  if not ok then
    return false, err
  end

  return true
end

return M
