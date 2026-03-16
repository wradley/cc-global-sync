--[[
Minimal file-backed logger for ComputerCraft
programs.

Usage:
  local log = require("log")

  log.config({
    output = {
      file = "var/log.txt",
      level = "info",
      mirror_to_term = false,
      timestamp = "utc",
    },
    retention = {
      mode = "none",
      max_lines = 1000,
    },
  })

  log.info("Started warehouse %s", warehouseId)
  log.warn("Storage %s is stale", storageId)
  log.error("Assignment failed for %s", itemName)
  log.panic("Missing required modem: %s", side)
]]

---@alias LogLevelName
---| "info"
---| "warn"
---| "error"
---| "panic"

---@alias LogTimestampMode
---| "utc"
---| "epoch"

---@alias LogRetentionMode
---| "none"
---| "truncate"

---@class LogOutputConfig
---@field file string Log file path relative to the computer root.
---@field level LogLevelName Minimum level written to the configured outputs.
---@field mirror_to_term boolean When true, also print written log lines to the terminal.
---@field timestamp LogTimestampMode Timestamp rendering mode for each log line.

---@class LogRetentionConfig
---@field mode LogRetentionMode Retention policy applied after writes.
---@field max_lines integer Maximum lines kept when `mode` is `truncate`.

---@class LogConfig
---@field output LogOutputConfig
---@field retention LogRetentionConfig

---@class LogModule
---@field VERSION string
---@field config fun(options?: LogConfig): LogConfig
---@field info fun(fmt: any, ...: any): string
---@field warn fun(fmt: any, ...: any): string
---@field error fun(fmt: any, ...: any): string
---@field panic fun(fmt: any, ...: any): nil

---@type LogModule
local M = {
  VERSION = "0.1.0",
}

local LEVELS = {
  info = 1,
  warn = 2,
  error = 3,
  panic = 4,
}

-- Output controls where logs go and how they are rendered.
-- Retention controls how much history is kept on disk.
local state = {
  output = {
    level = LEVELS.info,
    file = "var/log.txt",
    mirror_to_term = false,
    timestamp = "utc",
  },
  retention = {
    mode = "none",
    max_lines = 1000,
  },
}

---@param level LogLevelName|integer
---@return integer
local function normalizeLevel(level)
  if type(level) == "number" then
    for _, value in pairs(LEVELS) do
      if value == level then
        return level
      end
    end
  elseif type(level) == "string" then
    local normalized = string.lower(level)
    if LEVELS[normalized] then
      return LEVELS[normalized]
    end
  end

  error("invalid log level: " .. tostring(level), 3)
end

---@param level integer
---@return string
local function levelName(level)
  for name, value in pairs(LEVELS) do
    if value == level then
      return string.upper(name)
    end
  end

  error("unknown log level: " .. tostring(level), 3)
end

---@param value LogTimestampMode
---@return LogTimestampMode
local function normalizeTimestampMode(value)
  if type(value) ~= "string" then
    error("invalid timestamp mode: " .. tostring(value), 3)
  end

  local normalized = string.lower(value)
  if normalized == "utc" or normalized == "epoch" then
    return normalized
  end

  error("invalid timestamp mode: " .. tostring(value), 3)
end

---@param value LogRetentionMode
---@return LogRetentionMode
local function normalizeRetentionMode(value)
  if type(value) ~= "string" then
    error("invalid retention mode: " .. tostring(value), 3)
  end

  local normalized = string.lower(value)
  if normalized == "none" or normalized == "truncate" then
    return normalized
  end

  error("invalid retention mode: " .. tostring(value), 3)
end

---@param fmt any
---@param ... any
---@return string
local function formatMessage(fmt, ...)
  if select("#", ...) == 0 then
    return tostring(fmt)
  end

  if type(fmt) ~= "string" then
    error("formatted log messages require a string format", 3)
  end

  return string.format(fmt, ...)
end

---@param path string
local function ensureParentDir(path)
  local dir = fs.getDir(path)
  if dir and dir ~= "" then
    fs.makeDir(dir)
  end
end

---@param path string
---@param line string
local function appendLine(path, line)
  ensureParentDir(path)

  local handle = fs.open(path, "a")
  if not handle then
    error("failed to open log file for append: " .. tostring(path), 3)
  end

  handle.writeLine(line)
  handle.close()
end

---@param level integer
---@return boolean
local function shouldLog(level)
  return level >= state.output.level
end

---@param contents string
---@return string[]
local function splitLines(contents)
  local lines = {}
  if contents == "" then
    return lines
  end

  contents = contents:gsub("\r\n", "\n")
  if contents:sub(-1) == "\n" then
    contents = contents:sub(1, -2)
  end

  for line in contents:gmatch("([^\n]*)\n?") do
    if line == "" and #lines > 0 and contents:sub(-1) ~= "\n" then
      break
    end
    if line ~= "" or #lines > 0 or contents ~= "" then
      lines[#lines + 1] = line
    end
  end

  return lines
end

---@param path string
---@param lines string[]
local function rewriteLines(path, lines)
  ensureParentDir(path)

  local handle = fs.open(path, "w")
  if not handle then
    error("failed to open log file for write: " .. tostring(path), 3)
  end

  for _, line in ipairs(lines) do
    handle.writeLine(line)
  end
  handle.close()
end

local function enforceRetention()
  if state.retention.mode ~= "truncate" then
    return
  end

  local path = state.output.file
  if not fs.exists(path) then
    return
  end

  local maxLines = state.retention.max_lines
  local handle = fs.open(path, "r")
  if not handle then
    error("failed to open log file for retention: " .. tostring(path), 3)
  end

  local contents = handle.readAll() or ""
  handle.close()

  local lines = splitLines(contents)
  if #lines <= maxLines then
    return
  end

  local kept = {}
  local startIndex = #lines - maxLines + 1
  for index = startIndex, #lines do
    kept[#kept + 1] = lines[index]
  end

  rewriteLines(path, kept)
end

---@param epochMs integer
---@return string
local function formatUtcTimestamp(epochMs)
  local totalSeconds = math.floor(epochMs / 1000)
  local second = totalSeconds % 60
  local totalMinutes = math.floor(totalSeconds / 60)
  local minute = totalMinutes % 60
  local totalHours = math.floor(totalMinutes / 60)
  local hour = totalHours % 24
  local totalDays = math.floor(totalHours / 24)

  local z = totalDays + 719468
  local era = math.floor(z / 146097)
  local doe = z - era * 146097
  local yoe = math.floor((doe - math.floor(doe / 1460) + math.floor(doe / 36524) - math.floor(doe / 146096)) / 365)
  local year = yoe + era * 400
  local doy = doe - (365 * yoe + math.floor(yoe / 4) - math.floor(yoe / 100))
  local mp = math.floor((5 * doy + 2) / 153)
  local day = doy - math.floor((153 * mp + 2) / 5) + 1
  local month = mp + 3
  if month > 12 then
    month = month - 12
  end
  year = year + (month <= 2 and 1 or 0)

  return string.format("%04d-%02d-%02d %02d:%02d:%02d UTC", year, month, day, hour, minute, second)
end

---@return string
local function formatTimestamp()
  local epochMs = os.epoch("utc")
  if state.output.timestamp == "epoch" then
    return tostring(epochMs)
  end

  return formatUtcTimestamp(epochMs)
end

---@param level integer
---@param message string
---@return string
local function buildLine(level, message)
  return string.format("[%s] [%s] %s", formatTimestamp(), levelName(level), message)
end

---@param level integer
---@param fmt any
---@param ... any
---@return string
local function write(level, fmt, ...)
  local message = formatMessage(fmt, ...)
  if not shouldLog(level) then
    return message
  end

  local line = buildLine(level, message)
  appendLine(state.output.file, line)
  enforceRetention()

  if state.output.mirror_to_term then
    print(line)
  end

  return message
end

---@param options? LogConfig
---@return LogConfig
function M.config(options)
  if options == nil then
    return {
      output = {
        level = levelName(state.output.level),
        file = state.output.file,
        mirror_to_term = state.output.mirror_to_term,
        timestamp = state.output.timestamp,
      },
      retention = {
        mode = state.retention.mode,
        max_lines = state.retention.max_lines,
      },
    }
  end

  if type(options) ~= "table" then
    error("log.config expects a table or nil", 2)
  end

  if options.output ~= nil then
    if type(options.output) ~= "table" then
      error("log.config output must be a table", 2)
    end

    if options.output.level ~= nil then
      state.output.level = normalizeLevel(options.output.level)
    end

    if options.output.file ~= nil then
      if type(options.output.file) ~= "string" or options.output.file == "" then
        error("log.config output.file must be a non-empty string", 2)
      end
      state.output.file = options.output.file
    end

    if options.output.mirror_to_term ~= nil then
      if type(options.output.mirror_to_term) ~= "boolean" then
        error("log.config output.mirror_to_term must be a boolean", 2)
      end
      state.output.mirror_to_term = options.output.mirror_to_term
    end

    if options.output.timestamp ~= nil then
      state.output.timestamp = normalizeTimestampMode(options.output.timestamp)
    end
  end

  if options.retention ~= nil then
    if type(options.retention) ~= "table" then
      error("log.config retention must be a table", 2)
    end

    if options.retention.mode ~= nil then
      state.retention.mode = normalizeRetentionMode(options.retention.mode)
    end

    if options.retention.max_lines ~= nil then
      if type(options.retention.max_lines) ~= "number" or options.retention.max_lines < 1 then
        error("log.config retention.max_lines must be a positive number", 2)
      end
      state.retention.max_lines = math.floor(options.retention.max_lines)
    end
  end

  return M.config()
end

---@param fmt any
---@param ... any
---@return string
function M.info(fmt, ...)
  return write(LEVELS.info, fmt, ...)
end

---@param fmt any
---@param ... any
---@return string
function M.warn(fmt, ...)
  return write(LEVELS.warn, fmt, ...)
end

---@param fmt any
---@param ... any
---@return string
function M.error(fmt, ...)
  return write(LEVELS.error, fmt, ...)
end

---@param fmt any
---@param ... any
---@return nil
function M.panic(fmt, ...)
  local message = write(LEVELS.panic, fmt, ...)
  error(message, 2)
end

return M