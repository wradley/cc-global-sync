-- Config initialization for inventory-coordinator.
-- Runs after cc-pkg installs program files.
--
-- Fresh install: copies the config template to /etc/inventory-coordinator/config.lua.
--   The template defaults are usable as-is; adjust coordinator.id and network settings
--   if needed before starting.
-- Upgrade: detects a version mismatch and informs the operator if manual
--   reconciliation is needed. Fields added in future versions will be handled here.

local CONFIG_PATH = "/etc/inventory-coordinator/config.lua"
local CONFIG_VERSION = 1

local scriptDir = fs.getDir(shell.getRunningProgram())
local installBase = fs.getDir(scriptDir)
local TEMPLATE_PATH = installBase .. "/install/templates/config.lua"

local function readFile(path)
  local f = fs.open(path, "r")
  if not f then return nil end
  local s = f.readAll()
  f.close()
  return s
end

local function writeFile(path, content)
  local dir = fs.getDir(path)
  if dir ~= "" and not fs.exists(dir) then fs.makeDir(dir) end
  local f = fs.open(path, "w")
  f.write(content)
  f.close()
end

local function loadConfig(path)
  local src = readFile(path)
  if not src then return nil, "not found: " .. path end
  local fn, err = load(src, path)
  if not fn then return nil, "parse error: " .. tostring(err) end
  local ok, result = pcall(fn)
  if not ok then return nil, "eval error: " .. tostring(result) end
  if type(result) ~= "table" then return nil, "config must return a table" end
  return result
end

-- Fresh install: no config exists yet.
if not fs.exists(CONFIG_PATH) then
  local template = readFile(TEMPLATE_PATH)
  if not template then
    printError("Config template not found: " .. TEMPLATE_PATH)
    return
  end
  writeFile(CONFIG_PATH, template)
  print("Config written to " .. CONFIG_PATH)
  print("Adjust coordinator.id and network.ender_modem if needed before starting.")
  return
end

-- Upgrade: config exists, check version.
local cfg, err = loadConfig(CONFIG_PATH)
if not cfg then
  printError("Could not read existing config (" .. tostring(err) .. "); leaving it unchanged.")
  return
end

local existingVersion = type(cfg.version) == "number" and cfg.version or 0

if existingVersion < CONFIG_VERSION then
  print("Config is version " .. existingVersion .. "; this release expects version " .. CONFIG_VERSION .. ".")
  print("Review " .. TEMPLATE_PATH .. " for any new required fields and update " .. CONFIG_PATH .. " manually.")
elseif existingVersion == CONFIG_VERSION then
  print("Config is current (version " .. CONFIG_VERSION .. ").")
else
  printError("Config version " .. existingVersion .. " is newer than expected (" .. CONFIG_VERSION .. ").")
  printError("Downgrading may be unsafe. Check your installation.")
end
