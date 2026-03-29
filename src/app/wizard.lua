---Peripheral setup wizard for inventory-coordinator.
---Runs interactively on first boot to discover the ender modem and write the config.
local Wizard = {}

--------------------------------------------------------------------------------
-- Filesystem helpers
--------------------------------------------------------------------------------

local function writeFile(path, content)
  local dir = fs.getDir(path)
  if dir ~= "" and not fs.exists(dir) then fs.makeDir(dir) end
  local f = fs.open(path, "w")
  f.write(content)
  f.close()
end

--------------------------------------------------------------------------------
-- Peripheral scanning
--------------------------------------------------------------------------------

---Scan all sides for modems, returning separate wired and wireless lists.
---@return string[] wiredSides
---@return string[] wirelessSides
local function scanModems()
  local wired, wireless = {}, {}
  for _, side in ipairs({"top", "bottom", "left", "right", "front", "back"}) do
    local p = peripheral.wrap(side)
    if p and type(p.isWireless) == "function" then
      if p.isWireless() then
        wireless[#wireless + 1] = side
      else
        wired[#wired + 1] = side
      end
    end
  end
  return wired, wireless
end

--------------------------------------------------------------------------------
-- UI helpers
--------------------------------------------------------------------------------

local function section(n, total, title)
  print("")
  local w = term.getSize()
  print(string.rep("-", w))
  if n > 0 then
    print(string.format("  %d / %d  %s", n, total, title))
  else
    print("  " .. title)
  end
  print(string.rep("-", w))
end

---@param label string
---@param default string|nil
---@return string
local function prompt(label, default)
  if default and default ~= "" then
    write("  " .. label .. " [" .. default .. "]: ")
    local v = read(nil, nil, nil, default)
    if not v or v == "" then return default end
    return v
  else
    while true do
      write("  " .. label .. ": ")
      local v = read()
      if v and v ~= "" then return v end
      printError("  Required.")
    end
  end
end

---@param question string
---@param defaultYes boolean
---@return boolean
local function yesno(question, defaultYes)
  local hint = defaultYes and "[Y/n]" or "[y/N]"
  write("  " .. question .. " " .. hint .. " ")
  local v = string.lower(read() or "")
  if v == "" then return defaultYes end
  return v == "y" or v == "yes"
end

---Poll until at least one item is found or the user presses a key to skip.
---@param label string
---@param scanFn fun(): string[]
---@return string[]
local function waitForPeripherals(label, scanFn)
  local found = scanFn()
  if #found > 0 then return found end

  print("  No " .. label .. " found. Connect it, then wait for detection.")
  term.write("  Scanning (any key to enter manually): ")

  local result = {}
  parallel.waitForAny(
    function()
      while true do
        os.sleep(1)
        result = scanFn()
        if #result > 0 then return end
        term.write(".")
      end
    end,
    function()
      os.pullEvent("key")
    end
  )
  print("")
  return result
end

---Pick a modem side from detected candidates or fall back to manual entry.
---@param label string
---@param found string[]
---@param required boolean
---@return string
local function pickSide(label, found, required)
  if #found == 1 then
    return prompt(label .. " side", found[1])
  elseif #found > 1 then
    print("  Multiple candidates found:")
    for i, side in ipairs(found) do
      print(string.format("    %d)  %s", i, side))
    end
    while true do
      write("  Choose number or type side name: ")
      local v = read()
      local n = tonumber(v)
      if n and n >= 1 and n <= #found then return found[n] end
      if v and v ~= "" then return v end
    end
  else
    if required then
      printError("  No modem found. Enter side manually.")
    end
    return prompt(label .. " side (top/bottom/left/right/front/back)", "")
  end
end

--------------------------------------------------------------------------------
-- Config serializer
--------------------------------------------------------------------------------

local function serializeVal(v, indent)
  local t = type(v)
  if t == "string" then
    return string.format("%q", v)
  elseif t == "number" or t == "boolean" then
    return tostring(v)
  elseif t == "table" then
    local inner = indent .. "  "
    local lines = {}
    if #v > 0 then
      for _, item in ipairs(v) do
        lines[#lines + 1] = inner .. serializeVal(item, inner)
      end
    else
      local keys = {}
      for k in pairs(v) do keys[#keys + 1] = k end
      table.sort(keys, function(a, b) return tostring(a) < tostring(b) end)
      for _, k in ipairs(keys) do
        local keyStr = type(k) == "string" and k or ("[" .. tostring(k) .. "]")
        lines[#lines + 1] = inner .. keyStr .. " = " .. serializeVal(v[k], inner)
      end
    end
    if #lines == 0 then return "{}" end
    return "{\n" .. table.concat(lines, ",\n") .. ",\n" .. indent .. "}"
  end
  return tostring(v)
end

--------------------------------------------------------------------------------
-- Wizard entry point
--------------------------------------------------------------------------------

---Run the peripheral setup wizard and write the config to configPath.
---Returns true if the config was written, false if the user cancelled.
---@param configPath string
---@return boolean
function Wizard.run(configPath)
  print("")
  print("=== inventory-coordinator Setup Wizard ===")
  print("Scans for connected peripherals.")

  --------------------------------------------------------------------------
  -- 1 / 2  Coordinator Identity
  --------------------------------------------------------------------------
  section(1, 2, "Coordinator Identity")

  local coordId    = prompt("ID (e.g. central)", "central")
  local nameDefault = coordId:sub(1,1):upper() .. coordId:sub(2) .. " Coordinator"
  local displayName = prompt("Display name", nameDefault)

  --------------------------------------------------------------------------
  -- 2 / 2  Ender Modem
  --------------------------------------------------------------------------
  section(2, 2, "Ender Modem")

  print("  Ender modem (warehouse communication):")
  local wirelessSides = waitForPeripherals("ender modem", function()
    local _, w = scanModems(); return w
  end)
  local enderModem = pickSide("Ender modem", wirelessSides, true)

  --------------------------------------------------------------------------
  -- Review
  --------------------------------------------------------------------------
  section(0, 0, "Review")

  print("  coordinator.id           = " .. coordId)
  print("  coordinator.display_name = " .. displayName)
  print("  network.ender_modem      = " .. enderModem)

  print("")
  if not yesno("Write config to " .. configPath .. "?", true) then
    print("Cancelled. Run inventory-coordinator again to configure.")
    return false
  end

  --------------------------------------------------------------------------
  -- Build and write config
  --------------------------------------------------------------------------
  local config = {
    version = 1,
    coordinator = {
      id           = coordId,
      display_name = displayName,
    },
    network = {
      ender_modem              = enderModem,
      protocol                 = "warehouse_sync_v1",
      heartbeat_timeout_seconds = 30,
    },
    timing = {
      display_refresh_seconds = 1,
      snapshot_poll_seconds   = 10,
      plan_refresh_seconds    = 10,
      sync_interval_seconds   = 600,
      persist_seconds         = 5,
    },
    logging = {
      output = {
        file           = "/var/inventory-coordinator/coordinator.log",
        level          = "info",
        mirror_to_term = false,
        timestamp      = "utc",
      },
      retention = {
        mode      = "truncate",
        max_lines = 1000,
      },
    },
  }

  writeFile(configPath, "return " .. serializeVal(config, "") .. "\n")
  print("")
  print("Config written to " .. configPath)

  --------------------------------------------------------------------------
  -- Startup registration (optional)
  --------------------------------------------------------------------------
  print("")
  if yesno("Start inventory-coordinator at boot?", true) then
    local STARTUP = "/startup.lua"
    local line    = 'shell.run("/bin/inventory-coordinator")\n'
    local existing = ""
    local f = fs.open(STARTUP, "r")
    if f then existing = f.readAll(); f.close() end
    if not existing:find('inventory%-coordinator', 1, false) then
      local out = fs.open(STARTUP, "w")
      out.write(existing)
      if existing ~= "" and existing:sub(-1) ~= "\n" then out.write("\n") end
      out.write(line)
      out.close()
      print("Added to " .. STARTUP)
    else
      print("Already present in " .. STARTUP .. " — skipped.")
    end
  end

  return true
end

return Wizard
