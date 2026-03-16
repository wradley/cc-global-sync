--- Main coordinator entrypoint and event-loop orchestration.

local warehouseRuntime = require("app.runtime")
local releaseService = require("app.release_service")
local log = require("deps.log")
local Cycle = require("model.cycle")
local Config = require("model.config")
local persistence = require("infra.persistence")
local Schedule = require("model.schedule")
local ui = require("ui.controller")

log.config({
    output = {
    file = "/var/inventory-coordinator/coordinator.log",
    level = "info",
    mirror_to_term = false,
    timestamp = "utc",
  },
  retention = {
    mode = "truncate",
    max_lines = 2000,
  },
})

local configOk, configOrError = pcall(Config.load, "/etc/inventory-coordinator/config.lua")
if not configOk then
  log.panic("Failed to load coordinator config: %s", tostring(configOrError))
end

local config = configOrError
log.config(config.logging)
log.info("Coordinator boot starting for %s", config.coordinator.id)

---@type CoordinatorState
local state = warehouseRuntime.new(config)

---Open rednet on the configured modem side if needed.
---@param side string
---@return boolean
local function openRednet(side)
  if not rednet.isOpen(side) then
    rednet.open(side)
  end

  return rednet.isOpen(side)
end

---Try to open all configured rednet modems and report how many succeeded.
---@return integer
local function openConfiguredModems()
  if openRednet(config.network.ender_modem) then
    return 1
  end

  return 0
end

local opened = openConfiguredModems()
if opened == 0 then
  log.panic("Could not open any configured modem for rednet")
end
log.info("Opened %d configured modem(s)", opened)

persistence.loadState(
  state,
  warehouseRuntime.ensureWarehouseState,
  function(data)
    return Schedule:new(config, data)
  end,
  function(data)
    return Cycle:new(data)
  end
)
state.warehouse_registry:bind(state.warehouses)
log.info("Coordinator state loaded; %d warehouse record(s) bound", #state.warehouse_registry:listedIds())
releaseService.refreshPlan(state)
log.info("Initial plan refresh complete")

---Release loop for scheduled syncs.
---@return nil
local function scheduleLoop()
  while true do
    os.sleep(1)
    if not state.execution_cycle.active and state.schedule:isDue(os.epoch("utc")) then
      releaseService.releaseCurrentPlan(state, warehouseRuntime, "scheduled")
    end
  end
end

---Inbound warehouse message loop.
---@return nil
local function messageLoop()
  while true do
    local senderId, message, protocol = rednet.receive(config.network.protocol)
    warehouseRuntime.handleMessage(state, senderId, message, protocol)
  end
end

---Periodic snapshot polling loop.
---@return nil
local function snapshotPollLoop()
  while true do
    for _, warehouseId in ipairs(state.warehouse_registry:sortedIds()) do
      local warehouseState = state.warehouses[warehouseId]
      if warehouseState and warehouseState.state == "accepted" and warehouseState.sender_id then
        rednet.send(warehouseState.sender_id, {
          type = "get_snapshot",
          coordinator_id = config.coordinator.id,
          cycle_active = state.execution_cycle.active or false,
          active_batch_id = releaseService.currentBatchIdForWarehouse(state, warehouseId),
          sent_at = os.epoch("utc"),
        }, config.network.protocol)
      end
    end
    os.sleep(config.timing.snapshot_poll_seconds)
  end
end

---Periodic plan refresh loop.
---@return nil
local function planRefreshLoop()
  while true do
    os.sleep(config.timing.plan_refresh_seconds)
    releaseService.refreshPlan(state)
  end
end

---Redraw on terminal resize events.
---@return nil
local function eventRedrawLoop()
  ui.draw(state)

  while true do
    local event = os.pullEvent()
    if event == "term_resize" then
      ui.draw(state)
    end
  end
end

---Periodic display refresh loop.
---@return nil
local function displayRefreshLoop()
  while true do
    os.sleep(config.timing.display_refresh_seconds)
    ui.draw(state)
  end
end

---Periodic persistence loop for dirty coordinator state.
---@return nil
local function persistenceLoop()
  while true do
    os.sleep(config.timing.persist_seconds)
    if state.state_dirty then
      persistence.saveState(state)
      state.state_dirty = false
    end
  end
end

---Operator input loop for terminal key commands.
---@return nil
local function uiInputLoop()
  while true do
    local _, char = os.pullEvent("char")
    ui.handleInput(state, warehouseRuntime, char)
    if state.ui.release_requested == "manual" then
      releaseService.releaseCurrentPlan(state, warehouseRuntime, "manual")
      state.ui.release_requested = nil
    end
    ui.draw(state)
  end
end

parallel.waitForAny(
  scheduleLoop,
  messageLoop,
  snapshotPollLoop,
  planRefreshLoop,
  eventRedrawLoop,
  displayRefreshLoop,
  persistenceLoop,
  uiInputLoop
)
