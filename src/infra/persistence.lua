---Coordinator persistence helpers for state, plan, and queue snapshots.
---@class Persistence
local M = {}

-- This module is intentionally limited to storage concerns.
-- It preserves the current on-disk layout while coordinator runtime objects
-- are refactored into narrower domain modules.

local function varDir()
  return "/var/inventory-coordinator"
end

local function statePath()
  return fs.combine(varDir(), "coordinator_state.txt")
end

local function planPath()
  return fs.combine(varDir(), "distribution_plan.txt")
end

local function transferQueuePath()
  return fs.combine(varDir(), "transfer_queue.txt")
end

local function ensureVarDir()
  local path = varDir()
  local parent = fs.getDir(path)
  if parent ~= "" and not fs.exists(parent) then
    fs.makeDir(parent)
  end
  if not fs.exists(path) then
    fs.makeDir(path)
  end
end

---Load persisted coordinator state and rehydrate runtime model objects.
---@param state CoordinatorState
---@param normalizeWarehouseState fun(warehouseState: WarehouseState)
---@param rehydrateSchedule fun(data: table): Schedule
---@param rehydrateCycle fun(data: table): Cycle
---@return nil
function M.loadState(state, normalizeWarehouseState, rehydrateSchedule, rehydrateCycle)
  ensureVarDir()

  local path = statePath()
  if not fs.exists(path) then
    return
  end

  local handle = fs.open(path, "r")
  if not handle then
    error("failed to open persisted state for reading", 0)
  end

  local serialized = handle.readAll()
  handle.close()

  local loaded = textutils.unserialize(serialized)
  if type(loaded) ~= "table" then
    error("failed to unserialize persisted state", 0)
  end

  if type(loaded.warehouses) == "table" then
    state.warehouses = loaded.warehouses
    for _, warehouseState in pairs(state.warehouses) do
      normalizeWarehouseState(warehouseState)
      if warehouseState.state == "pending" and warehouseState.snapshot then
        warehouseState.state = "accepted"
      end
    end
  end

  if type(loaded.schedule) == "table" then
    state.schedule = rehydrateSchedule(loaded.schedule)
  end

  if type(loaded.execution_cycle) == "table" then
    state.execution_cycle = rehydrateCycle(loaded.execution_cycle)
  end
end

---Persist the core coordinator runtime state to disk.
---@param state CoordinatorState
---@return nil
function M.saveState(state)
  ensureVarDir()

  local handle = fs.open(statePath(), "w")
  if not handle then
    error("failed to open persisted state for writing", 0)
  end

  handle.write(textutils.serialize({
    warehouses = state.warehouses,
    schedule = state.schedule,
    execution_cycle = state.execution_cycle,
    saved_at = os.epoch("utc"),
  }))
  handle.close()
end

---Persist the latest computed plan snapshot to disk.
---@param plan Plan
---@param refreshedAt number
---@return nil
function M.savePlan(plan, refreshedAt)
  ensureVarDir()

  local handle = fs.open(planPath(), "w")
  if not handle then
    error("failed to open plan file for writing", 0)
  end

  handle.write(textutils.serialize({
    plan = plan,
    saved_at = refreshedAt,
  }))
  handle.close()
end

---Persist the latest computed transfer queue snapshot to disk.
---@param queue TransferQueue
---@param refreshedAt number
---@return nil
function M.saveTransferQueue(queue, refreshedAt)
  ensureVarDir()

  local handle = fs.open(transferQueuePath(), "w")
  if not handle then
    error("failed to open transfer queue file for writing", 0)
  end

  handle.write(textutils.serialize({
    queue = queue,
    saved_at = refreshedAt,
  }))
  handle.close()
end

return M
