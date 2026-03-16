local fmt = require("ui.fmt")

---Warehouse detail screen renderer for the coordinator UI.
---@class DetailView
local M = {}

local function printPageFooter(state, warehouseState)
  print("")
  if warehouseState.state == "pending" then
    print("[a] accept [r] remove [b] back")
  elseif warehouseState.state == "accepted" then
    print("[r] remove [b] back")
  else
    print("[b] back")
  end
  print("[o] overview [e] exec [n] net")
  print("[m] main [h] health [g] config")
end

local function drawOverviewPage(state, warehouseId, warehouseState, snapshot, warehousePlan, warehouseQueue)
  term.clear()
  term.setCursorPos(1, 1)
  print("Warehouse Overview")
  print(warehouseId .. " (" .. fmt.stateLabel(state, warehouseState) .. ")")
  print("Addr: " .. tostring(warehouseState.warehouse_address))
  print("Computer: " .. tostring(warehouseState.sender_id))
  if snapshot then
    local itemTypes = 0
    for _ in pairs(snapshot.inventory or {}) do
      itemTypes = itemTypes + 1
    end
    print("Items: " .. tostring(itemTypes))
    print("Slots: " .. fmt.formatSlotsLine(snapshot))
    print("Observed: " .. fmt.ageFromEpoch(snapshot.observed_at))
  else
    print("Snapshot: none")
    print("Slots: unknown")
    print("Observed: never")
  end

  if warehousePlan then
    print("Target: " .. tostring(warehousePlan.target_share_percent) .. "%")
    print("Plan recv: " .. tostring(warehousePlan.planned_receive_count))
    print("Plan send: " .. tostring(warehousePlan.planned_send_count))
  else
    print("Target: unavailable")
    print("Plan recv: unavailable")
    print("Plan send: unavailable")
  end

  if warehouseQueue then
    print("Queue in: " .. tostring(warehouseQueue.incoming_transfers) .. " / " .. tostring(warehouseQueue.incoming_items))
    print("Queue out: " .. tostring(warehouseQueue.outgoing_transfers) .. " / " .. tostring(warehouseQueue.outgoing_items))
  else
    print("Queue in: unavailable")
    print("Queue out: unavailable")
  end

  printPageFooter(state, warehouseState)
end

local function drawExecutionPage(warehouseState, cycleEntry)
  local execution = warehouseState.last_assignment_execution

  term.clear()
  term.setCursorPos(1, 1)
  print("Warehouse Execution")
  print(tostring(warehouseState.warehouse_id or "?"))
  print("Last batch: " .. tostring(warehouseState.last_assignment_sent_batch_id or "none"))
  print("Sent tx: " .. tostring(warehouseState.last_assignment_count or 0))
  print("Sent items: " .. tostring(warehouseState.last_assignment_item_count or 0))
  print("Ack batch: " .. tostring(warehouseState.last_assignment_ack_batch_id or "none"))
  print("Exec batch: " .. tostring(warehouseState.last_assignment_execution_batch_id or "none"))
  if execution then
    print("Exec status: " .. tostring(execution.status))
    print("Queued: " .. tostring(execution.total_items_queued or 0) .. "/" .. tostring(execution.total_items_requested or 0))
  else
    print("Exec status: none")
    print("Queued: n/a")
  end

  if cycleEntry then
    print("Cycle dep: " .. tostring(cycleEntry.departures_seen or 0) .. "/" .. tostring(cycleEntry.required_departures or 0))
    print("Cycle done: " .. tostring(cycleEntry.completed))
    print("Cycle exec: " .. tostring(cycleEntry.execution_reported))
    print("Last train: " .. tostring(cycleEntry.last_train_name or "none"))
  else
    print("Cycle dep: n/a")
    print("Cycle done: n/a")
    print("Cycle exec: n/a")
    print("Last train: n/a")
  end

  print("")
  if warehouseState.state == "pending" then
    print("[a] accept [r] remove [b] back")
  elseif warehouseState.state == "accepted" then
    print("[r] remove [b] back")
  else
    print("[b] back")
  end
  print("[o] overview [e] exec [n] net")
  print("[m] main [h] health [g] config")
end

local function drawNetworkPage(state, warehouseState)
  local heartbeatAge = state.warehouse_registry:heartbeatAgeSeconds(warehouseState)
  local snapshotAge = state.warehouse_registry:snapshotAgeSeconds(warehouseState)
  local assignmentAckAge = state.warehouse_registry:assignmentAckAgeSeconds(warehouseState)
  local assignmentExecutionAge = state.warehouse_registry:assignmentExecutionAgeSeconds(warehouseState)
  local trainDepartureAge = state.warehouse_registry:trainDepartureAgeSeconds(warehouseState)

  term.clear()
  term.setCursorPos(1, 1)
  print("Warehouse Network")
  print(tostring(warehouseState.warehouse_id or "?"))
  print("State: " .. tostring(warehouseState.state))
  print("Online: " .. tostring(state.warehouse_registry:isOnline(warehouseState)))
  print("Computer: " .. tostring(warehouseState.sender_id))
  print("Heartbeat: " .. (heartbeatAge and (fmt.formatElapsed(heartbeatAge) .. " ago") or "never"))
  print("Snapshot: " .. (snapshotAge and (fmt.formatElapsed(snapshotAge) .. " ago") or "never"))
  print("Ack: " .. (assignmentAckAge and (fmt.formatElapsed(assignmentAckAge) .. " ago") or "never"))
  print("Exec: " .. (assignmentExecutionAge and (fmt.formatElapsed(assignmentExecutionAge) .. " ago") or "never"))
  print("Departure: " .. (trainDepartureAge and (fmt.formatElapsed(trainDepartureAge) .. " ago") or "never"))
  print("Address: " .. tostring(warehouseState.warehouse_address))
  print("")
  if warehouseState.state == "pending" then
    print("[a] accept [r] remove [b] back")
  elseif warehouseState.state == "accepted" then
    print("[r] remove [b] back")
  else
    print("[b] back")
  end
  print("[o] overview [e] exec [n] net")
  print("[m] main [h] health [g] config")
end

---Draw the detail screen for one warehouse, or fall back to summary if missing.
---@param state CoordinatorState
---@param warehouseId string
---@return boolean drawn True when the requested warehouse existed.
function M.draw(state, warehouseId)
  local warehouseState = state.warehouses[warehouseId]
  if not warehouseState then
    return false
  end

  local snapshot = warehouseState.snapshot
  local warehousePlan = state.latest_plan and state.latest_plan.warehouses and state.latest_plan.warehouses[warehouseId]
  local warehouseQueue = state.latest_transfer_queue and state.latest_transfer_queue.by_warehouse and state.latest_transfer_queue.by_warehouse[warehouseId]
  local cycleEntry = state.execution_cycle and state.execution_cycle.warehouses and state.execution_cycle.warehouses[warehouseId]

  if state.ui.warehouse_page == "network" then
    drawNetworkPage(state, warehouseState)
    return true
  end

  if state.ui.warehouse_page == "execution" then
    drawExecutionPage(warehouseState, cycleEntry)
    return true
  end

  drawOverviewPage(state, warehouseId, warehouseState, snapshot, warehousePlan, warehouseQueue)
  return true
end

return M
