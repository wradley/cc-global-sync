---@class TransferQueueWarehouseSummary
---@field outgoing_transfers integer
---@field outgoing_items integer
---@field incoming_transfers integer
---@field incoming_items integer

---@class TransferQueueTransfer
---@field transfer_id string
---@field item string
---@field count integer
---@field source string
---@field destination string
---@field reason string
---@field status string

---@class TransferQueueAssignmentItem
---@field name string
---@field count integer
---@field transfer_id string

---@class TransferQueueAssignment
---@field assignment_id string
---@field source string
---@field destination string
---@field reason string
---@field status string
---@field items TransferQueueAssignmentItem[]
---@field total_items integer
---@field line_count integer

---@class TransferQueueSourceEntry
---@field source string
---@field assignments TransferQueueAssignment[]
---@field total_items integer
---@field total_assignments integer

---Coordinator transfer queue state and queue-building logic.
---@class TransferQueue
---@field transfers TransferQueueTransfer[] Flat transfer list for the current plan.
---@field assignments_by_source table<string, TransferQueueSourceEntry> Assignment batches keyed by source warehouse id.
---@field by_warehouse table<string, TransferQueueWarehouseSummary> Aggregate in/out counts keyed by warehouse id.
---@field total_transfers integer Total transfer rows in the queue.
---@field total_assignments integer Total source-to-destination assignments in the queue.
---@field total_items integer Total item count across all transfers.
local TransferQueue = {}
TransferQueue.__index = TransferQueue

local function sortedKeys(map)
  local keys = {}
  for key in pairs(map) do
    keys[#keys + 1] = key
  end
  table.sort(keys)
  return keys
end

---Create a transfer queue object from persisted data or fresh defaults.
---@param data? TransferQueue
---@return TransferQueue
function TransferQueue:new(data)
  local instance = data or {
    transfers = {},
    assignments_by_source = {},
    by_warehouse = {},
    total_transfers = 0,
    total_assignments = 0,
    total_items = 0,
  }

  if type(instance.transfers) ~= "table" then
    instance.transfers = {}
  end
  if type(instance.assignments_by_source) ~= "table" then
    instance.assignments_by_source = {}
  end
  if type(instance.by_warehouse) ~= "table" then
    instance.by_warehouse = {}
  end

  return setmetatable(instance, self)
end

---Build a transfer queue from the latest plan diffs.
---@param plan Plan Latest distribution plan.
---@return TransferQueue
function TransferQueue.fromPlan(plan)
  local queue = TransferQueue:new()
  local items = {}

  for warehouseId, warehousePlan in pairs(plan.warehouses or {}) do
    queue.by_warehouse[warehouseId] = {
      outgoing_transfers = 0,
      outgoing_items = 0,
      incoming_transfers = 0,
      incoming_items = 0,
    }

    for itemName, diff in pairs(warehousePlan.diffs or {}) do
      local itemState = items[itemName] or { sources = {}, destinations = {} }
      if diff < 0 then
        itemState.sources[#itemState.sources + 1] = {
          warehouse_id = warehouseId,
          count = -diff,
        }
      elseif diff > 0 then
        itemState.destinations[#itemState.destinations + 1] = {
          warehouse_id = warehouseId,
          count = diff,
        }
      end
      items[itemName] = itemState
    end
  end

  local transferIndex = 1
  local assignmentIndex = 1
  for _, itemName in ipairs(sortedKeys(items)) do
    local itemState = items[itemName]
    table.sort(itemState.sources, function(left, right)
      return left.warehouse_id < right.warehouse_id
    end)
    table.sort(itemState.destinations, function(left, right)
      return left.warehouse_id < right.warehouse_id
    end)

    local sourceIndex = 1
    local destinationIndex = 1
    while sourceIndex <= #itemState.sources and destinationIndex <= #itemState.destinations do
      local source = itemState.sources[sourceIndex]
      local destination = itemState.destinations[destinationIndex]
      local transferCount = math.min(source.count, destination.count)

      local transfer = {
        transfer_id = string.format("xfer-%05d", transferIndex),
        item = itemName,
        count = transferCount,
        source = source.warehouse_id,
        destination = destination.warehouse_id,
        reason = "rebalance",
        status = "planned",
      }

      queue.transfers[#queue.transfers + 1] = transfer
      queue.total_transfers = queue.total_transfers + 1
      queue.total_items = queue.total_items + transferCount

      local sourceSummary = queue.by_warehouse[source.warehouse_id]
      sourceSummary.outgoing_transfers = sourceSummary.outgoing_transfers + 1
      sourceSummary.outgoing_items = sourceSummary.outgoing_items + transferCount

      local destinationSummary = queue.by_warehouse[destination.warehouse_id]
      destinationSummary.incoming_transfers = destinationSummary.incoming_transfers + 1
      destinationSummary.incoming_items = destinationSummary.incoming_items + transferCount

      source.count = source.count - transferCount
      destination.count = destination.count - transferCount
      transferIndex = transferIndex + 1

      if source.count == 0 then
        sourceIndex = sourceIndex + 1
      end
      if destination.count == 0 then
        destinationIndex = destinationIndex + 1
      end
    end
  end

  local groupedAssignments = {}
  for _, transfer in ipairs(queue.transfers) do
    local sourceAssignments = groupedAssignments[transfer.source]
    if not sourceAssignments then
      sourceAssignments = {}
      groupedAssignments[transfer.source] = sourceAssignments
    end

    local assignment = sourceAssignments[transfer.destination]
    if not assignment then
      assignment = {
        assignment_id = string.format("assign-%05d", assignmentIndex),
        source = transfer.source,
        destination = transfer.destination,
        reason = "rebalance",
        status = "planned",
        items = {},
        total_items = 0,
        line_count = 0,
      }
      sourceAssignments[transfer.destination] = assignment
      queue.total_assignments = queue.total_assignments + 1
      assignmentIndex = assignmentIndex + 1
    end

    assignment.items[#assignment.items + 1] = {
      name = transfer.item,
      count = transfer.count,
      transfer_id = transfer.transfer_id,
    }
    assignment.total_items = assignment.total_items + transfer.count
    assignment.line_count = assignment.line_count + 1
  end

  for source, destinations in pairs(groupedAssignments) do
    local sourceEntry = {
      source = source,
      assignments = {},
      total_items = 0,
      total_assignments = 0,
    }

    for _, destination in ipairs(sortedKeys(destinations)) do
      local assignment = destinations[destination]
      table.sort(assignment.items, function(left, right)
        if left.count == right.count then
          return left.name < right.name
        end
        return left.count > right.count
      end)

      sourceEntry.assignments[#sourceEntry.assignments + 1] = assignment
      sourceEntry.total_items = sourceEntry.total_items + assignment.total_items
      sourceEntry.total_assignments = sourceEntry.total_assignments + 1
    end

    queue.assignments_by_source[source] = sourceEntry
  end

  return queue
end

return TransferQueue
