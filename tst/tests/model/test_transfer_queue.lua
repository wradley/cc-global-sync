local lu = require("deps.luaunit")
local TransferQueue = require("model.transfer_queue")

local M = {}

function M.testGroupsTransfersIntoAssignmentsBySource()
  local queue = TransferQueue.fromPlan({
    warehouses = {
      alpha = {
        diffs = {
          ["minecraft:stone"] = -4,
          ["minecraft:dirt"] = -2,
        },
      },
      beta = {
        diffs = {
          ["minecraft:stone"] = 4,
          ["minecraft:dirt"] = 2,
        },
      },
    },
  })

  lu.assertEquals(queue.total_transfers, 2)
  lu.assertEquals(queue.total_assignments, 1)
  lu.assertEquals(queue.total_items, 6)
  lu.assertEquals(queue.assignments_by_source.alpha.total_assignments, 1)
  lu.assertEquals(queue.assignments_by_source.alpha.assignments[1].destination, "beta")
  lu.assertEquals(#queue.assignments_by_source.alpha.assignments[1].items, 2)
end

return M
