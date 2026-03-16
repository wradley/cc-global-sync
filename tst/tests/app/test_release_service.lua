local lu = require("deps.luaunit")
local ccEnv = require("support.cc_test_env")
local Config = require("model.config")

local M = {}

local originalLog
local originalPersistence
local originalModule

local savedPlans
local savedQueues
local logMessages

local function resetModule(name)
  if package and package.loaded then
    package.loaded[name] = nil
  end
end

function M:setUp()
  ccEnv.install({ epoch = 5000 })

  savedPlans = {}
  savedQueues = {}
  logMessages = { info = {}, warn = {} }

  originalLog = package.loaded["deps.log"]
  originalPersistence = package.loaded["infra.persistence"]
  originalModule = package.loaded["app.release_service"]

  package.loaded["deps.log"] = {
    info = function(fmt, ...)
      logMessages.info[#logMessages.info + 1] = string.format(fmt, ...)
    end,
    warn = function(fmt, ...)
      logMessages.warn[#logMessages.warn + 1] = string.format(fmt, ...)
    end,
  }
  package.loaded["infra.persistence"] = {
    savePlan = function(plan, refreshedAt)
      savedPlans[#savedPlans + 1] = {
        plan = plan,
        refreshed_at = refreshedAt,
      }
    end,
    saveTransferQueue = function(queue, refreshedAt)
      savedQueues[#savedQueues + 1] = {
        queue = queue,
        refreshed_at = refreshedAt,
      }
    end,
  }
  resetModule("app.release_service")
end

function M:tearDown()
  ccEnv.restore()
  package.loaded["deps.log"] = originalLog
  package.loaded["infra.persistence"] = originalPersistence
  package.loaded["app.release_service"] = originalModule
end

function M:testRefreshPlanBuildsAndPersistsArtifacts()
  local releaseService = require("app.release_service")
  local state = {
    warehouse_registry = {
      plannableWarehouses = function()
        return {
          {
            warehouse_id = "alpha",
            snapshot = {
              inventory = {
                ["minecraft:stone"] = 8,
              },
              capacity = {
                slot_capacity_total = 10,
              },
            },
          },
          {
            warehouse_id = "beta",
            snapshot = {
              inventory = {},
              capacity = {
                slot_capacity_total = 10,
              },
            },
          },
        }
      end,
    },
  }

  releaseService.refreshPlan(state)

  lu.assertEquals(state.last_plan_refresh_at, 5000)
  lu.assertEquals(state.latest_plan.total_capacity, 20)
  lu.assertEquals(state.latest_transfer_queue.total_transfers, 1)
  lu.assertEquals(#savedPlans, 1)
  lu.assertEquals(#savedQueues, 1)
  lu.assertEquals(savedPlans[1].refreshed_at, 5000)
  lu.assertEquals(savedQueues[1].refreshed_at, 5000)
end

function M:testReleaseCurrentPlanRecordsScheduleAndDispatchesAssignments()
  local releaseService = require("app.release_service")
  local config = Config.default()
  local recordReleaseCalls = {}
  local markedBatches = {}
  local state = {
    config = config,
    latest_transfer_queue = {
      assignments_by_source = {
        alpha = {
          source = "alpha",
          assignments = {
            {
              assignment_id = "assign-1",
              source = "alpha",
              destination = "beta",
              reason = "rebalance",
              status = "planned",
              items = {
                { name = "minecraft:stone", count = 2, transfer_id = "xfer-1" },
              },
              total_items = 2,
              line_count = 1,
            },
          },
          total_items = 2,
          total_assignments = 1,
        },
      },
    },
    execution_cycle = {
      active = false,
    },
    warehouses = {
      alpha = {
        state = "accepted",
        sender_id = 17,
        warehouse_address = "A1",
      },
      beta = {
        state = "accepted",
        sender_id = 18,
        warehouse_address = "B1",
      },
    },
    warehouse_registry = {
      sortedIds = function()
        return { "alpha", "beta" }
      end,
      isOnline = function(_, warehouseState)
        return warehouseState.sender_id == 17
      end,
    },
    schedule = {
      recordRelease = function(_, kind, releasedAt)
        recordReleaseCalls[#recordReleaseCalls + 1] = {
          kind = kind,
          released_at = releasedAt,
        }
      end,
    },
    state_dirty = false,
  }
  local warehouseRuntime = {
    beginExecutionCycle = function(runtimeState, queue)
      runtimeState.execution_cycle = {
        active = true,
        released_queue = queue,
        plan_refreshed_at = 4321,
      }
      return true
    end,
    markCycleBatchSent = function(_, warehouseId, batchId)
      markedBatches[#markedBatches + 1] = {
        warehouse_id = warehouseId,
        batch_id = batchId,
      }
    end,
  }

  lu.assertTrue(releaseService.releaseCurrentPlan(state, warehouseRuntime, "manual"))

  local sent = ccEnv.getSentMessages()
  lu.assertEquals(#recordReleaseCalls, 1)
  lu.assertEquals(recordReleaseCalls[1].kind, "manual")
  lu.assertEquals(#sent, 1)
  lu.assertEquals(sent[1].target_id, 17)
  lu.assertEquals(sent[1].message.type, "assignment_batch")
  lu.assertEquals(sent[1].message.plan_refreshed_at, 4321)
  lu.assertEquals(#markedBatches, 1)
  lu.assertEquals(markedBatches[1].warehouse_id, "alpha")
  lu.assertTrue(state.state_dirty)
end

return M
