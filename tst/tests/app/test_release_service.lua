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

local function resetRednetContracts()
  resetModule("rednet_contracts")
  resetModule("rednet_contracts.init")
  resetModule("rednet_contracts.errors")
  resetModule("rednet_contracts.schema_validation")
  resetModule("rednet_contracts.mrpc_v1")
  resetModule("rednet_contracts.services.warehouse_v1")
end

local function queueWarehouseResponse(message)
  ccEnv.queueRednetReceive(17, message, "rc.mrpc_v1")
end

local function assignmentBatchId(warehouseId, sourceEntry)
  local serialized = textutils.serialize({
    warehouse_id = warehouseId,
    total_assignments = sourceEntry.total_assignments or 0,
    total_items = sourceEntry.total_items or 0,
    assignments = sourceEntry.assignments or {},
  })

  local checksum = 0
  for index = 1, #serialized do
    checksum = (checksum + (string.byte(serialized, index) * index)) % 2147483647
  end

  return string.format("%s:%d:%d:%d", warehouseId, sourceEntry.total_assignments or 0, sourceEntry.total_items or 0, checksum)
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
  resetRednetContracts()
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
  local state
  state = {
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
      observeAssignmentAck = function(_, senderId, message)
        state.warehouses.alpha.last_assignment_ack_batch_id = message.transfer_request_id
        state.warehouses.alpha.last_assignment_ack = message
        state.warehouses.alpha.last_assignment_ack_at = message.sent_at
      end,
      observeTransferRequestStatus = function(_, senderId, message)
        state.warehouses.alpha.last_assignment_execution_batch_id = message.transfer_request_id
        state.warehouses.alpha.last_assignment_execution = {
          batch_id = message.transfer_request_id,
          status = message.status,
          total_items_requested = message.total_items_requested,
          total_items_queued = message.total_items_queued,
        }
        state.warehouses.alpha.last_assignment_execution_at = message.sent_at
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
  local expectedBatchId = assignmentBatchId("alpha", state.latest_transfer_queue.assignments_by_source.alpha)

  queueWarehouseResponse({
    type = "response",
    protocol = {
      name = "warehouse",
      version = 1,
    },
    request_id = "req-5000-1",
    ok = true,
    result = {
      warehouse_id = "alpha",
      warehouse_address = "A1",
      transfer_request_id = expectedBatchId,
      assignment_count = 1,
      item_count = 2,
      accepted = true,
      sent_at = 5000,
    },
    sent_at = 5000,
  })
  queueWarehouseResponse({
    type = "response",
    protocol = {
      name = "warehouse",
      version = 1,
    },
    request_id = "req-5000-2",
    ok = true,
    result = {
      warehouse_id = "alpha",
      warehouse_address = "A1",
      transfer_request_id = expectedBatchId,
      status = "queued",
      executed_at = 5000,
      total_assignments = 1,
      total_items_requested = 2,
      total_items_queued = 2,
      assignments = {},
      packages = {
        ["in"] = {},
        ["out"] = {},
      },
      sent_at = 5000,
    },
    sent_at = 5000,
  })

  lu.assertTrue(releaseService.releaseCurrentPlan(state, warehouseRuntime, "manual"))

  local sent = ccEnv.getSentMessages()
  lu.assertEquals(#recordReleaseCalls, 1)
  lu.assertEquals(recordReleaseCalls[1].kind, "manual")
  lu.assertEquals(#sent, 2)
  lu.assertEquals(sent[1].target_id, 17)
  lu.assertEquals(sent[1].message.type, "request")
  lu.assertEquals(sent[1].message.method, "assign_transfer_request")
  lu.assertEquals(sent[1].message.params.warehouse_id, "alpha")
  lu.assertEquals(sent[2].message.method, "get_transfer_request_status")
  lu.assertEquals(#markedBatches, 1)
  lu.assertEquals(markedBatches[1].warehouse_id, "alpha")
  lu.assertEquals(state.warehouses.alpha.last_assignment_ack_batch_id, expectedBatchId)
  lu.assertEquals(state.warehouses.alpha.last_assignment_execution.status, "queued")
  lu.assertTrue(state.state_dirty)
end

return M
