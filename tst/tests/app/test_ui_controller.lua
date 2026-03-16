local lu = require("deps.luaunit")

local M = {}

local originalDetailView
local originalSummaryView
local originalModule

local summaryDraws
local detailDrawResult
local detailDrawCalls

local function resetModule(name)
  if package and package.loaded then
    package.loaded[name] = nil
  end
end

function M:setUp()
  summaryDraws = 0
  detailDrawResult = true
  detailDrawCalls = {}

  originalDetailView = package.loaded["ui.detail_view"]
  originalSummaryView = package.loaded["ui.summary_view"]
  originalModule = package.loaded["ui.controller"]

  package.loaded["ui.detail_view"] = {
    draw = function(state, warehouseId)
      detailDrawCalls[#detailDrawCalls + 1] = {
        state = state,
        warehouse_id = warehouseId,
      }
      return detailDrawResult
    end,
  }
  package.loaded["ui.summary_view"] = {
    draw = function()
      summaryDraws = summaryDraws + 1
    end,
  }

  resetModule("ui.controller")
end

function M:tearDown()
  package.loaded["ui.detail_view"] = originalDetailView
  package.loaded["ui.summary_view"] = originalSummaryView
  package.loaded["ui.controller"] = originalModule
end

function M:testSummaryInputRequestsManualReleaseWhenIdle()
  local controller = require("ui.controller")
  local state = {
    execution_cycle = { active = false },
    ui = {
      view = "summary",
      warehouse_page = "overview",
      release_requested = nil,
    },
  }

  controller.handleInput(state, {}, "x")

  lu.assertEquals(state.ui.release_requested, "manual")
end

function M:testSummaryInputTogglesPauseAndMarksStateDirty()
  local controller = require("ui.controller")
  local resumedAt
  local state = {
    execution_cycle = { active = false },
    schedule = {
      paused = true,
      resumeFromNow = function(_, now)
        resumedAt = now
      end,
      pause = function()
        error("pause should not be called while already paused")
      end,
    },
    state_dirty = false,
    ui = {
      view = "summary",
      warehouse_page = "overview",
    },
  }

  local oldOs = _G.os
  _G.os = setmetatable({
    epoch = function()
      return 1234
    end,
  }, {
    __index = oldOs,
  })

  controller.handleInput(state, {}, "p")

  _G.os = oldOs

  lu.assertEquals(resumedAt, 1234)
  lu.assertTrue(state.state_dirty)
end

function M:testDetailInputRemoveReturnsToSummary()
  local controller = require("ui.controller")
  local removedWarehouseId
  local state = {
    ui = {
      view = "warehouse",
      selected_warehouse_id = "alpha",
      warehouse_page = "overview",
    },
  }
  local stateLib = {
    removeWarehouse = function(_, warehouseId)
      removedWarehouseId = warehouseId
    end,
  }

  controller.handleInput(state, stateLib, "r")

  lu.assertEquals(removedWarehouseId, "alpha")
  lu.assertEquals(state.ui.view, "summary")
  lu.assertNil(state.ui.selected_warehouse_id)
  lu.assertEquals(state.ui.warehouse_page, "overview")
end

function M:testDrawFallsBackToSummaryWhenDetailWarehouseMissing()
  local controller = require("ui.controller")
  local state = {
    ui = {
      view = "warehouse",
      selected_warehouse_id = "missing",
      warehouse_page = "overview",
    },
  }
  detailDrawResult = false

  controller.draw(state)

  lu.assertEquals(#detailDrawCalls, 1)
  lu.assertEquals(detailDrawCalls[1].warehouse_id, "missing")
  lu.assertEquals(summaryDraws, 1)
  lu.assertEquals(state.ui.view, "summary")
  lu.assertNil(state.ui.selected_warehouse_id)
  lu.assertEquals(state.ui.warehouse_page, "overview")
end

function M:testSummaryInputOpensCoordinatorHealthPage()
  local controller = require("ui.controller")
  local state = {
    ui = {
      view = "summary",
      warehouse_page = "overview",
    },
  }

  controller.handleInput(state, {}, "h")

  lu.assertEquals(state.ui.view, "health")
end

function M:testSummaryInputOpensWarehouseDetailOnOverviewPage()
  local controller = require("ui.controller")
  local state = {
    warehouse_registry = {
      listedIds = function()
        return { "alpha", "beta" }
      end,
    },
    ui = {
      view = "summary",
      warehouse_page = "network",
    },
  }

  controller.handleInput(state, {}, "2")

  lu.assertEquals(state.ui.view, "warehouse")
  lu.assertEquals(state.ui.selected_warehouse_id, "beta")
  lu.assertEquals(state.ui.warehouse_page, "overview")
end

return M
