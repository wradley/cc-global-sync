local lu = require("deps.luaunit")
local ccEnv = require("support.cc_test_env")
local Config = require("model.config")
local Schedule = require("model.schedule")

local M = {}

function M:setUp()
  ccEnv.install({ epoch = 10000 })
end

function M:tearDown()
  ccEnv.restore()
end

function M:testRemainingSecondsClampsAtZero()
  local schedule = Schedule:new(Config.default())

  ccEnv.setEpoch(schedule.next_release_due_at + 5000)

  lu.assertEquals(schedule:remainingSeconds(), 0)
  lu.assertTrue(schedule:isDue())
end

return M
