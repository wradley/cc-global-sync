local function projectRoot()
  local running = shell and shell.getRunningProgram and shell.getRunningProgram() or "tst/run.lua"
  return fs.getDir(fs.getDir(running))
end

local function prependPackagePath(path)
  if not package or type(package.path) ~= "string" then
    error("package.path is unavailable in this environment", 0)
  end

  package.path = table.concat({
    path,
    package.path,
  }, ";")
end

local root = projectRoot()
-- In CC:Tweaked, require resolves relative to the running program, so the test
-- runner must prepend the project roots explicitly for `src/*` and `tst/*`.
prependPackagePath("/"..fs.combine(root, "src/?.lua"))
prependPackagePath("/"..fs.combine(root, "src/?/init.lua"))
prependPackagePath("/"..fs.combine(root, "tst/?.lua"))
prependPackagePath("/"..fs.combine(root, "tst/?/init.lua"))

os.getenv = settings.get
-- LuaUnit expects these globals to exist. Mirror the CC equivalents here.
os.exit = function(code, ...)
  if code == 0 then
    term.setTextColour(colors.green)
    print("Success!")
    term.setTextColour(colors.white)
  else
    printError("Failure!", ...)
  end
end

local lu = require("deps.luaunit")

_G.TestConfigModel = require("tests.model.test_config")
_G.TestScheduleModel = require("tests.model.test_schedule")
_G.TestPlanModel = require("tests.model.test_plan")
_G.TestTransferQueueModel = require("tests.model.test_transfer_queue")
_G.TestCycleModel = require("tests.model.test_cycle")
_G.TestWarehouseRegistryModel = require("tests.model.test_warehouse_registry")
_G.TestReleaseServiceApp = require("tests.app.test_release_service")
_G.TestUiControllerApp = require("tests.app.test_ui_controller")

return os.exit(lu.LuaUnit.run())
