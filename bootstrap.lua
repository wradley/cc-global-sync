-- Stable installer entrypoint for wget run.
-- Uses the baked-in release tag by default, or `-b <branch>` / `-c <commit>`
-- to fetch and run that source's real installer. All other args pass through.
local PROGRAM = "inventory-coordinator"
local DEFAULT_VERSION = "0.1.1"
local REPO_RAW_BASE_URL = "https://raw.githubusercontent.com/wradley/cc-global-sync"
local INSTALLER_SOURCE_PATH = "install/installer.lua"
local TEMP_INSTALLER_PATH = "/.inventory-coordinator-installer.tmp.lua"

local function combineUrl(base, path)
  return string.gsub(base, "/+$", "") .. "/" .. string.gsub(path, "^/+", "")
end

local function parseArgs(argv)
  local options = {
    ref_type = "tag",
    ref_value = "v" .. DEFAULT_VERSION,
    passthrough_args = {},
  }

  local function setRef(refType, refValue)
    if options.ref_type ~= "tag" or options.ref_value ~= "v" .. DEFAULT_VERSION then
      error("choose only one of -b or -c", 0)
    end
    options.ref_type = refType
    options.ref_value = refValue
  end

  local index = 1
  while index <= #argv do
    local arg = argv[index]

    if arg == "-b" then
      local value = argv[index + 1]
      if not value or value == "" then
        error("missing branch name after -b", 0)
      end
      setRef("branch", value)
      index = index + 2
    elseif arg == "-c" then
      local value = argv[index + 1]
      if not value or value == "" then
        error("missing commit hash after -c", 0)
      end
      setRef("commit", value)
      index = index + 2
    else
      table.insert(options.passthrough_args, arg)
      index = index + 1
    end
  end

  return options
end

local function resolveBaseUrl(options)
  if options.ref_type == "branch" then
    return combineUrl(REPO_RAW_BASE_URL, "refs/heads/" .. options.ref_value)
  end
  if options.ref_type == "commit" then
    return combineUrl(REPO_RAW_BASE_URL, options.ref_value)
  end
  return combineUrl(REPO_RAW_BASE_URL, "refs/tags/" .. options.ref_value)
end

local function download(url, path)
  if fs.exists(path) then
    fs.delete(path)
  end

  if not shell.run("wget", url, path) then
    error("failed to download " .. url, 0)
  end
end

local function runInstaller(path, baseUrl, passthroughArgs)
  local installerArgs = { "--source-base-url", baseUrl }
  for _, arg in ipairs(passthroughArgs) do
    table.insert(installerArgs, arg)
  end

  local ok, err = shell.run(path, table.unpack(installerArgs))

  if not ok then
    error(err or ("installer failed for " .. PROGRAM), 0)
  end
end

local options = parseArgs({ ... })
local baseUrl = resolveBaseUrl(options)
local installerUrl = combineUrl(baseUrl, INSTALLER_SOURCE_PATH)

download(installerUrl, TEMP_INSTALLER_PATH)
local ok, err = pcall(runInstaller, TEMP_INSTALLER_PATH, baseUrl, options.passthrough_args)
if fs.exists(TEMP_INSTALLER_PATH) then
  fs.delete(TEMP_INSTALLER_PATH)
end
if not ok then
  error(err, 0)
end
