local PROGRAM = "inventory-coordinator"
local ROLE = "coordinator"
local VERSION = "0.1.0"
local SOURCE_BASE_URL = "https://raw.githubusercontent.com/wradley/cc-global-sync/refs/tags/v" .. VERSION
local MANIFEST_SOURCE_PATH = "install/manifests/0.1.0.lua"
local GENERATED_STARTUP_MARKER = "-- inventory-coordinator generated launcher"

local function combineUrl(base, path)
  return string.gsub(base, "/+$", "") .. "/" .. string.gsub(path, "^/+", "")
end

local function readRequired(prompt)
  write(prompt)
  local value = read()
  if not value or value == "" then
    error("required value was empty", 0)
  end
  return value
end

local function ensureDir(path)
  if path == "" or fs.exists(path) then
    return
  end

  local parent = fs.getDir(path)
  if parent ~= "" and not fs.exists(parent) then
    ensureDir(parent)
  end
  fs.makeDir(path)
end

local function download(url, path)
  local parent = fs.getDir(path)
  if parent ~= "" then
    ensureDir(parent)
  end

  if fs.exists(path) then
    fs.delete(path)
  end

  if not shell.run("wget", url, path) then
    error("failed to download " .. url, 0)
  end
end

local function startupContents()
  return table.concat({
    GENERATED_STARTUP_MARKER,
    'local versionFile = "/etc/inventory-coordinator/version.txt"',
    'local programRoot = "/programs/inventory-coordinator"',
    'if not fs.exists(versionFile) then',
    '  error("missing active version file: " .. versionFile, 0)',
    "end",
    'local handle = fs.open(versionFile, "r")',
    'if not handle then',
    '  error("failed to open active version file: " .. versionFile, 0)',
    "end",
    'local version = (handle.readAll() or ""):gsub("^%s+", ""):gsub("%s+$", "")',
    "handle.close()",
    'if version == "" then',
    '  error("active version file is empty: " .. versionFile, 0)',
    "end",
    'local entrypointPath = fs.combine(programRoot, fs.combine(version, "src/main.lua"))',
    'if not fs.exists(entrypointPath) then',
    '  error("missing installed entrypoint: " .. entrypointPath, 0)',
    "end",
    "shell.run(entrypointPath)",
    "",
  }, "\n")
end

local function isGeneratedStartup(path)
  if not fs.exists(path) then
    return false
  end

  local handle = fs.open(path, "r")
  if not handle then
    return false
  end

  local contents = handle.readAll() or ""
  handle.close()
  return string.find(contents, GENERATED_STARTUP_MARKER, 1, true) ~= nil
end

local function writeStartup()
  local startupPath = "/startup.lua"
  local backupPath = "/startup.pre-inventory-coordinator.bak.lua"

  if fs.exists(startupPath) and not isGeneratedStartup(startupPath) then
    if fs.exists(backupPath) then
      fs.delete(backupPath)
    end
    fs.move(startupPath, backupPath)
  end

  local handle = fs.open(startupPath, "w")
  if not handle then
    error("failed to open root startup for writing", 0)
  end
  handle.write(startupContents())
  handle.close()
end

local function writeActiveVersion(version)
  ensureDir("/etc/inventory-coordinator")
  local handle = fs.open("/etc/inventory-coordinator/version.txt", "w")
  if not handle then
    error("failed to open version file for writing", 0)
  end
  handle.write(version .. "\n")
  handle.close()
end

local function readActiveVersion()
  local path = "/etc/inventory-coordinator/version.txt"
  if not fs.exists(path) then
    return nil
  end

  local handle = fs.open(path, "r")
  if not handle then
    return nil
  end

  local version = (handle.readAll() or ""):gsub("^%s+", ""):gsub("%s+$", "")
  handle.close()
  return version ~= "" and version or nil
end

local function ensureConfigTemplate(baseUrl, manifest)
  if not manifest.config_template then
    return
  end

  local targetPath = manifest.config_template.path
  if fs.exists(targetPath) then
    return
  end

  download(combineUrl(baseUrl, manifest.config_template.source_path), targetPath)
end

local function loadManifest(baseUrl)
  local tempPath = "/.inventory-coordinator-manifest.tmp.lua"
  download(combineUrl(baseUrl, MANIFEST_SOURCE_PATH), tempPath)
  local manifest = dofile(tempPath)
  fs.delete(tempPath)

  if type(manifest) ~= "table" then
    error("installer manifest did not return a table", 0)
  end
  if manifest.program ~= PROGRAM or manifest.role ~= ROLE or manifest.version ~= VERSION then
    error("installer manifest metadata did not match bootstrap constants", 0)
  end
  if type(manifest.files) ~= "table" or #manifest.files == 0 then
    error("installer manifest files list was empty", 0)
  end

  return manifest
end

local function installFiles(baseUrl, manifest)
  local installRoot = fs.combine("/programs/" .. PROGRAM, VERSION)
  ensureDir(installRoot)

  for _, file in ipairs(manifest.files) do
    if type(file.path) ~= "string" or file.path == "" then
      error("manifest file entry missing path", 0)
    end
    if type(file.source_path) ~= "string" or file.source_path == "" then
      error("manifest file entry missing source_path for " .. file.path, 0)
    end

    download(combineUrl(baseUrl, file.source_path), fs.combine(installRoot, file.path))
  end
end

local function shouldActivate()
  local activeVersion = readActiveVersion()
  if not activeVersion or activeVersion == VERSION then
    return true
  end

  write("Activate " .. PROGRAM .. " " .. VERSION .. " now? [y/N] ")
  local answer = string.lower(read() or "")
  return answer == "y" or answer == "yes"
end

local baseUrl = SOURCE_BASE_URL
if baseUrl == "REPLACE_WITH_PROGRAM_ROOT_RAW_URL" then
  baseUrl = readRequired("Program root raw URL: ")
end

local manifest = loadManifest(baseUrl)
installFiles(baseUrl, manifest)
ensureConfigTemplate(baseUrl, manifest)
writeStartup()

if shouldActivate() then
  writeActiveVersion(VERSION)
  print("Activated " .. PROGRAM .. " " .. VERSION .. " for " .. ROLE .. ".")
else
  print("Installed " .. PROGRAM .. " " .. VERSION .. " for " .. ROLE .. " without changing the active version.")
end
