---@class ScheduleConfig
---@field timing { sync_interval_seconds: number }

---Coordinator release schedule state and transitions.
---@class Schedule
---@field paused boolean Whether scheduled releases are currently paused.
---@field interval_seconds number Active release cadence in seconds.
---@field next_release_due_at number|nil Epoch milliseconds when the next scheduled release becomes eligible.
---@field last_release_at number|nil Epoch milliseconds when the last release occurred.
---@field last_release_kind string|nil Last recorded release reason, such as `manual` or `scheduled`.
local Schedule = {}
Schedule.__index = Schedule

---Create a schedule object from config and optional persisted data.
---@param config Config Coordinator timing configuration.
---@param data? Schedule Persisted schedule state.
---@return Schedule
function Schedule:new(config, data)
  local intervalSeconds = config.timing.sync_interval_seconds
  local now = os.epoch("utc")
  local instance = data or {
    paused = false,
    interval_seconds = intervalSeconds,
    next_release_due_at = now + (intervalSeconds * 1000),
    last_release_at = nil,
    last_release_kind = nil,
  }

  instance.interval_seconds = intervalSeconds
  if type(instance.paused) ~= "boolean" then
    instance.paused = false
  end
  if type(instance.next_release_due_at) ~= "number" then
    instance.next_release_due_at = now + (intervalSeconds * 1000)
  end

  return setmetatable(instance, self)
end

---Set the next eligible scheduled release time from the provided base time.
---@param fromAt? number Epoch milliseconds to schedule from. Defaults to current time.
---@return nil
function Schedule:setNextReleaseFrom(fromAt)
  local baseAt = fromAt or os.epoch("utc")
  self.next_release_due_at = baseAt + (self.interval_seconds * 1000)
end

---Pause automatic scheduled releases without changing the existing due time.
---@return nil
function Schedule:pause()
  self.paused = true
end

---Resume automatic releases by resetting the next release out from "now".
---@param now? number Epoch milliseconds to resume from. Defaults to current time.
---@return nil
function Schedule:resumeFromNow(now)
  self.paused = false
  self:setNextReleaseFrom(now or os.epoch("utc"))
end

---Record that a release occurred and advance the next scheduled due time.
---@param kind string Release reason label.
---@param releasedAt? number Epoch milliseconds when the release occurred.
---@return nil
function Schedule:recordRelease(kind, releasedAt)
  local now = releasedAt or os.epoch("utc")
  self.last_release_at = now
  self.last_release_kind = kind
  self:setNextReleaseFrom(now)
end

---Report whether the schedule is currently eligible to trigger a release.
---@param now? number Epoch milliseconds used for the due check.
---@return boolean
function Schedule:isDue(now)
  if self.paused then
    return false
  end

  return type(self.next_release_due_at) == "number" and (now or os.epoch("utc")) >= self.next_release_due_at
end

---Return whole seconds until the next release, clamped at zero when overdue.
---@param now? number Epoch milliseconds used for the remaining-time calculation.
---@return integer|nil
function Schedule:remainingSeconds(now)
  if type(self.next_release_due_at) ~= "number" then
    return nil
  end

  local diffMs = self.next_release_due_at - (now or os.epoch("utc"))
  if diffMs <= 0 then
    return 0
  end

  return math.floor(diffMs / 1000)
end

return Schedule
