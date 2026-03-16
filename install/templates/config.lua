return {
  version = 1,
  coordinator = {
    id = "central",
    display_name = "Central Coordinator",
  },
  network = {
    ender_modem = "top",
    protocol = "warehouse_sync_v1",
    heartbeat_timeout_seconds = 30,
  },
  timing = {
    display_refresh_seconds = 1,
    snapshot_poll_seconds = 10,
    plan_refresh_seconds = 10,
    sync_interval_seconds = 10 * 60,
    persist_seconds = 5,
  },
  execution = {
    departures_required_per_warehouse = 2,
  },
  logging = {
    output = {
      file = "/var/inventory-coordinator/coordinator.log",
      level = "info",
      mirror_to_term = false,
      timestamp = "utc",
    },
    retention = {
      mode = "truncate",
      max_lines = 1000,
    },
  },
}
