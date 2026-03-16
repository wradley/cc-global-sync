# cc-global-sync
CC:Tweaked central coordinator program for the Create warehouse sync system

It has two separate responsibilities:

1. planning
2. release and execution control

Those are intentionally not the same thing.

## High-level model

The coordinator continuously watches warehouse snapshots and keeps an up-to-date transfer plan.

That does **not** mean it immediately tells warehouses to move items.

Actual warehouse execution is gated behind a coordinator-controlled release cycle so the system does not keep dispatching new exports while the previous wave is still unresolved.

## Runtime model

The coordinator now uses a composed runtime model:

- `warehouses`: latest known warehouse records and snapshots
- `schedule`: the release timing object
- `cycle`: the active or most recent execution cycle object
- `latest_plan`: the latest computed plan object
- `latest_transfer_queue`: the latest computed transfer queue object
- `ui`: operator-facing view state

The top-level `state` table is the composition root, while long-lived concepts such as schedule, cycle, plan, and transfer queue own their behavior through narrow public APIs.

Those long-lived domain objects now live under `src/model/`.

## Filesystem layout

Development layout:

- `startup.lua`: development launcher that runs `/src/main.lua`
- `src/main.lua`: runnable coordinator entrypoint
- `src/`: runtime coordinator source code
- `src/app/`: coordinator orchestration and release flow
- `src/model/`: long-lived domain objects such as schedule, cycle, plan, and transfer queue
- `src/infra/`: persistence and external boundaries
- `src/ui/`: coordinator terminal UI
- `src/deps/`: vendored runtime dependencies such as logging
- `install/`: installer, manifest, and config template for this program
- `tst/`: test code and test-only dependencies

Installed layout:

- `/startup.lua`: generated launcher that selects the active installed version and runs `/programs/inventory-coordinator/<version>/src/main.lua`
- `/programs/inventory-coordinator/<version>/src/main.lua`: runnable installed coordinator entrypoint
- `/etc/inventory-coordinator/config.lua`: machine-local coordinator config when installed
- `/var/inventory-coordinator/`: generated persisted state, queue snapshots, and logs when installed

Runtime convention:

- `startup.lua` is only a launcher
- `src/main.lua` is the executable entrypoint
- all other files under `src/` are modules loaded with `require`

## Persistence boundary

Persistence exists so the coordinator can recover from common ComputerCraft restarts or unloads.

It should not own coordinator behavior.

Persistence should:

- save and load serialized coordinator data,
- keep storage paths private to the persistence module,
- allow the coordinator to rebuild runtime objects from loaded data.

The current persistence extraction is transitional. It preserves the old persisted layout first, and later refactor steps will align it with the new object model.

## Network flow

```text
computer/0 coordinator                        computer/2 warehouse
----------------------                        --------------------

heartbeat <---------------------------------- heartbeat broadcast

get_snapshot --------------------------------> reconcile local batch state
                                             -> rebuild local snapshot
snapshot <----------------------------------- targeted snapshot reply

(on release only)
assignment_batch ----------------------------> persist batch
                                             -> execute batch
assignment_ack <----------------------------- acknowledge receipt
assignment_execution <----------------------- report queued work

train_departure_notice <--------------------- report post-execution departures
```

The important distinction is that `get_snapshot` happens continuously, while
`assignment_batch` is only sent during an active release cycle.

`get_snapshot` is also the reconciliation path. If a warehouse rebooted with a
stale persisted batch, the coordinator can now tell it that no active batch
exists and the warehouse will clear that stale local state before replying.

When installed through the versioned launcher flow, the coordinator now loads
config from `/etc/inventory-coordinator/config.lua` and writes state/logs under
`/var/inventory-coordinator/`.

That config file is required and startup now fails loudly if it is missing.

## Layers

### 1. Observation

Warehouse controllers send:

- heartbeats
- snapshots
- assignment acknowledgements
- assignment execution results

The coordinator stores the latest warehouse state and uses it for planning and operator visibility.

### 2. Planning

Planning runs on the `plan_refresh_seconds` interval from `config.lua`.

Planning means:

- aggregate latest accepted warehouse snapshots
- compute target distribution
- build a transfer queue
- persist the latest plan and queue under `/var/inventory-coordinator/`

Planning is only a calculation step.

Planning does **not** send items.

### 3. Release

Release is the moment the coordinator turns the current plan into an executable wave for warehouses.

In the current implementation, release is schedule-driven:

- the coordinator keeps a sync interval in `config.lua`
- when the interval is due, the coordinator releases the current plan if no cycle is active
- `x` forces a one-off sync immediately if no cycle is active
- `p` pauses or resumes the schedule
- `c` manually clears the active cycle if you need an override

If no cycle is active, planning continues and the next due scheduled release may open one automatically.

If a cycle is active, the schedule does not open a second cycle.

### 4. Warehouse execution

Once a cycle is active, the coordinator sends each accepted online warehouse its current `assignment_batch`.

The warehouse controller:

- persists the batch locally
- acknowledges receipt
- executes the batch immediately on receipt
- persists the local execution result
- reports the execution result back to the coordinator

At the moment, execution mostly means queueing Create stock ticker requests for outbound shipments.

If a warehouse is offline during a release, the current design does not try to
re-dispatch mid-cycle automatically. That warehouse instead reconciles its local
state during later snapshot polls and joins again on the next release wave.

## Planning vs release

This distinction is the most important part of the current design.

### Planning

Planning answers:

- what should move if we choose to sync now?
- which warehouse should send what?
- which warehouse should receive what?

Planning is cheap and frequent.

Planning may change every few seconds as snapshots change.

### Release

Release answers:

- are we actually sending this wave now?

Release is intentionally gated.

Release should be much less frequent than planning.

Without that gate, the coordinator would see items disappear from the source warehouse before they arrive at the destination warehouse and would keep generating new shipments from distorted in-transit state.

## Current cycle behavior

An execution cycle is a coordinator-side record for one released wave.

When a cycle is opened:

- the coordinator snapshots the current queue as the released wave
- online accepted warehouses are included in the cycle
- dispatch becomes allowed for that wave

For each warehouse in the cycle, the coordinator tracks:

- whether a batch was sent
- the released `batch_id`
- whether that warehouse reported execution
- how many qualifying train departures have been seen after that execution
- the reported execution status

The summary screen shows cycle progress as `completed/total`.

Right now, a warehouse is considered cycle-complete when:

- the warehouse reported an `assignment_execution` result for the released batch
- and the coordinator has seen the configured number of train departures for that warehouse after execution

By default, the coordinator requires `2` departures per warehouse that had outbound work in the released cycle.

Warehouses with an empty outbound batch require `0` departures and auto-complete for that cycle.

It does **not** mean:

- the train arrived
- packages were unloaded
- destination inventory has reconciled

## Why cycles exist

The cycle is mainly a safety lock.

It prevents the coordinator from repeatedly releasing new execution waves while the previous wave is still operationally unresolved.

This matters especially when:

- a warehouse is offline
- a warehouse misses a batch
- items are already in transit
- an operator wants time to inspect the previous wave before sending another

## What clearing a cycle means

Clearing a cycle with `c` means:

- the coordinator is allowed to release another wave later

Normally, cycles now clear automatically once all participating warehouses satisfy the execution-plus-departure rule.

Manual clear remains available as an override.

Automatic clear still does **not** verify exact item arrival or full inventory reconciliation.

Right now, clearing is an operator decision.

In the future, clearing may instead be driven by a schedule, train-state rules, reservation reconciliation, or other automation.

## Current operator workflow

Typical current flow:

1. Let planning run and inspect the current queue in the coordinator UI.
2. Let the schedule release the next wave automatically, or press `x` for a one-off sync.
3. Watch warehouse execution status and departure progress on the detail screens.
4. Let the cycle auto-clear after each warehouse completes its departure requirement.
5. Use `c` only if you need to override the automatic hold.
6. Use `p` if you want to pause or resume scheduled syncs.

## Terminology

### Warehouse

A local controller computer for one physical warehouse site.

### Snapshot

A warehouse's latest observed local inventory and capacity summary.

### Plan

The coordinator's latest computed desired distribution state from current snapshots.

### Transfer queue

The planning output derived from the plan. It describes proposed warehouse-to-warehouse moves.

### Assignment

A group of item lines to be exported from one source warehouse to one destination warehouse.

### Assignment batch

The full message sent from the coordinator to one source warehouse for the current released wave.

### Batch ID

A stable identifier for the content of one warehouse's assignment batch. The same content should produce the same batch ID.

### Assignment acknowledgement

A warehouse message that confirms it received the batch.

### Assignment execution

A warehouse message reporting what it actually queued from that batch.

### Release

The coordinator action that allows the current planned queue to be sent as executable batches.

### Execution cycle

The coordinator-side record for one released wave of assignments.

### Completed warehouse

A warehouse included in the current cycle that has reported an execution result for its released batch.

## Known limitations

- The coordinator does not yet reserve stock while items are in transit.
- The coordinator does not yet track arrivals or in-transit packages.
- Cycle completion is based on execution plus coarse train-departure signals, not item-level reconciliation.
- Manual clear is still available as an override.
- Planning remains frequent, but execution is intentionally gated.

## Likely next direction

The likely next step after this is refining how a cycle is considered complete:

- keep the schedule-driven release model
- decide whether cycle clearing should stay manual or become rule-driven
- potentially use additional signals to decide when the next release is safe
