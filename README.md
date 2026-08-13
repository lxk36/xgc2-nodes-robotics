# xgc2-nodes-robotics

Open XGC orchestration nodes for robotics, simulation, fleets, and experiment
automation. This pack depends only on `xgc2-orchestration-core`; it does not
import XGC2 product internals.

Current nodes:

- `xgc.robotics.fleet-spec/v1` normalizes a heterogeneous fleet;
- `xgc.robotics.topology-assert/v1` fails before mutation unless the actual
  PX4/Scout/mecanum topology exactly matches the authored profile;
- `xgc.robotics.runtime-topology-assert/v1` consumes real ready-process
  Receipts, rejects duplicate binding/external identities, and proves exact
  robot-kind counts before an experiment enters Hold;
- `xgc.robotics.process-launch/v1` emits a Run-owned managed-process Effect,
  waits for
  immutable provider Receipts, and folds the terminal result through pure
  `Resume` without re-running the effectful node; the core compensates applied
  launches in reverse order on normal completion or failure;
- `xgc.robotics.process-stop/v1` consumes only the prior external identity
  reference and producing Run owner, while the private provider boundary
  restores the original spec and exact PID/PGID/start-tick identity; and
- `xgc.robotics.fs150-model-spawn/v1` emits one finite structured Render+Spawn
  Effect while keeping SDF paths, helper scripts, and ROS environment private
  to the installed product provider.

Executable paths, argument/environment values, working directories, log paths,
idempotency keys, and capability tokens are resolved behind the core process
adapter. They are never present in the public node input or durable workflow
state.

Linux acceptance tests under `acceptance/` execute both required profiles twice
continuously through the public orchestration core and local process provider:

- E1: exactly six PX4 plus four Scout targets, with 20 real starts, 20 exact
  stops, and zero live fixture processes after two rounds;
- E2: exactly five PX4 plus two mecanum targets, with 14 real starts, 14 exact
  stops, and zero live fixture processes after two rounds;
- topology mismatch: no Effect or process may be created; and
- resolver failure: the Effect becomes `uncertain` and the Run remains
  closure-blocked in `stopping` until reconciliation proves the external state;
- mid-launch failure: every earlier applied process is compensated and zero
  fixture processes remain live even while the uncertain Effect blocks closure.
