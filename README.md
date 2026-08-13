# xgc2-nodes-robotics

Open XGC orchestration nodes for robotics, simulation, fleets, and experiment
automation. This pack depends only on `xgc2-orchestration-core`; it does not
import XGC2 product internals.

Initial node: `xgc.robotics.fleet-spec/v1` validates and freezes a heterogeneous
fleet specification (PX4, Scout, mecanum, or future kinds) as deterministic
structured output. Simulator/process/ROS mutations are separate effectful
nodes/providers and cannot be hidden inside this pure node.
