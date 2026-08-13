package fleetspec

import (
	"context"
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/conformance/nodepack"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
	nodesdk "github.com/lxk36/xgc2-orchestration-core/sdk/go/node"
)

func TestNodePackConformance(t *testing.T) {
	executor := New()
	input := map[string]any{"fleetId": "six-px4-four-scout", "members": []any{
		map[string]any{"id": "scout", "kind": "scout", "count": 4}, map[string]any{"id": "px4", "kind": "px4", "count": 6},
	}}
	digest, _ := canonicaljson.DigestValue(input)
	t0 := time.Date(2026, 8, 13, 2, 0, 0, 0, time.UTC)
	request := contracts.NodeInvocationRequest{InvocationID: "inv-1", RunID: "run-1", NodeID: "fleet", TypeRef: executor.Descriptor().TypeRef, DescriptorDigest: executor.Descriptor().DescriptorDigest, AttemptID: "att-1", AttemptOrdinal: 1, Input: input, InputDigest: digest, RequestedAt: t0, Deadline: t0.Add(time.Minute)}
	report, err := nodepack.Validate(context.Background(), nodepack.Suite{PackageRef: "xgc2-nodes-robotics", Executors: []nodesdk.Executor{executor}, Cases: []nodepack.Case{{Name: "six PX4 and four Scout", Executor: executor, Request: request, ExpectedStatus: contracts.NodeResultSucceeded}}})
	if err != nil || report.DescriptorCount != 1 {
		t.Fatalf("report = %#v, err %v", report, err)
	}
	result, _ := executor.Execute(context.Background(), request)
	if result.Output["total"] != int64(10) {
		t.Fatalf("fleet total = %#v", result.Output["total"])
	}
}
