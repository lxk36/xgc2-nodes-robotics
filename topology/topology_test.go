package topology

import (
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func TestExactProfilesAndMismatch(t *testing.T) {
	executor := New()
	registry := protocol.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	actual := []any{map[string]any{"kind": "scout", "count": int64(4)}, map[string]any{"kind": "px4", "count": int64(6)}}
	expected := []any{map[string]any{"kind": "px4", "count": int64(6)}, map[string]any{"kind": "scout", "count": int64(4)}}
	invocation := topologyRequest(t, executor, "e1-six-px4-four-scout", actual, expected)
	result, err := registry.Execute(t.Context(), invocation)
	if err != nil || result.Status != contracts.NodeResultSucceeded || result.Output["total"] != int64(10) {
		t.Fatalf("E1 topology = %#v, err=%v", result, err)
	}
	invocation = topologyRequest(t, executor, "e2-five-px4-two-mecanum",
		[]any{map[string]any{"kind": "px4", "count": int64(5)}, map[string]any{"kind": "mecanum", "count": int64(1)}},
		[]any{map[string]any{"kind": "px4", "count": int64(5)}, map[string]any{"kind": "mecanum", "count": int64(2)}},
	)
	result, err = registry.Execute(t.Context(), invocation)
	if err != nil || result.Status != contracts.NodeResultFailed || result.Failure.Code != "topology.mismatch" {
		t.Fatalf("mismatch topology = %#v, err=%v", result, err)
	}
}

func topologyRequest(t *testing.T, executor *Executor, profile string, actual, expected []any) contracts.NodeInvocationRequest {
	t.Helper()
	input := map[string]any{"profileId": profile, "actual": actual, "expected": expected}
	digest, _ := canonicaljson.DigestValue(input)
	t0 := time.Date(2026, 8, 13, 4, 30, 0, 0, time.UTC)
	return contracts.NodeInvocationRequest{
		SchemaVersion: protocol.InvocationSchemaVersion,
		InvocationID:  "inv-topology", RunID: "run-topology", NodeID: "topology",
		TypeRef: executor.Descriptor().TypeRef, DescriptorDigest: executor.Descriptor().DescriptorDigest,
		AttemptID: "attempt-topology", AttemptOrdinal: 1, Input: input, InputDigest: digest,
		RequestedAt: t0, Deadline: t0.Add(time.Minute),
	}
}
