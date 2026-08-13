package runtimetopology

import (
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func TestRuntimeTopologyRequiresUniqueReadyIdentitiesAndExactCounts(t *testing.T) {
	executor := New()
	input := map[string]any{
		"profileId": "e1-six-px4-four-scout",
		"expected": []any{
			map[string]any{"kind": "px4-multirotor", "count": int64(2)},
			map[string]any{"kind": "scout-mini", "count": int64(1)},
		},
		"observed": []any{
			map[string]any{"bindingId": "px4-01", "kind": "px4-multirotor"},
			map[string]any{"bindingId": "px4-02", "kind": "px4-multirotor"},
			map[string]any{"bindingId": "scout-01", "kind": "scout-mini"},
		},
		"externalIdentities": []any{"process-px4-01", "process-px4-02", "process-scout-01"},
	}
	digest, _ := canonicaljson.DigestValue(input)
	request := contracts.NodeInvocationRequest{Input: input, InputDigest: digest, RequestedAt: time.Now().UTC(), Deadline: time.Now().UTC().Add(time.Minute)}
	result, err := executor.Execute(t.Context(), request)
	if err != nil || result.Status != contracts.NodeResultSucceeded || result.Output["total"] != int64(3) {
		t.Fatalf("runtime topology result=%#v err=%v", result, err)
	}
	duplicate := cloneInput(input)
	duplicate["externalIdentities"].([]any)[1] = "process-px4-01"
	request.Input = duplicate
	result, err = executor.Execute(t.Context(), request)
	if err != nil || result.Status != contracts.NodeResultFailed || result.Failure == nil || result.Failure.Code != "runtime-topology.observed-invalid" {
		t.Fatalf("duplicate runtime topology result=%#v err=%v", result, err)
	}
}

func cloneInput(input map[string]any) map[string]any {
	raw, _ := canonicaljson.Marshal(input)
	var clone map[string]any
	_ = canonicaljson.UnmarshalStrict(raw, &clone)
	return clone
}
