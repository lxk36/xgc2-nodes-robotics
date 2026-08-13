package fs150model

import (
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const testPolicyDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestFS150ModelSpawnEmitsOnlyFiniteStructuredIntent(t *testing.T) {
	executor := New()
	input := map[string]any{
		"bindingId": "px4-01-model", "modelName": "uav1",
		"pose":           map[string]any{"x": 1.0, "y": 2.0, "z": 0.2, "roll": 0.0, "pitch": 0.0, "yaw": 0.5},
		"sitlInstanceId": int64(0), "qgcUdpPort": int64(14550), "rosMasterPort": int64(11311),
		"existingModelPolicy": "replace", "stripMag": false, "stripBaro": false,
	}
	digest, _ := canonicaljson.DigestValue(input)
	now := time.Now().UTC()
	result, err := executor.Execute(t.Context(), contracts.NodeInvocationRequest{
		InvocationID: "invocation-1", RunID: "run-1", NodeID: "spawn-model",
		TypeRef: executor.Descriptor().TypeRef, DescriptorDigest: executor.Descriptor().DescriptorDigest,
		AttemptID: "attempt-1", AttemptOrdinal: 1, Input: input, InputDigest: digest,
		CapabilityGrants: []contracts.CapabilityGrant{{CapabilityRef: "simulation.control", Scope: "target", HandleRef: "grant-1", AuthorizationDigest: testPolicyDigest, ExpiresAt: now.Add(time.Minute)}},
		RequestedAt:      now, Deadline: now.Add(time.Minute),
	})
	if err != nil || result.Status != contracts.NodeResultWaiting || len(result.Effects) != 1 || result.Effects[0].Kind != Kind {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, leaked := result.Effects[0].Intent["executable"]; leaked {
		t.Fatal("public FS150 effect leaked an executable")
	}
	resolutionPayload := map[string]any{"effectId": "effect-1", "resultDigest": testPolicyDigest, "externalIdentity": "uav1"}
	resolutionDigest, _ := canonicaljson.DigestValue(resolutionPayload)
	resumed, err := executor.Resume(t.Context(), contracts.NodeResumeRequest{
		InvocationID: "invocation-1", RunID: "run-1", NodeID: "spawn-model",
		TypeRef: executor.Descriptor().TypeRef, DescriptorDigest: executor.Descriptor().DescriptorDigest,
		AttemptID: "attempt-1", AttemptOrdinal: 1, Input: input, InputDigest: digest,
		Wait: *result.Wait,
		Resolution: contracts.NodeWaitResolution{
			Kind: contracts.NodeWaitEffect, SubjectRef: result.Wait.SubjectRef, ConditionDigest: result.Wait.ConditionDigest,
			Status: contracts.NodeWaitResolvedSucceeded, Payload: resolutionPayload, PayloadDigest: resolutionDigest, ObservedAt: now,
		},
		RequestedAt: now,
	})
	if err != nil || resumed.Status != contracts.NodeResultSucceeded || resumed.Output["externalIdentity"] != "uav1" {
		t.Fatalf("resumed=%+v err=%v", resumed, err)
	}
}
