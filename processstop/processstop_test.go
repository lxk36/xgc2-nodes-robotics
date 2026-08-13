package processstop

import (
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/provider/processadapter"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestStopProposesExactDetachedIdentityAndPureResume(t *testing.T) {
	executor := New()
	registry := protocol.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	input := map[string]any{
		"bindingId": "e1-px4-1", "ownerRunRef": "start-run-1",
		"externalIdentityRef": "wf-process-1",
	}
	inputDigest, _ := canonicaljson.DigestValue(input)
	t0 := time.Date(2026, 8, 13, 5, 0, 0, 0, time.UTC)
	request := contracts.NodeInvocationRequest{
		InvocationID: "inv-stop", RunID: "run-stop", NodeID: "stop",
		TypeRef: executor.Descriptor().TypeRef, DescriptorDigest: executor.Descriptor().DescriptorDigest,
		AttemptID: "attempt-stop", AttemptOrdinal: 1, Input: input, InputDigest: inputDigest,
		CapabilityGrants: []contracts.CapabilityGrant{{
			CapabilityRef: "process.control", Scope: "target", HandleRef: "grant-process",
			AuthorizationDigest: testDigest, ExpiresAt: t0.Add(time.Minute),
		}},
		RequestedAt: t0, Deadline: t0.Add(time.Minute),
	}
	result, err := registry.Execute(t.Context(), request)
	if err != nil || result.Status != contracts.NodeResultWaiting || len(result.Effects) != 1 ||
		result.Effects[0].Kind != processadapter.KindStop || result.Effects[0].TargetRef != "wf-process-1" ||
		result.Effects[0].Ownership != contracts.EffectAttached || result.Effects[0].CompensationPolicy != contracts.CompensationNone {
		t.Fatalf("stop result = %#v, err=%v", result, err)
	}
	payload := map[string]any{
		"effectId": "effect-stop", "state": contracts.EffectApplied,
		"resultDigest": testDigest, "resultArtifactRef": "", "externalIdentity": "wf-process-1",
	}
	payloadDigest, _ := canonicaljson.DigestValue(payload)
	resumed, err := registry.Resume(t.Context(), contracts.NodeResumeRequest{
		InvocationID: request.InvocationID, RunID: request.RunID, NodeID: request.NodeID,
		TypeRef: request.TypeRef, DescriptorDigest: request.DescriptorDigest,
		AttemptID: request.AttemptID, AttemptOrdinal: request.AttemptOrdinal,
		Input: input, InputDigest: inputDigest, Wait: *result.Wait,
		Resolution: contracts.NodeWaitResolution{
			Kind: contracts.NodeWaitEffect, SubjectRef: result.Wait.SubjectRef,
			ConditionDigest: result.Wait.ConditionDigest, Status: contracts.NodeWaitResolvedSucceeded,
			Payload: payload, PayloadDigest: payloadDigest, ObservedAt: t0.Add(time.Second),
		}, RequestedAt: t0.Add(2 * time.Second),
	})
	if err != nil || resumed.Status != contracts.NodeResultSucceeded || resumed.Output["externalIdentity"] != "wf-process-1" {
		t.Fatalf("resume result = %#v, err=%v", resumed, err)
	}
}
