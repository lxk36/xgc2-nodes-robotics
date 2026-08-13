package processlaunch

import (
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/provider/processadapter"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestLaunchProposesProcessEffectAndPureResume(t *testing.T) {
	executor := New()
	registry := protocol.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	input := map[string]any{
		"bindingId": "e1-px4-1", "processId": "px4-1", "version": "1.4.0",
		"definitionDigest": testDigest, "executableRef": "robot-fixture", "argumentTemplateDigest": testDigest,
		"parameterSetRef": "defaults", "parameterSetDigest": testDigest,
		"stdoutArtifactRef": "stdout-e1-px4-1", "stderrArtifactRef": "stderr-e1-px4-1",
		"gracePeriodMillis": int64(100), "killWaitMillis": int64(1000),
	}
	inputDigest, _ := canonicaljson.DigestValue(input)
	t0 := time.Date(2026, 8, 13, 4, 0, 0, 0, time.UTC)
	request := contracts.NodeInvocationRequest{
		InvocationID: "inv-launch", RunID: "run-launch", NodeID: "launch",
		TypeRef: executor.Descriptor().TypeRef, DescriptorDigest: executor.Descriptor().DescriptorDigest,
		AttemptID: "attempt-launch", AttemptOrdinal: 1, Input: input, InputDigest: inputDigest,
		CapabilityGrants: []contracts.CapabilityGrant{{
			CapabilityRef: "process.control", Scope: "target", HandleRef: "grant-process",
			AuthorizationDigest: testDigest, ExpiresAt: t0.Add(time.Minute),
		}},
		RequestedAt: t0, Deadline: t0.Add(time.Minute),
	}
	result, err := registry.Execute(t.Context(), request)
	if err != nil || result.Status != contracts.NodeResultWaiting || len(result.Effects) != 1 ||
		result.Effects[0].Kind != processadapter.KindStart || result.Effects[0].TargetRef != "e1-px4-1" {
		t.Fatalf("launch result = %#v, err=%v", result, err)
	}
	payload := map[string]any{
		"effectId": "effect-launch", "state": contracts.EffectApplied,
		"resultDigest": testDigest, "resultArtifactRef": "", "externalIdentity": "pid-123-456",
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
	if err != nil || resumed.Status != contracts.NodeResultSucceeded || resumed.Output["bindingId"] != "e1-px4-1" {
		t.Fatalf("resume result = %#v, err=%v", resumed, err)
	}
}
