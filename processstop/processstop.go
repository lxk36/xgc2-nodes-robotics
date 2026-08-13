// Package processstop provides the effectful boundary for stopping one exact
// detached process returned by a prior managed process launch. It addresses a
// provider identity plus the producing Run owner; it never scans process names
// or receives a raw PID from ambient host state.
package processstop

import (
	"context"
	"errors"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/provider/processadapter"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const packageDigest = "sha256:636208b171786c44a42f05982e429a39defa6fc0e5e1805857c0af94ae82aacb"

type Executor struct{ descriptor contracts.NodeDescriptor }

func New() *Executor {
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	input := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "ownerRunRef": stringSchema, "externalIdentityRef": stringSchema,
	}, Required: []string{"bindingId", "ownerRunRef", "externalIdentityRef"}}
	output := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "effectId": stringSchema,
		"resultDigest": stringSchema, "externalIdentity": stringSchema,
	}, Required: []string{"bindingId", "effectId", "resultDigest", "externalIdentity"}}
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: protocol.DescriptorSchemaVersion,
		TypeRef:       "xgc.robotics.process-stop/v1", DisplayName: "Exact detached process stop",
		PackageRef: "xgc2-nodes-robotics", PackageDigest: packageDigest,
		InputSchema: input, OutputSchema: output,
		Mode: contracts.NodeEffectful, Determinism: contracts.NodeDeterministic,
		RequiredCapabilities: []contracts.CapabilityRequirement{{CapabilityRef: "process.control", Scope: "target"}},
		AllowedEffectKinds:   []string{processadapter.KindStop},
		MaxInputBytes:        65536, MaxOutputBytes: 65536,
	}
	descriptor.DescriptorDigest, _ = protocol.DescriptorDigest(descriptor)
	return &Executor{descriptor: descriptor}
}

func (executor *Executor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *Executor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	if len(request.CapabilityGrants) != 1 || request.CapabilityGrants[0].CapabilityRef != "process.control" {
		return contracts.NodeResult{}, errors.New("process stop requires exactly one process.control grant")
	}
	bindingID, _ := request.Input["bindingId"].(string)
	ownerRunRef, _ := request.Input["ownerRunRef"].(string)
	externalIdentity, _ := request.Input["externalIdentityRef"].(string)
	if !contracts.ValidIdentifier(bindingID) || !contracts.ValidIdentifier(ownerRunRef) || !contracts.ValidIdentifier(externalIdentity) {
		return contracts.NodeResult{}, errors.New("process stop input contains an invalid exact identity")
	}
	intent := map[string]any{"externalIdentityRef": externalIdentity, "ownerRunRef": ownerRunRef}
	intentDigest, err := canonicaljson.DigestValue(intent)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	effectKey := "stop-" + bindingID
	proposal := contracts.EffectProposal{
		EffectKey: effectKey, Kind: processadapter.KindStop, TargetRef: externalIdentity,
		IntentSchemaDigest: packageDigest, Intent: intent, IntentDigest: intentDigest,
		Ownership: contracts.EffectAttached, CompensationPolicy: contracts.CompensationNone,
		RequiredCapabilityRefs: []string{"process.control"},
		PolicyDigest:           request.CapabilityGrants[0].AuthorizationDigest, Deadline: request.Deadline,
	}
	evidence, err := canonicaljson.DigestValue(proposal)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{
		Status: contracts.NodeResultWaiting, Effects: []contracts.EffectProposal{proposal},
		Wait: &contracts.NodeWait{
			Kind: contracts.NodeWaitEffect, SubjectRef: effectKey,
			ConditionDigest: intentDigest, ExpiresAt: timePointer(request.Deadline),
		},
		EvidenceDigest: evidence,
	}, nil
}

func (executor *Executor) Resume(_ context.Context, request contracts.NodeResumeRequest) (contracts.NodeResult, error) {
	if request.Resolution.Status != contracts.NodeWaitResolvedSucceeded {
		return contracts.NodeResult{}, errors.New("process stop can only resume a successful effect")
	}
	effectID, _ := request.Resolution.Payload["effectId"].(string)
	resultDigest, _ := request.Resolution.Payload["resultDigest"].(string)
	externalIdentity, _ := request.Resolution.Payload["externalIdentity"].(string)
	bindingID, _ := request.Input["bindingId"].(string)
	if !contracts.ValidIdentifier(effectID) || !contracts.ValidDigest(resultDigest) || !contracts.ValidIdentifier(externalIdentity) {
		return contracts.NodeResult{}, errors.New("process stop resolution lacks durable result identity")
	}
	output := map[string]any{
		"bindingId": bindingID, "effectId": effectID,
		"resultDigest": resultDigest, "externalIdentity": externalIdentity,
	}
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	evidence, err := canonicaljson.DigestValue(map[string]any{
		"outputDigest": digest, "resolutionDigest": request.Resolution.PayloadDigest,
	})
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{
		Status: contracts.NodeResultSucceeded, Output: output,
		OutputDigest: digest, EvidenceDigest: evidence,
	}, nil
}

func timePointer(value time.Time) *time.Time { return &value }
