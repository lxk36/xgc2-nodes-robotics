// Package processlaunch provides the effectful boundary for starting one
// managed robotics/simulation process. It emits a public ProcessSpec and waits;
// executable paths, arguments, environment values, log paths, and capability
// tokens remain private provider data.
package processlaunch

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/provider/processadapter"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const packageDigest = "sha256:5732626fc21cc28c3ec47526c4e5934bd14fef15589ad3f9982af4ad49f0774b"

type Executor struct{ descriptor contracts.NodeDescriptor }

func New() *Executor {
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	integerSchema := contracts.Schema{Type: contracts.TypeInteger}
	input := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "processId": stringSchema, "version": stringSchema,
		"definitionDigest": stringSchema, "executableRef": stringSchema, "argumentTemplateDigest": stringSchema,
		"parameterSetRef": stringSchema, "parameterSetDigest": stringSchema,
		"stdoutArtifactRef": stringSchema, "stderrArtifactRef": stringSchema,
		"gracePeriodMillis": integerSchema, "killWaitMillis": integerSchema,
	}, Required: []string{
		"bindingId", "processId", "version", "definitionDigest", "executableRef", "argumentTemplateDigest",
		"parameterSetRef", "parameterSetDigest",
		"stdoutArtifactRef", "stderrArtifactRef", "gracePeriodMillis", "killWaitMillis",
	}}
	output := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "effectId": stringSchema,
		"resultDigest": stringSchema, "externalIdentity": stringSchema,
	}, Required: []string{"bindingId", "effectId", "resultDigest", "externalIdentity"}}
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: protocol.DescriptorSchemaVersion,
		TypeRef:       "xgc.robotics.process-launch/v1", DisplayName: "Managed robotics process launch",
		PackageRef: "xgc2-nodes-robotics", PackageDigest: packageDigest,
		InputSchema: input, OutputSchema: output,
		Mode: contracts.NodeEffectful, Determinism: contracts.NodeDeterministic,
		RequiredCapabilities: []contracts.CapabilityRequirement{{CapabilityRef: "process.control", Scope: "target"}},
		AllowedEffectKinds:   []string{processadapter.KindStart},
		MaxInputBytes:        65536, MaxOutputBytes: 65536,
	}
	descriptor.DescriptorDigest, _ = protocol.DescriptorDigest(descriptor)
	return &Executor{descriptor: descriptor}
}

func (executor *Executor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *Executor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	if len(request.CapabilityGrants) != 1 || request.CapabilityGrants[0].CapabilityRef != "process.control" {
		return contracts.NodeResult{}, errors.New("process launch requires exactly one process.control grant")
	}
	bindingID, _ := request.Input["bindingId"].(string)
	processID, _ := request.Input["processId"].(string)
	version, _ := request.Input["version"].(string)
	definitionDigest, _ := request.Input["definitionDigest"].(string)
	executableRef, _ := request.Input["executableRef"].(string)
	argumentDigest, _ := request.Input["argumentTemplateDigest"].(string)
	parameterSetRef, _ := request.Input["parameterSetRef"].(string)
	parameterSetDigest, _ := request.Input["parameterSetDigest"].(string)
	stdoutRef, _ := request.Input["stdoutArtifactRef"].(string)
	stderrRef, _ := request.Input["stderrArtifactRef"].(string)
	grace, graceOK := integer(request.Input["gracePeriodMillis"])
	killWait, killOK := integer(request.Input["killWaitMillis"])
	if !contracts.ValidIdentifier(bindingID) || !contracts.ValidIdentifier(processID) || !contracts.ValidVersion(version) ||
		!contracts.ValidDigest(definitionDigest) || !contracts.ValidIdentifier(executableRef) ||
		!contracts.ValidDigest(argumentDigest) || !contracts.ValidIdentifier(parameterSetRef) ||
		!contracts.ValidDigest(parameterSetDigest) ||
		!contracts.ValidIdentifier(stdoutRef) || !contracts.ValidIdentifier(stderrRef) ||
		!graceOK || !killOK || grace < 10 || killWait < 10 {
		return contracts.NodeResult{}, errors.New("process launch input contains an invalid identity, digest, or timeout")
	}
	spec := contracts.ProcessSpec{
		ProcessID: processID, Version: version, DescriptorDigest: executor.descriptor.DescriptorDigest,
		DefinitionDigest: definitionDigest, ExecutableRef: executableRef, ArgumentTemplateDigest: argumentDigest,
		ParameterSetRef: parameterSetRef, ParameterSetDigest: parameterSetDigest,
		StdoutArtifactRef: stdoutRef, StderrArtifactRef: stderrRef,
		GracePeriodMillis: uint64(grace), KillWaitMillis: uint64(killWait),
	}
	intent := map[string]any{"spec": spec}
	intentDigest, err := canonicaljson.DigestValue(intent)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	effectKey := "start-" + bindingID
	proposal := contracts.EffectProposal{
		EffectKey: effectKey, Kind: processadapter.KindStart, TargetRef: bindingID,
		IntentSchemaDigest: packageDigest, Intent: intent, IntentDigest: intentDigest,
		Ownership: contracts.EffectDetached, CompensationPolicy: contracts.CompensationNone,
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
		return contracts.NodeResult{}, errors.New("process launch can only resume a successful effect")
	}
	effectID, _ := request.Resolution.Payload["effectId"].(string)
	resultDigest, _ := request.Resolution.Payload["resultDigest"].(string)
	externalIdentity, _ := request.Resolution.Payload["externalIdentity"].(string)
	bindingID, _ := request.Input["bindingId"].(string)
	if !contracts.ValidIdentifier(effectID) || !contracts.ValidDigest(resultDigest) || !contracts.ValidIdentifier(externalIdentity) {
		return contracts.NodeResult{}, errors.New("process launch resolution lacks durable result identity")
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

func integer(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		return parsed, err == nil
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case float64:
		return int64(typed), typed == float64(int64(typed))
	default:
		return 0, false
	}
}

func timePointer(value time.Time) *time.Time { return &value }
