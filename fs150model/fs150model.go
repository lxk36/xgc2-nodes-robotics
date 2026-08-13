// Package fs150model publishes one finite, structured FS150 Gazebo model
// spawn boundary. The node never receives a script, executable, host path, or
// shell fragment; an installed product provider resolves those private facts.
package fs150model

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const (
	Kind          = "xgc.robotics.fs150-model-spawn/v1"
	packageDigest = "sha256:7e7186476cb215978c5ca9e0eadd787148bd348b3210472b076038ccab55bb9e"
)

type Executor struct{ descriptor contracts.NodeDescriptor }

func New() *Executor {
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	integerSchema := contracts.Schema{Type: contracts.TypeInteger}
	numberSchema := contracts.Schema{Type: contracts.TypeNumber}
	poseSchema := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"x": numberSchema, "y": numberSchema, "z": numberSchema,
		"roll": numberSchema, "pitch": numberSchema, "yaw": numberSchema,
	}, Required: []string{"x", "y", "z", "roll", "pitch", "yaw"}}
	input := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "modelName": stringSchema, "pose": poseSchema,
		"sitlInstanceId": integerSchema, "qgcUdpPort": integerSchema, "rosMasterPort": integerSchema,
		"existingModelPolicy": stringSchema, "stripMag": {Type: contracts.TypeBoolean}, "stripBaro": {Type: contracts.TypeBoolean},
	}, Required: []string{
		"bindingId", "modelName", "pose", "sitlInstanceId", "qgcUdpPort", "rosMasterPort",
		"existingModelPolicy", "stripMag", "stripBaro",
	}}
	output := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "effectId": stringSchema,
		"resultDigest": stringSchema, "externalIdentity": stringSchema,
	}, Required: []string{"bindingId", "effectId", "resultDigest", "externalIdentity"}}
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: protocol.DescriptorSchemaVersion,
		TypeRef:       "xgc.robotics.fs150-model-spawn/v1", DisplayName: "Finite FS150 Gazebo model spawn",
		PackageRef: "xgc2-nodes-robotics", PackageDigest: packageDigest,
		InputSchema: input, OutputSchema: output,
		Mode: contracts.NodeEffectful, Determinism: contracts.NodeDeterministic,
		RequiredCapabilities: []contracts.CapabilityRequirement{{CapabilityRef: "simulation.control", Scope: "target"}},
		AllowedEffectKinds:   []string{Kind},
		MaxInputBytes:        65536, MaxOutputBytes: 65536,
	}
	descriptor.DescriptorDigest, _ = protocol.DescriptorDigest(descriptor)
	return &Executor{descriptor: descriptor}
}

func (executor *Executor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *Executor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	if len(request.CapabilityGrants) != 1 || request.CapabilityGrants[0].CapabilityRef != "simulation.control" {
		return contracts.NodeResult{}, errors.New("FS150 model spawn requires exactly one simulation.control grant")
	}
	bindingID, _ := request.Input["bindingId"].(string)
	modelName, _ := request.Input["modelName"].(string)
	policy, _ := request.Input["existingModelPolicy"].(string)
	instance, instanceOK := integer(request.Input["sitlInstanceId"])
	qgcPort, qgcOK := integer(request.Input["qgcUdpPort"])
	masterPort, masterOK := integer(request.Input["rosMasterPort"])
	pose, poseOK := request.Input["pose"].(map[string]any)
	stripMag, stripMagOK := request.Input["stripMag"].(bool)
	stripBaro, stripBaroOK := request.Input["stripBaro"].(bool)
	if !contracts.ValidIdentifier(bindingID) || !contracts.ValidIdentifier(modelName) ||
		(policy != "replace" && policy != "fail") || !instanceOK || instance < 0 || instance > 9 ||
		!qgcOK || qgcPort < 1 || qgcPort > 65535 || !masterOK || masterPort < 1 || masterPort > 65535 ||
		!poseOK || !validPose(pose) || !stripMagOK || !stripBaroOK {
		return contracts.NodeResult{}, errors.New("FS150 model spawn input is invalid")
	}
	intent := map[string]any{"spec": map[string]any{
		"bindingId": bindingID, "modelName": modelName, "pose": pose,
		"sitlInstanceId": instance, "qgcUdpPort": qgcPort, "rosMasterPort": masterPort,
		"existingModelPolicy": policy, "stripMag": stripMag, "stripBaro": stripBaro,
	}}
	intentDigest, err := canonicaljson.DigestValue(intent)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	effectKey := "spawn-" + bindingID
	proposal := contracts.EffectProposal{
		EffectKey: effectKey, Kind: Kind, TargetRef: modelName,
		IntentSchemaDigest: packageDigest, Intent: intent, IntentDigest: intentDigest,
		Ownership: contracts.EffectAttached, CompensationPolicy: contracts.CompensationNone,
		RequiredCapabilityRefs: []string{"simulation.control"},
		PolicyDigest:           request.CapabilityGrants[0].AuthorizationDigest, Deadline: request.Deadline,
	}
	evidence, err := canonicaljson.DigestValue(proposal)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{
		Status: contracts.NodeResultWaiting, Effects: []contracts.EffectProposal{proposal},
		Wait:           &contracts.NodeWait{Kind: contracts.NodeWaitEffect, SubjectRef: effectKey, ConditionDigest: intentDigest, ExpiresAt: timePointer(request.Deadline)},
		EvidenceDigest: evidence,
	}, nil
}

func (executor *Executor) Resume(_ context.Context, request contracts.NodeResumeRequest) (contracts.NodeResult, error) {
	if request.Resolution.Status != contracts.NodeWaitResolvedSucceeded {
		return contracts.NodeResult{}, errors.New("FS150 model spawn can only resume a successful effect")
	}
	effectID, _ := request.Resolution.Payload["effectId"].(string)
	resultDigest, _ := request.Resolution.Payload["resultDigest"].(string)
	externalIdentity, _ := request.Resolution.Payload["externalIdentity"].(string)
	bindingID, _ := request.Input["bindingId"].(string)
	modelName, _ := request.Input["modelName"].(string)
	if !contracts.ValidIdentifier(effectID) || !contracts.ValidDigest(resultDigest) || externalIdentity != modelName {
		return contracts.NodeResult{}, errors.New("FS150 model spawn resolution lacks the exact model identity")
	}
	output := map[string]any{
		"bindingId": bindingID, "effectId": effectID,
		"resultDigest": resultDigest, "externalIdentity": externalIdentity,
	}
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	evidence, err := canonicaljson.DigestValue(map[string]any{"outputDigest": digest, "resolutionDigest": request.Resolution.PayloadDigest})
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{Status: contracts.NodeResultSucceeded, Output: output, OutputDigest: digest, EvidenceDigest: evidence}, nil
}

func validPose(pose map[string]any) bool {
	if len(pose) != 6 {
		return false
	}
	for _, name := range []string{"x", "y", "z", "roll", "pitch", "yaw"} {
		if _, ok := number(pose[name]); !ok {
			return false
		}
	}
	return true
}

func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
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
