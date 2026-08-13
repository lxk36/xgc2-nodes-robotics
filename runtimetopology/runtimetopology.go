// Package runtimetopology validates the exact set of ready runtime identities
// produced by managed robot launches against an expected fleet profile.
package runtimetopology

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const packageDigest = "sha256:14c897362644a668e60456689720214b4e577a6ea41d124f58ee81c65948a904"

type Executor struct{ descriptor contracts.NodeDescriptor }

func New() *Executor {
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	member := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"kind": stringSchema, "count": {Type: contracts.TypeInteger},
	}, Required: []string{"kind", "count"}}
	observedBinding := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "kind": stringSchema,
	}, Required: []string{"bindingId", "kind"}}
	outputBinding := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "kind": stringSchema, "externalIdentity": stringSchema,
	}, Required: []string{"bindingId", "kind", "externalIdentity"}}
	members := contracts.Schema{Type: contracts.TypeArray, Items: &member}
	observedBindings := contracts.Schema{Type: contracts.TypeArray, Items: &observedBinding}
	outputBindings := contracts.Schema{Type: contracts.TypeArray, Items: &outputBinding}
	externalIdentities := contracts.Schema{Type: contracts.TypeArray, Items: &stringSchema}
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: protocol.DescriptorSchemaVersion,
		TypeRef:       "xgc.robotics.runtime-topology-assert/v1", DisplayName: "Ready runtime topology assertion",
		PackageRef: "xgc2-nodes-robotics", PackageDigest: packageDigest,
		InputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"profileId": stringSchema, "expected": members, "observed": observedBindings,
			"externalIdentities": externalIdentities,
		}, Required: []string{"profileId", "expected", "observed", "externalIdentities"}},
		OutputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"profileId": stringSchema, "members": members, "bindings": outputBindings,
			"total": {Type: contracts.TypeInteger}, "matched": {Type: contracts.TypeBoolean},
		}, Required: []string{"profileId", "members", "bindings", "total", "matched"}},
		Mode: contracts.NodePure, Determinism: contracts.NodeDeterministic,
		MaxInputBytes: 262144, MaxOutputBytes: 262144,
	}
	descriptor.DescriptorDigest, _ = protocol.DescriptorDigest(descriptor)
	return &Executor{descriptor: descriptor}
}

func (executor *Executor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *Executor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	profileID, _ := request.Input["profileId"].(string)
	if !contracts.ValidIdentifier(profileID) {
		return failed("runtime-topology.profile-invalid", errors.New("runtime topology profile identity is invalid")), nil
	}
	expected, expectedTotal, err := normalizeMembers(request.Input["expected"])
	if err != nil {
		return failed("runtime-topology.expected-invalid", err), nil
	}
	bindings, counts, err := normalizeBindings(request.Input["observed"], request.Input["externalIdentities"])
	if err != nil {
		return failed("runtime-topology.observed-invalid", err), nil
	}
	actual := memberValues(counts)
	actualDigest, err := canonicaljson.DigestValue(actual)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	expectedDigest, err := canonicaljson.DigestValue(expected)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	if actualDigest != expectedDigest || int64(len(bindings)) != expectedTotal {
		return failed("runtime-topology.mismatch", errors.New("ready runtime identities do not exactly match the expected robotics profile")), nil
	}
	output := map[string]any{
		"profileId": profileID, "members": actual, "bindings": bindings,
		"total": int64(len(bindings)), "matched": true,
	}
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{Status: contracts.NodeResultSucceeded, Output: output, OutputDigest: digest, EvidenceDigest: digest}, nil
}

func normalizeMembers(value any) ([]any, int64, error) {
	raw, ok := value.([]any)
	if !ok || len(raw) == 0 {
		return nil, 0, errors.New("expected runtime topology members are required")
	}
	counts := make(map[string]int64, len(raw))
	var total int64
	for _, item := range raw {
		member, ok := item.(map[string]any)
		if !ok {
			return nil, 0, errors.New("expected runtime topology member is not an object")
		}
		kind, kindOK := member["kind"].(string)
		count, countOK := integer(member["count"])
		if !kindOK || !contracts.ValidIdentifier(kind) || !countOK || count <= 0 || counts[kind] != 0 {
			return nil, 0, errors.New("expected runtime topology kind/count is invalid or duplicated")
		}
		counts[kind] = count
		total += count
	}
	return memberValues(counts), total, nil
}

func normalizeBindings(value, identitiesValue any) ([]any, map[string]int64, error) {
	raw, ok := value.([]any)
	if !ok || len(raw) == 0 {
		return nil, nil, errors.New("observed ready runtime bindings are required")
	}
	identities, ok := identitiesValue.([]any)
	if !ok || len(identities) != len(raw) {
		return nil, nil, errors.New("one external runtime identity is required per observed binding")
	}
	bindings := make([]any, 0, len(raw))
	counts := make(map[string]int64)
	seenBindings := make(map[string]struct{}, len(raw))
	seenExternal := make(map[string]struct{}, len(raw))
	for index, item := range raw {
		binding, ok := item.(map[string]any)
		if !ok {
			return nil, nil, errors.New("observed runtime binding is not an object")
		}
		bindingID, bindingOK := binding["bindingId"].(string)
		kind, kindOK := binding["kind"].(string)
		external, externalOK := identities[index].(string)
		_, duplicateBinding := seenBindings[bindingID]
		_, duplicateExternal := seenExternal[external]
		if !bindingOK || !kindOK || !externalOK || !contracts.ValidIdentifier(bindingID) ||
			!contracts.ValidIdentifier(kind) || !contracts.ValidIdentifier(external) || duplicateBinding || duplicateExternal {
			return nil, nil, errors.New("observed runtime binding identity/kind is invalid or duplicated")
		}
		seenBindings[bindingID] = struct{}{}
		seenExternal[external] = struct{}{}
		counts[kind]++
		bindings = append(bindings, map[string]any{"bindingId": bindingID, "kind": kind, "externalIdentity": external})
	}
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].(map[string]any)["bindingId"].(string) < bindings[j].(map[string]any)["bindingId"].(string)
	})
	return bindings, counts, nil
}

func memberValues(counts map[string]int64) []any {
	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	result := make([]any, 0, len(kinds))
	for _, kind := range kinds {
		result = append(result, map[string]any{"kind": kind, "count": counts[kind]})
	}
	return result
}

func failed(code string, cause error) contracts.NodeResult {
	failure := &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: code, Message: cause.Error()}
	evidence, _ := canonicaljson.DigestValue(failure)
	return contracts.NodeResult{Status: contracts.NodeResultFailed, Failure: failure, EvidenceDigest: evidence}
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
