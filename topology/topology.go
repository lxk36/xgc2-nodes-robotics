// Package topology validates an authored heterogeneous robotics topology
// against an exact expected profile. It is pure and deterministic so profile
// failures occur before any simulator or robot process is started.
package topology

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const packageDigest = "sha256:9d6e93c1e31c79c5a6860eb291532480e3c1f2e52fbd1d444b03d235828db5a1"

type Executor struct{ descriptor contracts.NodeDescriptor }

func New() *Executor {
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	member := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"kind": stringSchema, "count": {Type: contracts.TypeInteger},
	}, Required: []string{"kind", "count"}}
	members := contracts.Schema{Type: contracts.TypeArray, Items: &member}
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: protocol.DescriptorSchemaVersion,
		TypeRef:       "xgc.robotics.topology-assert/v1", DisplayName: "Exact robotics topology assertion",
		PackageRef: "xgc2-nodes-robotics", PackageDigest: packageDigest,
		InputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"profileId": stringSchema, "actual": members, "expected": members,
		}, Required: []string{"profileId", "actual", "expected"}},
		OutputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"profileId": stringSchema, "members": members, "total": {Type: contracts.TypeInteger}, "matched": {Type: contracts.TypeBoolean},
		}, Required: []string{"profileId", "members", "total", "matched"}},
		Mode: contracts.NodePure, Determinism: contracts.NodeDeterministic,
		MaxInputBytes: 65536, MaxOutputBytes: 65536,
	}
	descriptor.DescriptorDigest, _ = protocol.DescriptorDigest(descriptor)
	return &Executor{descriptor: descriptor}
}

func (executor *Executor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *Executor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	actual, actualTotal, err := normalize(request.Input["actual"])
	if err != nil {
		return failed("topology.actual-invalid", err), nil
	}
	expected, _, err := normalize(request.Input["expected"])
	if err != nil {
		return failed("topology.expected-invalid", err), nil
	}
	actualDigest, err := canonicaljson.DigestValue(actual)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	expectedDigest, err := canonicaljson.DigestValue(expected)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	if actualDigest != expectedDigest {
		return failed("topology.mismatch", errors.New("actual fleet does not exactly match the expected robotics profile")), nil
	}
	output := map[string]any{
		"profileId": request.Input["profileId"], "members": actual,
		"total": actualTotal, "matched": true,
	}
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{Status: contracts.NodeResultSucceeded, Output: output, OutputDigest: digest, EvidenceDigest: digest}, nil
}

func normalize(value any) ([]any, int64, error) {
	raw, ok := value.([]any)
	if !ok || len(raw) == 0 {
		return nil, 0, errors.New("topology members are required")
	}
	counts := make(map[string]int64, len(raw))
	total := int64(0)
	for _, item := range raw {
		member, ok := item.(map[string]any)
		if !ok {
			return nil, 0, errors.New("topology member is not an object")
		}
		kind, kindOK := member["kind"].(string)
		count, countOK := integer(member["count"])
		if !kindOK || !contracts.ValidIdentifier(kind) || !countOK || count <= 0 || counts[kind] != 0 {
			return nil, 0, errors.New("topology kind/count is invalid or duplicated")
		}
		counts[kind] = count
		total += count
	}
	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	result := make([]any, 0, len(kinds))
	for _, kind := range kinds {
		result = append(result, map[string]any{"kind": kind, "count": counts[kind]})
	}
	return result, total, nil
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
