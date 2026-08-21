package fleetspec

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const packageDigest = "sha256:af241f23504b6bb97b6a72f45a4ee739b3b418156995afbe5bf6a821972734d9"

type Executor struct{ descriptor contracts.NodeDescriptor }

func New() *Executor {
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: protocol.DescriptorSchemaVersion, TypeRef: "xgc.robotics.fleet-spec/v1", DisplayName: "Robotics fleet specification",
		PackageRef: "xgc2-nodes-robotics", PackageDigest: packageDigest,
		InputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"fleetId": {Type: contracts.TypeString}, "members": {Type: contracts.TypeArray, Items: &contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
				"id": {Type: contracts.TypeString}, "kind": {Type: contracts.TypeString}, "count": {Type: contracts.TypeInteger},
			}, Required: []string{"id", "kind", "count"}}},
		}, Required: []string{"fleetId", "members"}},
		OutputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"fleetId": {Type: contracts.TypeString}, "members": {Type: contracts.TypeArray, Items: &contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
				"id": {Type: contracts.TypeString}, "kind": {Type: contracts.TypeString}, "count": {Type: contracts.TypeInteger},
			}, Required: []string{"id", "kind", "count"}}}, "total": {Type: contracts.TypeInteger},
		}, Required: []string{"fleetId", "members", "total"}},
		Mode: contracts.NodePure, Determinism: contracts.NodeDeterministic, MaxInputBytes: 65536, MaxOutputBytes: 65536,
	}
	descriptor.DescriptorDigest, _ = protocol.DescriptorDigest(descriptor)
	return &Executor{descriptor: descriptor}
}

func (executor *Executor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *Executor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	members, ok := request.Input["members"].([]any)
	if !ok || len(members) == 0 {
		return contracts.NodeResult{}, errors.New("fleet members are required")
	}
	normalized := make([]any, 0, len(members))
	seen := make(map[string]bool, len(members))
	total := int64(0)
	for _, raw := range members {
		member, ok := raw.(map[string]any)
		if !ok {
			return contracts.NodeResult{}, errors.New("fleet member is not an object")
		}
		id, idOK := member["id"].(string)
		kind, kindOK := member["kind"].(string)
		count, countOK := integer(member["count"])
		if !idOK || !kindOK || !countOK || id == "" || kind == "" || count <= 0 || seen[id] {
			return contracts.NodeResult{}, errors.New("fleet member identity, kind, or count is invalid")
		}
		seen[id] = true
		total += count
		normalized = append(normalized, map[string]any{"id": id, "kind": kind, "count": count})
	}
	sort.Slice(normalized, func(left, right int) bool {
		return normalized[left].(map[string]any)["id"].(string) < normalized[right].(map[string]any)["id"].(string)
	})
	output := map[string]any{"fleetId": request.Input["fleetId"], "members": normalized, "total": total}
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{SchemaVersion: protocol.ResultSchemaVersion, Status: contracts.NodeResultSucceeded, Output: output, OutputDigest: digest, EvidenceDigest: digest}, nil
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
