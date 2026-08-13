//go:build linux

package acceptance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lxk36/xgc2-nodes-robotics/processlaunch"
	"github.com/lxk36/xgc2-nodes-robotics/topology"
	"github.com/lxk36/xgc2-orchestration-core/controller"
	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	"github.com/lxk36/xgc2-orchestration-core/durable/worker"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	effectport "github.com/lxk36/xgc2-orchestration-core/provider/effect"
	"github.com/lxk36/xgc2-orchestration-core/provider/processadapter"
	"github.com/lxk36/xgc2-orchestration-core/provider/processlocal"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const acceptanceDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

type profile struct {
	id      string
	members []member
	total   int
}

type member struct {
	kind  string
	count int
}

var (
	e1 = profile{id: "e1-six-px4-four-scout", members: []member{{kind: "px4", count: 6}, {kind: "scout", count: 4}}, total: 10}
	e2 = profile{id: "e2-five-px4-two-mecanum", members: []member{{kind: "px4", count: 5}, {kind: "mecanum", count: 2}}, total: 7}
)

type grants struct{}

func (grants) ResolveGrants(_ context.Context, _ contracts.Run, descriptor contracts.NodeDescriptor, deadline time.Time) ([]contracts.CapabilityGrant, error) {
	if len(descriptor.RequiredCapabilities) == 0 {
		return []contracts.CapabilityGrant{}, nil
	}
	return []contracts.CapabilityGrant{{
		CapabilityRef: "process.control", Scope: "target", HandleRef: "grant-process-control",
		AuthorizationDigest: acceptanceDigest, ExpiresAt: deadline,
	}}, nil
}

type credentials struct{}

func (credentials) ResolveEffectCredentials(_ context.Context, _ contracts.EffectRecord, ledger contracts.CommandLedger) (effectport.DispatchCredentials, error) {
	return effectport.DispatchCredentials{
		IdempotencyKey:      "idempotency-" + ledger.Envelope.CommandID,
		CapabilityToken:     "capability-" + ledger.Envelope.CommandID,
		AuthorizationDigest: acceptanceDigest,
	}, nil
}

type fixtureResolver struct {
	directory string
	failRef   string
	mu        sync.Mutex
	started   []string
}

func (resolver *fixtureResolver) ResolveProcess(_ context.Context, prepared contracts.EffectIntent, intent processadapter.Intent) (processadapter.Resolution, error) {
	if intent.Spec.ExecutableRef == resolver.failRef {
		return processadapter.Resolution{}, errors.New("fixture executable reference was deliberately denied")
	}
	if intent.Spec.ExecutableRef != "robot-fixture" {
		return processadapter.Resolution{}, fmt.Errorf("unknown executable ref %q", intent.Spec.ExecutableRef)
	}
	resolver.mu.Lock()
	resolver.started = append(resolver.started, prepared.TargetRef)
	resolver.mu.Unlock()
	return processadapter.Resolution{
		Executable:  "/bin/sh",
		Arguments:   []string{"-c", "printf '%s\\n' \"$XGC_ROBOT_ID\"; sleep 0.02"},
		Environment: []string{"PATH=/usr/bin:/bin", "XGC_ROBOT_ID=" + prepared.TargetRef},
		StdoutPath:  filepath.Join(resolver.directory, prepared.TargetRef+".stdout"),
		StderrPath:  filepath.Join(resolver.directory, prepared.TargetRef+".stderr"),
	}, nil
}

func (resolver *fixtureResolver) targets() []string {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	result := append([]string(nil), resolver.started...)
	sort.Strings(result)
	return result
}

type harness struct {
	t          *testing.T
	store      *filestore.Store
	controller *controller.Controller
	resolver   *fixtureResolver
	worker     worker.Worker
	launch     *processlaunch.Executor
	topology   *topology.Executor
	nextFence  uint64
}

func newHarness(t *testing.T, failRef string) *harness {
	t.Helper()
	directory := t.TempDir()
	durable, err := filestore.Open(filepath.Join(directory, "orchestration.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	launch := processlaunch.New()
	topologyExecutor := topology.New()
	registry := protocol.NewRegistry()
	if err := registry.Register(topologyExecutor); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(launch); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Seal(); err != nil {
		t.Fatal(err)
	}
	orchestrator, err := controller.New(controller.Config{
		Store: durable, Nodes: registry, OwnerRef: "robotics-acceptance-controller",
		LeaseDuration: time.Minute, InvocationTimeout: 10 * time.Second, Grants: grants{},
	})
	if err != nil {
		t.Fatal(err)
	}
	processProvider, err := processlocal.New("local-process", acceptanceDigest)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &fixtureResolver{directory: directory, failRef: failRef}
	adapter, err := processadapter.New(processadapter.Config{
		Kind: processadapter.KindStart, ProviderRef: "local-process", ProviderDigest: acceptanceDigest,
		Provider: processProvider, Resolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := controller.NewEffectOutboxHandler(orchestrator, credentials{}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	waits, err := controller.NewWaitResolutionHandler(orchestrator)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		t: t, store: durable, controller: orchestrator, resolver: resolver,
		worker: worker.Worker{
			Store: durable, OwnerRef: "robotics-intent-worker",
			Handlers: map[contracts.DurableIntentKind]worker.Handler{
				contracts.IntentOutbox: outbox, contracts.IntentWaitResolution: waits,
			},
		},
		launch: launch, topology: topologyExecutor, nextFence: 100,
	}
}

func TestE1SixPX4FourScoutRunsTwiceContinuously(t *testing.T) {
	h := newHarness(t, "")
	for round := 1; round <= 2; round++ {
		runID := fmt.Sprintf("e1-round-%d", round)
		run := h.runProfile(runID, e1, "robot-fixture")
		if run.Status != contracts.RunSucceeded {
			t.Fatalf("%s = %s, failure=%+v", runID, run.Status, run.PrimaryFailure)
		}
		h.verifyMarkers(runID, e1)
	}
	if targets := h.resolver.targets(); len(targets) != 20 {
		t.Fatalf("E1 two rounds started %d process targets: %#v", len(targets), targets)
	}
}

func TestE2FivePX4TwoMecanumRunsTwiceContinuously(t *testing.T) {
	h := newHarness(t, "")
	for round := 1; round <= 2; round++ {
		runID := fmt.Sprintf("e2-round-%d", round)
		run := h.runProfile(runID, e2, "robot-fixture")
		if run.Status != contracts.RunSucceeded {
			t.Fatalf("%s = %s, failure=%+v", runID, run.Status, run.PrimaryFailure)
		}
		h.verifyMarkers(runID, e2)
	}
	if targets := h.resolver.targets(); len(targets) != 14 {
		t.Fatalf("E2 two rounds started %d process targets: %#v", len(targets), targets)
	}
}

func TestTopologyMismatchFailsBeforeAnyProcessEffect(t *testing.T) {
	h := newHarness(t, "")
	bad := profile{id: e1.id, members: []member{{kind: "px4", count: 6}, {kind: "scout", count: 3}}, total: 9}
	definition, action := h.workflow("e1-invalid", e1, "robot-fixture")
	run := h.invokeAndDrive("e1-invalid", definition, action, bad, e1)
	if run.Status != contracts.RunFailed || run.PrimaryFailure == nil || run.PrimaryFailure.Code != "topology.mismatch" {
		t.Fatalf("mismatched topology run = %#v, failure=%+v", run, run.PrimaryFailure)
	}
	effects, err := h.controller.ListEffects(t.Context(), "", 100)
	if err != nil || len(effects) != 0 || len(h.resolver.targets()) != 0 {
		t.Fatalf("mismatch produced effects=%d targets=%d err=%v", len(effects), len(h.resolver.targets()), err)
	}
}

func TestResolverFailureBecomesUncertainAndFailsRunClosed(t *testing.T) {
	h := newHarness(t, "denied-fixture")
	run := h.runProfile("e2-denied", e2, "denied-fixture")
	if run.Status != contracts.RunFailed || run.PrimaryFailure == nil || run.PrimaryFailure.Class != contracts.FailureUncertain {
		t.Fatalf("resolver failure run = %#v, failure=%+v", run, run.PrimaryFailure)
	}
	effects, err := h.controller.ListEffects(t.Context(), "", 100)
	if err != nil || len(effects) != 1 || effects[0].State != contracts.EffectUncertain {
		t.Fatalf("resolver failure effects = %#v, err=%v", effects, err)
	}
}

func (h *harness) runProfile(runID string, desired profile, executableRef string) contracts.Run {
	h.t.Helper()
	definition, action := h.workflow(runID, desired, executableRef)
	return h.invokeAndDrive(runID, definition, action, desired, desired)
}

func (h *harness) invokeAndDrive(runID string, definition contracts.WorkflowDefinition, action contracts.ActionVersion, actual, expected profile) contracts.Run {
	h.t.Helper()
	now := time.Now().UTC()
	input := profileInput(actual, expected)
	invoked, err := h.controller.Invoke(h.t.Context(), controller.InvokeRequest{
		RunID: runID, NamespaceID: "robotics-acceptance", Action: action, Definition: definition,
		Trigger: contracts.TriggerEvent{
			EventID: "event-" + runID, Kind: contracts.TriggerManual, Version: "v1",
			OccurredAt: now, ReceivedAt: now, SourceRef: "robotics-acceptance", ActorRef: "test-operator",
			PayloadSchemaDigest: acceptanceDigest, Payload: map[string]any{},
		},
		Candidate: input, CandidateOrigin: contracts.OriginCaller, CandidateRef: "acceptance-fixture",
		Scope: map[string]any{}, CommandID: "invoke-" + runID,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	run, err := h.controller.Drive(h.t.Context(), invoked.Run.RunID)
	if err != nil {
		h.t.Fatal(err)
	}
	for step := 0; step < 100 && run.Status == contracts.RunWaiting; step++ {
		effects, listErr := h.controller.ListEffects(h.t.Context(), "", 1000)
		if listErr != nil {
			h.t.Fatal(listErr)
		}
		var prepared *contracts.EffectRecord
		for index := range effects {
			if effects[index].Intent.RunID == runID && effects[index].State == contracts.EffectPrepared {
				copy := effects[index]
				prepared = &copy
				break
			}
		}
		if prepared == nil {
			h.t.Fatalf("run %s waits without a prepared effect", runID)
		}
		h.nextFence++
		commandID := "dispatch-" + prepared.EffectID
		_, beginErr := h.controller.BeginEffect(h.t.Context(), controller.BeginEffectRequest{
			EffectID: prepared.EffectID, CommandID: commandID,
			IdempotencyKey: "idempotency-" + commandID, CapabilityToken: "capability-" + commandID,
			Action: "process.start", ActorRef: "test-operator", SourceRef: "robotics-acceptance",
			ReasonCode: "experiment.launch", Risk: contracts.RiskHigh,
			Fence: contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{
				BindingID: prepared.Intent.TargetRef, Generation: 1, FencingToken: h.nextFence,
			}},
			CancellationID: "cancel-" + prepared.EffectID,
		})
		if beginErr != nil {
			h.t.Fatal(beginErr)
		}
		batchNow := time.Now().UTC()
		outboxResult, workerErr := h.worker.RunOnce(h.t.Context(), worker.Batch{
			Kinds:      []contracts.DurableIntentKind{contracts.IntentOutbox},
			LeaseToken: "lease-outbox-" + prepared.EffectID,
			Now:        batchNow, LeaseExpiresAt: batchNow.Add(time.Minute), Limit: 100,
		})
		if workerErr != nil || outboxResult.Completed != 1 {
			h.t.Fatalf("outbox %s = %#v, err=%v", prepared.EffectID, outboxResult, workerErr)
		}
		batchNow = time.Now().UTC()
		waitResult, workerErr := h.worker.RunOnce(h.t.Context(), worker.Batch{
			Kinds:      []contracts.DurableIntentKind{contracts.IntentWaitResolution},
			LeaseToken: "lease-wait-" + prepared.EffectID,
			Now:        batchNow, LeaseExpiresAt: batchNow.Add(time.Minute), Limit: 100,
		})
		if workerErr != nil || waitResult.Completed != 1 {
			h.t.Fatalf("wait %s = %#v, err=%v", prepared.EffectID, waitResult, workerErr)
		}
		run, err = h.controller.GetRun(h.t.Context(), runID)
		if err != nil {
			h.t.Fatal(err)
		}
	}
	if !run.Status.Terminal() {
		h.t.Fatalf("run %s did not terminate: %s", runID, run.Status)
	}
	return run
}

func (h *harness) workflow(runID string, desired profile, executableRef string) (contracts.WorkflowDefinition, contracts.ActionVersion) {
	h.t.Helper()
	topologyDescriptor := h.topology.Descriptor()
	launchDescriptor := h.launch.Descriptor()
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	definition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "robotics-" + runID, Version: "v1",
		InputSchema: topologyDescriptor.InputSchema, ResultSchema: topologyDescriptor.OutputSchema,
		TriggerSchema: empty, ScopeSchema: empty, Entrypoints: map[string]string{"main": "topology"},
		Nodes: []contracts.WorkflowNodeDefinition{{
			NodeID: "topology", TypeRef: topologyDescriptor.TypeRef,
			DescriptorDigest: topologyDescriptor.DescriptorDigest,
			InputSchema:      topologyDescriptor.InputSchema, OutputSchema: topologyDescriptor.OutputSchema,
			Bindings: []contracts.ValueBinding{
				{Target: "/profileId", Value: contracts.ValueExpr{Ref: "inputs.profileId"}},
				{Target: "/actual", Value: contracts.ValueExpr{Ref: "inputs.actual"}},
				{Target: "/expected", Value: contracts.ValueExpr{Ref: "inputs.expected"}},
			},
		}},
		ResultBindings: map[string][]contracts.ValueBinding{"main": {
			{Target: "/profileId", Value: contracts.ValueExpr{Ref: "nodes.topology.output.profileId"}},
			{Target: "/members", Value: contracts.ValueExpr{Ref: "nodes.topology.output.members"}},
			{Target: "/total", Value: contracts.ValueExpr{Ref: "nodes.topology.output.total"}},
			{Target: "/matched", Value: contracts.ValueExpr{Ref: "nodes.topology.output.matched"}},
		}},
	}
	previous := "topology"
	for _, instance := range expand(runID, desired) {
		nodeID := "launch-" + instance
		definition.Nodes = append(definition.Nodes, contracts.WorkflowNodeDefinition{
			NodeID: nodeID, TypeRef: launchDescriptor.TypeRef,
			DescriptorDigest: launchDescriptor.DescriptorDigest,
			InputSchema:      launchDescriptor.InputSchema, OutputSchema: launchDescriptor.OutputSchema,
			FixedInputs: map[string]any{
				"bindingId": instance, "processId": instance, "version": "v1",
				"definitionDigest": acceptanceDigest, "executableRef": executableRef,
				"argumentTemplateDigest": acceptanceDigest,
				"parameterSetRef":        "defaults", "parameterSetDigest": acceptanceDigest,
				"stdoutArtifactRef": "stdout-" + instance, "stderrArtifactRef": "stderr-" + instance,
				"gracePeriodMillis": int64(100), "killWaitMillis": int64(1000),
			},
		})
		definition.Edges = append(definition.Edges, contracts.WorkflowEdge{From: previous, To: nodeID, Kind: contracts.EdgeControl})
		previous = nodeID
	}
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		h.t.Fatal(err)
	}
	action := contracts.ActionVersion{
		ActionID: definition.WorkflowID, Version: definition.Version, DefinitionDigest: plan.DefinitionDigest,
		Entrypoint: "main", InputSchema: definition.InputSchema, ResultSchema: definition.ResultSchema,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerManual},
	}
	return definition, action
}

func (h *harness) verifyMarkers(runID string, desired profile) {
	h.t.Helper()
	for _, target := range expand(runID, desired) {
		path := filepath.Join(h.resolver.directory, target+".stdout")
		deadline := time.Now().Add(3 * time.Second)
		for {
			raw, err := os.ReadFile(path)
			if err == nil && strings.TrimSpace(string(raw)) == target {
				break
			}
			if time.Now().After(deadline) {
				h.t.Fatalf("marker %s is missing or invalid: %q, err=%v", path, raw, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func profileInput(actual, expected profile) map[string]any {
	return map[string]any{"profileId": expected.id, "actual": memberValues(actual), "expected": memberValues(expected)}
}

func memberValues(value profile) []any {
	result := make([]any, 0, len(value.members))
	for _, item := range value.members {
		result = append(result, map[string]any{"kind": item.kind, "count": int64(item.count)})
	}
	return result
}

func expand(runID string, value profile) []string {
	result := make([]string, 0, value.total)
	for _, item := range value.members {
		for index := 1; index <= item.count; index++ {
			result = append(result, fmt.Sprintf("%s-%s-%02d", runID, item.kind, index))
		}
	}
	return result
}
