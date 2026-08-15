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

	"github.com/XGC-Team/xgc2-nodes-process/processlaunch"
	"github.com/XGC-Team/xgc2-nodes-process/processstop"
	"github.com/XGC-Team/xgc2-nodes-robotics/topology"
	"github.com/lxk36/xgc2-orchestration-core/controller"
	"github.com/lxk36/xgc2-orchestration-core/durable/filestore"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	workflowkernel "github.com/lxk36/xgc2-orchestration-core/kernel/workflow"
	effectport "github.com/lxk36/xgc2-orchestration-core/provider/effect"
	processport "github.com/lxk36/xgc2-orchestration-core/provider/process"
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

type effectPlan struct{}

func (effectPlan) PlanEffectDispatch(_ context.Context, current contracts.EffectRecord) (controller.BeginEffectRequest, error) {
	action, reason := processport.ActionStart, "experiment.launch"
	if current.Intent.Kind == processadapter.KindStop {
		action, reason = processport.ActionStop, "experiment.stop"
	}
	commandID := "dispatch-" + current.EffectID
	return controller.BeginEffectRequest{
		EffectID: current.EffectID, CommandID: commandID,
		IdempotencyKey: "idempotency-" + commandID, CapabilityToken: "capability-" + commandID,
		Action: action, ActorRef: "test-operator", SourceRef: "robotics-acceptance",
		ReasonCode: reason, Risk: contracts.RiskHigh,
		Fence: contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{
			BindingID: current.Intent.TargetRef, Generation: 1, FencingToken: 1,
		}},
		Deadline: current.Intent.Deadline, CancellationID: "cancel-" + current.EffectID,
	}, nil
}

func (effectPlan) PlanEffectCompensation(_ context.Context, current contracts.EffectRecord) (controller.BeginEffectRequest, error) {
	commandID := "compensate-" + current.EffectID
	return controller.BeginEffectRequest{
		EffectID: current.EffectID, CommandID: commandID,
		IdempotencyKey: "idempotency-" + commandID, CapabilityToken: "capability-" + commandID,
		Action: processport.ActionStop, ActorRef: "test-operator", SourceRef: "robotics-acceptance",
		ReasonCode: "experiment.compensate", Risk: contracts.RiskHigh,
		Fence: contracts.TargetFence{Kind: contracts.FenceGeneration, Generation: &contracts.GenerationFence{
			BindingID: current.ExternalIdentity, Generation: 1, FencingToken: 1,
		}},
		Deadline: time.Now().UTC().Add(time.Minute), CancellationID: "cancel-" + commandID,
	}, nil
}

type privateProcess struct {
	ownerRunID string
	spec       contracts.ProcessSpec
	identity   contracts.ProcessIdentity
}

// fixtureRuntime models the private provider-side reference store. Public
// stop workflows see only an external identity ref and producing Run owner;
// exact PID/PGID/start-tick data is restored behind ResolveProcess.
type fixtureRuntime struct {
	directory string
	failRef   string
	provider  *processlocal.Provider
	mu        sync.Mutex
	started   []string
	pending   map[string]privateProcess
	instances map[string]privateProcess
	stopped   []string
}

func (runtime *fixtureRuntime) ResolveProcess(_ context.Context, prepared contracts.EffectIntent, intent processadapter.Intent) (processadapter.Resolution, error) {
	if prepared.Kind == processadapter.KindStop {
		runtime.mu.Lock()
		instance, found := runtime.instances[intent.ExternalIdentityRef]
		runtime.mu.Unlock()
		if !found || intent.OwnerRunRef != instance.ownerRunID {
			return processadapter.Resolution{}, errors.New("exact process identity or producing Run owner was not found")
		}
		return processadapter.Resolution{Spec: &instance.spec, KnownIdentity: &instance.identity}, nil
	}
	if intent.Spec.ExecutableRef == runtime.failRef {
		return processadapter.Resolution{}, errors.New("fixture executable reference was deliberately denied")
	}
	if intent.Spec.ExecutableRef != "robot-fixture" {
		return processadapter.Resolution{}, fmt.Errorf("unknown executable ref %q", intent.Spec.ExecutableRef)
	}
	runtime.mu.Lock()
	runtime.started = append(runtime.started, prepared.TargetRef)
	runtime.pending[prepared.TargetRef] = privateProcess{ownerRunID: prepared.RunID, spec: intent.Spec}
	runtime.mu.Unlock()
	return processadapter.Resolution{
		Executable:  "/bin/sh",
		Arguments:   []string{"-c", "printf '%s\\n' \"$XGC_ROBOT_ID\"; exec sleep 60"},
		Environment: []string{"PATH=/usr/bin:/bin", "XGC_ROBOT_ID=" + prepared.TargetRef},
		StdoutPath:  filepath.Join(runtime.directory, prepared.TargetRef+".stdout"),
		StderrPath:  filepath.Join(runtime.directory, prepared.TargetRef+".stderr"),
	}, nil
}

func (runtime *fixtureRuntime) Start(ctx context.Context, dispatch processport.Dispatch) (processport.Result, error) {
	result, err := runtime.provider.Start(ctx, dispatch)
	if err != nil || result.Observation == nil || result.Observation.Identity == nil || len(result.Ledger.Receipts) == 0 {
		return result, err
	}
	externalIdentity := result.Ledger.Receipts[len(result.Ledger.Receipts)-1].ExternalIdentity
	runtime.mu.Lock()
	instance, found := runtime.pending[dispatch.Envelope.TargetRef]
	if found {
		instance.identity = *result.Observation.Identity
		runtime.instances[externalIdentity] = instance
		delete(runtime.pending, dispatch.Envelope.TargetRef)
	}
	runtime.mu.Unlock()
	return result, nil
}

func (runtime *fixtureRuntime) Stop(ctx context.Context, dispatch processport.Dispatch) (processport.Result, error) {
	result, err := runtime.provider.Stop(ctx, dispatch)
	if err == nil && result.Observation != nil && result.Observation.State == contracts.RuntimeObservedStopped {
		runtime.mu.Lock()
		delete(runtime.instances, dispatch.Envelope.TargetRef)
		runtime.stopped = append(runtime.stopped, dispatch.Envelope.TargetRef)
		runtime.mu.Unlock()
	}
	return result, err
}

func (runtime *fixtureRuntime) Inspect(ctx context.Context, request processport.InspectRequest) (contracts.ProcessObservation, error) {
	return runtime.provider.Inspect(ctx, request)
}

func (runtime *fixtureRuntime) targets() []string {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	result := append([]string(nil), runtime.started...)
	sort.Strings(result)
	return result
}

func (runtime *fixtureRuntime) activeCount() int {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return len(runtime.instances)
}

func (runtime *fixtureRuntime) stoppedCount() int {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return len(runtime.stopped)
}

type harness struct {
	t           *testing.T
	store       *filestore.Store
	controller  *controller.Controller
	coordinator *controller.Coordinator
	runtime     *fixtureRuntime
	launch      *processlaunch.Executor
	stop        *processstop.Executor
	topology    *topology.Executor
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
	stop := processstop.New()
	topologyExecutor := topology.New()
	registry := protocol.NewRegistry()
	if err := registry.Register(topologyExecutor); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(launch); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(stop); err != nil {
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
	runtime := &fixtureRuntime{
		directory: directory, failRef: failRef, provider: processProvider,
		pending: map[string]privateProcess{}, instances: map[string]privateProcess{},
	}
	startAdapter, err := processadapter.New(processadapter.Config{
		Kind: processadapter.KindStart, ProviderRef: "local-process", ProviderDigest: acceptanceDigest,
		Provider: runtime, Resolver: runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopAdapter, err := processadapter.New(processadapter.Config{
		Kind: processadapter.KindStop, ProviderRef: "local-process", ProviderDigest: acceptanceDigest,
		Provider: runtime, Resolver: runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := controller.NewCoordinator(controller.CoordinatorConfig{
		Controller: orchestrator, Store: durable, Planner: effectPlan{}, Credentials: credentials{},
		Adapters: []controller.EffectAdapter{startAdapter, stopAdapter}, OwnerRef: "robotics-intent-worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		t: t, store: durable, controller: orchestrator, coordinator: coordinator, runtime: runtime,
		launch: launch, stop: stop, topology: topologyExecutor,
	}
}

func TestE1SixPX4FourScoutRunsTwiceContinuously(t *testing.T) {
	h := newHarness(t, "")
	for round := 1; round <= 2; round++ {
		runID := fmt.Sprintf("e1-round-%d", round)
		run := h.runProfile(runID, e1, "robot-fixture")
		if run.Status != contracts.RunSucceeded {
			effects, _ := h.controller.ListEffects(t.Context(), "", 1000)
			t.Fatalf("%s = %s, termination=%+v effects=%#v", runID, run.Status, run.Termination, effects)
		}
		h.verifyMarkers(runID, e1)
		if h.runtime.activeCount() != 0 {
			effects, _ := h.controller.ListEffects(t.Context(), "", 1000)
			t.Fatalf("%s active=%d after compensation, effects=%#v", runID, h.runtime.activeCount(), effects)
		}
	}
	if targets := h.runtime.targets(); len(targets) != 20 || h.runtime.stoppedCount() != 20 {
		t.Fatalf("E1 two rounds started %d process targets: %#v", len(targets), targets)
	}
}

func TestE2FivePX4TwoMecanumRunsTwiceContinuously(t *testing.T) {
	h := newHarness(t, "")
	for round := 1; round <= 2; round++ {
		runID := fmt.Sprintf("e2-round-%d", round)
		run := h.runProfile(runID, e2, "robot-fixture")
		if run.Status != contracts.RunSucceeded {
			effects, _ := h.controller.ListEffects(t.Context(), "", 1000)
			t.Fatalf("%s = %s, termination=%+v effects=%#v", runID, run.Status, run.Termination, effects)
		}
		h.verifyMarkers(runID, e2)
		if h.runtime.activeCount() != 0 {
			effects, _ := h.controller.ListEffects(t.Context(), "", 1000)
			t.Fatalf("%s active=%d after compensation, effects=%#v", runID, h.runtime.activeCount(), effects)
		}
	}
	if targets := h.runtime.targets(); len(targets) != 14 || h.runtime.stoppedCount() != 14 {
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
	if err != nil || len(effects) != 0 || len(h.runtime.targets()) != 0 {
		t.Fatalf("mismatch produced effects=%d targets=%d err=%v", len(effects), len(h.runtime.targets()), err)
	}
}

func TestResolverFailureBecomesUncertainAndFailsRunClosed(t *testing.T) {
	h := newHarness(t, "denied-fixture")
	run := h.runProfile("e2-denied", e2, "denied-fixture")
	if run.Status != contracts.RunStopping || run.Termination == nil || run.Termination.PrimaryFailure == nil ||
		run.Termination.PrimaryFailure.Class != contracts.FailureUncertain {
		t.Fatalf("resolver failure run = %#v", run)
	}
	effects, err := h.controller.ListEffects(t.Context(), "", 100)
	if err != nil || len(effects) != 1 || effects[0].State != contracts.EffectUncertain {
		t.Fatalf("resolver failure effects = %#v, err=%v", effects, err)
	}
	if _, err := h.controller.Drive(t.Context(), run.RunID); !errors.Is(err, controller.ErrRunClosureOpen) {
		t.Fatalf("uncertain run did not remain closure-blocked: %v", err)
	}
}

func TestMidLaunchFailureCompensatesEveryPreviouslyAppliedProcess(t *testing.T) {
	h := newHarness(t, "denied-fixture")
	definition, action := h.workflow("e2-mid-failure", e2, "robot-fixture")
	failedNode := 3 // topology + two successful launches, then deny the third.
	definition.Nodes[failedNode].FixedInputs["executableRef"] = "denied-fixture"
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	action.DefinitionDigest = plan.DefinitionDigest
	run := h.invokeAndDrive("e2-mid-failure", definition, action, e2, e2)
	if run.Status != contracts.RunStopping || run.Termination == nil || run.Termination.PrimaryFailure == nil ||
		run.Termination.PrimaryFailure.Class != contracts.FailureUncertain {
		t.Fatalf("mid-launch failure run = %#v", run)
	}
	if h.runtime.activeCount() != 0 || h.runtime.stoppedCount() != 2 {
		t.Fatalf("mid-launch compensation active=%d stopped=%d", h.runtime.activeCount(), h.runtime.stoppedCount())
	}
	effects, err := h.controller.ListEffects(t.Context(), "", 100)
	if err != nil || len(effects) != 3 {
		t.Fatalf("mid-launch effects = %#v, err=%v", effects, err)
	}
	succeededCompensations, uncertain := 0, 0
	for _, current := range effects {
		if current.CompensationState == contracts.EffectCompensationSucceeded {
			succeededCompensations++
		}
		if current.State == contracts.EffectUncertain {
			uncertain++
		}
	}
	if succeededCompensations != 2 || uncertain != 1 {
		t.Fatalf("mid-launch compensation evidence: succeeded=%d uncertain=%d effects=%#v", succeededCompensations, uncertain, effects)
	}
}

func (h *harness) runProfile(runID string, desired profile, executableRef string) contracts.Run {
	h.t.Helper()
	definition, action := h.workflow(runID, desired, executableRef)
	return h.invokeAndDrive(runID, definition, action, desired, desired)
}

func (h *harness) invokeAndDrive(runID string, definition contracts.WorkflowDefinition, action contracts.ActionVersion, actual, expected profile) contracts.Run {
	return h.invokeInputAndDrive(runID, definition, action, profileInput(actual, expected))
}

func (h *harness) invokeInputAndDrive(runID string, definition contracts.WorkflowDefinition, action contracts.ActionVersion, input map[string]any) contracts.Run {
	h.t.Helper()
	now := time.Now().UTC()
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
	run, err := h.coordinator.AdvanceRun(h.t.Context(), invoked.Run.RunID)
	if err != nil && !errors.Is(err, controller.ErrRunClosureOpen) {
		h.t.Fatal(err)
	}
	if !run.Status.Terminal() && run.Status != contracts.RunStopping {
		h.t.Fatalf("run %s did not terminate: %s", runID, run.Status)
	}
	return run
}

func (h *harness) stopProfile(startRun contracts.Run, desired profile) contracts.Run {
	h.t.Helper()
	snapshot, _, err := h.controller.GetSnapshot(h.t.Context(), startRun.RunID)
	if err != nil {
		h.t.Fatal(err)
	}
	descriptor := h.stop.Descriptor()
	empty := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{}}
	stopRunID := startRun.RunID + "-stop"
	definition := contracts.WorkflowDefinition{
		SchemaVersion: workflowkernel.SchemaVersion, WorkflowID: "robotics-" + stopRunID, Version: "v1",
		InputSchema: empty, ResultSchema: empty, TriggerSchema: empty, ScopeSchema: empty,
		Entrypoints: map[string]string{"main": ""}, ResultBindings: map[string][]contracts.ValueBinding{"main": {}},
	}
	previous := ""
	for _, instance := range expand(startRun.RunID, desired) {
		output := snapshot.NodeOutputs["launch-"+instance]
		externalIdentity, ok := output["externalIdentity"].(string)
		if !ok || !contracts.ValidIdentifier(externalIdentity) {
			h.t.Fatalf("start output for %s has no exact external identity: %#v", instance, output)
		}
		nodeID := "stop-" + instance
		definition.Nodes = append(definition.Nodes, contracts.WorkflowNodeDefinition{
			NodeID: nodeID, TypeRef: descriptor.TypeRef, DescriptorDigest: descriptor.DescriptorDigest,
			InputSchema: descriptor.InputSchema, OutputSchema: descriptor.OutputSchema,
			FixedInputs: map[string]any{
				"bindingId": instance, "ownerRunRef": startRun.RunID, "externalIdentityRef": externalIdentity,
			},
		})
		if previous == "" {
			definition.Entrypoints["main"] = nodeID
		} else {
			definition.Edges = append(definition.Edges, contracts.WorkflowEdge{From: previous, To: nodeID, Kind: contracts.EdgeControl})
		}
		previous = nodeID
	}
	plan, err := workflowkernel.Compile(definition)
	if err != nil {
		h.t.Fatal(err)
	}
	action := contracts.ActionVersion{
		ActionID: definition.WorkflowID, Version: definition.Version, DefinitionDigest: plan.DefinitionDigest,
		Entrypoint: "main", InputSchema: empty, ResultSchema: empty,
		AcceptedTriggerKinds: []contracts.TriggerKind{contracts.TriggerManual},
	}
	return h.invokeInputAndDrive(stopRunID, definition, action, map[string]any{})
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
		path := filepath.Join(h.runtime.directory, target+".stdout")
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
