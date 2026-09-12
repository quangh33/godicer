package client

import (
	"math/rand"
	"time"

	"github.com/quangh33/godicer/pkg/common"
	dicerproto "github.com/quangh33/godicer/pkg/proto"
)

// Redirect represents a redirection instruction received from an assigner or server.
type Redirect struct {
	Address *string
	Token   []byte
}

// IsEmpty returns true if no redirect address is set.
func (r Redirect) IsEmpty() bool {
	return r.Address == nil || *r.Address == ""
}

// EmptyRedirect represents the absence of a redirect.
var EmptyRedirect = Redirect{}

// Event defines input signals to the state machine.
type Event interface {
	isEvent()
}

// EventReadSuccess is emitted when a watch RPC finishes successfully.
type EventReadSuccess struct {
	Address  *string
	OpID     int64
	Response *dicerproto.ClientResponseP
}

func (EventReadSuccess) isEvent() {}

// EventReadFailure is emitted when a watch RPC fails or errors.
type EventReadFailure struct {
	OpID int64
	Err  error
}

func (EventReadFailure) isEvent() {}

// EventCancel stops and marks the state machine as inactive.
type EventCancel struct{}

func (EventCancel) isEvent() {}

// DriverAction defines actions emitted by the state machine to be executed by the driver.
type DriverAction interface {
	isAction()
}

// ActionUseAssignment tells the driver to update the locally cached assignment.
type ActionUseAssignment struct {
	Assignment *common.Assignment
}

func (ActionUseAssignment) isAction() {}

// ActionSendRequest tells the driver to transmit a Watch RPC request.
type ActionSendRequest struct {
	Redirect  Redirect
	OpID      int64
	SyncState *dicerproto.SyncAssignmentStateP
	Timeout   time.Duration
}

func (ActionSendRequest) isAction() {}

// StateMachineConfig holds timing and endpoint configuration for the state machine.
type StateMachineConfig struct {
	DefaultWatchAddress string
	WatchRPCTimeout     time.Duration
	MinRetryDelay       time.Duration
	MaxRetryDelay       time.Duration
}

// DefaultStateMachineConfig returns production-ready default timing configuration.
func DefaultStateMachineConfig() StateMachineConfig {
	return StateMachineConfig{
		DefaultWatchAddress: "localhost:50051",
		WatchRPCTimeout:     30 * time.Second,
		MinRetryDelay:       50 * time.Millisecond,
		MaxRetryDelay:       2 * time.Second,
	}
}

type readState struct {
	opID     int64
	deadline time.Time
}

type remoteKnownGeneration struct {
	address    *string
	generation common.Generation
}

// AssignmentSyncStateMachine implements the passive state machine controlling assignment syncing.
// Ported directly from com.databricks.dicer.client.AssignmentSyncStateMachine in Scala.
type AssignmentSyncStateMachine struct {
	config StateMachineConfig
	rng    *rand.Rand

	isCancelled        bool
	assignment         *common.Assignment
	lastOpID           int64
	latestReadState    *readState
	redirect           Redirect
	remoteKnown        remoteKnownGeneration
	watchRPCTimeout    time.Duration
	inBackoff          bool
	currentBackoffWait time.Duration
	scheduledReadTime  time.Time
}

// NewAssignmentSyncStateMachine constructs an AssignmentSyncStateMachine.
func NewAssignmentSyncStateMachine(config StateMachineConfig, seed int64) *AssignmentSyncStateMachine {
	if config.WatchRPCTimeout <= 0 {
		config.WatchRPCTimeout = 30 * time.Second
	}
	if config.MinRetryDelay <= 0 {
		config.MinRetryDelay = 50 * time.Millisecond
	}
	if config.MaxRetryDelay <= 0 {
		config.MaxRetryDelay = 2 * time.Second
	}

	src := rand.NewSource(seed)
	return &AssignmentSyncStateMachine{
		config:             config,
		rng:                rand.New(src),
		watchRPCTimeout:    config.WatchRPCTimeout,
		currentBackoffWait: config.MinRetryDelay,
		remoteKnown: remoteKnownGeneration{
			address:    nil,
			generation: common.EmptyGeneration,
		},
	}
}

// IsCancelled returns whether the state machine was cancelled.
func (sm *AssignmentSyncStateMachine) IsCancelled() bool {
	return sm.isCancelled
}

// IsInBackoff returns whether the state machine is currently waiting out an error backoff.
func (sm *AssignmentSyncStateMachine) IsInBackoff() bool {
	return sm.inBackoff
}

// CurrentAssignment returns the latest valid assignment known to the state machine.
func (sm *AssignmentSyncStateMachine) CurrentAssignment() *common.Assignment {
	return sm.assignment
}

// OnEvent processes an external event and returns any actions to be taken.
func (sm *AssignmentSyncStateMachine) OnEvent(now time.Time, event Event) []DriverAction {
	if sm.isCancelled {
		return nil
	}

	var actions []DriverAction
	switch ev := event.(type) {
	case EventReadSuccess:
		sm.onReadSuccess(now, ev.Address, ev.OpID, ev.Response, &actions)
	case EventReadFailure:
		sm.onReadFailure(now, ev.OpID)
	case EventCancel:
		sm.isCancelled = true
		return nil
	}

	sm.onAdvanceInternal(now, &actions)
	return actions
}

// OnAdvance evaluates time progression and returns any scheduled actions.
func (sm *AssignmentSyncStateMachine) OnAdvance(now time.Time) []DriverAction {
	if sm.isCancelled {
		return nil
	}
	var actions []DriverAction
	sm.onAdvanceInternal(now, &actions)
	return actions
}

func (sm *AssignmentSyncStateMachine) onReadSuccess(
	now time.Time,
	address *string,
	opID int64,
	resp *dicerproto.ClientResponseP,
	actions *[]DriverAction,
) {
	if resp == nil {
		return
	}

	// 1. Incorporate sync state if newer
	isNew := sm.incorporateSyncState(resp.SyncAssignmentState, actions)

	// 2. Incorporate redirect from response
	if resp.Redirect != nil {
		addr := resp.Redirect.Address
		sm.redirect = Redirect{
			Address: &addr,
			Token:   resp.Redirect.RedirectToken,
		}
	} else {
		sm.redirect = EmptyRedirect
	}

	var respGen common.Generation
	if resp.SyncAssignmentState != nil {
		switch s := resp.SyncAssignmentState.State.(type) {
		case *dicerproto.SyncAssignmentStateP_KnownGeneration:
			respGen = common.GenerationFromProto(s.KnownGeneration)
		case *dicerproto.SyncAssignmentStateP_KnownAssignment:
			if s.KnownAssignment != nil {
				respGen = common.GenerationFromProto(s.KnownAssignment.Generation)
			}
		}
	}

	isLatest := sm.latestReadState != nil && opID == sm.latestReadState.opID
	if !isLatest {
		// Old op response
		if isNew {
			sm.remoteKnown = remoteKnownGeneration{
				address:    address,
				generation: respGen,
			}
			if resp.SuggestedRpcTimeoutMillis > 0 {
				sm.watchRPCTimeout = time.Duration(resp.SuggestedRpcTimeoutMillis) * time.Millisecond
			}
		}
	} else {
		// Latest read succeeded: clear inflight state, reset backoff
		sm.latestReadState = nil
		sm.resetBackoff()
		if resp.SuggestedRpcTimeoutMillis > 0 {
			sm.watchRPCTimeout = time.Duration(resp.SuggestedRpcTimeoutMillis) * time.Millisecond
		}
		sm.remoteKnown = remoteKnownGeneration{
			address:    address,
			generation: respGen,
		}
		// Schedule next read immediately
		sm.scheduledReadTime = now
	}
}

func (sm *AssignmentSyncStateMachine) onReadFailure(now time.Time, opID int64) {
	if sm.latestReadState == nil || opID != sm.latestReadState.opID {
		// Stale failure, ignore
		return
	}
	sm.setupRetry(now)
}

func (sm *AssignmentSyncStateMachine) incorporateSyncState(
	syncState *dicerproto.SyncAssignmentStateP,
	actions *[]DriverAction,
) bool {
	if syncState == nil {
		return false
	}
	assignmentProto := syncState.GetKnownAssignment()
	if assignmentProto == nil {
		return false
	}

	newAssignment, err := common.AssignmentFromProto(assignmentProto)
	if err != nil {
		return false
	}

	// Check if this assignment is strictly newer than our current assignment
	if sm.assignment == nil || newAssignment.Generation.Compare(sm.assignment.Generation) > 0 {
		sm.assignment = newAssignment
		*actions = append(*actions, ActionUseAssignment{Assignment: newAssignment})
		return true
	}
	return false
}

func (sm *AssignmentSyncStateMachine) setupRetry(now time.Time) {
	sm.latestReadState = nil
	// On error, clear redirect to fall back to default watch server
	sm.redirect = EmptyRedirect
	sm.inBackoff = true

	// Exponential backoff with jitter
	delay := sm.currentBackoffWait
	// Add jitter in [0.5 * delay, 1.5 * delay]
	jitterFactor := 0.5 + sm.rng.Float64()
	actualDelay := time.Duration(float64(delay) * jitterFactor)

	sm.scheduledReadTime = now.Add(actualDelay)

	// Exponential increment capped at MaxRetryDelay
	nextWait := sm.currentBackoffWait * 2
	if nextWait > sm.config.MaxRetryDelay {
		nextWait = sm.config.MaxRetryDelay
	}
	sm.currentBackoffWait = nextWait
}

func (sm *AssignmentSyncStateMachine) resetBackoff() {
	sm.inBackoff = false
	sm.currentBackoffWait = sm.config.MinRetryDelay
}

func (sm *AssignmentSyncStateMachine) onAdvanceInternal(now time.Time, actions *[]DriverAction) {
	if sm.latestReadState != nil {
		// Check if outstanding read has exceeded its deadline
		if !now.Before(sm.latestReadState.deadline) {
			sm.setupRetry(now)
		}
	}

	if sm.latestReadState != nil {
		// Read still outstanding
		return
	}

	// Check if scheduled retry / next read time is reached
	if !now.Before(sm.scheduledReadTime) {
		sm.inBackoff = false
		sm.lastOpID++
		opID := sm.lastOpID
		deadline := now.Add(sm.watchRPCTimeout)
		sm.latestReadState = &readState{
			opID:     opID,
			deadline: deadline,
		}

		// Prepare sync state
		var syncState *dicerproto.SyncAssignmentStateP
		if sm.assignment != nil &&
			sm.assignment.Generation.Compare(sm.remoteKnown.generation) > 0 &&
			sameAddress(sm.redirect.Address, sm.remoteKnown.address) {
			// Remote server is lagging, send our assignment
			syncState = &dicerproto.SyncAssignmentStateP{
				State: &dicerproto.SyncAssignmentStateP_KnownAssignment{
					KnownAssignment: common.AssignmentToProto(sm.assignment),
				},
			}
		} else {
			var knownGen common.Generation
			if sm.assignment != nil {
				knownGen = sm.assignment.Generation
			}
			syncState = &dicerproto.SyncAssignmentStateP{
				State: &dicerproto.SyncAssignmentStateP_KnownGeneration{
					KnownGeneration: common.GenerationToProto(knownGen),
				},
			}
		}

		*actions = append(*actions, ActionSendRequest{
			Redirect:  sm.redirect,
			OpID:      opID,
			SyncState: syncState,
			Timeout:   sm.watchRPCTimeout,
		})
	}
}

func sameAddress(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
