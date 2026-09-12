package client

import (
	"context"
	"sync"
	"time"

	"github.com/quangh33/godicer/pkg/common"
	dicerproto "github.com/quangh33/godicer/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SliceLookup actively and asynchronously watches and caches the Assignment from the Assigner.
// Backed by AtomicAssignmentCell, it is completely thread-safe and lock-free for readers.
// Ported directly from com.databricks.dicer.client.SliceLookup in Scala.
type SliceLookup struct {
	target       string
	clientName   string
	cfg          StateMachineConfig
	cell         *common.AtomicAssignmentCell
	router       *ResourceRouter
	sm           *AssignmentSyncStateMachine
	grpcClientFn func(addr string) (dicerproto.AssignmentServiceClient, func(), error)

	readyOnce sync.Once
	readyChan chan struct{}

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// SliceLookupOption customizes the SliceLookup instance.
type SliceLookupOption func(*SliceLookup)

// WithGRPCClientFactory overrides the client factory (useful for tests or custom connection pooling).
func WithGRPCClientFactory(fn func(addr string) (dicerproto.AssignmentServiceClient, func(), error)) SliceLookupOption {
	return func(sl *SliceLookup) {
		sl.grpcClientFn = fn
	}
}

// NewSliceLookup constructs a new SliceLookup.
func NewSliceLookup(
	target string,
	clientName string,
	cfg StateMachineConfig,
	opts ...SliceLookupOption,
) *SliceLookup {
	cell := common.NewAtomicAssignmentCell(nil)
	router := NewResourceRouter(cell)
	sm := NewAssignmentSyncStateMachine(cfg, time.Now().UnixNano())

	sl := &SliceLookup{
		target:     target,
		clientName: clientName,
		cfg:        cfg,
		cell:       cell,
		router:     router,
		sm:         sm,
		readyChan:  make(chan struct{}),
	}

	// Default gRPC factory
	sl.grpcClientFn = func(addr string) (dicerproto.AssignmentServiceClient, func(), error) {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		client := dicerproto.NewAssignmentServiceClient(conn)
		cleanup := func() { _ = conn.Close() }
		return client, cleanup, nil
	}

	for _, opt := range opts {
		opt(sl)
	}

	return sl
}

// Cell returns the underlying AtomicAssignmentCell.
func (sl *SliceLookup) Cell() *common.AtomicAssignmentCell {
	return sl.cell
}

// Router returns the ResourceRouter.
func (sl *SliceLookup) Router() *ResourceRouter {
	return sl.router
}

// Ready returns a channel that is closed when the initial assignment has been received from the Assigner.
// Equivalent to Clerk.ready Future in Scala.
func (sl *SliceLookup) Ready() <-chan struct{} {
	return sl.readyChan
}

// WaitReady blocks until the initial assignment is received or ctx is cancelled.
func (sl *SliceLookup) WaitReady(ctx context.Context) error {
	select {
	case <-sl.readyChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start launches the background synchronization goroutine.
func (sl *SliceLookup) Start() {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if sl.ctx != nil {
		return // already started
	}

	sl.ctx, sl.cancel = context.WithCancel(context.Background())
	sl.wg.Add(1)
	go sl.runLoop()
}

// Stop gracefully stops the background synchronization loop.
func (sl *SliceLookup) Stop() {
	sl.mu.Lock()
	cancel := sl.cancel
	sl.mu.Unlock()

	if cancel != nil {
		cancel()
		sl.wg.Wait()
	}
}

func (sl *SliceLookup) runLoop() {
	defer sl.wg.Done()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sl.ctx.Done():
			sl.mu.Lock()
			sl.sm.OnEvent(time.Now(), EventCancel{})
			sl.mu.Unlock()
			return
		case now := <-ticker.C:
			sl.mu.Lock()
			actions := sl.sm.OnAdvance(now)
			sl.mu.Unlock()

			for _, action := range actions {
				sl.handleAction(action)
			}
		}
	}
}

func (sl *SliceLookup) handleAction(action DriverAction) {
	switch act := action.(type) {
	case ActionUseAssignment:
		sl.cell.Set(act.Assignment)
		sl.readyOnce.Do(func() {
			close(sl.readyChan)
		})
	case ActionSendRequest:
		go sl.executeSendRequest(act)
	}
}

func (sl *SliceLookup) executeSendRequest(act ActionSendRequest) {
	addr := sl.cfg.DefaultWatchAddress
	if !act.Redirect.IsEmpty() {
		addr = *act.Redirect.Address
	}

	client, cleanup, err := sl.grpcClientFn(addr)
	if err != nil {
		sl.mu.Lock()
		actions := sl.sm.OnEvent(time.Now(), EventReadFailure{
			OpID: act.OpID,
			Err:  err,
		})
		sl.mu.Unlock()
		for _, a := range actions {
			sl.handleAction(a)
		}
		return
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(sl.ctx, act.Timeout)
	defer cancel()

	req := &dicerproto.ClientRequestP{
		Target: &dicerproto.TargetP{
			Name: sl.target,
		},
		SyncAssignmentState:    act.SyncState,
		SubscriberDebugName:    sl.clientName,
		ChosenRpcTimeoutMillis: act.Timeout.Milliseconds(),
		SubscriberData:         &dicerproto.ClientRequestP_ClerkFields{ClerkFields: &dicerproto.ClerkDataP{ClientId: sl.clientName}},
		RedirectToken:          act.Redirect.Token,
	}

	resp, err := client.Watch(ctx, req)
	now := time.Now()

	sl.mu.Lock()
	var actions []DriverAction
	if err != nil {
		actions = sl.sm.OnEvent(now, EventReadFailure{
			OpID: act.OpID,
			Err:  err,
		})
	} else {
		actions = sl.sm.OnEvent(now, EventReadSuccess{
			Address:  &addr,
			OpID:     act.OpID,
			Response: resp,
		})
	}
	sl.mu.Unlock()

	for _, a := range actions {
		sl.handleAction(a)
	}
}
