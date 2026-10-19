package memoryprojection

import (
	"agi-assistant/internal/domain/memory/consistency"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeOutbox struct {
	events    []consistency.OutboxEvent
	processed []string
	retried   []string
	dead      []string
}

func (f *fakeOutbox) Claim(context.Context, consistency.Target, string, int, time.Duration) ([]consistency.OutboxEvent, error) {
	return f.events, nil
}
func (f *fakeOutbox) MarkProcessed(_ context.Context, id string) error {
	f.processed = append(f.processed, id)
	return nil
}
func (f *fakeOutbox) MarkRetry(_ context.Context, id, _ string, _ time.Time) error {
	f.retried = append(f.retried, id)
	return nil
}
func (f *fakeOutbox) MarkDead(_ context.Context, id, _ string) error {
	f.dead = append(f.dead, id)
	return nil
}

type fakeProjector struct{ err error }

func (f fakeProjector) Target() consistency.Target                           { return consistency.TargetMilvus }
func (f fakeProjector) Apply(context.Context, consistency.OutboxEvent) error { return f.err }

func TestWorkerMarksSuccessfulEventProcessed(t *testing.T) {
	repo := &fakeOutbox{events: []consistency.OutboxEvent{{EventID: "e1"}}}
	w := NewWorker(repo, fakeProjector{}, Config{WorkerID: "w1", BatchSize: 10, MaxAttempts: 3})
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.processed) != 1 || repo.processed[0] != "e1" {
		t.Fatalf("processed=%v", repo.processed)
	}
}

func TestWorkerRetriesFailureThenMarksDeadAtLimit(t *testing.T) {
	repo := &fakeOutbox{events: []consistency.OutboxEvent{{EventID: "e1", Attempts: 1}, {EventID: "e2", Attempts: 2}}}
	w := NewWorker(repo, fakeProjector{err: errors.New("down")}, Config{WorkerID: "w1", BatchSize: 10, MaxAttempts: 3})
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.retried) != 1 || repo.retried[0] != "e1" {
		t.Fatalf("retried=%v", repo.retried)
	}
	if len(repo.dead) != 1 || repo.dead[0] != "e2" {
		t.Fatalf("dead=%v", repo.dead)
	}
}

type checkOutbox struct {
	events                 []consistency.OutboxEvent
	claimErr, statusErr    error
	processed, retry, dead []string
	at                     time.Time
	reason                 string
	claim                  func(context.Context)
	claimed                int
	target                 consistency.Target
	worker                 string
	limit                  int
	lease                  time.Duration
}

func (f *checkOutbox) Claim(ctx context.Context, target consistency.Target, worker string, limit int, lease time.Duration) ([]consistency.OutboxEvent, error) {
	f.claimed++
	f.target = target
	f.worker = worker
	f.limit = limit
	f.lease = lease
	if f.claim != nil {
		f.claim(ctx)
	}
	return f.events, f.claimErr
}
func (f *checkOutbox) MarkProcessed(_ context.Context, id string) error {
	f.processed = append(f.processed, id)
	return f.statusErr
}
func (f *checkOutbox) MarkRetry(_ context.Context, id, msg string, at time.Time) error {
	f.retry = append(f.retry, id)
	f.at = at
	f.reason = msg
	return f.statusErr
}
func (f *checkOutbox) MarkDead(_ context.Context, id, msg string) error {
	f.dead = append(f.dead, id)
	f.reason = msg
	return f.statusErr
}

type checkProjector struct {
	apply func(context.Context, consistency.OutboxEvent) error
}

func (checkProjector) Target() consistency.Target { return consistency.TargetMilvus }
func (p checkProjector) Apply(ctx context.Context, e consistency.OutboxEvent) error {
	if p.apply != nil {
		return p.apply(ctx, e)
	}
	return nil
}
func workerEvent(id string, attempt int) consistency.OutboxEvent {
	return consistency.OutboxEvent{EventID: id, Attempts: attempt, Target: consistency.TargetMilvus}
}
func TestWorkerPassesConfigurationAndEmptyBatch(t *testing.T) {
	r := &checkOutbox{}
	w := NewWorker(r, checkProjector{}, Config{WorkerID: "w"})
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.worker != "w" || r.target != consistency.TargetMilvus || r.limit != 100 || r.lease != 30*time.Second {
		t.Fatalf("claim=%+v", r)
	}
}
func TestWorkerDoesNotAcknowledgeAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &checkOutbox{events: []consistency.OutboxEvent{workerEvent("e1", 0), workerEvent("e2", 0)}}
	calls := 0
	w := NewWorker(r, checkProjector{apply: func(context.Context, consistency.OutboxEvent) error { calls++; cancel(); return nil }}, Config{WorkerID: "w"})
	if e := w.RunOnce(ctx); !errors.Is(e, context.Canceled) {
		t.Fatalf("err=%v", e)
	}
	if calls != 1 || len(r.processed)+len(r.retry)+len(r.dead) != 0 {
		t.Fatalf("calls=%d repo=%+v", calls, r)
	}
}
func TestWorkerLeavesExpiredClaimForRecovery(t *testing.T) {
	r := &checkOutbox{events: []consistency.OutboxEvent{workerEvent("e1", 0)}}
	w := NewWorker(r, checkProjector{apply: func(ctx context.Context, _ consistency.OutboxEvent) error {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
		}
		return nil
	}}, Config{WorkerID: "w", Lease: 5 * time.Millisecond})
	if e := w.RunOnce(context.Background()); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("err=%v", e)
	}
	if len(r.processed)+len(r.retry)+len(r.dead) != 0 {
		t.Fatal("expired claim acknowledged")
	}
}
func TestWorkerDoesNotApplyLateOrCanceledClaims(t *testing.T) {
	for _, kind := range []string{"already_canceled", "late_claim"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := &checkOutbox{events: []consistency.OutboxEvent{workerEvent("e1", 0)}}
			if kind == "already_canceled" {
				cancel()
			} else {
				r.claim = func(ctx context.Context) {
					if _, ok := ctx.Deadline(); ok {
						<-ctx.Done()
					}
				}
			}
			applied := 0
			w := NewWorker(r, checkProjector{apply: func(context.Context, consistency.OutboxEvent) error { applied++; return nil }}, Config{WorkerID: "w", Lease: 5 * time.Millisecond})
			if e := w.RunOnce(ctx); e == nil {
				t.Fatal("canceled request succeeded")
			}
			if applied != 0 || len(r.processed) != 0 || (kind == "already_canceled" && r.claimed != 0) {
				t.Fatalf("applied=%d claimed=%d", applied, r.claimed)
			}
		})
	}
}
func TestWorkerBoundsRetryBackoffAndAttempts(t *testing.T) {
	for _, attempt := range []int{-1, 0, 2, 20, math.MaxInt} {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			r := &checkOutbox{events: []consistency.OutboxEvent{workerEvent("e", attempt)}}
			before := time.Now()
			w := NewWorker(r, checkProjector{apply: func(context.Context, consistency.OutboxEvent) error { return errors.New("backend down") }}, Config{WorkerID: "w", MaxAttempts: 100})
			if e := w.RunOnce(context.Background()); e != nil {
				t.Fatal(e)
			}
			if attempt >= 99 {
				if len(r.dead) != 1 {
					t.Fatal("overflow attempt not dead")
				}
			} else {
				delay := time.Duration(1<<min(max(attempt, 0), 10)) * time.Second
				if len(r.retry) != 1 || r.at.Before(before.Add(delay)) || r.at.After(time.Now().Add(delay)) || r.reason != "backend down" {
					t.Fatalf("retry=%+v delay=%v", r, delay)
				}
			}
		})
	}
}
func TestWorkerRecoversPanicAndContinuesBatch(t *testing.T) {
	r := &checkOutbox{events: []consistency.OutboxEvent{workerEvent("bad", 0), workerEvent("good", 0)}}
	w := NewWorker(r, checkProjector{apply: func(_ context.Context, e consistency.OutboxEvent) error {
		if e.EventID == "bad" {
			panic("synthetic")
		}
		return nil
	}}, Config{WorkerID: "w"})
	if e := w.RunOnce(context.Background()); e != nil || len(r.retry) != 1 || len(r.processed) != 1 || r.processed[0] != "good" {
		t.Fatalf("repo=%+v err=%v", r, e)
	}
}
func TestWorkerPropagatesClaimAndStatusErrors(t *testing.T) {
	for _, kind := range []string{"claim", "processed", "retry", "dead"} {
		t.Run(kind, func(t *testing.T) {
			failure := errors.New("injected")
			r := &checkOutbox{events: []consistency.OutboxEvent{workerEvent("e", 0)}}
			p := checkProjector{}
			cfg := Config{WorkerID: "w"}
			if kind == "claim" {
				r.claimErr = failure
			} else {
				r.statusErr = failure
			}
			if kind == "retry" || kind == "dead" {
				p.apply = func(context.Context, consistency.OutboxEvent) error { return errors.New("projector down") }
			}
			if kind == "dead" {
				cfg.MaxAttempts = 1
			}
			if e := NewWorker(r, p, cfg).RunOnce(context.Background()); !errors.Is(e, failure) {
				t.Fatalf("err=%v", e)
			}
		})
	}
}
func TestWorkerRejectsUnavailableDependenciesAndEmptyIdentity(t *testing.T) {
	r := &checkOutbox{}
	ctx := context.Background()
	if e := NewWorker(nil, checkProjector{}, Config{WorkerID: "w"}).RunOnce(ctx); e == nil {
		t.Fatal("nil repo accepted")
	}
	if e := NewWorker(r, nil, Config{WorkerID: "w"}).RunOnce(ctx); e == nil {
		t.Fatal("nil projector accepted")
	}
	if e := NewWorker(r, checkProjector{}, Config{}).RunOnce(ctx); e == nil || r.claimed != 0 {
		t.Fatal("empty worker identity accepted")
	}
}
func TestWorkerSerializesConcurrentBatches(t *testing.T) {
	r := &checkOutbox{events: []consistency.OutboxEvent{workerEvent("e", 0)}}
	var active, maxActive atomic.Int32
	var wg sync.WaitGroup
	w := NewWorker(r, checkProjector{apply: func(context.Context, consistency.OutboxEvent) error {
		n := active.Add(1)
		for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
		}
		time.Sleep(time.Millisecond)
		active.Add(-1)
		return nil
	}}, Config{WorkerID: "w"})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := w.RunOnce(context.Background()); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if maxActive.Load() != 1 {
		t.Fatalf("overlapping batches=%d", maxActive.Load())
	}
}
