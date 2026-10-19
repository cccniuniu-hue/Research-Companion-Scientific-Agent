package versioned

import (
	"agi-assistant/internal/domain/memory/consistency"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

var ErrProjectionConflict = errors.New("projection: equal version hash conflict")

type Store interface {
	Get(context.Context, int64) (consistency.ProjectionState, bool, error)
	Upsert(context.Context, consistency.MemoryRecord) error
	Delete(context.Context, int64, int64) error
}
type Projector struct {
	mu          sync.Mutex
	Store       Store
	TargetValue consistency.Target
	Upserts     map[consistency.EventType]bool
	Deletes     map[consistency.EventType]bool
}

func (p *Projector) Target() consistency.Target { return p.TargetValue }
func (p *Projector) Apply(ctx context.Context, e consistency.OutboxEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.Store == nil {
		return fmt.Errorf("projection: store unavailable")
	}
	upsert, del := p.Upserts[e.Type], p.Deletes[e.Type]
	if e.Target != p.TargetValue || (!upsert && !del) {
		return fmt.Errorf("projection: unsupported target or event type")
	}
	var r consistency.MemoryRecord
	if err := json.Unmarshal(e.Payload, &r); err != nil {
		return err
	}
	if r.ID <= 0 || r.Version < 0 || e.AggregateID != r.ID || e.AggregateVersion != r.Version || e.UserID != r.UserID {
		return fmt.Errorf("projection: event and record identity/version mismatch")
	}
	if upsert && (r.Version == 0 || r.UserID == "" || r.DeletedAt != nil) {
		return fmt.Errorf("projection: invalid upsert record")
	}
	if r.ContentHash != "" {
		hash, err := consistency.ComputeContentHash(r)
		if err != nil {
			return err
		}
		if hash != r.ContentHash {
			return fmt.Errorf("projection: payload hash mismatch")
		}
	}
	// shortcut: one instance serializes read/write; separate processes need a
	// single writer or backend compare-and-set before concurrent deployment.
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	current, ok, err := p.Store.Get(ctx, r.ID)
	if err != nil {
		return err
	}
	if ok && (current.ID != r.ID || current.Version < 0) {
		return fmt.Errorf("projection: invalid stored state")
	}
	if ok && r.Version < current.Version {
		return nil
	}
	if p.Upserts[e.Type] {
		if ok && r.Version == current.Version {
			if current.Deleted || r.ContentHash != current.ContentHash {
				return ErrProjectionConflict
			}
			return nil
		}
		return p.Store.Upsert(ctx, r)
	}
	if p.Deletes[e.Type] {
		if ok && r.Version == current.Version {
			if r.ContentHash != "" && r.ContentHash != current.ContentHash {
				return ErrProjectionConflict
			}
			if current.Deleted {
				return nil
			}
		}
		if store, ok := p.Store.(interface {
			DeleteRecord(context.Context, consistency.MemoryRecord) error
		}); ok {
			return store.DeleteRecord(ctx, r)
		}
		// Legacy stores cannot retain deletion watermarks across restart.
		return p.Store.Delete(ctx, r.ID, r.Version)
	}
	return nil
}
