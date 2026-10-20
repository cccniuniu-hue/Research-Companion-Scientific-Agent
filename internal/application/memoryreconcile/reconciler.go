package memoryreconcile

import (
	"agi-assistant/internal/domain/memory/consistency"
	"context"
	"encoding/json"
	"fmt"
)

// Both readers must return at most limit rows in strictly increasing ID order.
type Source interface {
	LoadPage(context.Context, int64, int) ([]consistency.MemoryRecord, error)
}
type TargetReader interface {
	ListPage(context.Context, int64, int) ([]consistency.ProjectionState, error)
}
type RepairQueue interface {
	EnqueueRepair(context.Context, consistency.OutboxEvent, string) (bool, error)
}
type Report struct{ Checked, Missing, Stale, Orphan, RepairEnqueued, RepairSkipped, Conflicts int64 }
type Reconciler struct {
	source     Source
	target     TargetReader
	queue      RepairQueue
	targetName consistency.Target
	page       int
}

func New(s Source, t TargetReader, q RepairQueue, target consistency.Target, page int) *Reconciler {
	if page <= 0 {
		page = 500
	}
	return &Reconciler{s, t, q, target, page}
}

func (r *Reconciler) RunOnce(ctx context.Context) (out Report, err error) {
	if err = ctx.Err(); err != nil {
		return out, err
	}
	if r.source == nil || r.target == nil || r.queue == nil {
		return out, fmt.Errorf("reconcile: unavailable source, target or repair queue")
	}
	if r.targetName != consistency.TargetMilvus && r.targetName != consistency.TargetNeo4j {
		return out, fmt.Errorf("reconcile: unsupported target")
	}
	// Complete both reads before deciding absence. Comparing only the first
	// independently paged batches can turn valid later rows into false orphans.
	// shortcut: O(source+target) records are retained; use a snapshot-aware merge
	// scan when memory volume warrants it. There is no cross-store snapshot.
	var pg []consistency.MemoryRecord
	authoritative := map[int64]bool{}
	for after := int64(0); ; {
		if err = ctx.Err(); err != nil {
			return out, err
		}
		page, e := r.source.LoadPage(ctx, after, r.page)
		if e != nil {
			return out, e
		}
		if len(page) == 0 {
			break
		}
		if len(page) > r.page {
			return out, fmt.Errorf("reconcile: source page exceeds limit")
		}
		for _, m := range page {
			if m.ID <= after || m.Version <= 0 || m.UserID == "" {
				return out, fmt.Errorf("reconcile: unordered or invalid source page")
			}
			after = m.ID
			authoritative[m.ID] = true
			pg = append(pg, m)
		}
	}
	var ts []consistency.ProjectionState
	tm := map[int64]consistency.ProjectionState{}
	for after := int64(0); ; {
		if err = ctx.Err(); err != nil {
			return out, err
		}
		page, e := r.target.ListPage(ctx, after, r.page)
		if e != nil {
			return out, e
		}
		if len(page) == 0 {
			break
		}
		if len(page) > r.page {
			return out, fmt.Errorf("reconcile: target page exceeds limit")
		}
		for _, s := range page {
			if s.ID <= after || s.Version < 0 {
				return out, fmt.Errorf("reconcile: unordered or invalid target page")
			}
			after = s.ID
			tm[s.ID] = s
			ts = append(ts, s)
		}
	}
	for _, m := range pg {
		if err = r.compare(ctx, &out, m, tm); err != nil {
			return out, err
		}
	}
	for _, s := range ts {
		if authoritative[s.ID] || s.Deleted {
			continue
		}
		if err = ctx.Err(); err != nil {
			return out, err
		}
		// A source row may have committed after the initial scan. Recheck its exact
		// ID immediately before emitting an orphan delete to reduce that race.
		rows, e := r.source.LoadPage(ctx, s.ID-1, 1)
		if e != nil {
			return out, e
		}
		if len(rows) > 1 {
			return out, fmt.Errorf("reconcile: invalid orphan recheck page")
		}
		if len(rows) == 1 {
			m := rows[0]
			if m.ID < s.ID || m.Version <= 0 || m.UserID == "" {
				return out, fmt.Errorf("reconcile: invalid orphan recheck record")
			}
			if m.ID == s.ID {
				if err = r.compare(ctx, &out, m, tm); err != nil {
					return out, err
				}
				continue
			}
		}
		out.Orphan++
		m := consistency.MemoryRecord{ID: s.ID, Version: s.Version}
		if err = r.enqueue(ctx, &out, m, true, fmt.Sprintf("%s:%d:%d:delete:repair", r.targetName, s.ID, s.Version)); err != nil {
			return out, err
		}
	}
	return out, nil
}
func (r *Reconciler) compare(ctx context.Context, out *Report, m consistency.MemoryRecord, tm map[int64]consistency.ProjectionState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	out.Checked++
	s, ok := tm[m.ID]
	deleted := m.DeletedAt != nil
	if !ok {
		out.Missing++
	} else {
		if s.Version == m.Version && s.ContentHash == m.ContentHash && s.Deleted == deleted {
			return nil
		}
		// The projector deliberately rejects old writes and equal-version conflicts.
		// Report them for review instead of enqueueing work that cannot repair them.
		if s.Version > m.Version || (s.Version == m.Version && (s.ContentHash != m.ContentHash || (!deleted && s.Deleted))) {
			out.Conflicts++
			return nil
		}
		out.Stale++
	}
	typ := r.eventType(deleted)
	return r.enqueue(ctx, out, m, deleted, fmt.Sprintf("%s:%d:%d:%s:repair", r.targetName, m.ID, m.Version, typ))
}
func (r *Reconciler) eventType(deleted bool) consistency.EventType {
	if r.targetName == consistency.TargetNeo4j {
		if deleted {
			return consistency.EventDeleteMemoryGraphNode
		}
		return consistency.EventUpsertMemoryGraphNode
	}
	if deleted {
		return consistency.EventDeleteMemoryVector
	}
	return consistency.EventUpsertMemoryVector
}
func (r *Reconciler) enqueue(ctx context.Context, out *Report, m consistency.MemoryRecord, deleted bool, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("reconcile: encode repair: %w", err)
	}
	e := consistency.OutboxEvent{AggregateID: m.ID, UserID: m.UserID, AggregateVersion: m.Version, Type: r.eventType(deleted), Target: r.targetName, Payload: payload}
	added, err := r.queue.EnqueueRepair(ctx, e, key)
	if err != nil {
		return err
	}
	if added {
		out.RepairEnqueued++
	} else {
		out.RepairSkipped++
	}
	return nil
}
