package memoryprojection

import (
	"agi-assistant/internal/domain/memory/consistency"
	"context"
	"time"
)

// Outbox uses the current event-ID-only acknowledgment API. A context deadline
// limits one batch, but cannot atomically fence a stale owner after reclaim.
// Deploy one active consumer per target until the repository supports claim tokens.
type Outbox interface {
	Claim(context.Context, consistency.Target, string, int, time.Duration) ([]consistency.OutboxEvent, error)
	MarkProcessed(context.Context, string) error
	MarkRetry(context.Context, string, string, time.Time) error
	MarkDead(context.Context, string, string) error
}

type Projector interface {
	Target() consistency.Target
	Apply(context.Context, consistency.OutboxEvent) error
}
