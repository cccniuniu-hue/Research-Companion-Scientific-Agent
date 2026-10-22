package chat

import (
	"context"
	"fmt"
	"time"

	"agi-assistant/internal/domain/memory/consistency"
	"agi-assistant/internal/infrastructure/persistence/memorytx"
	"agi-assistant/internal/pkg/logger"
	"agi-assistant/internal/usercontext"
)

// commitMemory persists a new memory and its outbox events before making the
// committed row visible in the local cache.
func (a *UnifiedAgent) commitMemory(ctx context.Context, cmd memorytx.CreateCommand) (consistency.MemoryRecord, error) {
	if a.mem == nil || a.mem.ltm == nil || a.repos == nil || a.repos.memoryTx == nil {
		return consistency.MemoryRecord{}, fmt.Errorf("memory commit repository unavailable")
	}
	if owner := usercontext.UserIDFromContext(ctx); owner != "" && owner != cmd.UserID {
		return consistency.MemoryRecord{}, fmt.Errorf("memory commit owner mismatch")
	}
	a.mem.commitMu.Lock()
	defer a.mem.commitMu.Unlock()
	if err := ctx.Err(); err != nil {
		return consistency.MemoryRecord{}, err
	}
	changes, err := a.repos.memoryTx.Create(ctx, cmd)
	if err != nil {
		return consistency.MemoryRecord{}, err
	}
	if len(changes.Upserts) != 1 || len(changes.Deletes) != 0 {
		return consistency.MemoryRecord{}, fmt.Errorf("memory commit returned %d upserts, want 1", len(changes.Upserts))
	}
	record := changes.Upserts[0]
	if record.ID <= 0 || record.Version <= 0 || record.UserID == "" || record.UserID != cmd.UserID || record.DeletedAt != nil {
		return record, fmt.Errorf("memory commit returned an invalid authoritative record")
	}
	if err := a.applyCommittedMemory(ctx, changes); err != nil {
		// 数据库已经提交，不能以写入失败诱导重试创建；缓存失败单独报告。
		logger.C(ctx).Error("memory committed but cache unavailable", "memory_id", record.ID, "version", record.Version, "err", err)
	}
	return record, nil
}

func (a *UnifiedAgent) consolidateCommitted(ctx context.Context) error {
	if a.mem == nil || a.mem.ltm == nil || a.repos == nil || a.repos.memoryTx == nil {
		return fmt.Errorf("memory consolidation repository unavailable")
	}
	a.mem.commitMu.Lock()
	defer a.mem.commitMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	plan := a.mem.ltm.PlanConsolidation(time.Now())
	if len(plan.Updates) == 0 && len(plan.Deletes) == 0 {
		return nil
	}
	for i := range plan.Updates {
		u := &plan.Updates[i]
		stored, err := a.authoritativeMemory(ctx, u.Record.ID)
		if err != nil {
			return err
		}
		if stored.UserID != u.Record.UserID || stored.Version != u.ExpectedVersion {
			return memorytx.ErrVersionConflict
		}
		// 旧缓存没有保存这两个字段，不能把空值写回源记录。
		u.Record.EmbeddingModel, u.Record.EmbeddingRevision = stored.EmbeddingModel, stored.EmbeddingRevision
	}
	changes, err := a.repos.memoryTx.ApplyConsolidation(ctx, plan)
	if err != nil {
		return err
	}
	return a.applyCommittedMemory(ctx, changes)
}

// authoritativeMemory 通过有序页读取单个仍存在的源记录。
func (a *UnifiedAgent) authoritativeMemory(ctx context.Context, id int64) (consistency.MemoryRecord, error) {
	rows, err := a.repos.memoryTx.LoadPage(ctx, id-1, 1)
	if err != nil {
		return consistency.MemoryRecord{}, err
	}
	if id <= 0 || len(rows) != 1 || rows[0].ID != id || rows[0].Version <= 0 || rows[0].DeletedAt != nil {
		return consistency.MemoryRecord{}, fmt.Errorf("authoritative memory %d unavailable", id)
	}
	return rows[0], nil
}

// 调用方持有 commitMu；本实例的提交不能插入全量重载和缓存替换之间。
func (a *UnifiedAgent) applyCommittedMemory(ctx context.Context, changes consistency.CommittedChangeSet) error {
	if err := a.mem.ltm.ApplyCommitted(changes); err == nil {
		return nil
	}
	active, err := a.repos.memoryTx.LoadActive(ctx)
	if err != nil {
		// 无法确认缓存的版本时先停止召回，保留数据库中的已提交结果。
		a.mem.ltm.ReplaceCommitted(nil)
		return fmt.Errorf("committed memory cache refresh failed: %w", err)
	}
	a.mem.ltm.ReplaceCommitted(active)
	return nil
}
