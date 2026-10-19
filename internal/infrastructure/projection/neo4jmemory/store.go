package neo4jmemory

import (
	"agi-assistant/internal/domain/memory/consistency"
	pneo "agi-assistant/internal/infrastructure/platform/neo4j"
	"context"
	"fmt"
	driver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"sort"
)

type graphClient interface {
	Available() bool
	Session() driver.SessionWithContext
}
type Neo4jStore struct{ client graphClient }

func NewNeo4jStore(c *pneo.Client) *Neo4jStore {
	if c == nil {
		return &Neo4jStore{}
	}
	return &Neo4jStore{client: c}
}
func (s *Neo4jStore) available() error {
	if s.client == nil || !s.client.Available() {
		return fmt.Errorf("neo4j unavailable")
	}
	return nil
}

// Finish the result stream before accepting an autocommit read or a write.
func values(ctx context.Context, res driver.ResultWithContext, count int) ([]any, bool, error) {
	if !res.Next(ctx) {
		if err := res.Err(); err != nil {
			return nil, false, err
		}
		_, err := res.Consume(ctx)
		return nil, false, err
	}
	record := res.Record()
	if record == nil || len(record.Values) != count {
		return nil, false, fmt.Errorf("neo4j: malformed row")
	}
	row := append([]any(nil), record.Values...)
	if res.Next(ctx) {
		return nil, false, fmt.Errorf("neo4j: duplicate row")
	}
	if err := res.Err(); err != nil {
		return nil, false, err
	}
	if _, err := res.Consume(ctx); err != nil {
		return nil, false, err
	}
	return row, true, nil
}
func state(id int64, row []any) (consistency.ProjectionState, error) {
	version, vok := row[0].(int64)
	hash, hok := row[1].(string)
	deleted, dok := row[2].(bool)
	if !vok || !hok || !dok || version < 0 {
		return consistency.ProjectionState{}, fmt.Errorf("neo4j: invalid projection state")
	}
	return consistency.ProjectionState{ID: id, Version: version, ContentHash: hash, Deleted: deleted}, nil
}
func (s *Neo4jStore) Get(ctx context.Context, id int64) (consistency.ProjectionState, bool, error) {
	if err := s.available(); err != nil {
		return consistency.ProjectionState{}, false, err
	}
	session := s.client.Session()
	defer session.Close(ctx)
	res, err := session.Run(ctx, `MATCH (m:Memory {memory_id:$id}) RETURN coalesce(m.version,0),coalesce(m.content_hash,''),coalesce(m.deleted,false)`, map[string]any{"id": id})
	if err != nil {
		return consistency.ProjectionState{}, false, err
	}
	row, ok, err := values(ctx, res, 3)
	if err != nil || !ok {
		return consistency.ProjectionState{}, false, err
	}
	out, err := state(id, row)
	return out, err == nil, err
}
func (s *Neo4jStore) Upsert(ctx context.Context, r consistency.MemoryRecord) error {
	return s.write(ctx, r, false)
}
func (s *Neo4jStore) Delete(ctx context.Context, id, version int64) error {
	return s.DeleteRecord(ctx, consistency.MemoryRecord{ID: id, Version: version})
}
func (s *Neo4jStore) DeleteRecord(ctx context.Context, r consistency.MemoryRecord) error {
	return s.write(ctx, r, true)
}

// Node metadata and SUPERSEDES links form one transaction. Missing/foreign
// references roll it back so a later event cannot observe a half-written view.
func (s *Neo4jStore) write(ctx context.Context, r consistency.MemoryRecord, deleted bool) error {
	if err := s.available(); err != nil {
		return err
	}
	if r.ID <= 0 || r.Version < 0 || (!deleted && (r.UserID == "" || r.Version == 0)) {
		return fmt.Errorf("neo4j: invalid record")
	}
	ids := make([]int64, 0, len(r.Supersedes))
	seen := map[int64]bool{}
	if !deleted {
		for _, id := range r.Supersedes {
			if id <= 0 || id == r.ID {
				return fmt.Errorf("neo4j: invalid superseded memory ID")
			}
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	session := s.client.Session()
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx driver.ManagedTransaction) (any, error) {
		// The dependent update takes a write lock before reading the stored version.
		res, err := tx.Run(ctx, `MERGE (m:Memory {memory_id:$id}) ON CREATE SET m.version=-1
   SET m._projection_lock=coalesce(m._projection_lock,0)+1
   RETURN coalesce(m.version,0),coalesce(m.content_hash,''),coalesce(m.deleted,false),coalesce(m.user_id,'')`, map[string]any{"id": r.ID})
		if err != nil {
			return nil, err
		}
		row, ok, err := values(ctx, res, 4)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("neo4j: missing locked node")
		}
		version, vok := row[0].(int64)
		hash, hok := row[1].(string)
		wasDeleted, dok := row[2].(bool)
		owner, uok := row[3].(string)
		if !vok || !hok || !dok || !uok {
			return nil, fmt.Errorf("neo4j: malformed locked node")
		}
		if owner != "" && r.UserID != "" && owner != r.UserID {
			return nil, fmt.Errorf("neo4j: owner conflict")
		}
		if version > r.Version {
			return nil, nil
		}
		if version == r.Version && r.ContentHash != "" && (hash != r.ContentHash || (!deleted && wasDeleted)) {
			return nil, fmt.Errorf("neo4j: equal version conflict")
		}
		user := r.UserID
		if user == "" {
			user = owner
		}
		tags := append([]string{}, r.Tags...)
		content := r.Content
		if deleted {
			content = ""
			tags = []string{}
		}
		params := map[string]any{"id": r.ID, "version": r.Version, "hash": r.ContentHash, "user": user, "content": content, "category": r.Category, "tags": tags, "importance": r.Importance, "slot": r.SlotHint, "quarantined": r.Quarantined, "superseded": r.Superseded, "deleted": deleted, "supersedes": ids}
		res, err = tx.Run(ctx, `MATCH (m:Memory {memory_id:$id}) SET m.version=$version,m.content_hash=$hash,m.user_id=$user,m.content=$content,m.category=$category,m.tags=$tags,m.importance=$importance,m.slot_hint=$slot,m.quarantined=$quarantined,m.superseded=$superseded,m.deleted=$deleted`, params)
		if err != nil {
			return nil, err
		}
		if _, err = res.Consume(ctx); err != nil {
			return nil, err
		}
		if deleted {
			// Keep supersession audit links when their old endpoint is tombstoned.
			res, err = tx.Run(ctx, `MATCH (m:Memory {memory_id:$id}) OPTIONAL MATCH (m)-[rel]-() WHERE type(rel) <> 'SUPERSEDES' DELETE rel`, params)
			if err != nil {
				return nil, err
			}
			_, err = res.Consume(ctx)
			return nil, err
		}
		res, err = tx.Run(ctx, `MATCH (m:Memory {memory_id:$id}) OPTIONAL MATCH (m)-[rel:SUPERSEDES]->() DELETE rel
   WITH DISTINCT m UNWIND $supersedes AS oldID MATCH (old:Memory {memory_id:oldID}) WHERE old.user_id=$user
   MERGE (m)-[:SUPERSEDES]->(old) RETURN count(old)`, params)
		if err != nil {
			return nil, err
		}
		row, ok, err = values(ctx, res, 1)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("neo4j: missing relation result")
		}
		count, ok := row[0].(int64)
		if !ok || count != int64(len(ids)) {
			return nil, fmt.Errorf("neo4j: missing or foreign superseded memories")
		}
		return nil, nil
	})
	return err
}
func (s *Neo4jStore) ListPage(ctx context.Context, after int64, limit int) ([]consistency.ProjectionState, error) {
	if err := s.available(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("neo4j: page limit must be positive")
	}
	session := s.client.Session()
	defer session.Close(ctx)
	res, err := session.Run(ctx, `MATCH (m:Memory) WHERE m.memory_id > $after RETURN m.memory_id,coalesce(m.version,0),coalesce(m.content_hash,''),coalesce(m.deleted,false) ORDER BY m.memory_id LIMIT $limit`, map[string]any{"after": after, "limit": int64(limit)})
	if err != nil {
		return nil, err
	}
	var out []consistency.ProjectionState
	for res.Next(ctx) {
		record := res.Record()
		if record == nil || len(record.Values) != 4 {
			return nil, fmt.Errorf("neo4j: malformed page row")
		}
		id, ok := record.Values[0].(int64)
		if !ok || id <= after {
			return nil, fmt.Errorf("neo4j: invalid page ID")
		}
		st, e := state(id, record.Values[1:])
		if e != nil {
			return nil, e
		}
		out = append(out, st)
	}
	if err = res.Err(); err != nil {
		return nil, err
	}
	if _, err = res.Consume(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
