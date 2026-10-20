package milvusmemory

import (
	"agi-assistant/internal/domain/memory/consistency"
	"context"
	"fmt"
	mc "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"io"
	"math"
)

const collection = "long_term_memory_vectors"

type MilvusStore struct {
	client mc.Client
	dim    int
}

func NewMilvusStore(c mc.Client, dim int) *MilvusStore { return &MilvusStore{c, dim} }
func (s *MilvusStore) Init(ctx context.Context) error {
	if s.client == nil || s.dim <= 0 {
		return fmt.Errorf("milvus unavailable")
	}
	has, err := s.client.HasCollection(ctx, collection)
	if err != nil {
		return err
	}
	if has {
		meta, e := s.client.DescribeCollection(ctx, collection)
		if e != nil {
			return e
		}
		if meta == nil || meta.Schema == nil {
			return fmt.Errorf("milvus: missing collection schema")
		}
		fields := map[string]*entity.Field{}
		for _, f := range meta.Schema.Fields {
			fields[f.Name] = f
		}
		for name, typ := range map[string]entity.FieldType{"memory_id": entity.FieldTypeInt64, "user_id": entity.FieldTypeVarChar, "version": entity.FieldTypeInt64, "content_hash": entity.FieldTypeVarChar, "deleted": entity.FieldTypeBool, "embedding": entity.FieldTypeFloatVector} {
			if fields[name] == nil || fields[name].DataType != typ {
				return fmt.Errorf("milvus: incompatible field %s; migrate collection before use", name)
			}
		}
		if !fields["memory_id"].PrimaryKey || fields["embedding"].TypeParams["dim"] != fmt.Sprint(s.dim) {
			return fmt.Errorf("milvus: incompatible primary key or vector dimension")
		}
		indexes, e := s.client.DescribeIndex(ctx, collection, "embedding")
		if e != nil {
			return e
		}
		if len(indexes) == 0 {
			if e = s.createIndex(ctx); e != nil {
				return e
			}
		}
		return s.client.LoadCollection(ctx, collection, false)
	}
	schema := &entity.Schema{CollectionName: collection, Fields: []*entity.Field{{Name: "memory_id", DataType: entity.FieldTypeInt64, PrimaryKey: true}, {Name: "user_id", DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "256"}}, {Name: "version", DataType: entity.FieldTypeInt64}, {Name: "content_hash", DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "64"}}, {Name: "deleted", DataType: entity.FieldTypeBool}, {Name: "embedding", DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": fmt.Sprint(s.dim)}}}}
	if err = s.client.CreateCollection(ctx, schema, 1, mc.WithConsistencyLevel(entity.ClStrong)); err != nil {
		return err
	}
	if err = s.createIndex(ctx); err != nil {
		return err
	}
	return s.client.LoadCollection(ctx, collection, false)
}
func (s *MilvusStore) createIndex(ctx context.Context) error {
	index, err := entity.NewIndexFlat(entity.COSINE)
	if err != nil {
		return err
	}
	return s.client.CreateIndex(ctx, collection, "embedding", index, false)
}

type row struct {
	ID      int64  `milvus:"name:memory_id"`
	Version int64  `milvus:"name:version"`
	Hash    string `milvus:"name:content_hash"`
	Deleted bool   `milvus:"name:deleted"`
}

func (s *MilvusStore) Get(ctx context.Context, id int64) (consistency.ProjectionState, bool, error) {
	if s.client == nil {
		return consistency.ProjectionState{}, false, fmt.Errorf("milvus unavailable")
	}
	rs, err := s.client.Query(ctx, collection, nil, fmt.Sprintf("memory_id == %d", id), []string{"memory_id", "version", "content_hash", "deleted"}, mc.WithSearchQueryConsistencyLevel(entity.ClStrong))
	if err != nil {
		return consistency.ProjectionState{}, false, err
	}
	var rows []*row
	if err = validateColumns(rs); err != nil {
		return consistency.ProjectionState{}, false, err
	}
	if err = rs.Unmarshal(&rows); err != nil {
		return consistency.ProjectionState{}, false, err
	}
	if len(rows) == 0 {
		return consistency.ProjectionState{}, false, nil
	}
	if len(rows) != 1 || rows[0].ID != id {
		return consistency.ProjectionState{}, false, fmt.Errorf("milvus: unexpected memory ID or duplicate result")
	}
	return consistency.ProjectionState{ID: rows[0].ID, Version: rows[0].Version, ContentHash: rows[0].Hash, Deleted: rows[0].Deleted}, true, nil
}
func (s *MilvusStore) Upsert(ctx context.Context, r consistency.MemoryRecord) error {
	if len(r.Embedding) != s.dim || s.dim <= 0 {
		return fmt.Errorf("milvus: embedding dimension mismatch")
	}
	v := make([]float32, s.dim)
	for i, x := range r.Embedding {
		if math.IsNaN(x) || math.IsInf(x, 0) || math.Abs(x) > math.MaxFloat32 {
			return fmt.Errorf("milvus: non-finite float32 embedding")
		}
		v[i] = float32(x)
	}
	return s.write(ctx, r, v, false)
}
func (s *MilvusStore) write(ctx context.Context, r consistency.MemoryRecord, v []float32, deleted bool) error {
	if s.client == nil {
		return fmt.Errorf("milvus unavailable")
	}
	if r.ID <= 0 || r.Version < 0 || len(r.UserID) > 256 || len(r.ContentHash) > 64 {
		return fmt.Errorf("milvus: invalid record")
	}
	_, err := s.client.Upsert(ctx, collection, "", entity.NewColumnInt64("memory_id", []int64{r.ID}), entity.NewColumnVarChar("user_id", []string{r.UserID}), entity.NewColumnInt64("version", []int64{r.Version}), entity.NewColumnVarChar("content_hash", []string{r.ContentHash}), entity.NewColumnBool("deleted", []bool{deleted}), entity.NewColumnFloatVector("embedding", s.dim, [][]float32{v}))
	return err
}
func (s *MilvusStore) Delete(ctx context.Context, id, version int64) error {
	return s.DeleteRecord(ctx, consistency.MemoryRecord{ID: id, Version: version})
}

// Keep a scalar watermark and a dummy vector so delayed events cannot resurrect
// deleted memories. Any direct vector search must exclude deleted rows.
func (s *MilvusStore) DeleteRecord(ctx context.Context, r consistency.MemoryRecord) error {
	if s.dim <= 0 {
		return fmt.Errorf("milvus: invalid dimension")
	}
	v := make([]float32, s.dim)
	v[0] = 1
	return s.write(ctx, r, v, true)
}
func validateColumns(rs mc.ResultSet) error {
	if len(rs) == 0 {
		return nil
	}
	for _, name := range []string{"memory_id", "version", "content_hash", "deleted"} {
		col := rs.GetColumn(name)
		if col == nil || col.Len() != rs.Len() {
			return fmt.Errorf("milvus: missing or inconsistent column %s", name)
		}
	}
	return nil
}
func (s *MilvusStore) ListPage(ctx context.Context, after int64, limit int) ([]consistency.ProjectionState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.client == nil {
		return nil, fmt.Errorf("milvus unavailable")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("milvus: page limit must be positive")
	}
	// The SDK iterator requests primary-key ordering on the server. Sorting a
	// limited scalar result locally can permanently skip IDs between pages.
	meta, err := s.client.DescribeCollection(ctx, collection)
	if err != nil {
		return nil, err
	}
	if meta == nil || meta.ConsistencyLevel != entity.ClStrong {
		return nil, fmt.Errorf("milvus: ordered reconciliation requires a strong-consistency collection")
	}
	iterator, err := s.client.QueryIterator(ctx, mc.NewQueryIteratorOption(collection).WithExpr(fmt.Sprintf("memory_id > %d", after)).WithOutputFields("memory_id", "version", "content_hash", "deleted").WithBatchSize(limit))
	if err != nil {
		return nil, err
	}
	if iterator == nil {
		return nil, fmt.Errorf("milvus: missing query iterator")
	}
	rs, err := iterator.Next(ctx)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rows []*row
	if err = validateColumns(rs); err != nil {
		return nil, err
	}
	if err = rs.Unmarshal(&rows); err != nil {
		return nil, err
	}
	out := make([]consistency.ProjectionState, len(rows))
	last := after
	for i, r := range rows {
		if r.ID <= last || r.Version < 0 || len(rows) > limit {
			return nil, fmt.Errorf("milvus: unordered or invalid iterator page")
		}
		last = r.ID
		out[i] = consistency.ProjectionState{ID: r.ID, Version: r.Version, ContentHash: r.Hash, Deleted: r.Deleted}
	}
	return out, nil
}
