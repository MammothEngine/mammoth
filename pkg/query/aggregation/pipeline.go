// Package aggregation implements MongoDB-compatible aggregation pipelines
// with optimized execution using the Volcano iterator model.
package aggregation

import (
	"context"
	"fmt"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Pipeline represents an aggregation pipeline.
type Pipeline struct {
	stages []Stage
}

// Stage is a single aggregation pipeline stage.
type Stage interface {
	// Process applies the stage transformation to the input iterator.
	Process(ctx context.Context, input Iterator) (Iterator, error)
	// Name returns the stage name (e.g., "$match", "$group").
	Name() string
}

// Iterator iterates over documents in the aggregation pipeline.
type Iterator interface {
	// Next returns the next document in the pipeline.
	Next() (*bson.Document, error)
	// Close releases resources.
	Close() error
}

// NewPipeline creates a new aggregation pipeline from stage definitions.
func NewPipeline(stageDefs []StageDefinition) (*Pipeline, error) {
	if len(stageDefs) == 0 {
		return nil, fmt.Errorf("aggregation pipeline cannot be empty")
	}

	stages := make([]Stage, 0, len(stageDefs))
	for _, def := range stageDefs {
		stage, err := createStage(def)
		if err != nil {
			return nil, fmt.Errorf("create stage %s: %w", def.Name, err)
		}
		stages = append(stages, stage)
	}

	return &Pipeline{stages: stages}, nil
}

// StageDefinition defines a pipeline stage from user input.
type StageDefinition struct {
	Name  string
	Value interface{}
}

// Execute runs the pipeline starting from the source iterator.
func (p *Pipeline) Execute(ctx context.Context, source Iterator) (Iterator, error) {
	iterator := source
	var err error

	for _, stage := range p.stages {
		iterator, err = stage.Process(ctx, iterator)
		if err != nil {
			return nil, fmt.Errorf("stage %s: %w", stage.Name(), err)
		}
	}

	return iterator, nil
}

// createStage creates a Stage from a StageDefinition.
func createStage(def StageDefinition) (Stage, error) {
	switch def.Name {
	case "$match":
		return newMatchStage(def.Value)
	case "$project":
		return newProjectStage(def.Value)
	case "$group":
		return newGroupStage(def.Value)
	case "$sort":
		return newSortStage(def.Value)
	case "$limit":
		return newLimitStage(def.Value)
	case "$skip":
		return newSkipStage(def.Value)
	case "$unwind":
		return newUnwindStage(def.Value)
	case "$lookup":
		return newLookupStage(def.Value)
	default:
		return nil, fmt.Errorf("unsupported stage: %s", def.Name)
	}
}

// sliceIterator implements Iterator over a slice of documents.
type sliceIterator struct {
	docs []*bson.Document
	pos  int
}

func newSliceIterator(docs []*bson.Document) *sliceIterator {
	return &sliceIterator{docs: docs, pos: -1}
}

func (it *sliceIterator) Next() (*bson.Document, error) {
	it.pos++
	if it.pos >= len(it.docs) {
		return nil, nil
	}
	return it.docs[it.pos], nil
}

func (it *sliceIterator) Close() error {
	it.docs = nil
	return nil
}

// emptyIterator returns no documents.
type emptyIterator struct{}

func (it *emptyIterator) Next() (*bson.Document, error) {
	return nil, nil
}

func (it *emptyIterator) Close() error {
	return nil
}

// TransformIterator applies a transformation function.
type TransformIterator struct {
	source Iterator
	transform func(*bson.Document) (*bson.Document, error)
}

func (it *TransformIterator) Next() (*bson.Document, error) {
	doc, err := it.source.Next()
	if err != nil || doc == nil {
		return nil, err
	}
	return it.transform(doc)
}

func (it *TransformIterator) Close() error {
	return it.source.Close()
}

// FilterIterator filters documents based on a predicate.
type FilterIterator struct {
	source    Iterator
	predicate func(*bson.Document) (bool, error)
}

func (it *FilterIterator) Next() (*bson.Document, error) {
	for {
		doc, err := it.source.Next()
		if err != nil || doc == nil {
			return nil, err
		}
		match, err := it.predicate(doc)
		if err != nil {
			return nil, err
		}
		if match {
			return doc, nil
		}
	}
}

func (it *FilterIterator) Close() error {
	return it.source.Close()
}

// LimitIterator limits the number of documents.
type LimitIterator struct {
	source Iterator
	limit  int64
	count  int64
}

func newLimitIterator(source Iterator, limit int64) *LimitIterator {
	return &LimitIterator{source: source, limit: limit}
}

func (it *LimitIterator) Next() (*bson.Document, error) {
	if it.count >= it.limit {
		return nil, nil
	}
	doc, err := it.source.Next()
	if err != nil || doc == nil {
		return nil, err
	}
	it.count++
	return doc, nil
}

func (it *LimitIterator) Close() error {
	return it.source.Close()
}

// SkipIterator skips the first N documents.
type SkipIterator struct {
	source Iterator
	skip   int64
	count  int64
}

func newSkipIterator(source Iterator, skip int64) *SkipIterator {
	return &SkipIterator{source: source, skip: skip}
}

func (it *SkipIterator) Next() (*bson.Document, error) {
	for it.count < it.skip {
		doc, err := it.source.Next()
		if err != nil || doc == nil {
			return nil, err
		}
		it.count++
	}
	return it.source.Next()
}

func (it *SkipIterator) Close() error {
	return it.source.Close()
}

// Count returns the number of stages in the pipeline.
func (p *Pipeline) Count() int {
	return len(p.stages)
}

// StageNames returns the names of all stages in the pipeline.
func (p *Pipeline) StageNames() []string {
	names := make([]string, len(p.stages))
	for i, stage := range p.stages {
		names[i] = stage.Name()
	}
	return names
}
