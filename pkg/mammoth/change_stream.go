package mammoth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/repl"
)

// ChangeStream watches for changes in a collection or database.
type ChangeStream struct {
	mu sync.RWMutex

	// Configuration
	coll       *Collection
	db         *Database
	ns         string         // namespace being watched (db.coll or db)
	pipeline   []PipelineStage
	opts       ChangeStreamOptions
	replicator *repl.OplogReplicator

	// State
	ctx        context.Context
	cancel     context.CancelFunc
	tailClient *repl.TailClient
	buffer     []*ChangeEvent
	position   int
	closed     bool
	started    bool

	// Resume token
	resumeToken string

	// Error state
	err error
}

// ChangeEvent represents a single change notification.
type ChangeEvent struct {
	ID               ResumeToken              `json:"_id" bson:"_id"`
	OperationType    string                   `json:"operationType" bson:"operationType"`
	FullDocument     map[string]interface{}   `json:"fullDocument,omitempty" bson:"fullDocument,omitempty"`
	NS               Namespace                `json:"ns" bson:"ns"`
	To               *Namespace               `json:"to,omitempty" bson:"to,omitempty"` // For rename
	DocumentKey      map[string]interface{}   `json:"documentKey" bson:"documentKey"`
	UpdateDescription *UpdateDescription      `json:"updateDescription,omitempty" bson:"updateDescription,omitempty"`
	ClusterTime      time.Time                `json:"clusterTime" bson:"clusterTime"`
	TxnNumber        *int64                   `json:"txnNumber,omitempty" bson:"txnNumber,omitempty"`
	LSID             map[string]interface{}   `json:"lsid,omitempty" bson:"lsid,omitempty"`
}

// ResumeToken uniquely identifies a change event position.
type ResumeToken struct {
	Data string `json:"data" bson:"data"`
}

// Namespace identifies a collection.
type Namespace struct {
	DB   string `json:"db" bson:"db"`
	Coll string `json:"coll" bson:"coll"`
}

// UpdateDescription describes the changes in an update operation.
type UpdateDescription struct {
	UpdatedFields map[string]interface{}   `json:"updatedFields" bson:"updatedFields"`
	RemovedFields []string                 `json:"removedFields" bson:"removedFields"`
	TruncatedArrays []TruncatedArray       `json:"truncatedArrays,omitempty" bson:"truncatedArrays,omitempty"`
}

// TruncatedArray describes an array that was truncated.
type TruncatedArray struct {
	Field string      `json:"field" bson:"field"`
	NewSize int32     `json:"newSize" bson:"newSize"`
}

// ChangeStreamOptions configures change stream behavior.
type ChangeStreamOptions struct {
	FullDocument           FullDocumentOption
	ResumeAfter            *ResumeToken
	StartAtOperationTime   *time.Time
	BatchSize              int32
	MaxAwaitTime           time.Duration
	ShowExpandedEvents     bool
}

// FullDocumentOption controls full document lookup in change events.
type FullDocumentOption string

const (
	// Default does not include full document for updates.
	Default FullDocumentOption = "default"
	// Off never includes full document.
	Off FullDocumentOption = "off"
	// WhenAvailable includes full document when available.
	WhenAvailable FullDocumentOption = "whenAvailable"
	// Required includes full document, error if unavailable.
	Required FullDocumentOption = "required"
	// UpdateLookup includes full document for updates (post-image).
	UpdateLookup FullDocumentOption = "updateLookup"
)

// PipelineStage represents an aggregation pipeline stage for filtering.
type PipelineStage struct {
	Stage string
	Value interface{}
}

// Watch creates a change stream on a collection.
func (c *Collection) Watch(ctx context.Context, pipeline []PipelineStage, opts ...ChangeStreamOptions) (*ChangeStream, error) {
	opt := ChangeStreamOptions{FullDocument: UpdateLookup}
	if len(opts) > 0 {
		opt = opts[0]
	}

	return newChangeStream(ctx, c, nil, pipeline, opt, nil)
}

// Watch creates a change stream on a database (watches all collections).
func (db *Database) Watch(ctx context.Context, pipeline []PipelineStage, opts ...ChangeStreamOptions) (*ChangeStream, error) {
	opt := ChangeStreamOptions{FullDocument: UpdateLookup}
	if len(opts) > 0 {
		opt = opts[0]
	}

	return newChangeStream(ctx, nil, db, pipeline, opt, nil)
}

// newChangeStream creates a new change stream.
func newChangeStream(ctx context.Context, coll *Collection, db *Database, pipeline []PipelineStage, opts ChangeStreamOptions, replicator *repl.OplogReplicator) (*ChangeStream, error) {
	// Determine namespace
	var ns string
	if coll != nil {
		ns = fmt.Sprintf("%s.%s", coll.db, coll.name)
	} else if db != nil {
		ns = db.db
	}

	// Create context with cancel
	streamCtx, cancel := context.WithCancel(ctx)

	cs := &ChangeStream{
		coll:     coll,
		db:       db,
		ns:       ns,
		pipeline: pipeline,
		opts:     opts,
		ctx:      streamCtx,
		cancel:   cancel,
		buffer:   make([]*ChangeEvent, 0),
		position: -1,
	}

	// Set resume token if provided
	if opts.ResumeAfter != nil {
		cs.resumeToken = opts.ResumeAfter.Data
	}

	// Start background tailing
	if err := cs.start(replicator); err != nil {
		cancel()
		return nil, err
	}

	return cs, nil
}

// start begins tailing the oplog.
func (cs *ChangeStream) start(replicator *repl.OplogReplicator) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.started {
		return errors.New("change stream already started")
	}

	// For now, without a replicator, we can't tail
	// In a real implementation, this would get the replicator from the database
	if replicator == nil {
		// Create a mock/simplified tailing mechanism
		cs.started = true
		go cs.mockTailLoop()
		return nil
	}

	// Build filter based on namespace
	filter := make(map[string]interface{})
	if cs.coll != nil {
		// Watch specific collection
		filter["ns"] = cs.ns
	} else if cs.db != nil {
		// Watch all collections in database - filter by prefix
		filter["ns_prefix"] = cs.db.db
	}

	// Apply pipeline filters
	for _, stage := range cs.pipeline {
		if stage.Stage == "$match" {
			if matchFilter, ok := stage.Value.(map[string]interface{}); ok {
				for k, v := range matchFilter {
					filter[k] = v
				}
			}
		}
	}

	// Determine start time
	var since time.Time
	if cs.opts.StartAtOperationTime != nil {
		since = *cs.opts.StartAtOperationTime
	} else if cs.resumeToken != "" {
		// Decode resume token to get timestamp
		since = decodeResumeToken(cs.resumeToken)
	}

	// Start tailing
	tailClient, err := replicator.Tail(cs.ctx, since, filter)
	if err != nil {
		return fmt.Errorf("failed to start tailing: %w", err)
	}

	cs.tailClient = tailClient
	cs.started = true

	// Start processing loop
	go cs.processLoop()

	return nil
}

// mockTailLoop is a placeholder for when no replicator is available.
func (cs *ChangeStream) mockTailLoop() {
	// Without a replicator, we simply wait for context cancellation
	<-cs.ctx.Done()
	cs.mu.Lock()
	cs.closed = true
	cs.mu.Unlock()
}

// processLoop processes oplog entries and converts them to change events.
func (cs *ChangeStream) processLoop() {
	if cs.tailClient == nil {
		return
	}

	for {
		select {
		case <-cs.ctx.Done():
			return
		case entry, ok := <-cs.tailClient.Ch:
			if !ok {
				cs.mu.Lock()
				cs.err = errors.New("change stream closed")
				cs.mu.Unlock()
				return
			}

			event := cs.convertOplogEntry(entry)
			if event != nil && cs.matchesPipeline(event) {
				cs.mu.Lock()
				cs.buffer = append(cs.buffer, event)
				cs.mu.Unlock()
			}
		}
	}
}

// convertOplogEntry converts an oplog entry to a change event.
func (cs *ChangeStream) convertOplogEntry(entry *repl.OplogEntry) *ChangeEvent {
	event := &ChangeEvent{
		ID:          ResumeToken{Data: encodeResumeToken(entry.Timestamp, entry.Hash)},
		ClusterTime: entry.Timestamp,
		NS:          parseNamespace(entry.Namespace),
	}

	// Set resume token
	cs.mu.Lock()
	cs.resumeToken = event.ID.Data
	cs.mu.Unlock()

	// Convert operation type
	switch entry.Operation {
	case repl.OpInsert:
		event.OperationType = "insert"
		if entry.Object != nil {
			event.FullDocument = documentToMap(entry.Object)
			if idVal, ok := entry.Object.Get("_id"); ok {
				event.DocumentKey = map[string]interface{}{"_id": valueToInterface(idVal)}
			}
		}

	case repl.OpUpdate:
		event.OperationType = "update"
		if entry.Object2 != nil {
			if idVal, ok := entry.Object2.Get("_id"); ok {
				event.DocumentKey = map[string]interface{}{"_id": valueToInterface(idVal)}
			}
		}

		// Parse update description
		if entry.Object != nil {
			event.UpdateDescription = cs.parseUpdateDescription(entry.Object)

			// Lookup full document if requested
			if cs.opts.FullDocument == UpdateLookup || cs.opts.FullDocument == WhenAvailable || cs.opts.FullDocument == Required {
				if event.DocumentKey != nil && cs.coll != nil {
					fullDoc, _ := cs.coll.FindOne(event.DocumentKey)
					if fullDoc != nil {
						event.FullDocument = fullDoc
					} else if cs.opts.FullDocument == Required {
						// Should handle this error case
					}
				}
			}
		}

	case repl.OpDelete:
		event.OperationType = "delete"
		if entry.Object != nil {
			if idVal, ok := entry.Object.Get("_id"); ok {
				event.DocumentKey = map[string]interface{}{"_id": valueToInterface(idVal)}
			}
		}

	case repl.OpNoop:
		// Skip noop entries (heartbeats)
		return nil

	default:
		return nil
	}

	// Add transaction info if present
	if entry.TxnNumber > 0 {
		event.TxnNumber = &entry.TxnNumber
	}
	if entry.SessionID != "" {
		event.LSID = map[string]interface{}{"id": entry.SessionID}
	}

	return event
}

// parseUpdateDescription extracts update description from oplog object.
func (cs *ChangeStream) parseUpdateDescription(doc *bson.Document) *UpdateDescription {
	desc := &UpdateDescription{
		UpdatedFields: make(map[string]interface{}),
		RemovedFields: []string{},
	}

	for _, elem := range doc.Elements() {
		switch elem.Key {
		case "$set":
			if setDoc := elem.Value.DocumentValue(); setDoc != nil {
				for _, se := range setDoc.Elements() {
					desc.UpdatedFields[se.Key] = valueToInterface(se.Value)
				}
			}
		case "$unset":
			if unsetDoc := elem.Value.DocumentValue(); unsetDoc != nil {
				for _, ue := range unsetDoc.Elements() {
					desc.RemovedFields = append(desc.RemovedFields, ue.Key)
				}
			}
		default:
			// Direct field replacement
			desc.UpdatedFields[elem.Key] = valueToInterface(elem.Value)
		}
	}

	return desc
}

// matchesPipeline checks if an event matches the pipeline filters.
func (cs *ChangeStream) matchesPipeline(event *ChangeEvent) bool {
	for _, stage := range cs.pipeline {
		switch stage.Stage {
		case "$match":
			if !cs.matchFilter(event, stage.Value) {
				return false
			}
		case "$project":
			// Projection is applied after matching, skip here
		}
	}
	return true
}

// matchFilter checks if an event matches a filter.
func (cs *ChangeStream) matchFilter(event *ChangeEvent, filter interface{}) bool {
	matchFilter, ok := filter.(map[string]interface{})
	if !ok {
		return true
	}

	for key, value := range matchFilter {
		switch key {
		case "operationType":
			if event.OperationType != value.(string) {
				return false
			}
		case "fullDocument":
			if event.FullDocument == nil {
				return false
			}
			if docFilter, ok := value.(map[string]interface{}); ok {
				if !cs.matchDocument(event.FullDocument, docFilter) {
					return false
				}
			}
		case "ns":
			if nsFilter, ok := value.(map[string]interface{}); ok {
				if coll, ok := nsFilter["coll"]; ok {
					if event.NS.Coll != coll.(string) {
						return false
					}
				}
			}
		}
	}

	return true
}

// matchDocument checks if a document matches a filter.
func (cs *ChangeStream) matchDocument(doc map[string]interface{}, filter map[string]interface{}) bool {
	for key, value := range filter {
		if docValue, ok := doc[key]; ok {
			if docValue != value {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// Next returns the next change event, blocking until available or context cancelled.
func (cs *ChangeStream) Next() bool {
	cs.mu.Lock()
	if cs.closed {
		cs.mu.Unlock()
		return false
	}

	// Check if we have buffered events
	if cs.position+1 < len(cs.buffer) {
		cs.position++
		cs.mu.Unlock()
		return true
	}
	cs.mu.Unlock()

	// Wait for next event
	maxWait := cs.opts.MaxAwaitTime
	if maxWait == 0 {
		maxWait = 1 * time.Second
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(maxWait)

	for {
		select {
		case <-cs.ctx.Done():
			return false
		case <-timeout:
			return false
		case <-ticker.C:
			cs.mu.Lock()
			if cs.closed {
				cs.mu.Unlock()
				return false
			}
			if cs.position+1 < len(cs.buffer) {
				cs.position++
				cs.mu.Unlock()
				return true
			}
			cs.mu.Unlock()
		}
	}
}

// TryNext returns the next change event if available, without blocking.
func (cs *ChangeStream) TryNext() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.closed {
		return false
	}

	if cs.position+1 < len(cs.buffer) {
		cs.position++
		return true
	}

	return false
}

// Decode decodes the current change event into result.
func (cs *ChangeStream) Decode(result interface{}) error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.position < 0 || cs.position >= len(cs.buffer) {
		return errors.New("no current change event")
	}

	event := cs.buffer[cs.position]

	// If result is a pointer to ChangeEvent, assign directly
	if ce, ok := result.(*ChangeEvent); ok {
		*ce = *event
		return nil
	}

	// Otherwise marshal/unmarshal through JSON
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal change event: %w", err)
	}

	return json.Unmarshal(data, result)
}

// Err returns any error that occurred during iteration.
func (cs *ChangeStream) Err() error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.err
}

// Close closes the change stream.
func (cs *ChangeStream) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.closed {
		return nil
	}

	cs.closed = true
	cs.cancel()

	if cs.tailClient != nil {
		// Stop tailing would be called here if we had access to the replicator
	}

	return nil
}

// ResumeToken returns the current resume token for the change stream.
func (cs *ChangeStream) ResumeToken() ResumeToken {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return ResumeToken{Data: cs.resumeToken}
}

// ID returns the resume token (alias for ResumeToken, for MongoDB compatibility).
func (cs *ChangeStream) ID() ResumeToken {
	return cs.ResumeToken()
}

// SetReplicator sets the oplog replicator for testing purposes.
func (cs *ChangeStream) SetReplicator(replicator *repl.OplogReplicator) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.started {
		return errors.New("cannot set replicator after stream has started")
	}

	// Restart with replicator
	return cs.startWithReplicator(replicator)
}

// startWithReplicator starts the stream with a specific replicator.
func (cs *ChangeStream) startWithReplicator(replicator *repl.OplogReplicator) error {
	if replicator == nil {
		return nil
	}

	// Build filter based on namespace
	filter := make(map[string]interface{})
	if cs.coll != nil {
		filter["ns"] = cs.ns
	} else if cs.db != nil {
		filter["ns_prefix"] = cs.db.db
	}

	// Apply pipeline filters
	for _, stage := range cs.pipeline {
		if stage.Stage == "$match" {
			if matchFilter, ok := stage.Value.(map[string]interface{}); ok {
				for k, v := range matchFilter {
					filter[k] = v
				}
			}
		}
	}

	// Determine start time
	var since time.Time
	if cs.opts.StartAtOperationTime != nil {
		since = *cs.opts.StartAtOperationTime
	} else if cs.resumeToken != "" {
		since = decodeResumeToken(cs.resumeToken)
	}

	// Start tailing
	tailClient, err := replicator.Tail(cs.ctx, since, filter)
	if err != nil {
		return fmt.Errorf("failed to start tailing: %w", err)
	}

	cs.tailClient = tailClient
	cs.started = true

	// Start processing loop
	go cs.processLoop()

	return nil
}

// Helper functions

func parseNamespace(ns string) Namespace {
	parts := splitNamespace(ns)
	if len(parts) >= 2 {
		return Namespace{DB: parts[0], Coll: parts[1]}
	}
	return Namespace{DB: ns}
}

func splitNamespace(ns string) []string {
	// Split "db.collection" into ["db", "collection"]
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return []string{ns[:i], ns[i+1:]}
		}
	}
	return []string{ns}
}

func encodeResumeToken(ts time.Time, hash int64) string {
	data := fmt.Sprintf("%d:%d", ts.UnixNano(), hash)
	return base64.StdEncoding.EncodeToString([]byte(data))
}

func decodeResumeToken(token string) time.Time {
	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}
	}

	var tsUnix int64
	var hash int64
	fmt.Sscanf(string(data), "%d:%d", &tsUnix, &hash)

	return time.Unix(0, tsUnix)
}

// Match helper for pipeline stages

// Match creates a $match pipeline stage.
func Match(filter map[string]interface{}) PipelineStage {
	return PipelineStage{Stage: "$match", Value: filter}
}

// Project creates a $project pipeline stage.
func Project(projection map[string]interface{}) PipelineStage {
	return PipelineStage{Stage: "$project", Value: projection}
}

// OperationType creates a filter for matching specific operation types.
func OperationType(types ...string) map[string]interface{} {
	if len(types) == 1 {
		return map[string]interface{}{"operationType": types[0]}
	}
	return map[string]interface{}{"operationType": map[string]interface{}{"$in": types}}
}

// FullDocument lookup option helpers

// FullDocumentDefault returns the default full document option.
func FullDocumentDefault() FullDocumentOption {
	return Default
}

// FullDocumentOff returns the off option.
func FullDocumentOff() FullDocumentOption {
	return Off
}

// FullDocumentUpdateLookup returns the updateLookup option.
func FullDocumentUpdateLookup() FullDocumentOption {
	return UpdateLookup
}

// FullDocumentWhenAvailable returns the whenAvailable option.
func FullDocumentWhenAvailable() FullDocumentOption {
	return WhenAvailable
}

// FullDocumentRequired returns the required option.
func FullDocumentRequired() FullDocumentOption {
	return Required
}
