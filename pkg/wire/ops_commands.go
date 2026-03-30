package wire

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// opTracker tracks currently running operations.
type opTracker struct {
	mu     sync.RWMutex
	ops    map[uint64]*activeOp
	nextID atomic.Uint64
}

type activeOp struct {
	ID        uint64
	Command   string
	Namespace string
	Started   time.Time
}

func newOpTracker() *opTracker {
	return &opTracker{
		ops: make(map[uint64]*activeOp),
	}
}

func (t *opTracker) begin(cmd, ns string) uint64 {
	id := t.nextID.Add(1)
	t.mu.Lock()
	t.ops[id] = &activeOp{
		ID:        id,
		Command:   cmd,
		Namespace: ns,
		Started:   time.Now(),
	}
	t.mu.Unlock()
	return id
}

func (t *opTracker) end(id uint64) {
	t.mu.Lock()
	delete(t.ops, id)
	t.mu.Unlock()
}

func (t *opTracker) kill(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.ops[id]; ok {
		delete(t.ops, id)
		return true
	}
	return false
}

func (t *opTracker) snapshot() []activeOp {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]activeOp, 0, len(t.ops))
	for _, op := range t.ops {
		result = append(result, *op)
	}
	return result
}

func (h *Handler) handleCurrentOp(body *bson.Document) *bson.Document {
	inprog := bson.NewDocument()
	var inprogArr bson.Array

	if h.opTracker != nil {
		ops := h.opTracker.snapshot()
		for _, op := range ops {
			entry := bson.NewDocument()
			entry.Set("opid", bson.VInt64(int64(op.ID)))
			entry.Set("command", bson.VString(op.Command))
			entry.Set("ns", bson.VString(op.Namespace))
			entry.Set("secs_running", bson.VDouble(time.Since(op.Started).Seconds()))
			inprogArr = append(inprogArr, bson.VDoc(entry))
		}
	}

	inprog.Set("inprog", bson.VArray(inprogArr))
	doc := okDoc()
	doc.Set("inprog", bson.VArray(inprogArr))
	return doc
}

func (h *Handler) handleKillOp(body *bson.Document) *bson.Document {
	var opID uint64
	if v, ok := body.Get("op"); ok {
		switch v.Type {
		case bson.TypeInt32:
			opID = uint64(v.Int32())
		case bson.TypeInt64:
			opID = uint64(v.Int64())
		default:
			return errResponseWithCode("killOp", "op must be a number", CodeBadValue)
		}
	}
	if opID == 0 {
		return errResponseWithCode("killOp", "op id required", CodeBadValue)
	}

	killed := false
	if h.opTracker != nil {
		killed = h.opTracker.kill(opID)
	}

	info := fmt.Sprintf("opId:%d", opID)
	if killed {
		info += " killed"
	} else {
		info += " not found"
	}

	doc := okDoc()
	doc.Set("info", bson.VString(info))
	return doc
}
