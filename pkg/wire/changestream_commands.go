package wire

import (
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// handleChangeStream handles the $changeStream aggregation stage.
// It reads oplog entries and returns them as change event documents.
func (h *Handler) handleChangeStream(db, collName string, stageVal bson.Value) []*bson.Document {
	var resumeTS int64

	// Parse options from stage value
	if stageVal.Type == bson.TypeDocument {
		opts := stageVal.DocumentValue()
		if rt, ok := opts.Get("resumeAfter"); ok {
			if rt.Type == bson.TypeDocument {
				if ts, ok2 := rt.DocumentValue().Get("_data"); ok2 {
					switch ts.Type {
					case bson.TypeInt64:
						resumeTS = ts.Int64()
					case bson.TypeInt32:
						resumeTS = int64(ts.Int32())
					}
				}
			}
		}
	}

	// Create watcher and poll
	watcher := h.changeStreamMgr.Watch(db+"."+collName, resumeTS)
	defer h.changeStreamMgr.Remove(watcher.ID)

	entries := h.changeStreamMgr.Poll(watcher)

	var docs []*bson.Document
	for _, entry := range entries {
		event := changeEventFromEntry(entry)
		docs = append(docs, event)
	}

	return docs
}

// changeEventFromEntry converts an OplogEntry to a change event document.
func changeEventFromEntry(entry mongo.OplogEntry) *bson.Document {
	event := bson.NewDocument()

	// Resume token
	token := bson.NewDocument()
	token.Set("_data", bson.VInt64(entry.Timestamp))
	event.Set("_id", bson.VDoc(token))

	event.Set("operationType", bson.VString(opToOperationType(entry.Operation)))

	// Namespace
	ns := bson.NewDocument()
	nsSplit := splitNamespace(entry.Namespace)
	ns.Set("db", bson.VString(nsSplit[0]))
	if len(nsSplit) > 1 {
		ns.Set("coll", bson.VString(nsSplit[1]))
	}
	event.Set("ns", bson.VDoc(ns))

	// Document payload
	if entry.Document != nil {
		doc, err := bson.Decode(entry.Document)
		if err == nil {
			switch entry.Operation {
			case "i":
				event.Set("fullDocument", bson.VDoc(doc))
			case "u":
				event.Set("fullDocument", bson.VDoc(doc))
			case "d":
				event.Set("documentKey", bson.VDoc(doc))
			}
		}
	}

	// Wall time
	event.Set("wallTime", bson.VDateTime(entry.WallTime))
	event.Set("clusterTime", bson.VDateTime(entry.Timestamp*1000))

	return event
}

func opToOperationType(op string) string {
	switch op {
	case "i":
		return "insert"
	case "u":
		return "update"
	case "d":
		return "delete"
	default:
		return op
	}
}

func splitNamespace(ns string) []string {
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return []string{ns[:i], ns[i+1:]}
		}
	}
	return []string{ns}
}

// oplogWriteInsert writes an insert oplog entry from a BSON document.
func (h *Handler) oplogWriteInsert(db, coll string, doc *bson.Document) {
	if h.oplog != nil {
		docData := bson.Encode(doc)
		h.oplog.WriteInsert(db+"."+coll, docData)
		// Notify change stream watchers
		now := time.Now()
		entry := mongo.OplogEntry{
			Timestamp: now.Unix(),
			Operation: "i",
			Namespace: db + "." + coll,
			Document:  docData,
			WallTime:  now.UnixMilli(),
		}
		h.changeStreamMgr.Notify(entry)
	}
}

func (h *Handler) oplogWriteUpdate(db, coll string, newDoc *bson.Document) {
	if h.oplog != nil {
		docData := bson.Encode(newDoc)
		h.oplog.WriteUpdate(db+"."+coll, docData, nil)
		now := time.Now()
		entry := mongo.OplogEntry{
			Timestamp: now.Unix(),
			Operation: "u",
			Namespace: db + "." + coll,
			Document:  docData,
			WallTime:  now.UnixMilli(),
		}
		h.changeStreamMgr.Notify(entry)
	}
}

func (h *Handler) oplogWriteDelete(db, coll string, doc *bson.Document) {
	if h.oplog != nil {
		docData := bson.Encode(doc)
		h.oplog.WriteDelete(db+"."+coll, docData)
		now := time.Now()
		entry := mongo.OplogEntry{
			Timestamp: now.Unix(),
			Operation: "d",
			Namespace: db + "." + coll,
			Document:  docData,
			WallTime:  now.UnixMilli(),
		}
		h.changeStreamMgr.Notify(entry)
	}
}
