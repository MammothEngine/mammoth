package wire

import (
	"github.com/mammothengine/mammoth/pkg/bson"
)

func (h *Handler) handleGetMore(body *bson.Document) *bson.Document {
	// getMore: cursorID value, collection name
	ns := getStringFromBody(body, "collection")
	cursorID, _ := body.Get("getMore")

	var id uint64
	if cursorID.Type == bson.TypeInt64 {
		id = uint64(cursorID.Int64())
	} else if cursorID.Type == bson.TypeInt32 {
		id = uint64(cursorID.Int32())
	}

	cursor, ok := h.cursor.Get(id)
	if !ok {
		// Cursor not found - return empty result with cursor id 0
		cursorDoc := bson.NewDocument()
		cursorDoc.Set("nextBatch", bson.VArray(bson.Array{}))
		cursorDoc.Set("id", bson.VInt64(0))
		cursorDoc.Set("ns", bson.VString(ns))
		doc := okDoc()
		doc.Set("cursor", bson.VDoc(cursorDoc))
		return doc
	}

	batchSize := getInt32FromBody(body, "batchSize")
	if batchSize <= 0 {
		batchSize = 101
	}

	batch := cursor.GetBatch(int(batchSize))

	// Check if cursor is exhausted - return id 0 to signal end
	var nextCursorID int64
	if cursor.Exhausted() {
		nextCursorID = 0
		h.cursor.Kill([]uint64{id}) // Clean up exhausted cursor
	} else {
		nextCursorID = int64(cursor.ID())
	}

	cursorDoc := bson.NewDocument()
	cursorDoc.Set("nextBatch", bson.VArray(docsToValues(batch)))
	cursorDoc.Set("id", bson.VInt64(nextCursorID))
	cursorDoc.Set("ns", bson.VString(ns))

	doc := okDoc()
	doc.Set("cursor", bson.VDoc(cursorDoc))
	return doc
}

func (h *Handler) handleKillCursors(body *bson.Document) *bson.Document {
	ids := getArrayFromBody(body, "cursors")
	var killed []uint64
	for _, v := range ids {
		if v.Type == bson.TypeInt64 {
			killed = append(killed, uint64(v.Int64()))
		} else if v.Type == bson.TypeInt32 {
			killed = append(killed, uint64(v.Int32()))
		}
	}
	h.cursor.Kill(killed)

	doc := okDoc()
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}
