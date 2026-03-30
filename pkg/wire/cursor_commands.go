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
		return h.errResponse("getMore", "cursor not found")
	}

	batchSize := getInt32FromBody(body, "batchSize")
	if batchSize <= 0 {
		batchSize = 101
	}

	batch := cursor.GetBatch(int(batchSize))

	cursorDoc := bson.NewDocument()
	cursorDoc.Set("nextBatch", bson.VArray(docsToValues(batch)))
	cursorDoc.Set("id", bson.VInt64(int64(cursor.ID())))
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
