package wire

import (
	"github.com/mammothengine/mammoth/pkg/bson"
)

// handleStartTransaction begins a multi-document transaction.
func (h *Handler) handleStartTransaction(body *bson.Document, connID uint64) *bson.Document {
	db := extractDB(body)

	if h.sessionMgr.IsInTransaction(connID) {
		return errResponseWithCode("startTransaction", "transaction already in progress", CodeInternalError)
	}

	// Start the transaction
	if !h.sessionMgr.StartTransaction(connID, h.engine, db) {
		return errResponseWithCode("startTransaction", "failed to start transaction", CodeInternalError)
	}

	doc := okDoc()
	doc.Set("autocommit", bson.VBool(false))
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

// handleCommitTransaction commits the current transaction.
func (h *Handler) handleCommitTransaction(connID uint64) *bson.Document {
	if !h.sessionMgr.IsInTransaction(connID) {
		// No transaction to commit - this is ok per MongoDB spec
		doc := okDoc()
		doc.Set("ok", bson.VDouble(1.0))
		return doc
	}

	if err := h.sessionMgr.CommitTransaction(connID); err != nil {
		return errResponseWithCode("commitTransaction", err.Error(), CodeInternalError)
	}

	doc := okDoc()
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

// handleAbortTransaction rolls back the current transaction.
func (h *Handler) handleAbortTransaction(connID uint64) *bson.Document {
	if !h.sessionMgr.IsInTransaction(connID) {
		// No transaction to abort - this is ok per MongoDB spec
		doc := okDoc()
		doc.Set("ok", bson.VDouble(1.0))
		return doc
	}

	h.sessionMgr.AbortTransaction(connID)

	doc := okDoc()
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}
