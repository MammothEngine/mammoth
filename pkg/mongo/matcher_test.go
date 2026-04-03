package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func matchDoc(t *testing.T, filter, doc *bson.Document) bool {
	t.Helper()
	return NewMatcher(filter).Match(doc)
}

func TestMatch_EmptyFilter(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))
	if !matchDoc(t, nil, doc) {
		t.Error("nil filter should match everything")
	}
	if !matchDoc(t, bson.NewDocument(), doc) {
		t.Error("empty filter should match everything")
	}
}

func TestMatch_ImplicitEq(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))

	filter := bson.NewDocument()
	filter.Set("name", bson.VString("alice"))
	if !matchDoc(t, filter, doc) {
		t.Error("should match name=alice")
	}

	filter2 := bson.NewDocument()
	filter2.Set("name", bson.VString("bob"))
	if matchDoc(t, filter2, doc) {
		t.Error("should not match name=bob")
	}
}

func TestMatch_Eq(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(10))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$eq", bson.VInt32(10))
	filter.Set("x", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$eq 10 should match")
	}
}

func TestMatch_Ne(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(10))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$ne", bson.VInt32(5))
	filter.Set("x", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$ne 5 should match x=10")
	}

	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$ne", bson.VInt32(10))
	filter2.Set("x", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$ne 10 should not match x=10")
	}
}

func TestMatch_Gt(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(10))

	for _, tc := range []struct {
		op    string
		val   int32
		match bool
	}{
		{"$gt", 5, true},
		{"$gt", 10, false},
		{"$gt", 15, false},
		{"$gte", 10, true},
		{"$gte", 5, true},
		{"$gte", 15, false},
		{"$lt", 15, true},
		{"$lt", 10, false},
		{"$lte", 10, true},
		{"$lte", 5, false},
	} {
		filter := bson.NewDocument()
		op := bson.NewDocument()
		op.Set(tc.op, bson.VInt32(tc.val))
		filter.Set("x", bson.VDoc(op))
		got := matchDoc(t, filter, doc)
		if got != tc.match {
			t.Errorf("%s %d on x=10: got %v, want %v", tc.op, tc.val, got, tc.match)
		}
	}
}

func TestMatch_In(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(3))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$in", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2), bson.VInt32(3))))
	filter.Set("x", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$in should match")
	}

	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$in", bson.VArray(bson.A(bson.VInt32(4), bson.VInt32(5))))
	filter2.Set("x", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$in should not match")
	}
}

func TestMatch_InArray(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("tags", bson.VArray(bson.A(bson.VString("a"), bson.VString("b"))))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$in", bson.VArray(bson.A(bson.VString("b"))))
	filter.Set("tags", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$in on array should match when element found")
	}
}

func TestMatch_Nin(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(3))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$nin", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2))))
	filter.Set("x", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$nin should match when value not in list")
	}

	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$nin", bson.VArray(bson.A(bson.VInt32(3))))
	filter2.Set("x", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$nin should not match when value in list")
	}
}

func TestMatch_Exists(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$exists", bson.VBool(true))
	filter.Set("a", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$exists true should match existing field")
	}

	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$exists", bson.VBool(false))
	filter2.Set("b", bson.VDoc(op2))
	if !matchDoc(t, filter2, doc) {
		t.Error("$exists false should match missing field")
	}
}

func TestMatch_Type(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(42))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$type", bson.VString("int"))
	filter.Set("x", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$type int should match Int32")
	}

	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$type", bson.VString("string"))
	filter2.Set("x", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$type string should not match Int32")
	}
}

func TestMatch_Regex(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$regex", bson.VString("^al.*"))
	filter.Set("name", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$regex should match")
	}

	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$regex", bson.VString("^bo"))
	filter2.Set("name", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$regex should not match")
	}
}

func TestMatch_Size(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("arr", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2), bson.VInt32(3))))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$size", bson.VInt32(3))
	filter.Set("arr", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$size 3 should match array of length 3")
	}

	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$size", bson.VInt32(2))
	filter2.Set("arr", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$size 2 should not match array of length 3")
	}
}

func TestMatch_All(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("tags", bson.VArray(bson.A(bson.VString("a"), bson.VString("b"), bson.VString("c"))))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$all", bson.VArray(bson.A(bson.VString("a"), bson.VString("c"))))
	filter.Set("tags", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$all should match when all elements present")
	}

	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$all", bson.VArray(bson.A(bson.VString("a"), bson.VString("d"))))
	filter2.Set("tags", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$all should not match when element missing")
	}
}

func TestMatch_ElemMatch(t *testing.T) {
	doc := bson.NewDocument()
	sub1 := bson.NewDocument()
	sub1.Set("x", bson.VInt32(1))
	sub1.Set("y", bson.VInt32(10))
	sub2 := bson.NewDocument()
	sub2.Set("x", bson.VInt32(2))
	sub2.Set("y", bson.VInt32(20))
	doc.Set("items", bson.VArray(bson.A(bson.VDoc(sub1), bson.VDoc(sub2))))

	cond := bson.NewDocument()
	cond.Set("x", bson.VInt32(2))
	cond.Set("y", bson.VInt32(20))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$elemMatch", bson.VDoc(cond))
	filter.Set("items", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$elemMatch should match")
	}
}

func TestMatch_NotGt(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(5))

	// $not: {$gt: 10} should match x=5 (5 is NOT > 10)
	filter := bson.NewDocument()
	op := bson.NewDocument()
	notOp := bson.NewDocument()
	notOp.Set("$gt", bson.VInt32(10))
	op.Set("$not", bson.VDoc(notOp))
	filter.Set("x", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$not $gt 10 should match x=5")
	}

	// $not: {$gt: 3} should NOT match x=5 (5 IS > 3)
	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	notOp2 := bson.NewDocument()
	notOp2.Set("$gt", bson.VInt32(3))
	op2.Set("$not", bson.VDoc(notOp2))
	filter2.Set("x", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$not $gt 3 should not match x=5")
	}
}

func TestMatch_NotRegex(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))

	// $not: {$regex: "^bo"} should match "alice"
	filter := bson.NewDocument()
	op := bson.NewDocument()
	notOp := bson.NewDocument()
	notOp.Set("$regex", bson.VString("^bo"))
	op.Set("$not", bson.VDoc(notOp))
	filter.Set("name", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$not $regex ^bo should match 'alice'")
	}

	// $not: {$regex: "^al"} should NOT match "alice"
	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	notOp2 := bson.NewDocument()
	notOp2.Set("$regex", bson.VString("^al"))
	op2.Set("$not", bson.VDoc(notOp2))
	filter2.Set("name", bson.VDoc(op2))
	if matchDoc(t, filter2, doc) {
		t.Error("$not $regex ^al should not match 'alice'")
	}
}

func TestMatch_And(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("age", bson.VInt32(25))
	doc.Set("name", bson.VString("alice"))

	cond1 := bson.NewDocument()
	cond1.Set("age", bson.VInt32(25))
	cond2 := bson.NewDocument()
	cond2.Set("name", bson.VString("alice"))

	filter := bson.NewDocument()
	filter.Set("$and", bson.VArray(bson.A(bson.VDoc(cond1), bson.VDoc(cond2))))
	if !matchDoc(t, filter, doc) {
		t.Error("$and should match when both conditions match")
	}

	cond3 := bson.NewDocument()
	cond3.Set("name", bson.VString("bob"))
	filter2 := bson.NewDocument()
	filter2.Set("$and", bson.VArray(bson.A(bson.VDoc(cond1), bson.VDoc(cond3))))
	if matchDoc(t, filter2, doc) {
		t.Error("$and should not match when one condition fails")
	}
}

func TestMatch_Or(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(5))

	cond1 := bson.NewDocument()
	cond1.Set("x", bson.VInt32(5))
	cond2 := bson.NewDocument()
	cond2.Set("x", bson.VInt32(10))

	filter := bson.NewDocument()
	filter.Set("$or", bson.VArray(bson.A(bson.VDoc(cond1), bson.VDoc(cond2))))
	if !matchDoc(t, filter, doc) {
		t.Error("$or should match when one condition matches")
	}

	cond3 := bson.NewDocument()
	cond3.Set("x", bson.VInt32(20))
	filter2 := bson.NewDocument()
	filter2.Set("$or", bson.VArray(bson.A(bson.VDoc(cond2), bson.VDoc(cond3))))
	if matchDoc(t, filter2, doc) {
		t.Error("$or should not match when no conditions match")
	}
}

func TestMatch_Nor(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(5))

	cond1 := bson.NewDocument()
	cond1.Set("x", bson.VInt32(10))
	cond2 := bson.NewDocument()
	cond2.Set("x", bson.VInt32(20))

	filter := bson.NewDocument()
	filter.Set("$nor", bson.VArray(bson.A(bson.VDoc(cond1), bson.VDoc(cond2))))
	if !matchDoc(t, filter, doc) {
		t.Error("$nor should match when no conditions match")
	}
}

func TestMatch_DotNotation(t *testing.T) {
	inner := bson.NewDocument()
	inner.Set("city", bson.VString("Istanbul"))
	doc := bson.NewDocument()
	doc.Set("address", bson.VDoc(inner))

	filter := bson.NewDocument()
	filter.Set("address.city", bson.VString("Istanbul"))
	if !matchDoc(t, filter, doc) {
		t.Error("dot notation should match nested field")
	}

	filter2 := bson.NewDocument()
	filter2.Set("address.city", bson.VString("Ankara"))
	if matchDoc(t, filter2, doc) {
		t.Error("dot notation should not match wrong value")
	}
}

func TestMatch_NullMatchesMissing(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(1))

	filter := bson.NewDocument()
	filter.Set("missing_field", bson.VNull())
	if !matchDoc(t, filter, doc) {
		t.Error("null filter should match missing field")
	}
}

func TestMatch_MissingFieldGt(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(1))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$gt", bson.VInt32(0))
	filter.Set("missing_field", bson.VDoc(op))
	if matchDoc(t, filter, doc) {
		t.Error("$gt on missing field should not match")
	}
}

func TestMatch_Mod(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(14))

	// 14 % 7 == 0
	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$mod", bson.VArray(bson.A(bson.VInt32(7), bson.VInt32(0))))
	filter.Set("x", bson.VDoc(op))
	if !matchDoc(t, filter, doc) {
		t.Error("$mod [7, 0] should match x=14")
	}

	// 14 % 5 == 4
	filter2 := bson.NewDocument()
	op2 := bson.NewDocument()
	op2.Set("$mod", bson.VArray(bson.A(bson.VInt32(5), bson.VInt32(4))))
	filter2.Set("x", bson.VDoc(op2))
	if !matchDoc(t, filter2, doc) {
		t.Error("$mod [5, 4] should match x=14 (14 % 5 = 4)")
	}

	// 14 % 5 == 3 should not match
	filter3 := bson.NewDocument()
	op3 := bson.NewDocument()
	op3.Set("$mod", bson.VArray(bson.A(bson.VInt32(5), bson.VInt32(3))))
	filter3.Set("x", bson.VDoc(op3))
	if matchDoc(t, filter3, doc) {
		t.Error("$mod [5, 3] should not match x=14")
	}
}

func TestMatch_ModMissingField(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(1))

	filter := bson.NewDocument()
	op := bson.NewDocument()
	op.Set("$mod", bson.VArray(bson.A(bson.VInt32(2), bson.VInt32(0))))
	filter.Set("missing_field", bson.VDoc(op))
	if matchDoc(t, filter, doc) {
		t.Error("$mod on missing field should not match")
	}
}
