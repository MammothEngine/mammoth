package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func applyUpdate(t *testing.T, doc, update *bson.Document) *bson.Document {
	t.Helper()
	return ApplyUpdate(doc, update, false)
}

func TestUpdate_Set(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))

	update := bson.NewDocument()
	set := bson.NewDocument()
	set.Set("a", bson.VInt32(2))
	set.Set("b", bson.VString("hello"))
	update.Set("$set", bson.VDoc(set))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("a"); v.Int32() != 2 {
		t.Errorf("$set a = %d, want 2", v.Int32())
	}
	if v, _ := result.Get("b"); v.String() != "hello" {
		t.Errorf("$set b = %q, want hello", v.String())
	}
}

func TestUpdate_Unset(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))
	doc.Set("b", bson.VInt32(2))

	update := bson.NewDocument()
	unset := bson.NewDocument()
	unset.Set("a", bson.VString(""))
	update.Set("$unset", bson.VDoc(unset))

	result := applyUpdate(t, doc, update)
	if _, ok := result.Get("a"); ok {
		t.Error("$unset should remove field")
	}
	if _, ok := result.Get("b"); !ok {
		t.Error("$unset should not remove other fields")
	}
}

func TestUpdate_Inc(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(10))

	update := bson.NewDocument()
	inc := bson.NewDocument()
	inc.Set("x", bson.VInt32(5))
	update.Set("$inc", bson.VDoc(inc))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("x"); v.Int32() != 15 {
		t.Errorf("$inc x = %d, want 15", v.Int32())
	}
}

func TestUpdate_IncNewField(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))

	update := bson.NewDocument()
	inc := bson.NewDocument()
	inc.Set("b", bson.VInt32(42))
	update.Set("$inc", bson.VDoc(inc))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("b"); v.Int32() != 42 {
		t.Errorf("$inc new field b = %d, want 42", v.Int32())
	}
}

func TestUpdate_Mul(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(5))

	update := bson.NewDocument()
	mul := bson.NewDocument()
	mul.Set("x", bson.VInt32(3))
	update.Set("$mul", bson.VDoc(mul))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("x"); v.Int32() != 15 {
		t.Errorf("$mul x = %d, want 15", v.Int32())
	}
}

func TestUpdate_Min(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(10))

	update := bson.NewDocument()
	min := bson.NewDocument()
	min.Set("x", bson.VInt32(5))
	update.Set("$min", bson.VDoc(min))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("x"); v.Int32() != 5 {
		t.Errorf("$min x = %d, want 5", v.Int32())
	}

	// Higher value should not change
	update2 := bson.NewDocument()
	min2 := bson.NewDocument()
	min2.Set("x", bson.VInt32(20))
	update2.Set("$min", bson.VDoc(min2))

	result2 := applyUpdate(t, doc, update2)
	if v, _ := result2.Get("x"); v.Int32() != 10 {
		t.Errorf("$min x (higher) = %d, want 10", v.Int32())
	}
}

func TestUpdate_Max(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("x", bson.VInt32(10))

	update := bson.NewDocument()
	max := bson.NewDocument()
	max.Set("x", bson.VInt32(20))
	update.Set("$max", bson.VDoc(max))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("x"); v.Int32() != 20 {
		t.Errorf("$max x = %d, want 20", v.Int32())
	}
}

func TestUpdate_Rename(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))

	update := bson.NewDocument()
	rename := bson.NewDocument()
	rename.Set("a", bson.VString("b"))
	update.Set("$rename", bson.VDoc(rename))

	result := applyUpdate(t, doc, update)
	if _, ok := result.Get("a"); ok {
		t.Error("$rename should remove old field")
	}
	if v, ok := result.Get("b"); !ok || v.Int32() != 1 {
		t.Error("$rename should create new field with old value")
	}
}

func TestUpdate_CurrentDate(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("ts", bson.VInt32(0))

	update := bson.NewDocument()
	cd := bson.NewDocument()
	cd.Set("ts", bson.VBool(true))
	update.Set("$currentDate", bson.VDoc(cd))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("ts"); v.Type != bson.TypeDateTime {
		t.Errorf("$currentDate type = %v, want DateTime", v.Type)
	}
}

func TestUpdate_Push(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("arr", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2))))

	update := bson.NewDocument()
	push := bson.NewDocument()
	push.Set("arr", bson.VInt32(3))
	update.Set("$push", bson.VDoc(push))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("arr"); len(v.ArrayValue()) != 3 {
		t.Errorf("$push arr length = %d, want 3", len(v.ArrayValue()))
	}
}

func TestUpdate_PushEach(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("arr", bson.VArray(bson.A(bson.VInt32(1))))

	each := bson.NewDocument()
	each.Set("$each", bson.VArray(bson.A(bson.VInt32(2), bson.VInt32(3))))

	update := bson.NewDocument()
	push := bson.NewDocument()
	push.Set("arr", bson.VDoc(each))
	update.Set("$push", bson.VDoc(push))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("arr"); len(v.ArrayValue()) != 3 {
		t.Errorf("$push $each arr length = %d, want 3", len(v.ArrayValue()))
	}
}

func TestUpdate_PushSlice(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("arr", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2), bson.VInt32(3))))

	each := bson.NewDocument()
	each.Set("$each", bson.VArray(bson.A(bson.VInt32(4), bson.VInt32(5))))
	each.Set("$slice", bson.VInt32(3))

	update := bson.NewDocument()
	push := bson.NewDocument()
	push.Set("arr", bson.VDoc(each))
	update.Set("$push", bson.VDoc(push))

	result := applyUpdate(t, doc, update)
	arrVal, _ := result.Get("arr")
	arr := arrVal.ArrayValue()
	if len(arr) != 3 {
		t.Errorf("$push $slice arr length = %d, want 3", len(arr))
	}
	// [1,2,3] + $each [4,5] = [1,2,3,4,5], $slice:3 = [1,2,3]
	if arr[0].Int32() != 1 {
		t.Errorf("after $slice 3, first element = %d, want 1", arr[0].Int32())
	}
}

func TestUpdate_Pop(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("arr", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2), bson.VInt32(3))))

	update := bson.NewDocument()
	pop := bson.NewDocument()
	pop.Set("arr", bson.VInt32(1)) // remove last
	update.Set("$pop", bson.VDoc(pop))

	result := applyUpdate(t, doc, update)
	arrVal, _ := result.Get("arr")
	arr := arrVal.ArrayValue()
	if len(arr) != 2 || arr[1].Int32() != 2 {
		t.Errorf("$pop 1: got %v, want [1,2]", arr)
	}
}

func TestUpdate_PopFirst(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("arr", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2))))

	update := bson.NewDocument()
	pop := bson.NewDocument()
	pop.Set("arr", bson.VInt32(-1)) // remove first
	update.Set("$pop", bson.VDoc(pop))

	result := applyUpdate(t, doc, update)
	arrVal, _ := result.Get("arr")
	arr := arrVal.ArrayValue()
	if len(arr) != 1 || arr[0].Int32() != 2 {
		t.Errorf("$pop -1: got %v, want [2]", arr)
	}
}

func TestUpdate_AddToSet(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("arr", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2))))

	update := bson.NewDocument()
	add := bson.NewDocument()
	add.Set("arr", bson.VInt32(2)) // already exists
	update.Set("$addToSet", bson.VDoc(add))

	result := applyUpdate(t, doc, update)
	arrVal, _ := result.Get("arr")
	arr := arrVal.ArrayValue()
	if len(arr) != 2 {
		t.Errorf("$addToSet existing: length = %d, want 2", len(arr))
	}

	// Add new value
	update2 := bson.NewDocument()
	add2 := bson.NewDocument()
	add2.Set("arr", bson.VInt32(3))
	update2.Set("$addToSet", bson.VDoc(add2))

	result2 := applyUpdate(t, doc, update2)
	arrVal2, _ := result2.Get("arr")
	arr2 := arrVal2.ArrayValue()
	if len(arr2) != 3 {
		t.Errorf("$addToSet new: length = %d, want 3", len(arr2))
	}
}

func TestUpdate_Pull(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("arr", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2), bson.VInt32(3))))

	update := bson.NewDocument()
	pull := bson.NewDocument()
	pull.Set("arr", bson.VInt32(2))
	update.Set("$pull", bson.VDoc(pull))

	result := applyUpdate(t, doc, update)
	arrVal, _ := result.Get("arr")
	arr := arrVal.ArrayValue()
	if len(arr) != 2 {
		t.Errorf("$pull: length = %d, want 2", len(arr))
	}
}

func TestUpdate_MultipleOps(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))
	doc.Set("b", bson.VInt32(2))

	set := bson.NewDocument()
	set.Set("a", bson.VInt32(10))
	inc := bson.NewDocument()
	inc.Set("b", bson.VInt32(5))

	update := bson.NewDocument()
	update.Set("$set", bson.VDoc(set))
	update.Set("$inc", bson.VDoc(inc))

	result := applyUpdate(t, doc, update)
	if v, _ := result.Get("a"); v.Int32() != 10 {
		t.Errorf("$set a = %d, want 10", v.Int32())
	}
	if v, _ := result.Get("b"); v.Int32() != 7 {
		t.Errorf("$inc b = %d, want 7", v.Int32())
	}
}

func TestUpdate_DoesNotMutateOriginal(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))

	set := bson.NewDocument()
	set.Set("a", bson.VInt32(99))
	update := bson.NewDocument()
	update.Set("$set", bson.VDoc(set))

	result := ApplyUpdate(doc, update, false)

	// Original should not be modified
	if v, _ := doc.Get("a"); v.Int32() != 1 {
		t.Error("ApplyUpdate should not mutate the original document")
	}
	// Result should be modified
	if v, _ := result.Get("a"); v.Int32() != 99 {
		t.Errorf("result a = %d, want 99", v.Int32())
	}
}
