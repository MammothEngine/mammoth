package bson

import (
	"testing"
	"time"
)

func BenchmarkEncode_SmallDocument(b *testing.B) {
	doc := NewDocument()
	doc.Set("name", VString("Alice"))
	doc.Set("age", VInt32(30))
	doc.Set("active", VBool(true))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Encode(doc)
	}
}

func BenchmarkEncode_LargeDocument(b *testing.B) {
	doc := NewDocument()
	doc.Set("_id", VObjectID(NewObjectID()))
	doc.Set("name", VString("John Doe"))
	doc.Set("email", VString("john@example.com"))
	doc.Set("age", VInt32(35))
	doc.Set("score", VDouble(95.5))
	doc.Set("active", VBool(true))
	doc.Set("created", VDateTime(time.Now().UnixMilli()))
	doc.Set("tags", VArray(A(VString("tag1"), VString("tag2"), VString("tag3"))))

	inner := NewDocument()
	inner.Set("street", VString("123 Main St"))
	inner.Set("city", VString("Boston"))
	doc.Set("address", VDoc(inner))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Encode(doc)
	}
}

func BenchmarkDecode_SmallDocument(b *testing.B) {
	doc := NewDocument()
	doc.Set("name", VString("Alice"))
	doc.Set("age", VInt32(30))
	doc.Set("active", VBool(true))
	data := Encode(doc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Decode(data)
	}
}

func BenchmarkDecode_LargeDocument(b *testing.B) {
	doc := NewDocument()
	doc.Set("_id", VObjectID(NewObjectID()))
	doc.Set("name", VString("John Doe"))
	doc.Set("email", VString("john@example.com"))
	doc.Set("age", VInt32(35))
	doc.Set("score", VDouble(95.5))
	doc.Set("active", VBool(true))
	doc.Set("created", VDateTime(time.Now().UnixMilli()))
	doc.Set("tags", VArray(A(VString("tag1"), VString("tag2"), VString("tag3"))))

	inner := NewDocument()
	inner.Set("street", VString("123 Main St"))
	inner.Set("city", VString("Boston"))
	doc.Set("address", VDoc(inner))
	data := Encode(doc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Decode(data)
	}
}

func BenchmarkDocument_Set(b *testing.B) {
	doc := NewDocument()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc.Set("key", VInt32(int32(i)))
	}
}

func BenchmarkDocument_Get(b *testing.B) {
	doc := NewDocument()
	doc.Set("name", VString("Alice"))
	doc.Set("age", VInt32(30))
	doc.Set("active", VBool(true))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = doc.Get("age")
	}
}

func BenchmarkDocument_GetWithIndex(b *testing.B) {
	doc := NewDocument()
	for i := 0; i < 100; i++ {
		doc.Set(string(rune('a'+i%26)), VInt32(int32(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = doc.Get("m")
	}
}

func BenchmarkObjectID_New(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewObjectID()
	}
}

func BenchmarkObjectID_String(b *testing.B) {
	oid := NewObjectID()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = oid.String()
	}
}

func BenchmarkObjectID_Parse(b *testing.B) {
	oid := NewObjectID()
	s := oid.String()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseObjectID(s)
	}
}

func BenchmarkMarshal_SmallStruct(b *testing.B) {
	type Person struct {
		Name string `bson:"name"`
		Age  int32  `bson:"age"`
	}
	p := Person{Name: "Alice", Age: 30}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Marshal(p)
	}
}

func BenchmarkUnmarshal_SmallStruct(b *testing.B) {
	type Person struct {
		Name string `bson:"name"`
		Age  int32  `bson:"age"`
	}

	doc := NewDocument()
	doc.Set("name", VString("Alice"))
	doc.Set("age", VInt32(30))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p Person
		_ = Unmarshal(doc, &p)
	}
}

func BenchmarkRawDocument_Lookup(b *testing.B) {
	doc := NewDocument()
	doc.Set("name", VString("Alice"))
	doc.Set("age", VInt32(30))
	doc.Set("active", VBool(true))
	data := Encode(doc)
	raw := RawDocument(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = raw.Lookup("age")
	}
}

func BenchmarkCompareValues(b *testing.B) {
	v1 := VInt32(42)
	v2 := VInt32(42)
	v3 := VInt32(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CompareValues(v1, v2)
		_ = CompareValues(v1, v3)
	}
}

func BenchmarkArray_Append(b *testing.B) {
	arr := A()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arr = append(arr, VInt32(int32(i)))
	}
}
