package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupTextIndexTest(t *testing.T) (*engine.Engine, *TextIndex) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	spec := &IndexSpec{
		Name:      "body_text",
		Key:       []IndexKey{{Field: "body"}},
		IndexType: "text",
	}
	return eng, NewTextIndex("testdb", "articles", spec, eng)
}

func makeTextDoc(text string) *bson.Document {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("body", bson.VString(text))
	return doc
}

func TestTextIndex_Tokenize(t *testing.T) {
	_, ti := setupTextIndexTest(t)

	tokens := ti.Tokenize("The quick brown foxes jumped over the lazy dogs")

	// "the", "over" are stop words; "foxes" → "fox", "jumped" → "jump", "dogs" → "dog"
	// "quick", "brown", "lazi" (lazy→lazi via stemmer)
	for _, stop := range []string{"the", "over"} {
		for _, tok := range tokens {
			if tok == stop {
				t.Errorf("stop word %q should have been removed", stop)
			}
		}
	}

	if len(tokens) == 0 {
		t.Error("Tokenize returned no tokens")
	}
}

func TestTextIndex_AddAndSearch(t *testing.T) {
	_, ti := setupTextIndexTest(t)

	ti.AddEntry(makeTextDoc("database systems and storage engines"))
	ti.AddEntry(makeTextDoc("web development with modern frameworks"))
	ti.AddEntry(makeTextDoc("storage optimization for large databases"))

	results := ti.Search("storage database", 10)
	if len(results) == 0 {
		t.Fatal("Search returned no results")
	}

	// Both doc 1 and doc 3 contain "storage" and "database"
	// They should rank higher than doc 2
	if len(results) < 2 {
		t.Errorf("Search count = %d, want at least 2", len(results))
	}
}

func TestTextIndex_SearchNoMatch(t *testing.T) {
	_, ti := setupTextIndexTest(t)

	ti.AddEntry(makeTextDoc("hello world"))

	results := ti.Search("quantum physics", 10)
	if len(results) != 0 {
		t.Errorf("Search for unrelated terms = %d results, want 0", len(results))
	}
}

func TestTextIndex_RemoveEntry(t *testing.T) {
	eng, ti := setupTextIndexTest(t)

	doc := makeTextDoc("removable document content")
	ti.AddEntry(doc)

	results := ti.Search("removable", 10)
	if len(results) != 1 {
		t.Fatalf("before remove: results = %d, want 1", len(results))
	}

	ti.RemoveEntry(doc)

	// Verify entries are removed from engine
	prefix := ti.textKeyPrefix()
	count := 0
	eng.Scan(prefix, func(_, _ []byte) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("after remove: index entries = %d, want 0", count)
	}
}

func TestTextIndex_SearchLimit(t *testing.T) {
	_, ti := setupTextIndexTest(t)

	for i := 0; i < 10; i++ {
		ti.AddEntry(makeTextDoc("database performance tuning"))
	}

	results := ti.Search("database", 3)
	if len(results) > 3 {
		t.Errorf("Search with limit=3 returned %d results", len(results))
	}
}

func TestTextIndex_EmptyQuery(t *testing.T) {
	_, ti := setupTextIndexTest(t)

	ti.AddEntry(makeTextDoc("some content"))

	results := ti.Search("", 10)
	if len(results) != 0 {
		t.Errorf("Search for empty string = %d results, want 0", len(results))
	}
}
