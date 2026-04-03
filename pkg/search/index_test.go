package search

import (
	"context"
	"strings"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestNewIndex(t *testing.T) {
	idx := NewIndex("test", "content")
	if idx.name != "test" {
		t.Errorf("expected name 'test', got %s", idx.name)
	}
	if idx.field != "content" {
		t.Errorf("expected field 'content', got %s", idx.field)
	}
}

func TestIndex_AddDocument(t *testing.T) {
	idx := NewIndex("test", "")

	doc := &Document{
		ID:      "doc1",
		Content: "The quick brown fox jumps over the lazy dog",
	}

	if err := idx.AddDocument(doc); err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	// Check document was added
	if idx.docCount != 1 {
		t.Errorf("expected 1 document, got %d", idx.docCount)
	}

	// Check terms were indexed
	if len(idx.terms) == 0 {
		t.Error("expected terms to be indexed")
	}

	// Verify specific terms (stemmed forms)
	expectedTerms := []string{"quick", "brown", "fox", "jump", "over", "dog"}
	for _, term := range expectedTerms {
		if _, exists := idx.terms[term]; !exists {
			t.Errorf("expected term '%s' to be indexed", term)
		}
	}
}

func TestIndex_AddDocument_EmptyID(t *testing.T) {
	idx := NewIndex("test", "")

	doc := &Document{
		ID:      "",
		Content: "test content",
	}

	if err := idx.AddDocument(doc); err == nil {
		t.Error("expected error for empty document ID")
	}
}

func TestIndex_RemoveDocument(t *testing.T) {
	idx := NewIndex("test", "")

	doc := &Document{
		ID:      "doc1",
		Content: "test content here",
	}

	idx.AddDocument(doc)
	termCount := len(idx.terms)

	if err := idx.RemoveDocument("doc1"); err != nil {
		t.Fatalf("RemoveDocument failed: %v", err)
	}

	if idx.docCount != 0 {
		t.Errorf("expected 0 documents, got %d", idx.docCount)
	}

	// Some terms should be removed
	if len(idx.terms) >= termCount {
		t.Error("expected some terms to be removed")
	}
}

func TestIndex_Search(t *testing.T) {
	idx := NewIndex("test", "")

	// Add some documents
	docs := []*Document{
		{ID: "1", Content: "The quick brown fox"},
		{ID: "2", Content: "The lazy dog sleeps"},
		{ID: "3", Content: "The quick cat runs"},
	}

	for _, doc := range docs {
		idx.AddDocument(doc)
	}

	// Search for "quick"
	ctx := context.Background()
	results, err := idx.Search(ctx, "quick", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if results.Total != 2 {
		t.Errorf("expected 2 results for 'quick', got %d", results.Total)
	}

	// First result should be higher scored
	if len(results.Results) > 0 {
		first := results.Results[0]
		if first.DocID != "1" && first.DocID != "3" {
			t.Errorf("unexpected doc ID: %s", first.DocID)
		}
		if first.Score <= 0 {
			t.Error("expected positive score")
		}
	}
}

func TestIndex_Search_NoResults(t *testing.T) {
	idx := NewIndex("test", "")

	doc := &Document{
		ID:      "1",
		Content: "hello world",
	}
	idx.AddDocument(doc)

	ctx := context.Background()
	results, err := idx.Search(ctx, "nonexistent", SearchOptions{})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if results.Total != 0 {
		t.Errorf("expected 0 results, got %d", results.Total)
	}
}

func TestIndex_Search_Limit(t *testing.T) {
	idx := NewIndex("test", "")

	// Add multiple documents
	for i := 0; i < 10; i++ {
		doc := &Document{
			ID:      string(rune('a' + i)),
			Content: "test content for document",
		}
		idx.AddDocument(doc)
	}

	ctx := context.Background()
	results, err := idx.Search(ctx, "test", SearchOptions{Limit: 5})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results.Results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results.Results))
	}
}

func TestPorterStemmer(t *testing.T) {
	stemmer := NewPorterStemmer()

	tests := []struct {
		input    string
		expected string
	}{
		{"running", "runn"},  // Simplified stemmer removes 'ing'
		{"jumps", "jump"},    // Removes 's'
		{"dogs", "dog"},      // Removes 's'
		{"testing", "test"},  // Removes 'ing'
		{"walked", "walk"},   // Removes 'ed'
	}

	for _, tc := range tests {
		result := stemmer.Stem(tc.input)
		if result != tc.expected {
			t.Errorf("Stem(%s) = %s, expected %s", tc.input, result, tc.expected)
		}
	}
}

func TestSimpleTokenizer(t *testing.T) {
	tokenizer := &SimpleTokenizer{}
	text := "Hello, world! This is a test."

	tokens := tokenizer.Tokenize(text)

	expected := []string{"hello", "world", "this", "is", "a", "test"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}

	for i, exp := range expected {
		if tokens[i].Text != exp {
			t.Errorf("token %d: expected %s, got %s", i, exp, tokens[i].Text)
		}
	}
}

func TestIndex_Stats(t *testing.T) {
	idx := NewIndex("test_idx", "content")

	for i := 0; i < 5; i++ {
		doc := &Document{
			ID:      string(rune('a' + i)),
			Content: "test content here",
		}
		idx.AddDocument(doc)
	}

	stats := idx.Stats()
	if stats.Name != "test_idx" {
		t.Errorf("expected name 'test_idx', got %s", stats.Name)
	}
	if stats.Field != "content" {
		t.Errorf("expected field 'content', got %s", stats.Field)
	}
	if stats.DocCount != 5 {
		t.Errorf("expected 5 documents, got %d", stats.DocCount)
	}
	if stats.TermCount == 0 {
		t.Error("expected non-zero term count")
	}
}

func TestIndex_Clear(t *testing.T) {
	idx := NewIndex("test", "")

	doc := &Document{
		ID:      "1",
		Content: "test content",
	}
	idx.AddDocument(doc)

	idx.Clear()

	if idx.docCount != 0 {
		t.Errorf("expected 0 documents after clear, got %d", idx.docCount)
	}
	if len(idx.terms) != 0 {
		t.Errorf("expected 0 terms after clear, got %d", len(idx.terms))
	}
}

func TestSearchCollection_AddBSONDocument(t *testing.T) {
	sc := NewSearchCollection("test", "title")

	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("doc1"))
	doc.Set("title", bson.VString("Hello World"))
	doc.Set("content", bson.VString("This is a test"))

	if err := sc.AddBSONDocument("doc1", doc); err != nil {
		t.Fatalf("AddBSONDocument failed: %v", err)
	}

	// Search
	ctx := context.Background()
	results, err := sc.Search(ctx, "hello", SearchOptions{})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if results.Total != 1 {
		t.Errorf("expected 1 result, got %d", results.Total)
	}
}

func TestFilterStopWords(t *testing.T) {
	tokens := []Token{
		{Text: "the", Position: 0},
		{Text: "quick", Position: 1},
		{Text: "brown", Position: 2},
		{Text: "a", Position: 3},
		{Text: "fox", Position: 4},
	}

	filtered := FilterStopWords(tokens)

	if len(filtered) != 3 {
		t.Errorf("expected 3 tokens after filtering, got %d", len(filtered))
	}

	expected := []string{"quick", "brown", "fox"}
	for i, exp := range expected {
		if filtered[i].Text != exp {
			t.Errorf("token %d: expected %s, got %s", i, exp, filtered[i].Text)
		}
	}
}

func TestParseTextQuery(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("$search", bson.VString("hello world"))
	doc.Set("$caseSensitive", bson.VBool(true))

	tq, err := ParseTextQuery(doc)
	if err != nil {
		t.Fatalf("ParseTextQuery failed: %v", err)
	}

	if tq.Search != "hello world" {
		t.Errorf("expected search 'hello world', got %s", tq.Search)
	}
	if !tq.Case {
		t.Error("expected case sensitive")
	}
}

func TestParseTextQuery_NoSearch(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("$caseSensitive", bson.VBool(true))

	_, err := ParseTextQuery(doc)
	if err == nil {
		t.Error("expected error for missing $search")
	}
}

func TestIndex_Snippet(t *testing.T) {
	idx := NewIndex("test", "")

	doc := &Document{
		ID:      "1",
		Content: "This is a very long document with many words that should be searchable. The quick brown fox jumps over the lazy dog.",
	}
	idx.AddDocument(doc)

	ctx := context.Background()
	results, err := idx.Search(ctx, "fox", SearchOptions{})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results.Results) == 0 {
		t.Fatal("expected at least one result")
	}

	snippet := results.Results[0].Snippet
	if !strings.Contains(snippet, "fox") {
		t.Errorf("snippet should contain 'fox': %s", snippet)
	}
	if !strings.HasPrefix(snippet, "...") {
		t.Error("snippet should start with '...'")
	}
}

func TestIndex_TF_IDF(t *testing.T) {
	idx := NewIndex("test", "")

	// Add documents with different term frequencies
	docs := []*Document{
		{ID: "1", Content: "java java java programming"},
		{ID: "2", Content: "java programming"},
		{ID: "3", Content: "python programming"},
	}

	for _, doc := range docs {
		idx.AddDocument(doc)
	}

	ctx := context.Background()
	results, err := idx.Search(ctx, "java", SearchOptions{})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Doc 1 should have highest score (more "java" occurrences)
	if len(results.Results) < 2 {
		t.Fatal("expected at least 2 results")
	}

	if results.Results[0].DocID != "1" {
		t.Errorf("expected doc 1 first (higher TF), got %s", results.Results[0].DocID)
	}
}

func TestIndex_UpdateDocument(t *testing.T) {
	idx := NewIndex("test", "")

	doc := &Document{
		ID:      "1",
		Content: "original content",
	}
	idx.AddDocument(doc)

	// Update with same ID
	doc2 := &Document{
		ID:      "1",
		Content: "updated content",
	}
	idx.AddDocument(doc2)

	if idx.docCount != 1 {
		t.Errorf("expected 1 document after update, got %d", idx.docCount)
	}

	// Search for old content should return nothing
	ctx := context.Background()
	results, _ := idx.Search(ctx, "original", SearchOptions{})
	if results.Total != 0 {
		t.Error("old content should not be searchable")
	}

	// Search for new content should work
	results, _ = idx.Search(ctx, "updated", SearchOptions{})
	if results.Total != 1 {
		t.Errorf("new content should be searchable, got %d results", results.Total)
	}
}

// Test Index SetTokenizer
func TestIndex_SetTokenizer(t *testing.T) {
	idx := NewIndex("test", "")

	// Create a custom tokenizer that splits by comma
	customTokenizer := &SimpleTokenizer{}

	// Set the custom tokenizer
	idx.SetTokenizer(customTokenizer)

	// Add a document
	doc := &Document{
		ID:      "1",
		Content: "hello,world,test",
	}
	idx.AddDocument(doc)

	// Search should work with the custom tokenizer
	ctx := context.Background()
	results, err := idx.Search(ctx, "hello", SearchOptions{})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if results.Total != 1 {
		t.Errorf("expected 1 result, got %d", results.Total)
	}
}

// Test Index SetStemmer
func TestIndex_SetStemmer(t *testing.T) {
	idx := NewIndex("test", "")

	// Create a custom stemmer (no-op for this test)
	customStemmer := &PorterStemmer{}

	// Set the custom stemmer
	idx.SetStemmer(customStemmer)

	// Add a document
	doc := &Document{
		ID:      "1",
		Content: "running jumped",
	}
	idx.AddDocument(doc)

	// Search should work
	ctx := context.Background()
	results, err := idx.Search(ctx, "running", SearchOptions{})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	// With stemming, "running" should match
	if results.Total != 1 {
		t.Errorf("expected 1 result, got %d", results.Total)
	}
}

// Test SearchCollection Index() getter
func TestSearchCollection_Index(t *testing.T) {
	sc := NewSearchCollection("test", "title")

	// Index() should return the underlying index
	idx := sc.Index()
	if idx == nil {
		t.Fatal("Index() returned nil")
	}

	// Verify we can use the index
	doc := &Document{
		ID:      "1",
		Content: "test content",
	}
	err := idx.AddDocument(doc)
	if err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	// Search through the collection should work
	ctx := context.Background()
	results, err := sc.Search(ctx, "test", SearchOptions{})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if results.Total != 1 {
		t.Errorf("expected 1 result, got %d", results.Total)
	}
}
