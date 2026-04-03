// Package search provides full-text search capabilities with inverted index.
package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Index represents an inverted index for full-text search.
type Index struct {
	name      string
	field     string
	docs      map[string]*Document // docID -> document
	terms     map[string]*PostingList // term -> posting list
	docCount  int
	tokenizer Tokenizer
	stemmer   Stemmer
	mu        sync.RWMutex
}

// Document represents a searchable document.
type Document struct {
	ID      string
	Content string
	Fields  map[string]string
	Score   float64
}

// PostingList contains document IDs and term frequencies.
type PostingList struct {
	Docs      map[string]*Posting // docID -> posting
	DocFreq   int                 // Number of documents containing this term
	TotalFreq int                 // Total occurrences across all docs
}

// Posting represents a term's occurrence in a document.
type Posting struct {
	DocID     string
	Positions []int // Word positions for phrase queries
	Freq      int   // Term frequency in document
}

// SearchResult represents a search result.
type SearchResult struct {
	DocID   string
	Score   float64
	Snippet string
	Doc     *Document
}

// Tokenizer breaks text into tokens.
type Tokenizer interface {
	Tokenize(text string) []Token
}

// Stemmer reduces words to their root form.
type Stemmer interface {
	Stem(word string) string
}

// Token represents a tokenized word.
type Token struct {
	Text     string
	Position int
}

// SimpleTokenizer is a basic tokenizer.
type SimpleTokenizer struct{}

// Tokenize breaks text into tokens.
func (t *SimpleTokenizer) Tokenize(text string) []Token {
	var tokens []Token
	position := 0

	for _, word := range strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}) {
		word = strings.ToLower(word)
		if len(word) > 0 {
			tokens = append(tokens, Token{
				Text:     word,
				Position: position,
			})
			position++
		}
	}

	return tokens
}

// PorterStemmer implements the Porter stemming algorithm.
type PorterStemmer struct {
	suffixes []suffixRule
}

type suffixRule struct {
	suffix  string
	replace string
	minLen  int
}

// NewPorterStemmer creates a new Porter stemmer.
func NewPorterStemmer() *PorterStemmer {
	return &PorterStemmer{
		suffixes: []suffixRule{
			{"ational", "ate", 7},
			{"tional", "tion", 6},
			{"enci", "ence", 4},
			{"anci", "ance", 4},
			{"izer", "ize", 4},
			{"abli", "able", 4},
			{"alli", "al", 4},
			{"entli", "ent", 5},
			{"eli", "e", 3},
			{"ousli", "ous", 5},
			{"ization", "ize", 7},
			{"ation", "ate", 5},
			{"ator", "ate", 4},
			{"alism", "al", 5},
			{"iveness", "ive", 7},
			{"fulness", "ful", 7},
			{"ousness", "ous", 7},
			{"aliti", "al", 5},
			{"iviti", "ive", 5},
			{"biliti", "ble", 6},
			{"ing", "", 3},
			{"ed", "", 2},
			{"s", "", 1},
		},
	}
}

// Stem reduces a word to its root form.
func (s *PorterStemmer) Stem(word string) string {
	word = strings.ToLower(word)

	for _, rule := range s.suffixes {
		if len(word) > rule.minLen && strings.HasSuffix(word, rule.suffix) {
			return word[:len(word)-len(rule.suffix)] + rule.replace
		}
	}

	return word
}

// NewIndex creates a new search index.
func NewIndex(name, field string) *Index {
	return &Index{
		name:      name,
		field:     field,
		docs:      make(map[string]*Document),
		terms:     make(map[string]*PostingList),
		tokenizer: &SimpleTokenizer{},
		stemmer:   NewPorterStemmer(),
	}
}

// SetTokenizer sets a custom tokenizer.
func (idx *Index) SetTokenizer(t Tokenizer) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.tokenizer = t
}

// SetStemmer sets a custom stemmer.
func (idx *Index) SetStemmer(s Stemmer) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.stemmer = s
}

// AddDocument adds a document to the index.
func (idx *Index) AddDocument(doc *Document) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if doc.ID == "" {
		return errors.New("document ID cannot be empty")
	}

	// Remove old version if exists
	if _, exists := idx.docs[doc.ID]; exists {
		idx.removeDocumentInternal(doc.ID)
	}

	// Store document
	idx.docs[doc.ID] = doc
	idx.docCount++

	// Index content
	content := doc.Content
	if content == "" && idx.field != "" {
		if fieldContent, ok := doc.Fields[idx.field]; ok {
			content = fieldContent
		}
	}

	tokens := idx.tokenizer.Tokenize(content)

	// Build term frequencies
	termFreqs := make(map[string][]int) // term -> positions
	for _, token := range tokens {
		stemmed := idx.stemmer.Stem(token.Text)
		termFreqs[stemmed] = append(termFreqs[stemmed], token.Position)
	}

	// Update index
	for term, positions := range termFreqs {
		if _, exists := idx.terms[term]; !exists {
			idx.terms[term] = &PostingList{
				Docs: make(map[string]*Posting),
			}
		}

		postingList := idx.terms[term]
		postingList.Docs[doc.ID] = &Posting{
			DocID:     doc.ID,
			Positions: positions,
			Freq:      len(positions),
		}
		postingList.DocFreq++
		postingList.TotalFreq += len(positions)
	}

	return nil
}

// RemoveDocument removes a document from the index.
func (idx *Index) RemoveDocument(docID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.removeDocumentInternal(docID)
}

func (idx *Index) removeDocumentInternal(docID string) error {
	doc, exists := idx.docs[docID]
	if !exists {
		return nil
	}

	// Remove from document list
	delete(idx.docs, docID)
	idx.docCount--

	// Remove from term index
	content := doc.Content
	if content == "" && idx.field != "" {
		if fieldContent, ok := doc.Fields[idx.field]; ok {
			content = fieldContent
		}
	}

	tokens := idx.tokenizer.Tokenize(content)
	uniqueTerms := make(map[string]bool)
	for _, token := range tokens {
		stemmed := idx.stemmer.Stem(token.Text)
		uniqueTerms[stemmed] = true
	}

	for term := range uniqueTerms {
		if postingList, exists := idx.terms[term]; exists {
			delete(postingList.Docs, docID)
			postingList.DocFreq--
			if postingList.DocFreq == 0 {
				delete(idx.terms, term)
			}
		}
	}

	return nil
}

// Search performs a full-text search.
func (idx *Index) Search(ctx context.Context, query string, opts SearchOptions) (*SearchResults, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Tokenize query
	queryTokens := idx.tokenizer.Tokenize(query)
	if len(queryTokens) == 0 {
		return &SearchResults{}, nil
	}

	// Collect postings for each query term
	var allPostings []*PostingList
	for _, token := range queryTokens {
		term := idx.stemmer.Stem(token.Text)
		if postingList, exists := idx.terms[term]; exists {
			allPostings = append(allPostings, postingList)
		}
	}

	if len(allPostings) == 0 {
		return &SearchResults{}, nil
	}

	// Score documents using TF-IDF
	docScores := make(map[string]float64)

	for _, postingList := range allPostings {
		idf := idx.idf(postingList.DocFreq)

		for docID, posting := range postingList.Docs {
			tf := float64(posting.Freq)
			// TF-IDF score
			score := tf * idf
			docScores[docID] += score
		}
	}

	// Build results
	var results []*SearchResult
	for docID, score := range docScores {
		if doc, exists := idx.docs[docID]; exists {
			results = append(results, &SearchResult{
				DocID:   docID,
				Score:   score,
				Snippet: idx.generateSnippet(doc, queryTokens),
				Doc:     doc,
			})
		}
	}

	// Sort by score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Apply limit
	if opts.Limit > 0 && len(results) > opts.Limit {
		results = results[:opts.Limit]
	}

	return &SearchResults{
		Results:   results,
		Total:     len(docScores),
		Query:     query,
		Processed: len(queryTokens),
	}, nil
}

// idf calculates inverse document frequency.
func (idx *Index) idf(docFreq int) float64 {
	if docFreq == 0 {
		return 0
	}
	return math.Log(float64(idx.docCount)/float64(docFreq)) + 1
}

// generateSnippet creates a preview snippet highlighting query terms.
func (idx *Index) generateSnippet(doc *Document, queryTokens []Token) string {
	content := doc.Content
	if content == "" && idx.field != "" {
		if fieldContent, ok := doc.Fields[idx.field]; ok {
			content = fieldContent
		}
	}

	// Find first occurrence of any query term
	contentLower := strings.ToLower(content)
	for _, token := range queryTokens {
		pos := strings.Index(contentLower, strings.ToLower(token.Text))
		if pos >= 0 {
			start := pos - 50
			if start < 0 {
				start = 0
			}
			end := pos + 100
			if end > len(content) {
				end = len(content)
			}
			snippet := content[start:end]
			if start > 0 {
				snippet = "..." + snippet
			}
			if end < len(content) {
				snippet = snippet + "..."
			}
			return snippet
		}
	}

	// Return first 150 chars if no match found
	if len(content) > 150 {
		return content[:150] + "..."
	}
	return content
}

// SearchOptions configures search behavior.
type SearchOptions struct {
	Limit  int
	Offset int
	SortBy string
}

// SearchResults contains search results.
type SearchResults struct {
	Results   []*SearchResult
	Total     int
	Query     string
	Processed int
}

// Stats returns index statistics.
func (idx *Index) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return IndexStats{
		Name:       idx.name,
		Field:      idx.field,
		DocCount:   idx.docCount,
		TermCount:  len(idx.terms),
	}
}

// IndexStats contains index statistics.
type IndexStats struct {
	Name      string
	Field     string
	DocCount  int
	TermCount int
}

// Clear removes all documents from the index.
func (idx *Index) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.docs = make(map[string]*Document)
	idx.terms = make(map[string]*PostingList)
	idx.docCount = 0
}

// SearchCollection indexes BSON documents from a collection.
type SearchCollection struct {
	index     *Index
	field     string
	documents map[string]*bson.Document
}

// NewSearchCollection creates a new searchable collection wrapper.
func NewSearchCollection(name, field string) *SearchCollection {
	return &SearchCollection{
		index:     NewIndex(name, field),
		field:     field,
		documents: make(map[string]*bson.Document),
	}
}

// AddBSONDocument adds a BSON document to the search index.
func (sc *SearchCollection) AddBSONDocument(docID string, doc *bson.Document) error {
	sc.documents[docID] = doc

	// Extract searchable content from document
	content := sc.extractContent(doc)

	return sc.index.AddDocument(&Document{
		ID:      docID,
		Content: content,
		Fields:  sc.extractFields(doc),
	})
}

// extractContent extracts searchable text from a BSON document.
func (sc *SearchCollection) extractContent(doc *bson.Document) string {
	var parts []string

	for _, elem := range doc.Elements() {
		if elem.Key == sc.field {
			parts = append(parts, bsonValueToString(elem.Value))
		}
	}

	return strings.Join(parts, " ")
}

// extractFields extracts all fields from a BSON document.
func (sc *SearchCollection) extractFields(doc *bson.Document) map[string]string {
	fields := make(map[string]string)

	for _, elem := range doc.Elements() {
		fields[elem.Key] = bsonValueToString(elem.Value)
	}

	return fields
}

// bsonValueToString converts a BSON value to string.
func bsonValueToString(v bson.Value) string {
	switch v.Type {
	case bson.TypeString:
		return v.String()
	case bson.TypeInt32:
		return fmt.Sprintf("%d", v.Int32())
	case bson.TypeInt64:
		return fmt.Sprintf("%d", v.Int64())
	case bson.TypeDouble:
		return fmt.Sprintf("%g", v.Double())
	default:
		return ""
	}
}

// Search performs a search on the collection.
func (sc *SearchCollection) Search(ctx context.Context, query string, opts SearchOptions) (*SearchResults, error) {
	return sc.index.Search(ctx, query, opts)
}

// Index returns the underlying index.
func (sc *SearchCollection) Index() *Index {
	return sc.index
}

// TextQuery represents a text search query for MongoDB compatibility.
type TextQuery struct {
	Search string   `bson:"$search"`
	Filter []string `bson:"$language,omitempty"`
	Case   bool     `bson:"$caseSensitive,omitempty"`
	Diacritics bool `bson:"$diacriticSensitive,omitempty"`
}

// ParseTextQuery parses a MongoDB-style text query.
func ParseTextQuery(doc *bson.Document) (*TextQuery, error) {
	tq := &TextQuery{}

	if val, ok := doc.Get("$search"); ok {
		tq.Search = val.String()
	}

	if val, ok := doc.Get("$caseSensitive"); ok {
		tq.Case = val.Boolean()
	}

	if val, ok := doc.Get("$diacriticSensitive"); ok {
		tq.Diacritics = val.Boolean()
	}

	if tq.Search == "" {
		return nil, errors.New("$search is required")
	}

	return tq, nil
}

// StopWords is a set of common words to ignore during indexing.
var StopWords = map[string]bool{
	"the": true, "be": true, "to": true, "of": true, "and": true,
	"a": true, "in": true, "that": true, "have": true, "i": true,
	"it": true, "for": true, "not": true, "on": true, "with": true,
	"he": true, "as": true, "you": true, "do": true, "at": true,
	"this": true, "but": true, "his": true, "by": true, "from": true,
	"they": true, "we": true, "say": true, "her": true, "she": true,
	"or": true, "an": true, "will": true, "my": true, "one": true,
	"all": true, "would": true, "there": true, "their": true,
}

// FilterStopWords removes stop words from a token list.
func FilterStopWords(tokens []Token) []Token {
	var filtered []Token
	for _, token := range tokens {
		if !StopWords[token.Text] {
			filtered = append(filtered, token)
		}
	}
	return filtered
}
