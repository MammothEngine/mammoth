package mongo

import (
	"encoding/binary"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// English stop words.
var stopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "but": true, "by": true, "for": true, "if": true, "in": true,
	"into": true, "is": true, "it": true, "no": true, "not": true, "of": true,
	"on": true, "or": true, "over": true, "such": true, "that": true, "the": true, "their": true,
	"then": true, "there": true, "these": true, "they": true, "this": true, "to": true,
	"was": true, "will": true, "with": true,
}

const textIndexPrefix = "\x00txt"

// TextIndex implements full-text search using an inverted index.
type TextIndex struct {
	spec    *IndexSpec
	db, coll string
	eng     *engine.Engine
	stemmer *PorterStemmer
}

// NewTextIndex creates a text index handle.
func NewTextIndex(db, coll string, spec *IndexSpec, eng *engine.Engine) *TextIndex {
	return &TextIndex{
		spec:    spec,
		db:      db,
		coll:    coll,
		eng:     eng,
		stemmer: NewPorterStemmer(),
	}
}

// SearchResult represents a document match with relevance score.
type SearchResult struct {
	ID    bson.ObjectID
	Score float64
}

// textKeyPrefix returns the prefix for all keys in this text index.
func (ti *TextIndex) textKeyPrefix() []byte {
	ns := EncodeNamespacePrefix(ti.db, ti.coll)
	prefix := make([]byte, 0, len(ns)+len(textIndexPrefix)+len(ti.spec.Name))
	prefix = append(prefix, ns...)
	prefix = append(prefix, textIndexPrefix...)
	prefix = append(prefix, ti.spec.Name...)
	return prefix
}

// buildTextKey builds: {ns}\x00txt{index_name}{token}{_id_bytes}
func (ti *TextIndex) buildTextKey(token string, id []byte) []byte {
	prefix := ti.textKeyPrefix()
	buf := make([]byte, 0, len(prefix)+len(token)+len(id))
	buf = append(buf, prefix...)
	buf = append(buf, token...)
	buf = append(buf, id...)
	return buf
}

// tokenize splits text into tokens: lowercase, split on non-alpha, remove stop words, stem.
func (ti *TextIndex) Tokenize(text string) []string {
	text = strings.ToLower(text)
	var tokens []string
	var current []rune
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			current = append(current, r)
		} else {
			if len(current) > 0 {
				word := string(current)
				current = nil
				if !stopWords[word] && len(word) > 1 {
					stemmed := ti.stemmer.Stem(word)
					tokens = append(tokens, stemmed)
				}
			}
		}
	}
	if len(current) > 0 {
		word := string(current)
		if !stopWords[word] && len(word) > 1 {
			tokens = append(tokens, ti.stemmer.Stem(word))
		}
	}
	return tokens
}

// AddEntry tokenizes the text field and writes inverted index entries with TF score.
func (ti *TextIndex) AddEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	if ti.spec.PartialFilterExpression != nil {
		m := NewMatcher(ti.spec.PartialFilterExpression)
		if !m.Match(doc) {
			return nil
		}
	}

	// Find the text field(s)
	var textContent string
	for _, ik := range ti.spec.Key {
		v, found := ResolveField(doc, ik.Field)
		if found && v.Type == bson.TypeString {
			textContent += " " + v.String()
		}
	}
	textContent = strings.TrimSpace(textContent)
	if textContent == "" {
		return nil
	}

	tokens := ti.Tokenize(textContent)
	if len(tokens) == 0 {
		return nil
	}

	// Count term frequencies
	tf := make(map[string]int)
	for _, t := range tokens {
		tf[t]++
	}
	total := float64(len(tokens))

	// Write inverted index entries
	for token, count := range tf {
		key := ti.buildTextKey(token, idBytes)
		score := float64(count) / total // TF score
		var scoreBytes [8]byte
		binary.BigEndian.PutUint64(scoreBytes[:], math.Float64bits(score))
		if err := ti.eng.Put(key, scoreBytes[:]); err != nil {
			return err
		}
	}
	return nil
}

// RemoveEntry deletes all inverted entries for a document.
func (ti *TextIndex) RemoveEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	// Collect all text fields
	var textContent string
	for _, ik := range ti.spec.Key {
		v, found := ResolveField(doc, ik.Field)
		if found && v.Type == bson.TypeString {
			textContent += " " + v.String()
		}
	}
	textContent = strings.TrimSpace(textContent)
	if textContent == "" {
		return nil
	}

	tokens := ti.Tokenize(textContent)
	seen := make(map[string]bool)
	for _, t := range tokens {
		if seen[t] {
			continue
		}
		seen[t] = true
		key := ti.buildTextKey(t, idBytes)
		_ = ti.eng.Delete(key)
	}
	return nil
}

// Search performs a full-text search query. Returns results sorted by TF-IDF score.
func (ti *TextIndex) Search(query string, limit int) []SearchResult {
	queryTokens := ti.Tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	}

	// Collect all matching documents with their scores
	// map from doc ID (as bytes) to accumulated score
	type docScore struct {
		id    []byte
		score float64
	}
	scores := make(map[string]*docScore)
	prefix := ti.textKeyPrefix()

	for _, token := range queryTokens {
		scanPrefix := append([]byte{}, prefix...)
		scanPrefix = append(scanPrefix, token...)

		var docCount int
		ti.eng.Scan(scanPrefix, func(_, value []byte) bool {
			docCount++
			return true
		})

		if docCount == 0 {
			continue
		}

		// IDF = log(N / df) where N is total docs, df is doc frequency
		idf := math.Log(float64(1000) / float64(docCount+1))

		ti.eng.Scan(scanPrefix, func(key, value []byte) bool {
			if len(key) <= len(scanPrefix) {
				return true
			}
			idBytes := key[len(scanPrefix):]
			if len(value) < 8 {
				return true
			}
			tfFloat := math.Float64frombits(binary.BigEndian.Uint64(value[:8]))
			idKey := string(idBytes)
			if _, ok := scores[idKey]; !ok {
				scores[idKey] = &docScore{id: append([]byte{}, idBytes...), score: 0}
			}
			scores[idKey].score += tfFloat * idf
			return true
		})
	}

	if len(scores) == 0 {
		return nil
	}

	// Sort by score descending
	var results []SearchResult
	for _, ds := range scores {
		if len(ds.id) == 12 {
			var oid bson.ObjectID
			copy(oid[:], ds.id)
			results = append(results, SearchResult{ID: oid, Score: ds.score})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}
	return results
}
