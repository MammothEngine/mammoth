package mongo

import (
	"strings"
	"unicode"
)

// PorterStemmer implements the Porter stemming algorithm for English.
type PorterStemmer struct{}

// NewPorterStemmer creates a new Porter stemmer.
func NewPorterStemmer() *PorterStemmer { return &PorterStemmer{} }

// Stem returns the stemmed form of an English word using the Porter algorithm.
func (ps *PorterStemmer) Stem(word string) string {
	if len(word) <= 2 {
		return word
	}
	word = strings.ToLower(word)
	w := []rune(word)
	w = ps.step1a(w)
	w = ps.step1b(w)
	w = ps.step1c(w)
	w = ps.step2(w)
	w = ps.step3(w)
	w = ps.step4(w)
	w = ps.step5a(w)
	w = ps.step5b(w)
	return string(w)
}

func (ps *PorterStemmer) step1a(w []rune) []rune {
	if hasSuffix(w, "sses") {
		return replaceSuffix(w, "sses", "ss")
	}
	if hasSuffix(w, "ies") {
		return replaceSuffix(w, "ies", "i")
	}
	if hasSuffix(w, "ss") {
		return w // no change
	}
	if hasSuffix(w, "s") && !hasSuffix(w, "us") && !hasSuffix(w, "ss") {
		return w[:len(w)-1]
	}
	return w
}

func (ps *PorterStemmer) step1b(w []rune) []rune {
	if hasSuffix(w, "eed") {
		if m := measure(w[:len(w)-3]); m > 0 {
			return replaceSuffix(w, "eed", "ee")
		}
		return w
	}
	if hasSuffix(w, "ed") {
		stem := w[:len(w)-2]
		if containsVowel(stem) {
			w = ps.step1bFix(stem)
			return w
		}
	}
	if hasSuffix(w, "ing") {
		stem := w[:len(w)-3]
		if containsVowel(stem) {
			w = ps.step1bFix(stem)
			return w
		}
	}
	return w
}

func (ps *PorterStemmer) step1bFix(w []rune) []rune {
	if hasSuffix(w, "at") || hasSuffix(w, "bl") || hasSuffix(w, "iz") {
		return append(w, 'e')
	}
	if len(w) >= 2 && w[len(w)-1] == w[len(w)-2] && isDoubleLetter(w[len(w)-1]) {
		return w[:len(w)-1]
	}
	if measure(w) == 1 && isCVC(w) {
		return append(w, 'e')
	}
	return w
}

func (ps *PorterStemmer) step1c(w []rune) []rune {
	if hasSuffix(w, "y") && containsVowel(w[:len(w)-1]) {
		return replaceSuffix(w, "y", "i")
	}
	return w
}

func (ps *PorterStemmer) step2(w []rune) []rune {
	pairs := []struct{ suffix, replacement string }{
		{"ational", "ate"}, {"tional", "tion"}, {"enci", "ence"},
		{"anci", "ance"}, {"izer", "ize"}, {"abli", "able"},
		{"alli", "al"}, {"entli", "ent"}, {"eli", "e"},
		{"ousli", "ous"}, {"ization", "ize"}, {"ation", "ate"},
		{"ator", "ate"}, {"alism", "al"}, {"iveness", "ive"},
		{"fulness", "ful"}, {"ousness", "ous"}, {"aliti", "al"},
		{"iviti", "ive"}, {"biliti", "ble"},
	}
	for _, p := range pairs {
		if hasSuffix(w, p.suffix) {
			stem := w[:len(w)-len([]rune(p.suffix))]
			if measure(stem) > 0 {
				return []rune(string(stem) + p.replacement)
			}
			return w
		}
	}
	return w
}

func (ps *PorterStemmer) step3(w []rune) []rune {
	pairs := []struct{ suffix, replacement string }{
		{"icate", "ic"}, {"ative", ""}, {"alize", "al"},
		{"iciti", "ic"}, {"ical", "ic"}, {"ful", ""}, {"ness", ""},
	}
	for _, p := range pairs {
		if hasSuffix(w, p.suffix) {
			stem := w[:len(w)-len([]rune(p.suffix))]
			if measure(stem) > 0 {
				return []rune(string(stem) + p.replacement)
			}
			return w
		}
	}
	return w
}

func (ps *PorterStemmer) step4(w []rune) []rune {
	suffixes := []string{
		"al", "ance", "ence", "er", "ic", "able", "ible", "ant",
		"ement", "ment", "ent", "ion", "ou", "ism", "ate", "iti",
		"ous", "ive", "ize",
	}
	for _, suf := range suffixes {
		if hasSuffix(w, suf) {
			stem := w[:len(w)-len([]rune(suf))]
			if suf == "ion" {
				if len(stem) > 0 && (stem[len(stem)-1] == 's' || stem[len(stem)-1] == 't') {
					if measure(stem) > 1 {
						return stem
					}
				}
			} else {
				if measure(stem) > 1 {
					return stem
				}
			}
			return w
		}
	}
	return w
}

func (ps *PorterStemmer) step5a(w []rune) []rune {
	if hasSuffix(w, "e") {
		stem := w[:len(w)-1]
		if measure(stem) > 1 {
			return stem
		}
		if measure(stem) == 1 && !isCVC(stem) {
			return stem
		}
	}
	return w
}

func (ps *PorterStemmer) step5b(w []rune) []rune {
	if hasSuffix(w, "ll") && measure(w) > 1 {
		return w[:len(w)-1]
	}
	return w
}

// --- helper functions ---

func hasSuffix(w []rune, suffix string) bool {
	s := []rune(suffix)
	if len(s) > len(w) {
		return false
	}
	return runeSliceEqual(w[len(w)-len(s):], s)
}

func replaceSuffix(w []rune, suffix, replacement string) []rune {
	s := []rune(suffix)
	r := []rune(replacement)
	out := make([]rune, len(w)-len(s)+len(r))
	copy(out, w[:len(w)-len(s)])
	copy(out[len(w)-len(s):], r)
	return out
}

func runeSliceEqual(a, b []rune) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsVowel(w []rune) bool {
	for _, r := range w {
		if isVowel(r) {
			return true
		}
	}
	return false
}

func isVowel(r rune) bool {
	return r == 'a' || r == 'e' || r == 'i' || r == 'o' || r == 'u'
}

func isConsonant(r rune) bool {
	return !isVowel(r) && unicode.IsLetter(r)
}

func isDoubleLetter(r rune) bool {
	return r == 'b' || r == 'd' || r == 'f' || r == 'g' || r == 'm' ||
		r == 'n' || r == 'p' || r == 'r' || r == 't'
}

// measure computes the Porter "m" value: number of VC sequences.
func measure(w []rune) int {
	if len(w) == 0 {
		return 0
	}
	// Skip initial consonants
	i := 0
	for i < len(w) && isConsonant(w[i]) {
		i++
	}
	m := 0
	for i < len(w) {
		// Skip vowels
		for i < len(w) && isVowel(w[i]) {
			i++
		}
		if i >= len(w) {
			break
		}
		// Skip consonants
		for i < len(w) && isConsonant(w[i]) {
			i++
		}
		m++
	}
	return m
}

// isCVC returns true if w ends with consonant-vowel-consonant where the
// last consonant is not w, x, or y.
func isCVC(w []rune) bool {
	if len(w) < 3 {
		return false
	}
	n := len(w)
	return isConsonant(w[n-3]) && isVowel(w[n-2]) &&
		isConsonant(w[n-1]) && w[n-1] != 'w' && w[n-1] != 'x' && w[n-1] != 'y'
}
