package bson

// Compare type ordering follows MongoDB's BSON comparison order:
// MinKey < Null < Numbers < String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < RegExp < DBRef < CodeScope < Code < Symbol < Int32 < Int64 < Decimal128 < MaxKey

// typeOrder returns the sort priority of a BSON type.
func typeOrder(t BSONType) int {
	switch t {
	case TypeMinKey:
		return -1
	case TypeNull:
		return 0
	case TypeInt32, TypeInt64, TypeDouble:
		return 1
	case TypeString:
		return 2
	case TypeDocument:
		return 3
	case TypeArray:
		return 4
	case TypeBinary:
		return 5
	case TypeObjectID:
		return 6
	case TypeBoolean:
		return 7
	case TypeDateTime:
		return 8
	case TypeTimestamp:
		return 9
	case TypeRegex:
		return 10
	case TypeDBPointer:
		return 11
	case TypeCodeScope:
		return 12
	case TypeJavaScript:
		return 13
	case TypeSymbol:
		return 14
	case TypeDecimal128:
		return 15
	case TypeMaxKey:
		return 16
	default:
		return 100
	}
}

// CompareValues compares two BSON Values. Returns -1, 0, or 1.
func CompareValues(a, b Value) int {
	oa, ob := typeOrder(a.Type), typeOrder(b.Type)

	// Normalize numeric types to the same order group
	if oa == 1 && ob == 1 {
		return compareNumbers(a, b)
	}

	if oa < ob {
		return -1
	}
	if oa > ob {
		return 1
	}

	// Same type order
	switch a.Type {
	case TypeNull:
		return 0
	case TypeBoolean:
		ab, bb := a.Boolean(), b.Boolean()
		if ab == bb {
			return 0
		}
		if !ab {
			return -1
		}
		return 1
	case TypeInt32:
		av, bv := a.Int32(), b.Int32()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case TypeInt64:
		av, bv := a.Int64(), b.Int64()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case TypeDouble:
		av, bv := a.Double(), b.Double()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case TypeString, TypeSymbol:
		av, bv := a.String(), b.String()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case TypeObjectID:
		aa, bb := a.ObjectID(), b.ObjectID()
		return compareBytes(aa[:], bb[:])
	case TypeDateTime:
		av, bv := a.DateTime(), b.DateTime()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case TypeTimestamp:
		av, bv := a.Timestamp(), b.Timestamp()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case TypeBinary:
		ab, bb := a.Binary(), b.Binary()
		if ab.Subtype < bb.Subtype {
			return -1
		}
		if ab.Subtype > bb.Subtype {
			return 1
		}
		return compareBytes(ab.Data, bb.Data)
	case TypeDocument:
		return CompareDocuments(a.DocumentValue(), b.DocumentValue())
	case TypeArray:
		return compareArrays(a.ArrayValue(), b.ArrayValue())
	case TypeRegex:
		ar, br := a.Regex(), b.Regex()
		c := compareStrings(ar.Pattern, br.Pattern)
		if c != 0 {
			return c
		}
		return compareStrings(ar.Options, br.Options)
	}

	return 0
}

func compareNumbers(a, b Value) int {
	af := toFloat64(a)
	bf := toFloat64(b)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
}

func toFloat64(v Value) float64 {
	switch v.Type {
	case TypeDouble:
		return v.Double()
	case TypeInt32:
		return float64(v.Int32())
	case TypeInt64:
		return float64(v.Int64())
	}
	return 0
}

// CompareDocuments compares two documents field by field.
func CompareDocuments(a, b *Document) int {
	ae, be := a.Elements(), b.Elements()
	minLen := len(ae)
	if len(be) < minLen {
		minLen = len(be)
	}
	for i := 0; i < minLen; i++ {
		if ae[i].Key < be[i].Key {
			return -1
		}
		if ae[i].Key > be[i].Key {
			return 1
		}
		c := CompareValues(ae[i].Value, be[i].Value)
		if c != 0 {
			return c
		}
	}
	if len(ae) < len(be) {
		return -1
	}
	if len(ae) > len(be) {
		return 1
	}
	return 0
}

func compareArrays(a, b Array) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		c := CompareValues(a[i], b[i])
		if c != 0 {
			return c
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func compareStrings(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
