package mongo

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

// D is an ordered BSON document. Order matters for MongoDB commands: the
// command name must be the first element.
type D []E

// E is a single BSON element.
type E struct {
	Key string
	Val any
}

// M is an unordered decoded document.
type M map[string]any

// A is a BSON array.
type A []any

// marshal encodes doc into BSON. Supported value types: string, int, int32,
// int64, float64, bool, D, M, A, nil.
func marshal(doc D) ([]byte, error) {
	buf := make([]byte, 4) // placeholder for length
	var err error
	for _, e := range doc {
		buf, err = appendElement(buf, e.Key, e.Val)
		if err != nil {
			return nil, err
		}
	}
	buf = append(buf, 0x00)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(buf)))
	return buf, nil
}

func appendElement(buf []byte, key string, val any) ([]byte, error) {
	appendHeader := func(t byte) {
		buf = append(buf, t)
		buf = append(buf, key...)
		buf = append(buf, 0x00)
	}

	switch v := val.(type) {
	case string:
		appendHeader(0x02)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(v)+1))
		buf = append(buf, v...)
		buf = append(buf, 0x00)
	case int32:
		appendHeader(0x10)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(v))
	case int:
		appendHeader(0x10)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(int32(v)))
	case int64:
		appendHeader(0x12)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(v))
	case float64:
		appendHeader(0x01)
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
	case bool:
		appendHeader(0x08)
		if v {
			buf = append(buf, 0x01)
		} else {
			buf = append(buf, 0x00)
		}
	case []byte:
		appendHeader(0x05)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, 0x00) // subtype: generic
		buf = append(buf, v...)
	case D:
		sub, err := marshal(v)
		if err != nil {
			return nil, err
		}
		appendHeader(0x03)
		buf = append(buf, sub...)
	case M:
		sub := make(D, 0, len(v))
		for k, mv := range v {
			sub = append(sub, E{k, mv})
		}
		enc, err := marshal(sub)
		if err != nil {
			return nil, err
		}
		appendHeader(0x03)
		buf = append(buf, enc...)
	case A:
		sub := make(D, 0, len(v))
		for idx, av := range v {
			sub = append(sub, E{strconv.Itoa(idx), av})
		}
		enc, err := marshal(sub)
		if err != nil {
			return nil, err
		}
		appendHeader(0x04)
		buf = append(buf, enc...)
	case nil:
		appendHeader(0x0A)
	default:
		return nil, fmt.Errorf("bson: unsupported type %T for key %q", val, key)
	}
	return buf, nil
}

// unmarshal decodes a BSON document into an unordered map. Types not needed
// for command replies are skipped where possible.
func unmarshal(data []byte) (M, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("bson: document too short (%d bytes)", len(data))
	}
	total := int(binary.LittleEndian.Uint32(data[0:4]))
	if total > len(data) {
		return nil, fmt.Errorf("bson: declared length %d exceeds buffer %d", total, len(data))
	}
	doc := M{}
	pos := 4
	for pos < total-1 {
		t := data[pos]
		pos++
		// key: cstring
		end := pos
		for end < total && data[end] != 0x00 {
			end++
		}
		if end >= total {
			return nil, fmt.Errorf("bson: unterminated key")
		}
		key := string(data[pos:end])
		pos = end + 1

		val, n, err := decodeValue(t, data[pos:total])
		if err != nil {
			return nil, fmt.Errorf("bson: key %q: %w", key, err)
		}
		doc[key] = val
		pos += n
	}
	return doc, nil
}

// decodeValue returns the decoded value and how many bytes it consumed.
func decodeValue(t byte, b []byte) (any, int, error) {
	need := func(n int) error {
		if len(b) < n {
			return fmt.Errorf("truncated value (type 0x%02x)", t)
		}
		return nil
	}

	switch t {
	case 0x01: // double
		if err := need(8); err != nil {
			return nil, 0, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b)), 8, nil
	case 0x02, 0x0D, 0x0E: // string, javascript, symbol
		if err := need(4); err != nil {
			return nil, 0, err
		}
		l := int(binary.LittleEndian.Uint32(b))
		if err := need(4 + l); err != nil {
			return nil, 0, err
		}
		return string(b[4 : 4+l-1]), 4 + l, nil
	case 0x03, 0x04: // document, array
		if err := need(4); err != nil {
			return nil, 0, err
		}
		l := int(binary.LittleEndian.Uint32(b))
		if err := need(l); err != nil {
			return nil, 0, err
		}
		sub, err := unmarshal(b[:l])
		if err != nil {
			return nil, 0, err
		}
		if t == 0x04 {
			arr := make(A, 0, len(sub))
			for i := 0; ; i++ {
				v, ok := sub[strconv.Itoa(i)]
				if !ok {
					break
				}
				arr = append(arr, v)
			}
			return arr, l, nil
		}
		return sub, l, nil
	case 0x05: // binary
		if err := need(5); err != nil {
			return nil, 0, err
		}
		l := int(binary.LittleEndian.Uint32(b))
		if err := need(4 + 1 + l); err != nil {
			return nil, 0, err
		}
		cp := make([]byte, l)
		copy(cp, b[5:5+l])
		return cp, 4 + 1 + l, nil
	case 0x07: // objectid
		if err := need(12); err != nil {
			return nil, 0, err
		}
		return fmt.Sprintf("%x", b[:12]), 12, nil
	case 0x08: // bool
		if err := need(1); err != nil {
			return nil, 0, err
		}
		return b[0] != 0, 1, nil
	case 0x09: // UTC datetime
		if err := need(8); err != nil {
			return nil, 0, err
		}
		return int64(binary.LittleEndian.Uint64(b)), 8, nil
	case 0x0A: // null
		return nil, 0, nil
	case 0x10: // int32
		if err := need(4); err != nil {
			return nil, 0, err
		}
		return int32(binary.LittleEndian.Uint32(b)), 4, nil
	case 0x11: // timestamp
		if err := need(8); err != nil {
			return nil, 0, err
		}
		return uint64(binary.LittleEndian.Uint64(b)), 8, nil
	case 0x12: // int64
		if err := need(8); err != nil {
			return nil, 0, err
		}
		return int64(binary.LittleEndian.Uint64(b)), 8, nil
	case 0x13: // decimal128 — return raw bytes
		if err := need(16); err != nil {
			return nil, 0, err
		}
		cp := make([]byte, 16)
		copy(cp, b[:16])
		return cp, 16, nil
	}
	return nil, 0, fmt.Errorf("unsupported BSON type 0x%02x", t)
}

// asFloat coerces BSON numeric types to float64.
func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}
