package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVarByteIntBoundaries checks every boundary in MQTT-5.0 Table 1-1 "Size
// of Variable Byte Integer" (§1.5.5). The table gives the exact byte sequences,
// so these are the spec's numbers rather than our own encoder's output.
func TestVarByteIntBoundaries(t *testing.T) {
	cases := []struct {
		value int
		bytes []byte
	}{
		{0, []byte{0x00}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{16383, []byte{0xFF, 0x7F}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{2097151, []byte{0xFF, 0xFF, 0x7F}},
		{2097152, []byte{0x80, 0x80, 0x80, 0x01}},
		{268435455, []byte{0xFF, 0xFF, 0xFF, 0x7F}},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.bytes, appendVarByteInt(nil, tc.value), "encoding %d", tc.value)
		assert.Equal(t, len(tc.bytes), varByteIntLen(tc.value), "varByteIntLen(%d)", tc.value)

		r := &reader{buf: tc.bytes}
		v, err := r.varByteInt()
		require.NoError(t, err, "decoding % X", tc.bytes)
		assert.Equal(t, tc.value, v, "decoding % X", tc.bytes)
	}
}

// A Variable Byte Integer "MUST use the minimum number of bytes necessary to
// represent the value" [MQTT-1.5.5-1], and is at most four bytes long.
func TestVarByteIntRejectsBadEncodings(t *testing.T) {
	cases := map[string][]byte{
		"non-minimal two-byte zero": {0x80, 0x00},
		"five bytes":                {0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
		"truncated continuation":    {0x80},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			r := &reader{buf: in}
			_, err := r.varByteInt()
			assert.ErrorIs(t, err, ErrMalformed)
		})
	}
}

// UTF-8 Encoded Strings must be well-formed and must not contain U+0000 or a
// surrogate code point (MQTT-5.0 §1.5.4, [MQTT-1.5.4-1], [MQTT-1.5.4-2]).
func TestStringValidation(t *testing.T) {
	for _, s := range []string{"", "a/b", "Accounts payable", "/finance", "héllo", "日本語"} {
		assert.NoError(t, validateUTF8(s), "%q should be a valid UTF-8 Encoded String", s)
	}

	invalid := map[string]string{
		"null character":   "a\x00b",
		"lone surrogate":   "\xed\xa0\x80",
		"invalid sequence": "\xff\xfe",
		"truncated 3-byte": "\xe2\x82",
	}
	for name, s := range invalid {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, validateUTF8(s), ErrMalformed)
		})
	}
}

func TestReaderRejectsTruncatedFields(t *testing.T) {
	t.Run("Two Byte Integer", func(t *testing.T) {
		r := &reader{buf: []byte{0x01}}
		_, err := r.uint16()
		assert.ErrorIs(t, err, ErrMalformed)
	})
	t.Run("Four Byte Integer", func(t *testing.T) {
		r := &reader{buf: []byte{0x01, 0x02, 0x03}}
		_, err := r.uint32()
		assert.ErrorIs(t, err, ErrMalformed)
	})
	t.Run("Binary Data length lies about the payload", func(t *testing.T) {
		r := &reader{buf: []byte{0x00, 0x10, 'a'}}
		_, err := r.binary()
		assert.ErrorIs(t, err, ErrMalformed)
	})
}
