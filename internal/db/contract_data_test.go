package db

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractSymbol(t *testing.T) {
	keyDecoded := map[string]string{
		"type":  "Vec",
		"value": "[XLM] some other data",
	}
	symbol := ExtractSymbol(keyDecoded)
	expected := "XLM"
	assert.Equal(t, expected, symbol, "expected %s, got %s", expected, symbol)
}

func TestExtractSymbol_NonVecKeysHaveNoSymbol(t *testing.T) {
	for _, scType := range []string{"String", "Symbol", "U32", "Address", "Map", ""} {
		symbol := ExtractSymbol(map[string]string{"type": scType, "value": "Balance"})
		assert.Empty(t, symbol, "type %q should not yield a symbol", scType)
	}
}

func TestExtractSymbol_EmptyAndNilInputs(t *testing.T) {
	assert.Empty(t, ExtractSymbol(nil))
	assert.Empty(t, ExtractSymbol(map[string]string{}))
	assert.Empty(t, ExtractSymbol(map[string]string{"type": "Vec", "value": ""}))
	assert.Empty(t, ExtractSymbol(map[string]string{"type": "Vec", "value": "   "}))
}

// TestExtractSymbol_RejectsUnrepresentableKeys is the regression test for the
// reported crash loop. A contract-data key is an arbitrary ScVal, and SCString
// carries unvalidated bytes, so a key may contain 0x00 -- which PostgreSQL
// cannot store in a text column. ExtractSymbol must never hand such a value to
// the upsert.
func TestExtractSymbol_RejectsUnrepresentableKeys(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"NUL inside the leading element", "[bal\x00ance]"},
		{"NUL as the entire element", "[\x00]"},
		{"NUL plus trailing elements", "[A\x00B GCEXAMPLE]"},
		{"other control byte", "[bal\x01ance]"},
		{"replacement rune from invalid UTF-8", "[bal�ance]"},
		{"non-ASCII", "[bålance]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			symbol := ExtractSymbol(map[string]string{"type": "Vec", "value": tt.value})
			assert.Empty(t, symbol)
			assert.NotContains(t, symbol, "\x00", "a NUL byte must never reach the key_symbol text column")
		})
	}
}

func TestSanitizeKeySymbol(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"typical SAC discriminant", "Balance", "Balance"},
		{"snake case symbol", "asset_code", "asset_code"},
		{"digits allowed", "AlphaNum12", "AlphaNum12"},
		{"leading underscore allowed", "_internal", "_internal"},
		{"empty stays empty", "", ""},
		{"at the 32 byte Symbol limit", strings.Repeat("a", 32), strings.Repeat("a", 32)},
		{"over the 32 byte Symbol limit", strings.Repeat("a", 33), ""},
		{"NUL rejected", "A\x00B", ""},
		{"tab rejected", "A\tB", ""},
		{"newline rejected", "A\nB", ""},
		{"hyphen rejected", "asset-code", ""},
		{"dot rejected", "asset.code", ""},
		{"space rejected", "asset code", ""},
		{"non-ASCII rejected", "bålance", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sanitizeKeySymbol(tt.input))
		})
	}
}
