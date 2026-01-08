package db

import (
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
