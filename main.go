package main

import (
	"fmt"
	"os"

	ledgerDataIndexer "github.com/stellar/stellar-ledger-data-indexer/cmd"
)

func main() {
	err := ledgerDataIndexer.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
