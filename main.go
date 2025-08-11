package main

import (
	"os"

	ledgerDataIndexer "github.com/stellar/stellar-ledger-data-indexer/cmd"
	"github.com/stellar/stellar-ledger-data-indexer/internal"
)

func main() {
	err := ledgerDataIndexer.Execute()
	if err != nil {
		internal.Logger.Errorf("Error executing stellar-ledger-data-indexer: %v", err)
		os.Exit(1)
	}
}
