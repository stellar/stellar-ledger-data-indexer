package contract

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/hash"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ExtractEntryFromChange gets the most recent state of an entry from an ingestion change, as well as if the entry was deleted
func ExtractEntryFromChange(change ingest.Change) (xdr.LedgerEntry, xdr.LedgerEntryChangeType, bool, error) {
	switch changeType := change.ChangeType; changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated, xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return *change.Post, changeType, false, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return *change.Pre, changeType, true, nil
	default:
		return xdr.LedgerEntry{}, changeType, false, fmt.Errorf("unable to extract ledger entry type from change")
	}
}

// TimePointToUTCTimeStamp takes in an xdr TimePoint and converts it to a time.Time struct in UTC
func TimePointToUTCTimeStamp(providedTime xdr.TimePoint) (time.Time, error) {
	intTime := int64(providedTime)
	if intTime < 0 {
		return time.Now(), fmt.Errorf("the timepoint is negative")
	}
	return time.Unix(intTime, 0).UTC(), nil
}

// LedgerEntryToLedgerKeyHash converts a ledger entry to a hex-encoded hash of its ledger key
func LedgerEntryToLedgerKeyHash(ledgerEntry xdr.LedgerEntry) string {
	ledgerKey, _ := ledgerEntry.LedgerKey()
	ledgerKeyByte, _ := ledgerKey.MarshalBinary()
	hashedLedgerKeyByte := hash.Hash(ledgerKeyByte)
	ledgerKeyHash := hex.EncodeToString(hashedLedgerKeyByte[:])
	return ledgerKeyHash
}

// HashToHexString converts an xdr.Hash type to a hex string
func HashToHexString(inputHash xdr.Hash) string {
	sliceHash := inputHash[:]
	hexString := hex.EncodeToString(sliceHash)
	return hexString
}
