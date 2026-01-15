package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/support/db/dbtest"
	"github.com/stellar/go/support/db"
	"github.com/stretchr/testify/suite"
)

type LedgerDataIndexerTestSuite struct {
	suite.Suite
	db             *dbtest.DB
	tempConfigFile string
	ctx            context.Context
}

func TestLedgerDataIndexerTestSuite(t *testing.T) {

	galexieGCSSuite := &LedgerDataIndexerTestSuite{
		tempConfigFile: "../config-test.toml",
	}
	suite.Run(t, galexieGCSSuite)
}

func (s *LedgerDataIndexerTestSuite) SetupSuite() {
	s.db = dbtest.Postgres(s.T())
	os.Setenv("POSTGRES_CONN_STRING", s.db.DSN)
	fmt.Println(s.db.DSN)
}

func (s *LedgerDataIndexerTestSuite) TearDownSuite() {
	s.db.Close()
}

func (s *LedgerDataIndexerTestSuite) TestContractDataAppend() {
	require := s.Require()

	rootCmd := DefineCommands()
	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)
	rootCmd.SetArgs([]string{"append", "--start", "59561994", "--end", "59562000", "--dataset", "contract_data", "--config-file", s.tempConfigFile})
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	output := outWriter.String()
	errOutput := errWriter.String()
	s.T().Log(output)
	s.T().Log(errOutput)

	sess := &db.Session{DB: s.db.Open()}
	defer sess.DB.Close()

	type ContractRow struct {
		ContractID     string `db:"contract_id"`
		LedgerSequence int64  `db:"ledger_sequence"`
		KeyHash        string `db:"key_hash"`
		Durability     string `db:"durability"`
		KeySymbol      string `db:"key_symbol"`
		Key            string `db:"key"`
		Val            string `db:"val"`
		ClosedAt       string `db:"closed_at"`
	}

	var actualTopRecords []ContractRow
	expectedTopRecords := []ContractRow{
		{
			ContractID:     "CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD",
			LedgerSequence: 59561994,
			KeyHash:        "36fe0c8c9820c31c664007bc0e8b0b9dda0792d680cf2e2c5c662c2ca00b9b8e",
			Durability:     "persistent",
			KeySymbol:      "Positions",
			Key:            "AAAAEAAAAAEAAAACAAAADwAAAAlQb3NpdGlvbnMAAAAAAAASAAAAAAAAAAAwzvi1/ZEwKCNnHK/ga7sh+ZbL98pt+bfr4jULxnvmHA==",
			Val:            "AAAAEQAAAAEAAAADAAAADwAAAApjb2xsYXRlcmFsAAAAAAARAAAAAQAAAAEAAAADAAAAAQAAAAoAAAAAAAAAAAAAACz2APNwAAAADwAAAAtsaWFiaWxpdGllcwAAAAARAAAAAQAAAAAAAAAPAAAABnN1cHBseQAAAAAAEQAAAAEAAAAA",
			ClosedAt:       "2025-10-26T17:15:02Z",
		},
		{
			ContractID:     "CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD",
			LedgerSequence: 59561994,
			KeyHash:        "ab35299631fa4e2c9dedf9eb4143e3470234a2e27dcf2cf440e9e95930558a13",
			Durability:     "persistent",
			KeySymbol:      "Positions",
			Key:            "AAAAEAAAAAEAAAACAAAADwAAAAlQb3NpdGlvbnMAAAAAAAASAAAAAWisTBR8X2z/wCNGgwgYLotCqrv+MkfhMsIEwLwvSF6l",
			Val:            "AAAAEQAAAAEAAAADAAAADwAAAApjb2xsYXRlcmFsAAAAAAARAAAAAQAAAAAAAAAPAAAAC2xpYWJpbGl0aWVzAAAAABEAAAABAAAAAAAAAA8AAAAGc3VwcGx5AAAAAAARAAAAAQAAAAEAAAADAAAAAQAAAAoAAAAAAAAAAAAAI/sF8TEG",
			ClosedAt:       "2025-10-26T17:15:02Z",
		},
	}
	require.NoError(sess.SelectRaw(context.Background(), &actualTopRecords, `SELECT contract_id, ledger_sequence, key_hash, durability, key_symbol, key, val, closed_at FROM contract_data where key_symbol != '' order by 1,2,3 limit 2;`))
	require.Equal(expectedTopRecords, actualTopRecords)

	var actualCount []int
	expectedCount := []int{981}
	require.NoError(sess.SelectRaw(context.Background(), &actualCount, `SELECT count(*) FROM contract_data;`))
	require.Equal(expectedCount, actualCount)

}

func (s *LedgerDataIndexerTestSuite) TestTTLDataAppend() {
	require := s.Require()

	rootCmd := DefineCommands()
	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)
	rootCmd.SetArgs([]string{"append", "--start", "59561994", "--end", "59562000", "--dataset", "ttl", "--config-file", s.tempConfigFile})
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	output := outWriter.String()
	errOutput := errWriter.String()
	s.T().Log(output)
	s.T().Log(errOutput)

	sess := &db.Session{DB: s.db.Open()}
	defer sess.DB.Close()

	type TTLRow struct {
		KeyHash                 string `db:"key_hash"`
		LiveUntilLedgerSequence int64  `db:"live_until_ledger_sequence"`
		LedgerSequence          int64  `db:"ledger_sequence"`
		ClosedAt                string `db:"closed_at"`
	}

	var actualTopRecords []TTLRow
	expectedTopRecords := []TTLRow{
		{
			KeyHash:                 "007264b48e74928a0091f24ee5b5bb375ac8d23f15199c19602dc6c2a574ad0e",
			LiveUntilLedgerSequence: 59578847,
			LedgerSequence:          59561999,
			ClosedAt:                "2025-10-26T17:15:30Z",
		},
		{
			KeyHash:                 "00c8aac02920eff07b3635e7022236ce3f2f200bcc15449de671e56ecaaac95a",
			LiveUntilLedgerSequence: 59578575,
			LedgerSequence:          59561994,
			ClosedAt:                "2025-10-26T17:15:02Z",
		},
	}
	require.NoError(sess.SelectRaw(context.Background(), &actualTopRecords, `SELECT key_hash, live_until_ledger_sequence, ledger_sequence, closed_at FROM ttl order by 1,2,3 limit 2;`))
	require.Equal(expectedTopRecords, actualTopRecords)

	var actualCount []int
	expectedCount := []int{358}
	require.NoError(sess.SelectRaw(context.Background(), &actualCount, `SELECT count(*) FROM ttl;`))
	require.Equal(expectedCount, actualCount)

}
