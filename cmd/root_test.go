package cmd

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/db/dbtest"
	"github.com/stretchr/testify/suite"
)

type LedgerDataIndexerTestSuite struct {
	suite.Suite
	db             *dbtest.DB
	tempConfigFile string
	ctx            context.Context
}

func TestLedgerDataIndexerTestSuite(t *testing.T) {

	indexerTestSuite := &LedgerDataIndexerTestSuite{
		tempConfigFile: "../config-test.toml",
	}
	suite.Run(t, indexerTestSuite)
}

func (s *LedgerDataIndexerTestSuite) SetupSuite() {
	s.db = dbtest.Postgres(s.T())
	os.Setenv("POSTGRES_CONN_STRING", s.db.DSN)
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
	expectedCount := []int{964}
	require.NoError(sess.SelectRaw(context.Background(), &actualCount, `SELECT count(*) FROM contract_data;`))
	require.Equal(expectedCount, actualCount)

	var actualHistoricalRecords []ContractRow
	expectedHistoricalRecords := []ContractRow{
		{
			ContractID:     "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA",
			LedgerSequence: 59562000,
			KeyHash:        "72b51b3784ece8d155164e1cb15488931742566809555e3b46b8734ef6fbd453",
			Durability:     "persistent",
			KeySymbol:      "Balance",
			Key:            "AAAAEAAAAAEAAAACAAAADwAAAAdCYWxhbmNlAAAAABIAAAABl2X1uJV1ZMN1DTnAMGclVEeDnW6g/S1+07aaqea1gQo=",
			Val:            "AAAAEQAAAAEAAAADAAAADwAAAAZhbW91bnQAAAAAAAoAAAAAAAAAAAAABJfeaJgvAAAADwAAAAphdXRob3JpemVkAAAAAAAAAAAAAQAAAA8AAAAIY2xhd2JhY2sAAAAAAAAAAA==",
			ClosedAt:       "2025-10-26T17:15:36Z",
		},
	}
	require.NoError(sess.SelectRaw(context.Background(), &actualHistoricalRecords, `SELECT contract_id, ledger_sequence, key_hash, durability, key_symbol, key, val, closed_at FROM contract_data where key_hash = '72b51b3784ece8d155164e1cb15488931742566809555e3b46b8734ef6fbd453' order by 1,2,3 limit 10;`))
	require.Equal(expectedHistoricalRecords, actualHistoricalRecords)

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

func (s *LedgerDataIndexerTestSuite) TestContractDataResumeFromMaxLedger() {
	require := s.Require()

	// First ingestion: load ledgers 59561994 to 59561997
	rootCmd := DefineCommands()
	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)
	rootCmd.SetArgs([]string{"append", "--start", "59561994", "--end", "59561997", "--dataset", "contract_data", "--config-file", s.tempConfigFile})
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	sess := &db.Session{DB: s.db.Open()}
	defer sess.DB.Close()

	// Verify first ingestion count
	var firstCount []int
	require.NoError(sess.SelectRaw(context.Background(), &firstCount, `SELECT count(*) FROM contract_data;`))
	s.T().Logf("After first ingestion: %d records", firstCount[0])
	require.Greater(firstCount[0], 0, "Should have ingested data from first run")

	// Second ingestion: request from start ledger 2 (should be ignored) to 59562000
	// The system should automatically resume from maxLedger (59561997) instead of 2
	rootCmd2 := DefineCommands()
	var errWriter2 bytes.Buffer
	var outWriter2 bytes.Buffer
	rootCmd2.SetErr(&errWriter2)
	rootCmd2.SetOut(&outWriter2)
	rootCmd2.SetArgs([]string{"append", "--start", "2", "--end", "59562000", "--dataset", "contract_data", "--config-file", s.tempConfigFile})
	err = rootCmd2.ExecuteContext(s.ctx)
	require.NoError(err)

	output2 := outWriter2.String()
	errOutput2 := errWriter2.String()
	s.T().Log("Second run output:", output2)
	s.T().Log("Second run errors:", errOutput2)

	// Verify that more data was added (should have data from 59561997 to 59562000)
	var secondCount []int
	require.NoError(sess.SelectRaw(context.Background(), &secondCount, `SELECT count(*) FROM contract_data;`))
	s.T().Logf("After second ingestion: %d records", secondCount[0])
	require.Greater(secondCount[0], firstCount[0], "Should have ingested additional data")

	// Verify we have data from the full range
	var maxLedger []int
	require.NoError(sess.SelectRaw(context.Background(), &maxLedger, `SELECT MAX(ledger_sequence) FROM contract_data;`))
	require.Equal(59562000, maxLedger[0], "Should have data up to the end ledger")

	// Third ingestion: request the same range again (should do nothing as all data is present)
	rootCmd3 := DefineCommands()
	var errWriter3 bytes.Buffer
	var outWriter3 bytes.Buffer
	rootCmd3.SetErr(&errWriter3)
	rootCmd3.SetOut(&outWriter3)
	rootCmd3.SetArgs([]string{"append", "--start", "2", "--end", "59562000", "--dataset", "contract_data", "--config-file", s.tempConfigFile})
	err = rootCmd3.ExecuteContext(s.ctx)
	require.NoError(err)

	output3 := outWriter3.String()
	errOutput3 := errWriter3.String()
	s.T().Log("Third run output:", output3)
	s.T().Log("Third run errors:", errOutput3)

	// Verify count didn't change (all data was already present)
	var thirdCount []int
	require.NoError(sess.SelectRaw(context.Background(), &thirdCount, `SELECT count(*) FROM contract_data;`))
	require.Equal(secondCount[0], thirdCount[0], "Count should not change when all data is already present")
}
