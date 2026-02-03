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
		ContractID              string `db:"contract_id"`
		LedgerSequence          int64  `db:"ledger_sequence"`
		KeyHash                 string `db:"key_hash"`
		Durability              string `db:"durability"`
		KeySymbol               string `db:"key_symbol"`
		Key                     string `db:"key"`
		Val                     string `db:"val"`
		ClosedAt                string `db:"closed_at"`
		LiveUntilLedgerSequence int64  `db:"live_until_ledger_sequence"`
	}

	var actualTopRecords []ContractRow
	expectedTopRecords := []ContractRow{
		{
			ContractID:              "CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD",
			LedgerSequence:          59561994,
			KeyHash:                 "36fe0c8c9820c31c664007bc0e8b0b9dda0792d680cf2e2c5c662c2ca00b9b8e",
			Durability:              "persistent",
			KeySymbol:               "Positions",
			Key:                     "AAAAEAAAAAEAAAACAAAADwAAAAlQb3NpdGlvbnMAAAAAAAASAAAAAAAAAAAwzvi1/ZEwKCNnHK/ga7sh+ZbL98pt+bfr4jULxnvmHA==",
			Val:                     "AAAAEQAAAAEAAAADAAAADwAAAApjb2xsYXRlcmFsAAAAAAARAAAAAQAAAAEAAAADAAAAAQAAAAoAAAAAAAAAAAAAACz2APNwAAAADwAAAAtsaWFiaWxpdGllcwAAAAARAAAAAQAAAAAAAAAPAAAABnN1cHBseQAAAAAAEQAAAAEAAAAA",
			ClosedAt:                "2025-10-26T17:15:02Z",
			LiveUntilLedgerSequence: 0,
		},
		{
			ContractID:              "CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD",
			LedgerSequence:          59561994,
			KeyHash:                 "ab35299631fa4e2c9dedf9eb4143e3470234a2e27dcf2cf440e9e95930558a13",
			Durability:              "persistent",
			KeySymbol:               "Positions",
			Key:                     "AAAAEAAAAAEAAAACAAAADwAAAAlQb3NpdGlvbnMAAAAAAAASAAAAAWisTBR8X2z/wCNGgwgYLotCqrv+MkfhMsIEwLwvSF6l",
			Val:                     "AAAAEQAAAAEAAAADAAAADwAAAApjb2xsYXRlcmFsAAAAAAARAAAAAQAAAAAAAAAPAAAAC2xpYWJpbGl0aWVzAAAAABEAAAABAAAAAAAAAA8AAAAGc3VwcGx5AAAAAAARAAAAAQAAAAEAAAADAAAAAQAAAAoAAAAAAAAAAAAAI/sF8TEG",
			ClosedAt:                "2025-10-26T17:15:02Z",
			LiveUntilLedgerSequence: 0,
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
			ContractID:              "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA",
			LedgerSequence:          59562000,
			KeyHash:                 "72b51b3784ece8d155164e1cb15488931742566809555e3b46b8734ef6fbd453",
			Durability:              "persistent",
			KeySymbol:               "Balance",
			Key:                     "AAAAEAAAAAEAAAACAAAADwAAAAdCYWxhbmNlAAAAABIAAAABl2X1uJV1ZMN1DTnAMGclVEeDnW6g/S1+07aaqea1gQo=",
			Val:                     "AAAAEQAAAAEAAAADAAAADwAAAAZhbW91bnQAAAAAAAoAAAAAAAAAAAAABJfeaJgvAAAADwAAAAphdXRob3JpemVkAAAAAAAAAAAAAQAAAA8AAAAIY2xhd2JhY2sAAAAAAAAAAA==",
			ClosedAt:                "2025-10-26T17:15:36Z",
			LiveUntilLedgerSequence: 0,
		},
	}
	require.NoError(sess.SelectRaw(context.Background(), &actualHistoricalRecords, `SELECT contract_id, ledger_sequence, key_hash, durability, key_symbol, key, val, closed_at FROM contract_data where key_hash = '72b51b3784ece8d155164e1cb15488931742566809555e3b46b8734ef6fbd453' order by 1,2,3 limit 10;`))
	require.Equal(expectedHistoricalRecords, actualHistoricalRecords)

	var actualRecordsWithTTL []ContractRow
	expectedRecordsWithTTL := []ContractRow{
		{
			ContractID:              "CAFJZQWSED6YAWZU3GWRTOCNPPCGBN32L7QV43XX5LZLFTK6JLN34DLN",
			LedgerSequence:          59561998,
			KeyHash:                 "083c5803f7556890f01084c5b339b331c6a484b482f128c912f5df78df60b00e",
			Durability:              "temporary",
			KeySymbol:               "",
			Key:                     "AAAACQAAAZohhCYgAAAAAAAAAAo=",
			Val:                     "AAAACgAAAAAAAAAAAAZ2mEKM0VI=",
			ClosedAt:                "2025-10-26T17:15:24Z",
			LiveUntilLedgerSequence: 61635599,
		},
		{
			ContractID:              "CAFJZQWSED6YAWZU3GWRTOCNPPCGBN32L7QV43XX5LZLFTK6JLN34DLN",
			LedgerSequence:          59561998,
			KeyHash:                 "0da09bff57620014be2b220c48ab6b880038b2cae4af7d4c00cf5b6bed4741f8",
			Durability:              "temporary",
			KeySymbol:               "",
			Key:                     "AAAACQAAAZohhCYgAAAAAAAAAAA=",
			Val:                     "AAAACgAAAAAAAAAAnc5VUZBdVuI=",
			ClosedAt:                "2025-10-26T17:15:24Z",
			LiveUntilLedgerSequence: 61635599,
		},
	}
	require.NoError(sess.SelectRaw(context.Background(), &actualRecordsWithTTL, `SELECT contract_id, ledger_sequence, key_hash, durability, key_symbol, key, val, closed_at, live_until_ledger_sequence FROM contract_data where live_until_ledger_sequence is not null order by 1,2,3 limit 2;`))
	require.Equal(expectedRecordsWithTTL, actualRecordsWithTTL)

}
