package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/docker/docker/client"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stellar/go-stellar-sdk/support/db/dbtest"
	"github.com/stellar/go/support/db"
	"github.com/stellar/stellar-ledger-data-indexer/internal"
	"github.com/stretchr/testify/suite"
)

type LedgerDataIndexerTestSuite struct {
	suite.Suite
	db                    *dbtest.DB
	tempConfigFile        string
	testTempDir           string
	ctx                   context.Context
	ctxStop               context.CancelFunc
	coreContainerID       string
	localStackContainerID string
	dockerCli             *client.Client
	gcsServer             *fakestorage.Server
	finishedSetup         bool
	config                internal.Config
	storageType           string // "GCS", "S3", or "Filesystem"
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

func (s *LedgerDataIndexerTestSuite) TestAppend() {
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
	}

	var actualTopRecords []ContractRow
	expectedTopRecords := []ContractRow{
		{ContractID: "CA6PUJLBYKZKUEKLZJMKBZLEKP2OTHANDEOWSFF44FTSYLKQPIICCJBE", LedgerSequence: 59561994},
		{ContractID: "CA6PUJLBYKZKUEKLZJMKBZLEKP2OTHANDEOWSFF44FTSYLKQPIICCJBE", LedgerSequence: 59561995},
	}
	require.NoError(sess.SelectRaw(context.Background(), &actualTopRecords, `SELECT contract_id, ledger_sequence FROM contract_data order by 1,2 limit 2;`))
	require.Equal(expectedTopRecords, actualTopRecords)

	var actualCount []int
	expectedCount := []int{981}
	require.NoError(sess.SelectRaw(context.Background(), &actualCount, `SELECT count(*) FROM contract_data;`))
	require.Equal(expectedCount, actualCount)

}
