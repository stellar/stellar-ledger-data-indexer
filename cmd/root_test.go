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
	rootCmd.SetArgs([]string{"append", "--start", "395", "--end", "400", "--dataset", "contract_data", "--config-file", s.tempConfigFile})
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	output := outWriter.String()
	errOutput := errWriter.String()
	s.T().Log(output)
	s.T().Log(errOutput)
}
