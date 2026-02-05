package internal

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/go-errors/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stellar/go/support/datastore"
	supporthttp "github.com/stellar/go/support/http"
	"github.com/stellar/stellar-ledger-data-indexer/internal/db"
	"github.com/stellar/stellar-ledger-data-indexer/internal/input"
	"github.com/stellar/stellar-ledger-data-indexer/internal/transform"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

func PostgresConnString(cfg PostgresConfig) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Database,
	)
}

func getProcessor(dataset string, outboundAdapters []utils.OutboundAdapter, passPhrase string, metricRecorder utils.MetricRecorder) (processor utils.Processor, err error) {
	switch dataset {
	case "contract_data":
		processor := &transform.ContractDataProcessor{
			BaseProcessor: utils.BaseProcessor{
				OutboundAdapters: outboundAdapters,
				Logger:           Logger,
				Passphrase:       passPhrase,
				MetricRecorder:   metricRecorder,
			},
		}
		return processor, nil
	case "ttl":
		processor := &transform.TTLDataProcessor{
			BaseProcessor: utils.BaseProcessor{
				OutboundAdapters: outboundAdapters,
				Logger:           Logger,
				Passphrase:       passPhrase,
				MetricRecorder:   metricRecorder,
			},
		}
		return processor, nil
	default:
		return nil, fmt.Errorf("unsupported dataset: %s", dataset)
	}
}

func getPostgresSession(ctx context.Context, postgresConfig PostgresConfig) (*db.DBSession, error) {
	envPostgresConnString := os.Getenv("POSTGRES_CONN_STRING")
	var connString string
	if envPostgresConnString != "" {
		connString = envPostgresConnString
		Logger.Infof("Using Postgres connection string from environment variable")
	} else {
		connString = PostgresConnString(postgresConfig)
	}

	Logger.Infof("Opening Postgres session")
	session, err := db.NewPostgresSession(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres session: %w", err)
	}
	Logger.Infof("Opened postgres and applied migrations")
	return session, nil
}

func getPostgresOutputAdapter(session *db.DBSession, dataset string, metricRecorder utils.MetricRecorder) (*utils.PostgresAdapter, error) {
	var dbOperator utils.DBOperator
	switch dataset {
	case "contract_data":
		dbOperator = db.NewContractDataDBOperator(*session, metricRecorder)
	case "ttl":
		dbOperator = db.NewTTLDBOperator(*session, metricRecorder)
	default:
		return nil, fmt.Errorf("unsupported dataset: %s", dataset)
	}
	postgresAdapter := &utils.PostgresAdapter{DBOperator: dbOperator, Logger: Logger}
	return postgresAdapter, nil
}

func newAdminServer(adminPort int, prometheusRegistry *prometheus.Registry) *http.Server {
	mux := supporthttp.NewMux(Logger)
	mux.Handle("/metrics", promhttp.HandlerFor(prometheusRegistry, promhttp.HandlerOpts{}))
	adminAddr := fmt.Sprintf(":%d", adminPort)
	return &http.Server{
		Addr:        adminAddr,
		Handler:     mux,
		ReadTimeout: adminServerReadTimeout,
	}
}

func IndexData(config Config) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	registry := prometheus.NewRegistry()
	adminServer := newAdminServer(config.MetricsPort, registry)
	go func() {
		Logger.Infof("Starting admin server on port %v", config.MetricsPort)
		if err := adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			Logger.Errorf("Admin server failed: %v", err)
		}
	}()

	dataStore, err := datastore.NewGCSDataStore(ctx, config.DataStoreConfig)
	if err != nil {
		Logger.Fatal("failed to create GCS data store:", err)
		return
	}
	metricRecorder := utils.GetNewMetricRecorder(ctx, Logger, registry, nameSpace)

	var outboundAdapters []utils.PostgresAdapter
	var processors []utils.Processor
	// Order is important here, as contract data entries needs to be processed before ttl entries
	// ttl entries are enrichment to base contract data
	datasets := []string{"contract_data", "ttl"}
	session, err := getPostgresSession(ctx, config.PostgresConfig)
	if err != nil {
		Logger.Fatal(err)
		return
	}
	for _, dataset := range datasets {
		postgresAdapter, err := getPostgresOutputAdapter(session, dataset, metricRecorder)
		if err != nil {
			Logger.Fatal(err)
			return
		}
		processor, err := getProcessor(dataset, []utils.OutboundAdapter{postgresAdapter}, config.StellarCoreConfig.NetworkPassphrase, metricRecorder)
		if err != nil {
			Logger.Fatal(err)
			return
		}

		outboundAdapters = append(outboundAdapters, *postgresAdapter)
		processors = append(processors, processor)
	}
	metricRecorder.RegisterMaxLedgerSequenceInGalexieMetric(ctx, registry, nameSpace, dataStore)
	metricRecorder.RegisterMaxLedgerSequenceIndexedMetric(ctx, registry, nameSpace, outboundAdapters[0].DBOperator)

	// Ensure adapters are closed on all exit paths
	defer func() {
		for _, adapter := range outboundAdapters {
			adapter.Close()
		}
	}()

	// Query max ledger sequence from database if not in backfill mode
	var maxLedgerInDB uint32
	if config.Backfill {
		maxLedgerInDB = 0
		Logger.Infof("Backfill mode enabled: Using exact start=%d and end=%d ledgers as provided", config.StartLedger, config.EndLedger)
	} else {
		// Both outbound adapters write to the same database, so querying from the first is sufficient
		maxLedgerInDB, err = outboundAdapters[0].GetMaxLedgerSequence(ctx)
		if err != nil {
			Logger.Errorf("Failed to get max ledger sequence from database: %v. Proceeding with requested start ledger.", err)
			maxLedgerInDB = 0
		} else {
			Logger.Infof("Max ledger sequence in database: %d", maxLedgerInDB)
		}
	}
	maxLedgerInGalexie, err := datastore.FindLatestLedgerSequence(ctx, dataStore)
	if err != nil {
		Logger.Fatal("failed to fetch latest ledger sequence from Galexie:", err)
		return
	}

	reader, err := input.NewLedgerMetadataReader(
		&config.DataStoreConfig,
		processors,
		config.StartLedger,
		config.EndLedger,
		config.Backfill,
		maxLedgerInDB,
		maxLedgerInGalexie,
		metricRecorder,
	)
	if err != nil {
		Logger.Fatal(err)
		return
	}

	err = reader.Run(ctx, Logger)

	if adminServer != nil {
		serverShutdownCtx, serverShutdownCancel := context.WithTimeout(context.Background(), adminServerShutdownTimeout)
		defer serverShutdownCancel()
		Logger.Info("shutting down admin server")
		if err := adminServer.Shutdown(serverShutdownCtx); err != nil {
			Logger.WithError(err).Warn("error in internalServer.Shutdown")
		}
	}

	if err != nil {
		Logger.Fatal("ingestion pipeline failed:", err)
	} else {
		Logger.Info("ingestion pipeline ended successfully")
	}
}
