package utils

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
)

type metricRecorder struct {
	UpsertCountMetric                *prometheus.CounterVec
	MaxLedgerSequenceIndexedMetric   *prometheus.GaugeVec
	ProcessingLedgerSequenceMetric   *prometheus.GaugeVec
	MaxLedgerSequenceInGalexieMetric *prometheus.GaugeVec
	LedgerRangeStartMetric           *prometheus.GaugeVec
	LedgerRangeEndMetric             *prometheus.GaugeVec
	Logger                           *log.Entry
	Registry                         *prometheus.Registry
}

type MetricRecorder interface {
	RecordUpsertCount(table string, count int64)
	RecordProcessingLedgerSequence(sequence uint32)
	RecordLedgerRangeStart(inputStartLedger uint32, inputEndLedger uint32, inputBackfill bool, maxLedgerInGalexie uint32, maxLedgerInIndexer uint32, actualStartLedger uint32)
	RecordLedgerRangeEnd(inputStartLedger uint32, inputEndLedger uint32, inputBackfill bool, maxLedgerInGalexie uint32, maxLedgerInIndexer uint32, actualEndLedger uint32)
	RegisterMaxLedgerSequenceIndexedMetric(ctx context.Context, registry *prometheus.Registry, nameSpace string, dbOperator DBOperator)
	RegisterMaxLedgerSequenceInGalexieMetric(ctx context.Context, registry *prometheus.Registry, nameSpace string, dataStore datastore.DataStore)
}

func GetNewMetricRecorder(ctx context.Context, logger *log.Entry, registry *prometheus.Registry, nameSpace string) MetricRecorder {
	var (
		upsertCountMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: nameSpace,
			Name:      "upsert_count",
			Help:      "Number of rows upserted into each table",
		},
			[]string{"table_name"},
		)

		processingLedgerSequenceMetric = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_sequence_processing",
				Help:      "The ledger sequence currently being processed",
			},
			[]string{},
		)
		ledgerRangeStartMetric = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_range_start",
				Help:      "Starting range of ledgers being processed",
			},
			[]string{"start_ledger", "end_ledger", "backfill_mode", "max_ledger_in_galexie", "max_ledger_in_indexer"},
		)
		ledgerRangeEndMetric = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_range_end",
				Help:      "Ending range of ledgers being processed",
			},
			[]string{"start_ledger", "end_ledger", "backfill_mode", "max_ledger_in_galexie", "max_ledger_in_indexer"},
		)
	)

	registry.MustRegister(upsertCountMetric, processingLedgerSequenceMetric, ledgerRangeStartMetric, ledgerRangeEndMetric)

	logger.Info("Prometheus metrics initialized")
	return &metricRecorder{
		UpsertCountMetric:              upsertCountMetric,
		ProcessingLedgerSequenceMetric: processingLedgerSequenceMetric,
		LedgerRangeStartMetric:         ledgerRangeStartMetric,
		LedgerRangeEndMetric:           ledgerRangeEndMetric,
		Logger:                         logger,
		Registry:                       registry,
	}
}

func (metricRecorder *metricRecorder) RecordUpsertCount(table string, count int64) {
	metricRecorder.UpsertCountMetric.With(prometheus.Labels{"table_name": table}).Add(float64(count))
}

func (metricRecorder *metricRecorder) RecordProcessingLedgerSequence(sequence uint32) {
	metricRecorder.ProcessingLedgerSequenceMetric.With(prometheus.Labels{}).Set(float64(sequence))
}

func (metricRecorder *metricRecorder) RecordLedgerRangeStart(inputStartLedger uint32, inputEndLedger uint32, inputBackfill bool, maxLedgerInGalexie uint32, maxLedgerInIndexer uint32, actualStartLedger uint32) {
	backfillMode := "false"
	if inputBackfill {
		backfillMode = "true"
	}
	metricRecorder.LedgerRangeStartMetric.With(prometheus.Labels{
		"start_ledger":          fmt.Sprintf("%d", inputStartLedger),
		"end_ledger":            fmt.Sprintf("%d", inputEndLedger),
		"backfill_mode":         backfillMode,
		"max_ledger_in_galexie": fmt.Sprintf("%d", maxLedgerInGalexie),
		"max_ledger_in_indexer": fmt.Sprintf("%d", maxLedgerInIndexer),
	}).Set(float64(actualStartLedger))
}

func (metricRecorder *metricRecorder) RecordLedgerRangeEnd(inputStartLedger uint32, inputEndLedger uint32, inputBackfill bool, maxLedgerInGalexie uint32, maxLedgerInIndexer uint32, actualEndLedger uint32) {
	backfillMode := "false"
	if inputBackfill {
		backfillMode = "true"
	}
	metricRecorder.LedgerRangeEndMetric.With(prometheus.Labels{
		"start_ledger":          fmt.Sprintf("%d", inputStartLedger),
		"end_ledger":            fmt.Sprintf("%d", inputEndLedger),
		"backfill_mode":         backfillMode,
		"max_ledger_in_galexie": fmt.Sprintf("%d", maxLedgerInGalexie),
		"max_ledger_in_indexer": fmt.Sprintf("%d", maxLedgerInIndexer),
	}).Set(float64(actualEndLedger))
}

func (metricRecorder *metricRecorder) RegisterMaxLedgerSequenceIndexedMetric(ctx context.Context, registry *prometheus.Registry, nameSpace string, dbOperator DBOperator) {
	maxLedgerSequenceIndexedMetric := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: nameSpace,
		Name:      "max_ledger_sequence_indexed",
		Help:      "The latest ledger sequence indexed in the database",
	},
		func() float64 {
			latestIndexedLedger, err := dbOperator.GetMaxLedgerSequence(ctx)
			if err != nil {
				metricRecorder.Logger.Errorf("Error fetching max ledger sequence indexed: %v", err)
				return 0
			}
			return float64(latestIndexedLedger)
		},
	)
	registry.MustRegister(maxLedgerSequenceIndexedMetric)
}

func (metricRecorder *metricRecorder) RegisterMaxLedgerSequenceInGalexieMetric(ctx context.Context, registry *prometheus.Registry, nameSpace string, dataStore datastore.DataStore) {
	maxLedgerSequenceInGalexieMetric := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: nameSpace,
			Name:      "max_ledger_sequence_in_galexie",
			Help:      "The latest ledger sequence available in Galexie data store(upstream data)",
		},
		func() float64 {
			latestNetworkLedger, err := datastore.FindLatestLedgerSequence(ctx, dataStore)
			if err != nil {
				metricRecorder.Logger.Errorf("Error fetching latest network ledger sequence: %v", err)
				return 0
			}
			return float64(latestNetworkLedger)
		},
	)
	registry.MustRegister(maxLedgerSequenceInGalexieMetric)
}
