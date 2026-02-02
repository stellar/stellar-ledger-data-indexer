package utils

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
)

type metricRecorder struct {
	UpsertCountMetric                *prometheus.GaugeVec
	MaxLedgerSequenceIndexedMetric   *prometheus.GaugeVec
	ProcessingLedgerSequenceMetric   *prometheus.GaugeVec
	MaxLedgerSequenceInGalexieMetric *prometheus.GaugeVec
	Logger                           *log.Entry
	Registry                         *prometheus.Registry
}

type MetricRecorder interface {
	RecordUpsertCount(table string, count int64)
	RecordProcessingLedgerSequence(sequence uint32)
	RegisterMaxLedgerSequenceIndexedMetric(ctx context.Context, registry *prometheus.Registry, nameSpace string, dbOperator DBOperator)
	RegisterMaxLedgerSequenceInGalexieMetric(ctx context.Context, registry *prometheus.Registry, nameSpace string, dataStore datastore.DataStore)
}

func GetNewMetricRecorder(ctx context.Context, logger *log.Entry, registry *prometheus.Registry, nameSpace string) MetricRecorder {
	var (
		upsertCountMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
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
	)

	registry.MustRegister(upsertCountMetric, processingLedgerSequenceMetric)

	logger.Info("Prometheus metrics initialized")
	return &metricRecorder{
		UpsertCountMetric:              upsertCountMetric,
		ProcessingLedgerSequenceMetric: processingLedgerSequenceMetric,
		Logger:                         logger,
		Registry:                       registry,
	}
}

func (metricRecorder *metricRecorder) RecordUpsertCount(table string, count int64) {
	metricRecorder.UpsertCountMetric.With(prometheus.Labels{"table_name": table}).Set(float64(count))
}

func (metricRecorder *metricRecorder) RecordProcessingLedgerSequence(sequence uint32) {
	metricRecorder.ProcessingLedgerSequenceMetric.With(prometheus.Labels{}).Set(float64(sequence))
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
