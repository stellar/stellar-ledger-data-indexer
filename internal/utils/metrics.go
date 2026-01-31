package utils

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/support/datastore"
)

type metricRecorder struct {
	UpsertCountMetric                *prometheus.GaugeVec
	MaxLedgerSequenceIndexedMetric   *prometheus.GaugeVec
	ProcessingLedgerSequenceMetric   *prometheus.GaugeVec
	MaxLedgerSequenceInGalexieMetric *prometheus.GaugeVec
}

type MetricRecorder interface {
	RecordUpsertCount(table string, count int64)
	RecordProcessingLedgerSequence(sequence uint32)
	RegisterMaxLedgerSequenceIndexedMetric(ctx context.Context, registry *prometheus.Registry, nameSpace string, dbOperator DBOperator)
	RegisterMaxLedgerSequenceInGalexieMetric(ctx context.Context, registry *prometheus.Registry, nameSpace string, dataStore datastore.DataStore)
}

func GetNewMetricRecorder(ctx context.Context, registry *prometheus.Registry, nameSpace string) MetricRecorder {
	var (
		upsertCountMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: nameSpace,
			Name:      "upsert_count",
		},
			[]string{"table_name"},
		)

		processingLedgerSequenceMetric = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_sequence_processing",
			},
			[]string{},
		)
	)

	registry.MustRegister(upsertCountMetric, processingLedgerSequenceMetric)

	fmt.Println("Prometheus metric registered")
	return &metricRecorder{
		UpsertCountMetric:              upsertCountMetric,
		ProcessingLedgerSequenceMetric: processingLedgerSequenceMetric,
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
	},
		func() float64 {
			latestIndexedLedger, err := dbOperator.GetMaxLedgerSequence(ctx)
			if err != nil {
				fmt.Printf("Error fetching max ledger sequence indexed: %v\n", err)
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
		},
		func() float64 {
			latestNetworkLedger, err := datastore.FindLatestLedgerSequence(ctx, dataStore)
			if err != nil {
				fmt.Printf("Error fetching latest network ledger sequence: %v\n", err)
				return 0
			}
			return float64(latestNetworkLedger)
		},
	)
	registry.MustRegister(maxLedgerSequenceInGalexieMetric)
}
