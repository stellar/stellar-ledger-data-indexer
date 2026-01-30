package utils

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type metricRecorder struct {
	UpsertCountMetric           *prometheus.GaugeVec
	LatestLedgerSequenceMetric  *prometheus.GaugeVec
	LedgerInputStartMetric      *prometheus.GaugeVec
	LedgerInputEndMetric        *prometheus.GaugeVec
	LedgerActualStartMetric     *prometheus.GaugeVec
	LedgerActualEndMetric       *prometheus.GaugeVec
	LedgerBackfillEnabledMetric *prometheus.GaugeVec
	LatestNetworkLedgerMetric   *prometheus.GaugeVec
}

type MetricRecorder interface {
	RecordUpsertCount(table string, count int64)
	RecordLatestLedgerSequence(table string, sequence uint32)
	RecordLedgerBounds(table string, inputStart uint32, inputEnd uint32, actualStart uint32, actualEnd uint32, backfill bool)
	RecordLatestNetworkLedger(table string, sequence uint32)
}

func GetNewMetricRecorder(registry *prometheus.Registry, nameSpace string) MetricRecorder {
	var (
		upsertCountMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: nameSpace,
			Name:      "upsert_count",
		},
			[]string{"table_name"},
		)

		latestLedgerSequenceMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: nameSpace,
			Name:      "latest_ledger_sequence",
		},
			[]string{"table_name"},
		)

		ledgerInputStart = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_input_start",
				Help:      "Requested start ledger",
			},
			[]string{"table_name"},
		)

		ledgerInputEnd = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_input_end",
				Help:      "Requested end ledger",
			},
			[]string{"table_name"},
		)

		ledgerActualStart = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_actual_start",
				Help:      "Actual start ledger processed",
			},
			[]string{"table_name"},
		)

		ledgerActualEnd = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_actual_end",
				Help:      "Actual end ledger processed",
			},
			[]string{"table_name"},
		)

		ledgerBackfillEnabled = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "ledger_backfill_enabled",
				Help:      "Whether backfill mode is enabled",
			},
			[]string{"table_name"},
		)

		latestNetworkLedgerMetric = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: nameSpace,
				Name:      "latest_network_ledger",
			},
			[]string{"table_name"},
		)
	)

	registry.MustRegister(upsertCountMetric, latestLedgerSequenceMetric, ledgerInputStart, ledgerInputEnd, ledgerActualStart, ledgerActualEnd, ledgerBackfillEnabled, latestNetworkLedgerMetric)

	fmt.Println("Prometheus metric registered")
	return &metricRecorder{
		UpsertCountMetric:           upsertCountMetric,
		LatestLedgerSequenceMetric:  latestLedgerSequenceMetric,
		LedgerInputStartMetric:      ledgerInputStart,
		LedgerInputEndMetric:        ledgerInputEnd,
		LedgerActualStartMetric:     ledgerActualStart,
		LedgerActualEndMetric:       ledgerActualEnd,
		LedgerBackfillEnabledMetric: ledgerBackfillEnabled,
		LatestNetworkLedgerMetric:   latestNetworkLedgerMetric,
	}
}

func (metricRecorder *metricRecorder) RecordUpsertCount(table string, count int64) {
	metricRecorder.UpsertCountMetric.With(prometheus.Labels{"table_name": table}).Set(float64(count))
}

func (metricRecorder *metricRecorder) RecordLatestLedgerSequence(table string, sequence uint32) {
	metricRecorder.LatestLedgerSequenceMetric.With(prometheus.Labels{"table_name": table}).Set(float64(sequence))
}

func (metricRecorder *metricRecorder) RecordLedgerBounds(table string, inputStart uint32, inputEnd uint32, actualStart uint32, actualEnd uint32, backfill bool) {
	metricRecorder.LedgerInputStartMetric.With(prometheus.Labels{"table_name": table}).Set(float64(inputStart))
	metricRecorder.LedgerInputEndMetric.With(prometheus.Labels{"table_name": table}).Set(float64(inputEnd))
	metricRecorder.LedgerActualStartMetric.With(prometheus.Labels{"table_name": table}).Set(float64(actualStart))
	metricRecorder.LedgerActualEndMetric.With(prometheus.Labels{"table_name": table}).Set(float64(actualEnd))
	if backfill {
		metricRecorder.LedgerBackfillEnabledMetric.With(prometheus.Labels{"table_name": table}).Set(1)
	} else {
		metricRecorder.LedgerBackfillEnabledMetric.With(prometheus.Labels{"table_name": table}).Set(0)
	}
}

func (metricRecorder *metricRecorder) RecordLatestNetworkLedger(table string, sequence uint32) {
	metricRecorder.LatestNetworkLedgerMetric.With(prometheus.Labels{"table_name": table}).Set(float64(sequence))
}
