// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/Metrics.h"

#include <chrono>
#include <memory>
#include <string>

#include "opentelemetry/exporters/prometheus/collector.h"
#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/sdk/metrics/aggregation/histogram_aggregation.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"
#include "opentelemetry/sdk/metrics/view/instrument_selector_factory.h"
#include "opentelemetry/sdk/metrics/view/meter_selector_factory.h"
#include "opentelemetry/sdk/metrics/view/view_factory.h"
#include "prometheus/text_serializer.h"

namespace metrics_api = opentelemetry::metrics;
namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace metrics_exp = opentelemetry::exporter::metrics;

namespace ad_utility::metrics {
namespace {

// Pull-based MetricReader that serializes metrics to Prometheus text format
// on demand. Mirrors PrometheusExporter but without the CivetWeb HTTP server.
class PullMetricReader : public metrics_sdk::MetricReader,
                         public MetricsReader {
 public:
  PullMetricReader()
      : collector_(std::make_shared<metrics_exp::PrometheusCollector>(
            this, /*populate_target_info=*/false,
            /*without_otel_scope=*/true)) {}

  std::string getMetricsText() const override {
    prometheus::TextSerializer serializer;
    return serializer.Serialize(collector_->Collect());
  }

  metrics_sdk::AggregationTemporality GetAggregationTemporality(
      metrics_sdk::InstrumentType) const noexcept override {
    return metrics_sdk::AggregationTemporality::kCumulative;
  }

 private:
  bool OnForceFlush(std::chrono::microseconds) noexcept override {
    return true;
  }
  bool OnShutDown(std::chrono::microseconds) noexcept override { return true; }

  std::shared_ptr<metrics_exp::PrometheusCollector> collector_;
};

}  // namespace

std::shared_ptr<MetricsReader> initialize(bool enabled) {
  if (!enabled) {
    return nullptr;
  }

  // Pull reader — metrics served via /metrics on the main server port.
  auto pullReader = std::make_shared<PullMetricReader>();

  auto provider = metrics_sdk::MeterProviderFactory::Create();

  // Custom buckets covering 1 ms – 5 min, suited to SPARQL query latencies.
  auto histogramConfig =
      std::make_shared<metrics_sdk::HistogramAggregationConfig>();
  histogramConfig->boundaries_ = {1,     5,     10,     50,     100,    500,
                                  1'000, 5'000, 30'000, 60'000, 300'000};
  provider->AddView(
      metrics_sdk::InstrumentSelectorFactory::Create(
          metrics_sdk::InstrumentType::kHistogram,
          "qlever.sparql_operation.duration", "ms"),
      metrics_sdk::MeterSelectorFactory::Create("qlever", "", ""),
      metrics_sdk::ViewFactory::Create("qlever.sparql_operation.duration", "",
                                       metrics_sdk::AggregationType::kHistogram,
                                       histogramConfig));

  provider->AddMetricReader(pullReader);

  metrics_api::Provider::SetMeterProvider(
      std::shared_ptr<metrics_api::MeterProvider>(std::move(provider)));

  return pullReader;
}

ActiveCounterGuard::ActiveCounterGuard(
    metrics_api::UpDownCounter<int64_t>& counter, std::string operation)
    : counter_(counter), operation_(std::move(operation)) {
  counter_.Add(1, {{"operation", operation_}});
}

ActiveCounterGuard::~ActiveCounterGuard() {
  counter_.Add(-1, {{"operation", operation_}});
}

}  // namespace ad_utility::metrics
