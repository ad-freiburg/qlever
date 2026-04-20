// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/Metrics.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include "opentelemetry/exporters/ostream/metric_exporter.h"
#include "opentelemetry/exporters/prometheus/exporter_factory.h"
#include "opentelemetry/exporters/prometheus/exporter_options.h"
#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"

namespace metrics_api = opentelemetry::metrics;
namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace metrics_exp = opentelemetry::exporter::metrics;

namespace ad_utility::metrics {

void initialize(bool enabled, uint16_t port) {
  if (!enabled) {
    return;
  }

  // OStream reader — periodic snapshots to stdout for debugging.
  metrics_sdk::PeriodicExportingMetricReaderOptions ostream_options;
  ostream_options.export_interval_millis = std::chrono::milliseconds(5000);
  ostream_options.export_timeout_millis = std::chrono::milliseconds(1000);
  auto ostream_reader =
      std::make_unique<metrics_sdk::PeriodicExportingMetricReader>(
          std::make_unique<metrics_exp::OStreamMetricExporter>(std::cout),
          ostream_options);

  // Prometheus reader — HTTP /metrics endpoint on the configured port.
  metrics_exp::PrometheusExporterOptions prometheus_options;
  prometheus_options.url = "localhost:" + std::to_string(port);
  auto prometheus_reader =
      metrics_exp::PrometheusExporterFactory::Create(prometheus_options);

  auto provider = metrics_sdk::MeterProviderFactory::Create();
  provider->AddMetricReader(std::move(ostream_reader));
  provider->AddMetricReader(std::move(prometheus_reader));

  metrics_api::Provider::SetMeterProvider(
      std::shared_ptr<metrics_api::MeterProvider>(std::move(provider)));
}

ActiveCounterGuard::ActiveCounterGuard(
    metrics_api::UpDownCounter<int64_t>& counter)
    : counter_(&counter) {
  counter_->Add(1);
}

ActiveCounterGuard::~ActiveCounterGuard() { counter_->Add(-1); }

}  // namespace ad_utility::metrics
