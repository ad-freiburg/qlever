// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_METRICS_H
#define QLEVER_SRC_UTIL_METRICS_H

#include <memory>
#include <string>

#include "opentelemetry/metrics/sync_instruments.h"

namespace ad_utility::metrics {

// Abstract interface for pulling a Prometheus text exposition from the
// registered metrics. Implemented by PullMetricReader in Metrics.cpp.
class MetricsReader {
 public:
  virtual ~MetricsReader() = default;
  virtual std::string getMetricsText() const = 0;
};

// Initialize the global OTEL MeterProvider. When `enabled` is false this
// function is a no-op and the provider remains the built-in no-op provider,
// so all subsequent Add/Record calls are safe but silently discarded.
// When `enabled` is true, periodic OStream snapshots are written to stdout
// and a MetricsReader is returned that exposes metrics in Prometheus format.
// Must be called once at startup before Server::run().
std::shared_ptr<MetricsReader> initialize(bool enabled);

// RAII guard: increments the given counter on construction, decrements on
// destruction. Safe to use in coroutines — the frame destructor fires on
// both normal completion and cancellation.
class ActiveCounterGuard {
 public:
  explicit ActiveCounterGuard(
      opentelemetry::metrics::UpDownCounter<int64_t>& counter,
      std::string operation);
  ~ActiveCounterGuard();
  ActiveCounterGuard(const ActiveCounterGuard&) = delete;
  ActiveCounterGuard& operator=(const ActiveCounterGuard&) = delete;

 private:
  opentelemetry::metrics::UpDownCounter<int64_t>& counter_;
  std::string operation_;
};

// Initializes a counter so that it appears in the metrics output by adding 0.
// This is done for the counter `counter` with the label `labelKey` once for
// each value in `labelValues`.
template <typename T>
void initializeCounter(T* counter, std::string labelKey,
                       const std::vector<std::string>& labelValues) {
  for (const auto& value : labelValues) {
    counter->Add(0, {{labelKey, value}});
  }
}

}  // namespace ad_utility::metrics

#endif  // QLEVER_SRC_UTIL_METRICS_H
