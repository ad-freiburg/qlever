// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_METRICS_SERVERMETRICS_H
#define QLEVER_SRC_UTIL_METRICS_SERVERMETRICS_H

#include <absl/functional/any_invocable.h>
#include <opentelemetry/metrics/async_instruments.h>
#include <opentelemetry/metrics/sync_instruments.h>

#include <memory>

#include "util/MemorySize/MemorySize.h"

// Owns all OTEL instruments and deregisters observable callbacks on
// destruction.
class ServerMetrics {
 public:
  ~ServerMetrics();
  ServerMetrics(const ServerMetrics&) = delete;
  ServerMetrics& operator=(const ServerMetrics&) = delete;

  // Synchronous instruments recorded directly by Server.
  std::unique_ptr<opentelemetry::metrics::Gauge<int64_t>> startTimeMetric_;
  std::unique_ptr<opentelemetry::metrics::Gauge<int64_t>> indexLoadMetric_;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>>
      startedSparqlOperations_;
  std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>>
      runningSparqlOperations_;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>>
      finishedSparqlOperations_;
  std::unique_ptr<opentelemetry::metrics::Histogram<double>>
      sparqlOperationDuration_;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> sparqlErrors_;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> httpErrors_;
  std::unique_ptr<opentelemetry::metrics::Gauge<int64_t>> memoryQueryTotal_;

  ServerMetrics(absl::AnyInvocable<int64_t() const> getDeltaTriples,
                absl::AnyInvocable<int64_t() const> getMemoryLeft,
                absl::AnyInvocable<int64_t() const> getCacheUsed,
                absl::AnyInvocable<int64_t() const> getCacheLimit,
                std::optional<ad_utility::MemorySize> maxMem);
  void registerCallbacks();

 private:
  template <absl::AnyInvocable<int64_t() const> ServerMetrics::*Getter>
  static void observeCallback(opentelemetry::metrics::ObserverResult result,
                              void* state);
  static void observe(opentelemetry::metrics::ObserverResult result,
                      int64_t value);

  absl::AnyInvocable<int64_t() const> getDeltaTriples_;
  absl::AnyInvocable<int64_t() const> getMemoryLeft_;
  absl::AnyInvocable<int64_t() const> getCacheUsed_;
  absl::AnyInvocable<int64_t() const> getCacheLimit_;

  // Observable instruments: SDK invokes callbacks on scrape; RemoveCallback
  // in ~ServerMetrics() blocks until any in-flight callback returns.
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      deltaTriplesMetric_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      memoryQueryFree_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      memoryCacheUsed_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      memoryCacheLimit_;
};

#endif  // QLEVER_SRC_UTIL_METRICS_SERVERMETRICS_H
