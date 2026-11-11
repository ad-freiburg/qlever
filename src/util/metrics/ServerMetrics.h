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
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/sync_instruments.h>

#include <memory>

#include "engine/UpdateMetadata.h"
#include "util/ExecutorStatistics.h"
#include "util/MemorySize/MemorySize.h"

// Owns all OTEL instruments and deregisters observable callbacks on
// destruction.
class ServerMetrics {
 public:
  using ThreadPoolSnapshot = ad_utility::ThreadPoolStatistics::Snapshot;
  ~ServerMetrics();
  ServerMetrics(const ServerMetrics&) = delete;
  ServerMetrics& operator=(const ServerMetrics&) = delete;

  // Synchronous instruments recorded directly by Server.
  std::unique_ptr<opentelemetry::metrics::Gauge<int64_t>> buildInfoMetric_;
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

  ServerMetrics(
      absl::AnyInvocable<DeltaTriplesCount() const> getDeltaTriples,
      absl::AnyInvocable<int64_t() const> getMemoryLeft,
      absl::AnyInvocable<int64_t() const> getCacheUsed,
      absl::AnyInvocable<int64_t() const> getCacheLimit,
      absl::AnyInvocable<int64_t() const> getRebuildInProgress,
      std::optional<ad_utility::MemorySize> maxMem,
      absl::AnyInvocable<int64_t() const> getNumTriplesIndex,
      absl::AnyInvocable<ThreadPoolSnapshot() const> getThreadPoolStatistics);
  void registerCallbacks();

 private:
  template <absl::AnyInvocable<int64_t() const> ServerMetrics::*Getter>
  static void observeCallback(opentelemetry::metrics::ObserverResult result,
                              void* state);
  static void deltaTriplesCallback(
      opentelemetry::metrics::ObserverResult result, void* state);
  static void observe(opentelemetry::metrics::ObserverResult result,
                      int64_t value);
  static void observe(opentelemetry::metrics::ObserverResult result,
                      int64_t value, const std::string& label,
                      const std::string& labelValue);

  absl::AnyInvocable<DeltaTriplesCount() const> getDeltaTriples_;
  absl::AnyInvocable<int64_t() const> getMemoryLeft_;
  absl::AnyInvocable<int64_t() const> getCacheUsed_;
  absl::AnyInvocable<int64_t() const> getCacheLimit_;
  absl::AnyInvocable<int64_t() const> getRebuildInProgress_;
  absl::AnyInvocable<int64_t() const> getNumTriplesIndex_;
  absl::AnyInvocable<ThreadPoolSnapshot() const> getThreadPoolStatistics_;

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
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      rebuildInProgressMetric_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      numTriplesIndex_;

  // The observable counters for the statistics of the thread pool (see
  // `util/ExecutorStatistics.h`), each with the callback that observes it.
  using Callback = void (*)(opentelemetry::metrics::ObserverResult, void*);
  struct ThreadPoolInstrument {
    std::shared_ptr<opentelemetry::metrics::ObservableInstrument> instrument_;
    Callback callback_;
  };
  std::vector<ThreadPoolInstrument> threadPoolInstruments_;
  void createThreadPoolInstruments(opentelemetry::metrics::Meter& meter);
  // Observe the time `Time`, with one value per class of tasks.
  template <uint64_t ad_utility::TaskStatistics::*Time>
  static void taskTimeCallback(opentelemetry::metrics::ObserverResult result,
                               void* state);
};

#endif  // QLEVER_SRC_UTIL_METRICS_SERVERMETRICS_H
