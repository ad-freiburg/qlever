// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SERVERMETRICS_H
#define QLEVER_SRC_ENGINE_SERVERMETRICS_H

#include <opentelemetry/metrics/async_instruments.h>
#include <opentelemetry/metrics/sync_instruments.h>

#include <memory>

#include "QueryExecutionContext.h"
#include "global/Id.h"
#include "index/Index.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"

// Owns all OTEL instruments and deregisters observable callbacks on
// destruction. Must be declared after index_, allocator_, and cache_ so
// that it is destroyed first and its destructor can safely access those
// members via the stored references.
class ServerMetrics {
 public:
  static std::unique_ptr<ServerMetrics> create(
      std::shared_ptr<Index> index,
      ad_utility::AllocatorWithLimit<Id>& allocator, QueryResultCache& cache,
      ad_utility::MemorySize maxMem);
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
  std::unique_ptr<opentelemetry::metrics::Gauge<int64_t>> memoryCacheLimit_;

 private:
  ServerMetrics(std::shared_ptr<Index> index,
                ad_utility::AllocatorWithLimit<Id>& allocator,
                QueryResultCache& cache, ad_utility::MemorySize maxMem);
  void registerCallbacks();

  static void observeDeltaTriples(opentelemetry::metrics::ObserverResult result,
                                  void* state);
  static void observeMemoryQueryFree(
      opentelemetry::metrics::ObserverResult result, void* state);
  static void observeMemoryCacheUsed(
      opentelemetry::metrics::ObserverResult result, void* state);
  static void observe(opentelemetry::metrics::ObserverResult result,
                      int64_t value);

  // References to Server state used by observable callbacks. Valid for the
  // entire lifetime of ServerMetrics because Server holds the shared_ptr and
  // its own members outlive it (declared earlier in Server).
  std::shared_ptr<Index> index_;
  ad_utility::AllocatorWithLimit<Id>& allocator_;
  QueryResultCache& cache_;

  // Observable instruments: SDK invokes callbacks on scrape; RemoveCallback
  // in ~ServerMetrics() blocks until any in-flight callback returns.
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      deltaTriplesMetric_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      memoryQueryFree_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      memoryCacheUsed_;
};

#endif  // QLEVER_SRC_ENGINE_SERVERMETRICS_H
