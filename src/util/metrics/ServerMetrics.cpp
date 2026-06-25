// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/metrics/ServerMetrics.h"

#include <opentelemetry/metrics/provider.h>

#include <utility>

#include "util/metrics/Metrics.h"

// _____________________________________________________________________________
ServerMetrics::ServerMetrics(std::function<int64_t()> getDeltaTriples,
                             std::function<int64_t()> getMemoryLeft,
                             std::function<int64_t()> getCacheUsed,
                             ad_utility::MemorySize maxMem)
    : getDeltaTriples_(std::move(getDeltaTriples)),
      getMemoryLeft_(std::move(getMemoryLeft)),
      getCacheUsed_(std::move(getCacheUsed)) {
  auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(
      "qlever", "0.0.1");
  startTimeMetric_ = meter->CreateInt64Gauge(
      "qlever.server.start_time",
      "Unix timestamp when the QLever server was started", "s");
  indexLoadMetric_ = meter->CreateInt64Gauge(
      "qlever.index.load_time",
      "Unix timestamp when the current index was loaded", "s");
  startedSparqlOperations_ = meter->CreateUInt64Counter(
      "qlever.sparql_operation.started",
      "Number of SPARQL operations started since server start");
  runningSparqlOperations_ = meter->CreateInt64UpDownCounter(
      "qlever.sparql_operation.running",
      "Number of SPARQL operations currently being processed");
  finishedSparqlOperations_ = meter->CreateUInt64Counter(
      "qlever.sparql_operation.finished",
      "Number of SPARQL operations successfully finished since server start");
  sparqlOperationDuration_ = meter->CreateDoubleHistogram(
      "qlever.sparql_operation.duration",
      "Execution time of successful SPARQL operations", "ms");
  deltaTriplesMetric_ = meter->CreateInt64ObservableGauge(
      "qlever.delta_triples",
      "Number of triples that are updated relative to the index");
  sparqlErrors_ = meter->CreateUInt64Counter(
      "qlever.sparql_operation.errors",
      "Errors during the execution of SPARQL operations");
  httpErrors_ = meter->CreateUInt64Counter(
      "qlever.http.errors",
      "Errors during the execution of non SPARQL operations");
  memoryQueryFree_ = meter->CreateInt64ObservableGauge(
      "qlever.memory_query_available", "Available memory for query processing",
      "By");
  memoryQueryTotal_ =
      meter->CreateInt64Gauge("qlever.memory_query_limit",
                              "Memory allocated for query processing", "By");
  memoryCacheUsed_ = meter->CreateInt64ObservableGauge(
      "qlever.memory_cache_used", "Memory used for caching", "By");
  memoryCacheLimit_ = meter->CreateInt64Gauge(
      "qlever.memory_cache_limit", "Memory allocated for caching", "By");

  auto now = std::chrono::duration_cast<std::chrono::seconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();
  startTimeMetric_->Record(now);
  indexLoadMetric_->Record(now);
  memoryQueryTotal_->Record(maxMem.getBytes());

  // Record 0 once per label so every combination appears from the start.
  ad_utility::metrics::initializeCounter(startedSparqlOperations_.get(),
                                         "operation", {"query", "update"});
  ad_utility::metrics::initializeCounter(runningSparqlOperations_.get(),
                                         "operation", {"query", "update"});
  ad_utility::metrics::initializeCounter(finishedSparqlOperations_.get(),
                                         "operation", {"query", "update"});
  ad_utility::metrics::initializeCounter(
      sparqlErrors_.get(), "type",
      {"system_error", "internal", "syntax", "timeout",
       "send_streamable_response", "protocol", "in_use"});
  ad_utility::metrics::initializeCounter(httpErrors_.get(), "type",
                                         {"internal", "http"});
}

// _____________________________________________________________________________
ServerMetrics::~ServerMetrics() {
  deltaTriplesMetric_->RemoveCallback(&observeDeltaTriples, this);
  memoryQueryFree_->RemoveCallback(&observeMemoryQueryFree, this);
  memoryCacheUsed_->RemoveCallback(&observeMemoryCacheUsed, this);
}

// _____________________________________________________________________________
void ServerMetrics::registerCallbacks() {
  deltaTriplesMetric_->AddCallback(&observeDeltaTriples, this);
  memoryQueryFree_->AddCallback(&observeMemoryQueryFree, this);
  memoryCacheUsed_->AddCallback(&observeMemoryCacheUsed, this);
}

// _____________________________________________________________________________
void ServerMetrics::observe(opentelemetry::metrics::ObserverResult result,
                            int64_t value) {
  opentelemetry::nostd::get<opentelemetry::nostd::shared_ptr<
      opentelemetry::metrics::ObserverResultT<int64_t>>>(result)
      ->Observe(value);
}

// _____________________________________________________________________________
void ServerMetrics::observeDeltaTriples(
    opentelemetry::metrics::ObserverResult result, void* state) {
  const auto& self = *static_cast<ServerMetrics*>(state);
  observe(std::move(result), self.getDeltaTriples_());
}

// _____________________________________________________________________________
void ServerMetrics::observeMemoryQueryFree(
    opentelemetry::metrics::ObserverResult result, void* state) {
  const auto& self = *static_cast<ServerMetrics*>(state);
  observe(std::move(result), self.getMemoryLeft_());
}

// _____________________________________________________________________________
void ServerMetrics::observeMemoryCacheUsed(
    opentelemetry::metrics::ObserverResult result, void* state) {
  const auto& self = *static_cast<ServerMetrics*>(state);
  observe(std::move(result), self.getCacheUsed_());
}
