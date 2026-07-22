// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_METRICS_METRICS_H
#define QLEVER_SRC_UTIL_METRICS_METRICS_H

#include <opentelemetry/metrics/sync_instruments.h>

#include <memory>
#include <string>

namespace ad_utility::metrics {

// Abstract interface for pulling a Prometheus text exposition from the
// registered metrics. Implemented by `PullMetricReader` in `Metrics.cpp`.
class MetricsReader {
 public:
  virtual ~MetricsReader() = default;
  virtual std::string getMetricsText() const = 0;
};

// Initialize the global OTEL MeterProvider. When `enabled` is false this
// function is a no-op and the provider remains the built-in no-op provider,
// so all subsequent Add/Record calls are safe but silently discarded.
// When `enabled` is true, a MetricsReader is returned that exposes metrics in
// Prometheus format via the /metrics endpoint.
// Must be called once at startup before Server::run().
std::shared_ptr<MetricsReader> initialize(bool enabled);

// RAII guard: increments the given counter on construction, decrements on
// destruction. Safe to use in coroutines — the frame destructor fires on
// both normal completion and cancellation.
class [[nodiscard(
    "The counter is only incremented while this guard is alive. Store it in a "
    "variable.")]] ActiveCounterGuard {
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

using MetricLabel = std::pair<std::string_view, std::string_view>;

// Exception types during request handling before the request has been parsed as
// SPARQL.
struct HttpErrorType {
 private:
  static constexpr std::string_view key = "type";

 public:
  static constexpr MetricLabel http{key, "http"};
  static constexpr MetricLabel internal{key, "internal"};
};

// Type of the SPARQL operation. Graph Store Protocol is not listed here because
// we translate it to a Query or Update as which it is then counted.
struct OperationType {
 private:
  static constexpr std::string_view key = "operation";

 public:
  static constexpr MetricLabel query{key, "query"};
  static constexpr MetricLabel update{key, "update"};
};

// Exception types during the execution of a SPARQL operation.
struct SparqlErrorType {
 private:
  static constexpr std::string_view key = "type";

 public:
  // A `boost::system::system_error` was thrown. Happens when something goes
  // wrong during the await of an async operation. Like the socket being closed.
  static constexpr MetricLabel systemError{key, "system_error"};
  // An unspecified error (except `systemError` above) while streaming the
  // response.
  static constexpr MetricLabel sendStreamableResponse{
      key, "send_streamable_response"};
  // An operation early returned a non-standard HTTP response status. Not always
  // actually an error.
  static constexpr MetricLabel protocol{key, "protocol"};
  // The operation's syntax was not valid.
  static constexpr MetricLabel syntax{key, "syntax"};
  // The manually selected query id is already in use.
  static constexpr MetricLabel inUse{key, "in_use"};
  // The execution of the operation timed out.
  static constexpr MetricLabel timeout{key, "timeout"};
  // Other unspecified errors.
  static constexpr MetricLabel internal{key, "internal"};
};

}  // namespace ad_utility::metrics

#endif  // QLEVER_SRC_UTIL_METRICS_METRICS_H
