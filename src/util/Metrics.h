// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_METRICS_H
#define QLEVER_SRC_UTIL_METRICS_H

#include "opentelemetry/metrics/sync_instruments.h"

namespace ad_utility::metrics {

// Initialize the global OTEL MeterProvider. When `enabled` is false this
// function is a no-op and the provider remains the built-in no-op provider,
// so all subsequent Add/Record calls are safe but silently discarded.
// When `enabled` is true a Prometheus scrape endpoint is opened on `port`
// and periodic OStream snapshots are written to stdout.
// Must be called once at startup before Server::run().
void initialize(bool enabled, uint16_t port);

// RAII guard: increments the given counter on construction, decrements on
// destruction. Safe to use in coroutines — the frame destructor fires on
// both normal completion and cancellation.
class ActiveCounterGuard {
 public:
  explicit ActiveCounterGuard(
      opentelemetry::metrics::UpDownCounter<int64_t>& counter);
  ~ActiveCounterGuard();
  ActiveCounterGuard(const ActiveCounterGuard&) = delete;
  ActiveCounterGuard& operator=(const ActiveCounterGuard&) = delete;

 private:
  opentelemetry::metrics::UpDownCounter<int64_t>& counter_;
};

}  // namespace ad_utility::metrics

#endif  // QLEVER_SRC_UTIL_METRICS_H
