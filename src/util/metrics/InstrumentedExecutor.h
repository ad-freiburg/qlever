// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HTTP_INSTRUMENTEDEXECUTOR_H
#define QLEVER_SRC_UTIL_HTTP_INSTRUMENTEDEXECUTOR_H

#include <absl/cleanup/cleanup.h>
#include <opentelemetry/metrics/sync_instruments.h>

#include <chrono>
#include <memory>

#include "util/http/beast.h"

namespace ad_utility {

namespace net = boost::asio;

struct IoContextMetrics {
  std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>>
      runningHandlers_;
  std::unique_ptr<opentelemetry::metrics::Gauge<int64_t>> maxHandlers_;
  std::unique_ptr<opentelemetry::metrics::Histogram<double>> handlerLatencyMs_;
};

// Factory defined in InstrumentedExecutor.cpp to keep OTEL provider calls out
// of headers.
IoContextMetrics makeIoContextMetrics();

// Executor wrapper that intercepts dispatch/post/defer/execute to record
// handler scheduling latency and concurrent running-handler count.
// Satisfies both the old-style TS Executor and the new-style Executor
// concepts, making it compatible with net::any_io_executor and net::strand<E>.
// Property queries (require/prefer/query) are forwarded to InnerExecutor so
// that any_io_executor can wrap this type.
template <typename InnerExecutor>
class InstrumentedExecutor {
 public:
  using inner_executor_type = InnerExecutor;

  InstrumentedExecutor(InnerExecutor inner,
                       std::shared_ptr<IoContextMetrics> metrics)
      : inner_(std::move(inner)), metrics_(std::move(metrics)) {}

  InstrumentedExecutor(const InstrumentedExecutor&) = default;
  InstrumentedExecutor& operator=(const InstrumentedExecutor&) = default;

  // Old-style executor concept (required by is_executor<T> detection).
  net::execution_context& context() const noexcept { return inner_.context(); }
  void on_work_started() const noexcept { inner_.on_work_started(); }
  void on_work_finished() const noexcept { inner_.on_work_finished(); }

  bool operator==(const InstrumentedExecutor& other) const noexcept {
    return inner_ == other.inner_;
  }
  bool operator!=(const InstrumentedExecutor& other) const noexcept {
    return !(*this == other);
  }

  template <typename Function, typename Allocator>
  void dispatch(Function&& f, const Allocator& a) const {
    inner_.dispatch(makeWrapped(std::forward<Function>(f)), a);
  }

  template <typename Function, typename Allocator>
  void post(Function&& f, const Allocator& a) const {
    inner_.post(makeWrapped(std::forward<Function>(f)), a);
  }

  template <typename Function, typename Allocator>
  void defer(Function&& f, const Allocator& a) const {
    inner_.defer(makeWrapped(std::forward<Function>(f)), a);
  }

  // New-style executor concept: enables execution::is_executor<T> and
  // wrapping by any_io_executor.
  template <typename Function>
  void execute(Function&& f) const {
    inner_.execute(makeWrapped(std::forward<Function>(f)));
  }

  // Forward property queries to InnerExecutor, wrapping results back in
  // InstrumentedExecutor. Required so that any_io_executor can wrap us (it
  // checks can_require/can_prefer for blocking, outstanding_work, etc.).
  template <typename Property>
  auto require(const Property& p) const
      -> InstrumentedExecutor<
          decltype(net::require(std::declval<const InnerExecutor&>(), p))> {
    return {net::require(inner_, p), metrics_};
  }

  template <typename Property>
  auto prefer(const Property& p) const
      -> InstrumentedExecutor<
          decltype(net::prefer(std::declval<const InnerExecutor&>(), p))> {
    return {net::prefer(inner_, p), metrics_};
  }

  template <typename Property>
  auto query(const Property& p) const
      -> decltype(net::query(std::declval<const InnerExecutor&>(), p)) {
    return net::query(inner_, p);
  }

 private:
  template <typename Function>
  auto makeWrapped(Function&& f) const {
    auto postTime = std::chrono::steady_clock::now();
    return [f = std::forward<Function>(f), postTime,
            metrics = metrics_]() mutable {
      double latencyMs = std::chrono::duration<double, std::milli>(
                             std::chrono::steady_clock::now() - postTime)
                             .count();
      metrics->handlerLatencyMs_->Record(latencyMs);
      metrics->runningHandlers_->Add(1);
      absl::Cleanup decrement{
          [&metrics]() noexcept { metrics->runningHandlers_->Add(-1); }};
      std::invoke(std::move(f));
    };
  }

  // decay_t prevents a dangling-reference member when ASIO's prefer CPO
  // deduces InnerExecutor as a reference type via its identity fallback.
  std::decay_t<InnerExecutor> inner_;
  std::shared_ptr<IoContextMetrics> metrics_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HTTP_INSTRUMENTEDEXECUTOR_H
