// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <future>

#include "./util/MetricsTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/SourceLocation.h"
#include "util/http/beast.h"
#include "util/jthread.h"
#include "util/metrics/InstrumentedExecutor.h"
#include "util/metrics/Metrics.h"

namespace net = boost::asio;

using namespace std::chrono_literals;

namespace {
constexpr std::string_view runningHandlers =
    "qlever_io_context_running_handlers";
constexpr std::string_view maxHandlers = "qlever_io_context_max_handlers";
constexpr std::string_view latencyCount =
    "qlever_io_context_handler_latency_milliseconds_count";
}  // namespace

// _____________________________________________________________________________
TEST(InstrumentedExecutor, recordsHandlerMetrics) {
  auto reader = ad_utility::metrics::initialize(true);
  ASSERT_NE(reader, nullptr);

  net::io_context ioContext;
  auto metrics = std::make_shared<ad_utility::IoContextMetrics>(
      ad_utility::makeIoContextMetrics());
  ad_utility::InstrumentedExecutor executor{ioContext.get_executor(), metrics};
  // The executor never touches this gauge itself; in production the
  // `HttpServer` records its number of threads.
  metrics->maxHandlers_->Record(3);

  auto expectMetrics = [&reader](const auto& matcher,
                                 ad_utility::source_location l =
                                     AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(reader->getMetricsText(), matcher);
  };

  expectMetrics(
      testing::AllOf(IsZero(runningHandlers), MetricIs(maxHandlers, "3")));

  // The handler signals that it has started and then blocks until we release
  // it. The wait is bounded so that a failing assertion below can never
  // deadlock the join of the worker thread.
  std::promise<void> handlerStarted;
  std::promise<void> unblockHandler;
  auto handlerStartedFuture = handlerStarted.get_future();
  auto unblockHandlerFuture = unblockHandler.get_future();
  net::post(executor, [&handlerStarted, &unblockHandlerFuture]() {
    handlerStarted.set_value();
    unblockHandlerFuture.wait_for(10s);
  });

  // Run the `io_context` on a separate thread, so that this thread stays free
  // to read the metrics while the handler is blocked.
  ad_utility::JThread worker{[&ioContext]() { ioContext.run(); }};

  ASSERT_EQ(handlerStartedFuture.wait_for(10s), std::future_status::ready);
  expectMetrics(testing::AllOf(MetricIs(runningHandlers, "1"),
                               MetricIs(latencyCount, "1")));

  unblockHandler.set_value();
  worker.join();

  expectMetrics(
      testing::AllOf(IsZero(runningHandlers), MetricIs(latencyCount, "1")));
}
