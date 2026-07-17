// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "util/metrics/InstrumentedExecutor.h"

#include <opentelemetry/metrics/provider.h>

namespace ad_utility {

IoContextMetrics makeIoContextMetrics() {
  auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(
      "qlever", "0.0.1");
  IoContextMetrics m;
  m.runningHandlers_ = meter->CreateInt64UpDownCounter(
      "qlever.io_context.running_handlers",
      "Number of ASIO handlers currently executing in the io_context");
  m.maxHandlers_ = meter->CreateInt64Gauge(
      "qlever.io_context.max_handlers",
      "Maximum number of ASIO handlers in the io_context");
  m.handlerLatencyMs_ = meter->CreateDoubleHistogram(
      "qlever.io_context.handler_latency",
      "Scheduling latency from handler posting to execution start", "ms");
  return m;
}

}  // namespace ad_utility
