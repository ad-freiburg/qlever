// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_TRACINGTESTHELPERS_H
#define QLEVER_TEST_UTIL_TRACINGTESTHELPERS_H

#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/exporters/memory/in_memory_span_data.h>
#include <opentelemetry/exporters/memory/in_memory_span_exporter_factory.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>

#include <memory>
#include <utility>
#include <vector>

namespace tracingTestHelpers {

// Installs a `TracerProvider` that collects all spans in memory, plus the same
// W3C propagator that `tracing::initialize` installs, and restores both on
// destruction. Both are global state, so without the restore tests would see
// each other's spans.
//
// The propagator belongs here rather than only in the tests that need it: code
// that reads an incoming `traceparent` silently does nothing when the default
// no-op propagator is installed, so a test without it would pass while
// asserting nothing.
//
// Uses a `SimpleSpanProcessor` rather than the `BatchSpanProcessor` used in
// production: the simple processor exports each span synchronously when it
// ends, so assertions can run immediately instead of having to wait for a
// background thread to flush.
class ScopedInMemoryTracer {
  using TracerProvider = opentelemetry::trace::TracerProvider;
  using SpanData = opentelemetry::sdk::trace::SpanData;
  using TextMapPropagator =
      opentelemetry::context::propagation::TextMapPropagator;

  opentelemetry::nostd::shared_ptr<TracerProvider> previousProvider_;
  opentelemetry::nostd::shared_ptr<TextMapPropagator> previousPropagator_;
  std::shared_ptr<opentelemetry::exporter::memory::InMemorySpanData> spanData_;

 public:
  ScopedInMemoryTracer()
      : previousProvider_{opentelemetry::trace::Provider::GetTracerProvider()},
        previousPropagator_{
            opentelemetry::context::propagation::GlobalTextMapPropagator::
                GetGlobalPropagator()} {
    auto exporter =
        opentelemetry::exporter::memory::InMemorySpanExporterFactory::Create(
            spanData_);
    auto provider = opentelemetry::sdk::trace::TracerProviderFactory::Create(
        opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(
            std::move(exporter)));
    opentelemetry::trace::Provider::SetTracerProvider(
        opentelemetry::nostd::shared_ptr<TracerProvider>{provider.release()});
    opentelemetry::context::propagation::GlobalTextMapPropagator::
        SetGlobalPropagator(opentelemetry::nostd::shared_ptr<TextMapPropagator>{
            new opentelemetry::trace::propagation::HttpTraceContext{}});
  }

  ~ScopedInMemoryTracer() {
    opentelemetry::trace::Provider::SetTracerProvider(previousProvider_);
    opentelemetry::context::propagation::GlobalTextMapPropagator::
        SetGlobalPropagator(previousPropagator_);
  }

  ScopedInMemoryTracer(const ScopedInMemoryTracer&) = delete;
  ScopedInMemoryTracer& operator=(const ScopedInMemoryTracer&) = delete;

  // The tracer that tests should create their spans with.
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer()
      const {
    return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
        "qlever.test");
  }

  // All spans that have ended since the last call. Note that this *drains* the
  // underlying buffer, so it must only be called once per assertion block.
  std::vector<std::unique_ptr<SpanData>> spans() const {
    return spanData_->GetSpans();
  }
};

}  // namespace tracingTestHelpers

#endif  // QLEVER_TEST_UTIL_TRACINGTESTHELPERS_H
