// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/metrics/Tracing.h"

#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/samplers/always_on_factory.h>
#include <opentelemetry/sdk/trace/samplers/parent_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/semconv/error_attributes.h>
#include <opentelemetry/semconv/exception_attributes.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/span_metadata.h>

#include <optional>
#include <string>
#include <utility>

#include "util/metrics/Resource.h"

namespace nostd = opentelemetry::nostd;
namespace otel_propagation = opentelemetry::context::propagation;
namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace semconv = opentelemetry::semconv;

namespace ad_utility::tracing {
namespace {

// Convert to the string view type of the OTEL API. Note that this has to keep
// the size: a `std::string_view` is not necessarily null-terminated, so passing
// its `data()` as a `const char*` attribute value would read out of bounds.
nostd::string_view toOtel(std::string_view view) {
  return {view.data(), view.size()};
}
}  // namespace

// _____________________________________________________________________________
TracingHandle::TracingHandle(
    std::shared_ptr<trace_sdk::TracerProvider> provider)
    : provider_{std::move(provider)} {}

// _____________________________________________________________________________
TracingHandle::TracingHandle(TracingHandle&& other) noexcept
    : provider_{std::move(other.provider_)} {}

// _____________________________________________________________________________
TracingHandle& TracingHandle::operator=(TracingHandle&& other) noexcept {
  if (this != &other) {
    shutdown();
    provider_ = std::move(other.provider_);
  }
  return *this;
}

// _____________________________________________________________________________
TracingHandle::~TracingHandle() { shutdown(); }

// _____________________________________________________________________________
void TracingHandle::shutdown() {
  if (provider_ == nullptr) {
    return;
  }
  // Uninstall first, so that anything that creates a span from here on gets the
  // no-op provider instead of one whose exporter is being torn down.
  trace_api::Provider::SetTracerProvider(
      nostd::shared_ptr<trace_api::TracerProvider>{});
  // The batch processor buffers spans and exports them from a background
  // thread, so without this the spans of the last few seconds would be lost.
  provider_->Shutdown();
  provider_ = nullptr;
}

// _____________________________________________________________________________
TracingHandle initialize() {
  // The endpoint is configured with the standard `OTEL_EXPORTER_OTLP_TRACES_*`
  // variables.
  auto exporter =
      opentelemetry::exporter::otlp::OtlpHttpExporterFactory::Create();
  auto provider = trace_sdk::TracerProviderFactory::Create(
      trace_sdk::BatchSpanProcessorFactory::Create(
          std::move(exporter), trace_sdk::BatchSpanProcessorOptions{}),
      metrics::sharedResource(),
      // If a span has a parent outside QLever respect the existing sampling
      // decision, otherwise collect all.
      trace_sdk::ParentBasedSamplerFactory::Create(
          trace_sdk::AlwaysOnSamplerFactory::Create()));

  auto sharedProvider = std::shared_ptr{std::move(provider)};
  trace_api::Provider::SetTracerProvider(
      nostd::shared_ptr<trace_api::TracerProvider>{sharedProvider});

  // Propagate the trace context using the W3C Trace Context standard.
  // NOTES:
  // - The propagation is currently only used for incoming context.
  // - The baggage (user-defined key/value pairs) is not propagated. We don't
  // use or need it. To use it we'd also need a `BaggagePropagator` among other
  // changes.
  otel_propagation::GlobalTextMapPropagator::SetGlobalPropagator(
      nostd::shared_ptr<otel_propagation::TextMapPropagator>{
          new trace_api::propagation::HttpTraceContext{}});

  return TracingHandle{std::move(sharedProvider)};
}

// _____________________________________________________________________________
nostd::shared_ptr<trace_api::Tracer> tracer() {
  return trace_api::Provider::GetTracerProvider()->GetTracer("qlever");
}

// _____________________________________________________________________________
SpanGuard::SpanGuard(std::string_view name,
                     std::optional<trace_api::SpanContext> parent) {
  trace_api::StartSpanOptions options;
  if (parent.has_value()) {
    options.parent = std::move(parent).value();
  } else {
    // Declaring a span as a root span (not the child of another span) has to be
    // done explicitly.
    options.parent = opentelemetry::context::Context{}.SetValue(
        trace_api::kIsRootSpanKey, true);
  }
  span_ = tracer()->StartSpan(toOtel(name), options);
}

// _____________________________________________________________________________
SpanGuard::~SpanGuard() {
  if (!statusRecorded_) {
    // Neither success nor a specific error was recorded, so the scope holding
    // this guard was left abnormally.
    span_->SetStatus(trace_api::StatusCode::kError, "unfinished");
  }
  span_->End();
}

// _____________________________________________________________________________
trace_api::SpanContext SpanGuard::context() const {
  return span_->GetContext();
}

// _____________________________________________________________________________
void SpanGuard::setOk() {
  span_->SetStatus(trace_api::StatusCode::kOk);
  statusRecorded_ = true;
}

// _____________________________________________________________________________
void SpanGuard::setError(std::string_view errorType, std::string_view message) {
  span_->SetAttribute(semconv::error::kErrorType, toOtel(errorType));
  span_->SetStatus(trace_api::StatusCode::kError, toOtel(message));
  statusRecorded_ = true;
}

// _____________________________________________________________________________
void SpanGuard::recordException(const std::exception& exception,
                                std::string_view errorType) {
  span_->AddEvent("exception",
                  {{semconv::exception::kExceptionType, toOtel(errorType)},
                   {semconv::exception::kExceptionMessage, exception.what()}});
  setError(errorType, exception.what());
}

}  // namespace ad_utility::tracing
