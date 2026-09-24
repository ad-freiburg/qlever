// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_METRICS_TRACING_H
#define QLEVER_SRC_UTIL_METRICS_TRACING_H

#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/semconv/client_attributes.h>
#include <opentelemetry/semconv/http_attributes.h>
#include <opentelemetry/semconv/url_attributes.h>
#include <opentelemetry/semconv/user_agent_attributes.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/version.h>

#include <boost/url/parse.hpp>
#include <boost/url/url_view.hpp>
#include <exception>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>

#include "util/Exception.h"
#include "util/UniqueCleanup.h"
#include "util/http/beast.h"

// Forward declaration, so that this header does not have to pull in the OTEL
// trace SDK. OTEL uses custom macros for its namespaces.
OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk::trace {
class TracerProvider;
}
OPENTELEMETRY_END_NAMESPACE

namespace ad_utility::tracing {

// Sets up tracing. On destruction tracing is reset to the no-op provider.
class [[nodiscard(
    "Tracing is only active while this handle is alive.")]] TracingHandle {
  // Empty when tracing is disabled. The SDK type rather than the API type,
  // because only the former can be shut down.
  using Provider = std::shared_ptr<opentelemetry::sdk::trace::TracerProvider>;

  // Uninstalls the global tracer provider and shuts `provider` down, flushing
  // spans buffered by the batch processor. No-op if `provider` is `nullptr`
  // (tracing disabled, or already shut down).
  static void shutdownProvider(Provider provider);

  unique_cleanup::UniqueCleanup<Provider> provider_{nullptr, &shutdownProvider};

 public:
  TracingHandle() = default;
  explicit TracingHandle(Provider provider);
};

// Sets up tracing. Configures tracing using the provided `OTEL_*` environment
// variables. When `TracingHandle` is dropped, no-op behaviour is restored.
[[nodiscard]] TracingHandle initialize();

// Returns the single tracer instance for this process.
std::shared_ptr<opentelemetry::trace::Tracer> tracer();

// Owns a span and ends it on destruction. If none of `setOk`, `setError` or
// `recordException` is called before the span ends, it is assumed that the
// call was aborted (e.g. through an unexpected exception or cancelled
// coroutine).
class [[nodiscard(
    "The span is ended when this guard is destroyed. Store it in a "
    "variable.")]] SpanGuard {
  std::shared_ptr<opentelemetry::trace::Span> span_;
  bool statusRecorded_ = false;

 public:
  // Start a span as a child of `parent`. Set `parent` to `std::nullopt` to
  // start a new root span. Note: we have to pass the parent explicitly because
  // the default in the SDK relies on thread local storage which doesn't work
  // with coroutines.
  SpanGuard(std::string_view name,
            std::optional<opentelemetry::trace::SpanContext> parent);
  ~SpanGuard();

  SpanGuard(const SpanGuard&) = delete;
  SpanGuard& operator=(const SpanGuard&) = delete;

  opentelemetry::trace::Span& span() { return *span_; }

  // The context of this span, to be passed as the parent of child spans.
  opentelemetry::trace::SpanContext context() const;

  // Record that the span finished successfully.
  void setOk();

  // Record a failure. For consistency `errorType` should be one of the values
  // also used for the `type` label of the error metrics.
  void setError(std::string_view errorType, std::string_view message);

  // Like `setError`, and additionally records the exception's type and message
  // as an `exception` event on the span.
  void recordException(const std::exception& exception,
                       std::string_view errorType);
};

// Adapter between the propagator machinery of OTEL and our concrete Boost.Beast
// HTTP types. `Get` reads a `traceparent` a client may have sent, `Set` writes
// one into an outgoing request, so that a trace continues into the service we
// call (for example a federated `SERVICE` request).
template <typename RequestT>
class RequestHeaderCarrier
    : public opentelemetry::context::propagation::TextMapCarrier {
  RequestT& request_;

  static boost::beast::string_view toBeast(std::string_view view) {
    return {view.data(), view.size()};
  }

 public:
  explicit RequestHeaderCarrier(RequestT& request) : request_{request} {}

  std::string_view Get(std::string_view key) const noexcept override {
    auto it = request_.base().find(toBeast(key));
    if (it == request_.base().end()) {
      return {};
    }
    return {it->value().data(), it->value().size()};
  }

  void Set(std::string_view key, std::string_view value) noexcept override {
    if constexpr (std::is_const_v<RequestT>) {
      // The interface is unfortunate, because it mixes injection and
      // extraction.
      AD_FAIL();
    } else {
      // Note: `set` is not `noexcept`.
      request_.base().set(toBeast(key), toBeast(value));
    }
  }
};

// Extract the span context a client sent via the `request`, using the
// propagator configured by `initialize`. Returns `std::nullopt` when there is
// no valid header.
template <typename RequestT>
std::optional<opentelemetry::trace::SpanContext> extractParentFromRequest(
    const RequestT& request) {
  RequestHeaderCarrier<const RequestT> carrier{request};
  // Dummy value required by the interface.
  opentelemetry::context::Context emptyContext{};
  auto context = opentelemetry::context::propagation::GlobalTextMapPropagator::
                     GetGlobalPropagator()
                         ->Extract(carrier, emptyContext);
  auto parent = opentelemetry::trace::GetSpan(context)->GetContext();
  if (!parent.IsValid()) {
    return std::nullopt;
  }
  return parent;
}

// Record the attributes of an incoming HTTP request on the root span of its
// trace, using the conventional attribute names, so that a backend can display
// and filter them without knowing anything about QLever.
template <typename RequestT>
void setRequestAttributes(opentelemetry::trace::Span& span,
                          const RequestT& request) {
  namespace semconv = opentelemetry::semconv;
  span.SetAttribute(semconv::http::kHttpRequestMethod,
                    std::string_view{request.method_string()});
  if (auto url = boost::urls::parse_origin_form(request.target())) {
    // Percent-encoded forms
    span.SetAttribute(semconv::url::kUrlPath,
                      std::string_view{url->encoded_path()});
    if (url->has_query()) {
      // Without leading `?`
      span.SetAttribute(semconv::url::kUrlQuery,
                        std::string_view{url->encoded_query()});
    }
  }
  auto setIfPresent = [&span, &request](std::string_view attribute,
                                        boost::beast::http::field field) {
    std::string_view value = request.base()[field];
    if (!value.empty()) {
      span.SetAttribute(attribute, value);
    }
  };
  setIfPresent(semconv::user_agent::kUserAgentOriginal,
               boost::beast::http::field::user_agent);
  // The socket peer address is not available and also isn't meaningful for a
  // common QLever setup which is behind a reverse proxy. Use the `X-Real-IP` if
  // available.
  std::string_view clientIp = request.base()["X-Real-IP"];
  if (!clientIp.empty()) {
    span.SetAttribute(semconv::client::kClientAddress, clientIp);
  }
}

}  // namespace ad_utility::tracing

#endif  // QLEVER_SRC_UTIL_METRICS_TRACING_H
