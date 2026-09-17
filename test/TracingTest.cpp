// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>

#include <optional>
#include <stdexcept>
#include <string>

#include "./util/TracingTestHelpers.h"
#include "util/HttpRequestHelpers.h"
#include "util/metrics/Tracing.h"

namespace {
using namespace ad_utility::tracing;
using tracingTestHelpers::ScopedInMemoryTracer;

// A `traceparent` header as defined by W3C Trace Context: version, trace id,
// span id and flags. The last byte is `01` for "sampled".
constexpr const char* TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
constexpr const char* SPAN_ID = "b7ad6b7169203331";
constexpr const char* TRACEPARENT_SAMPLED =
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

// Render an id as the lower-case hex string that also appears in a
// `traceparent` header.
template <typename Id>
std::string toHex(const Id& id) {
  std::string result(Id::kSize * 2, '\0');
  id.ToLowerBase16(opentelemetry::nostd::span<char, Id::kSize * 2>{
      result.data(), result.size()});
  return result;
}

// Silences the SDK's own diagnostic output and restores the previous handler on
// destruction. Only needed for the test that installs the real OTLP/HTTP
// exporter: that exporter tries to reach the configured endpoint, and the
// resulting connection error would look like a failure in the test log.
class ScopedSilentOtelLog {
  using LogHandler = opentelemetry::sdk::common::internal_log::LogHandler;
  using GlobalLogHandler =
      opentelemetry::sdk::common::internal_log::GlobalLogHandler;

  opentelemetry::nostd::shared_ptr<LogHandler> previousHandler_;

 public:
  ScopedSilentOtelLog() : previousHandler_{GlobalLogHandler::GetLogHandler()} {
    GlobalLogHandler::SetLogHandler(
        opentelemetry::nostd::shared_ptr<LogHandler>{
            new opentelemetry::sdk::common::internal_log::NoopLogHandler{}});
  }
  ~ScopedSilentOtelLog() { GlobalLogHandler::SetLogHandler(previousHandler_); }

  ScopedSilentOtelLog(const ScopedSilentOtelLog&) = delete;
  ScopedSilentOtelLog& operator=(const ScopedSilentOtelLog&) = delete;
};

// `traceparent` is not one of beast's known fields, so it has to be set by
// name rather than through `makeRequest`'s `http::field`-keyed header map.
auto makeRequestWithTraceparent(std::string_view traceparent) {
  auto request = ad_utility::testing::makeGetRequest("/sparql");
  request.set("traceparent", boost::beast::string_view{traceparent.data(),
                                                       traceparent.size()});
  return request;
}
}  // namespace

// _____________________________________________________________________________
// When tracing is disabled, `initialize` is simply never called and the handle
// stays default-constructed. Such a handle must neither install nor, on
// destruction, uninstall anything.
TEST(Tracing, defaultConstructedHandleDoesNothing) {
  auto providerBefore = opentelemetry::trace::Provider::GetTracerProvider();
  {
    TracingHandle handle;
    // The provider is untouched, so spans are created by the API's no-op
    // implementation and are not recording.
    EXPECT_EQ(opentelemetry::trace::Provider::GetTracerProvider(),
              providerBefore);
    auto span = tracer()->StartSpan("not recorded");
    EXPECT_FALSE(span->IsRecording());
    span->End();
  }
  EXPECT_EQ(opentelemetry::trace::Provider::GetTracerProvider(),
            providerBefore);
}

// _____________________________________________________________________________
// `initialize` always installs a provider with the OTLP/HTTP exporter; which
// exporter is used is not configurable. The span below is therefore really sent
// to the configured endpoint when the handle is destroyed at the end of the
// test; whether anything listens there does not matter for the assertions.
TEST(Tracing, initializeInstallsARecordingProvider) {
  // Declared before the handle, so that it is still in place when the handle
  // flushes the span in its destructor.
  ScopedSilentOtelLog silentOtelLog;
  auto providerBefore = opentelemetry::trace::Provider::GetTracerProvider();
  auto handle = initialize();
  EXPECT_NE(opentelemetry::trace::Provider::GetTracerProvider(),
            providerBefore);
  // A span created through the normal entry points has to be recorded, which is
  // what makes the instrumentation in `Server` effective.
  SpanGuard guard{"recorded", std::nullopt};
  EXPECT_TRUE(guard.span().IsRecording());
  EXPECT_TRUE(guard.context().IsValid());
  EXPECT_TRUE(guard.context().IsSampled());
  guard.setOk();
}

// _____________________________________________________________________________
TEST(Tracing, spanGuardEndsSpanAndRecordsSuccess) {
  ScopedInMemoryTracer scopedTracer;
  {
    SpanGuard guard{"work", std::nullopt};
    guard.setOk();
    // Not ended yet, so nothing has been exported.
    EXPECT_THAT(scopedTracer.spans(), testing::IsEmpty());
  }
  auto spans = scopedTracer.spans();
  ASSERT_EQ(spans.size(), 1);
  EXPECT_EQ(spans.at(0)->GetName(), "work");
  EXPECT_EQ(spans.at(0)->GetStatus(), opentelemetry::trace::StatusCode::kOk);
}

// _____________________________________________________________________________
TEST(Tracing, spanGuardMarksSpanWithoutStatusAsError) {
  ScopedInMemoryTracer scopedTracer;
  {
    // Neither `setOk` nor `setError`, which is what happens when the enclosing
    // coroutine frame is destroyed because the request was cancelled.
    SpanGuard guard{"cancelled", std::nullopt};
  }
  auto spans = scopedTracer.spans();
  ASSERT_EQ(spans.size(), 1);
  EXPECT_EQ(spans.at(0)->GetStatus(), opentelemetry::trace::StatusCode::kError);
  EXPECT_EQ(spans.at(0)->GetDescription(), "unfinished");
}

// _____________________________________________________________________________
TEST(Tracing, spanGuardRecordsErrorsAndExceptions) {
  ScopedInMemoryTracer scopedTracer;
  {
    SpanGuard guard{"failing", std::nullopt};
    // Deliberately a non-null-terminated view, to catch a `data()` being passed
    // on as a C string somewhere.
    std::string buffer = "timeoutAndMore";
    guard.setError(std::string_view{buffer}.substr(0, 7), "took too long");
  }
  {
    SpanGuard guard{"throwing", std::nullopt};
    guard.recordException(std::runtime_error{"something broke"}, "internal");
  }
  auto spans = scopedTracer.spans();
  ASSERT_EQ(spans.size(), 2);

  const auto& failing = *spans.at(0);
  EXPECT_EQ(failing.GetStatus(), opentelemetry::trace::StatusCode::kError);
  EXPECT_EQ(failing.GetDescription(), "took too long");
  ASSERT_EQ(failing.GetAttributes().count("error.type"), 1u);
  EXPECT_EQ(opentelemetry::nostd::get<std::string>(
                failing.GetAttributes().at("error.type")),
            "timeout");

  const auto& throwing = *spans.at(1);
  EXPECT_EQ(throwing.GetStatus(), opentelemetry::trace::StatusCode::kError);
  ASSERT_EQ(throwing.GetEvents().size(), 1);
  const auto& event = throwing.GetEvents().at(0);
  EXPECT_EQ(event.GetName(), "exception");
  EXPECT_EQ(opentelemetry::nostd::get<std::string>(
                event.GetAttributes().at("exception.type")),
            "internal");
  EXPECT_EQ(opentelemetry::nostd::get<std::string>(
                event.GetAttributes().at("exception.message")),
            "something broke");
}

// _____________________________________________________________________________
TEST(Tracing, spanGuardParentsChildrenExplicitly) {
  ScopedInMemoryTracer scopedTracer;
  {
    SpanGuard parent{"parent", std::nullopt};
    {
      SpanGuard child{"child", parent.context()};
      child.setOk();
    }
    parent.setOk();
  }
  auto spans = scopedTracer.spans();
  ASSERT_EQ(spans.size(), 2);
  const auto& child = *spans.at(0);
  const auto& parent = *spans.at(1);
  ASSERT_EQ(child.GetName(), "child");
  ASSERT_EQ(parent.GetName(), "parent");
  EXPECT_EQ(child.GetParentSpanId(), parent.GetSpanId());
  EXPECT_EQ(child.GetTraceId(), parent.GetTraceId());
}

// _____________________________________________________________________________
TEST(Tracing, nulloptStartsANewTraceEvenWithAnActiveSpan) {
  ScopedInMemoryTracer scopedTracer;
  {
    // An unrelated span is active on this thread. A span created with
    // `std::nullopt` must still be a root: merely leaving the parent unset
    // would make the SDK silently fall back to this context.
    auto unrelated = scopedTracer.tracer()->StartSpan("unrelated");
    auto scope = scopedTracer.tracer()->WithActiveSpan(unrelated);
    {
      SpanGuard guard{"root", std::nullopt};
      guard.setOk();
    }
    unrelated->End();
  }
  auto spans = scopedTracer.spans();
  ASSERT_EQ(spans.size(), 2);
  const auto& root = *spans.at(0);
  const auto& unrelated = *spans.at(1);
  ASSERT_EQ(root.GetName(), "root");
  ASSERT_EQ(unrelated.GetName(), "unrelated");
  EXPECT_FALSE(root.GetParentSpanId().IsValid());
  EXPECT_NE(root.GetTraceId(), unrelated.GetTraceId());
}

// _____________________________________________________________________________
TEST(Tracing, extractParentFromRequest) {
  // The propagator is global state that `initialize` installs. Install it
  // directly here, so that this test does not depend on an exporter.
  opentelemetry::context::propagation::GlobalTextMapPropagator::
      SetGlobalPropagator(
          opentelemetry::nostd::shared_ptr<
              opentelemetry::context::propagation::TextMapPropagator>{
              new opentelemetry::trace::propagation::HttpTraceContext{}});

  {
    auto request = makeRequestWithTraceparent(TRACEPARENT_SAMPLED);
    auto parent = extractParentFromRequest(request);
    ASSERT_TRUE(parent.has_value());
    EXPECT_EQ(toHex(parent->trace_id()), TRACE_ID);
    EXPECT_EQ(toHex(parent->span_id()), SPAN_ID);
    EXPECT_TRUE(parent->IsSampled());
  }
  {
    // No header at all: not an error, just no parent.
    auto request = ad_utility::testing::makeGetRequest("/sparql");
    EXPECT_FALSE(extractParentFromRequest(request).has_value());
  }
  {
    // A malformed header must not throw, because that would fail a request for
    // a reason that has nothing to do with the request itself. Proxies do send
    // such headers.
    for (const std::string_view traceparent :
         {"garbage", "", "00-tooshort-b7ad6b7169203331-01",
          "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
          // `ff` is the one version that the specification forbids.
          "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
          "00-00000000000000000000000000000000-b7ad6b7169203331-01",
          "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
          "zz-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}) {
      auto request = makeRequestWithTraceparent(traceparent);
      // Deliberately engaged, so that the check below cannot pass just because
      // the assignment never happened.
      std::optional<opentelemetry::trace::SpanContext> parent =
          opentelemetry::trace::SpanContext::GetInvalid();
      EXPECT_NO_THROW(parent = extractParentFromRequest(request))
          << "traceparent: " << traceparent;
      EXPECT_FALSE(parent.has_value()) << "traceparent: " << traceparent;
    }
  }
  {
    // An unknown but well-formed version has to be accepted, so that a future
    // version of the specification does not break propagation. See
    // https://www.w3.org/TR/trace-context/#versioning-of-traceparent.
    auto request = makeRequestWithTraceparent(
        "99-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    auto parent = extractParentFromRequest(request);
    ASSERT_TRUE(parent.has_value());
    EXPECT_EQ(toHex(parent->trace_id()), TRACE_ID);
  }
  {
    // The sampled flag is cleared, so the caller does not want this trace
    // recorded. The context is still a valid parent; it is the `ParentBased`
    // sampler that acts on the flag.
    auto request = makeRequestWithTraceparent(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00");
    auto parent = extractParentFromRequest(request);
    ASSERT_TRUE(parent.has_value());
    EXPECT_FALSE(parent->IsSampled());
  }
}
