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
using namespace tracingTestHelpers;
using tracingTestHelpers::ScopedInMemoryTracer;

// A `traceparent` header as defined by W3C Trace Context: version, trace id,
// span id and flags. The last byte is `01` for "sampled".
constexpr const char* TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
constexpr const char* SPAN_ID = "b7ad6b7169203331";
constexpr const char* TRACEPARENT_SAMPLED =
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

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

auto makeRequestWithTraceparent(std::string_view traceparent) {
  auto request = ad_utility::testing::makeGetRequest("/sparql");
  request.set("traceparent", boost::beast::string_view{traceparent.data(),
                                                       traceparent.size()});
  return request;
}
}  // namespace

// _____________________________________________________________________________
TEST(Tracing, defaultConstructedHandleDoesNothing) {
  auto providerBefore = opentelemetry::trace::Provider::GetTracerProvider();
  {
    TracingHandle handle;
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
TEST(Tracing, initializeInstallsARecordingProvider) {
  ScopedSilentOtelLog silentOtelLog;
  auto providerBefore = opentelemetry::trace::Provider::GetTracerProvider();
  auto handle = initialize();
  EXPECT_NE(opentelemetry::trace::Provider::GetTracerProvider(),
            providerBefore);
  SpanGuard guard{"recorded", std::nullopt};
  EXPECT_TRUE(guard.span().IsRecording());
  EXPECT_TRUE(guard.context().IsValid());
  EXPECT_TRUE(guard.context().IsSampled());
  guard.setOk();
}

// _____________________________________________________________________________
TEST(Tracing, spanGuardEndsSpanAndRecordsSuccess) {
  {
    ScopedInMemoryTracer scopedTracer;
    {
      SpanGuard guard{"work", std::nullopt};
      guard.setOk();
      // Not ended yet, so nothing has been exported.
      EXPECT_THAT(scopedTracer.spans(), testing::IsEmpty());
    }
    EXPECT_THAT(scopedTracer.spans(),
                testing::ElementsAre(SpanWithName(
                    "work", StatusIs(opentelemetry::trace::StatusCode::kOk))));
  }
  {
    ScopedInMemoryTracer scopedTracer;
    {
      // Neither `setOk` nor `setError` are called. Can happen when a coroutine
      // is cancelled.
      SpanGuard guard{"cancelled", std::nullopt};
    }
    EXPECT_THAT(
        scopedTracer.spans(),
        testing::ElementsAre(SpanWithName(
            "cancelled",
            testing::AllOf(StatusIs(opentelemetry::trace::StatusCode::kError),
                           DescriptionIs("unfinished")))));
  }
  {
    ScopedInMemoryTracer scopedTracer;
    {
      SpanGuard guard{"failing", std::nullopt};
      guard.setError("timeout", "took too long");
    }

    EXPECT_THAT(
        scopedTracer.spans(),
        testing::UnorderedElementsAre(SpanWithName(
            "failing",
            testing::AllOf(
                StatusIs(opentelemetry::trace::StatusCode::kError),
                DescriptionIs("took too long"),
                Attributes(testing::UnorderedElementsAre(
                    Attribute<std::string>("error.type", "timeout")))))));
  }
  {
    ScopedInMemoryTracer scopedTracer;
    {
      SpanGuard guard{"throwing", std::nullopt};
      guard.recordException(std::runtime_error{"something broke"}, "internal");
    }

    EXPECT_THAT(
        scopedTracer.spans(),
        testing::UnorderedElementsAre(SpanWithName(
            "throwing",
            testing::AllOf(
                StatusIs(opentelemetry::trace::StatusCode::kError),
                Events(testing::ElementsAre(Event(
                    "exception",
                    Attributes(testing::UnorderedElementsAre(
                        Attribute<std::string>("exception.type", "internal"),
                        Attribute<std::string>("exception.message",
                                               "something broke"))))))))));
  }
  {
    ScopedInMemoryTracer scopedTracer;
    {
      SpanGuard parent{"parent", std::nullopt};
      {
        SpanGuard child{"child", parent.context()};
        child.setOk();
      }
      parent.setOk();
    }
    EXPECT_THAT(
        scopedTracer.spans(),
        testing::AllOf(
            AllSpansAreDirectChildrenOfRoot("parent"),
            testing::UnorderedElementsAre(
                SpanWithName("parent",
                             StatusIs(opentelemetry::trace::StatusCode::kOk)),
                SpanWithName(
                    "child",
                    StatusIs(opentelemetry::trace::StatusCode::kOk)))));
  }
  {
    // A root span for a new trace is created while another trace is active on
    // the thread.
    ScopedInMemoryTracer scopedTracer;
    {
      auto unrelated = scopedTracer.tracer()->StartSpan("unrelated");
      auto scope = scopedTracer.tracer()->WithActiveSpan(unrelated);
      {
        SpanGuard guard{"root", std::nullopt};
        guard.setOk();
      }
      unrelated->End();
    }
    EXPECT_THAT(scopedTracer.spans(),
                testing::AllOf(SpansAreInDistinctTraces(),
                               testing::UnorderedElementsAre(
                                   SpanWithName("root", IsRootSpan()),
                                   SpanWithName("unrelated", IsRootSpan()))));
  }
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
  using namespace tracingTestHelpers;

  auto TraceId = [](const std::string& hex) {
    return AD_PROPERTY(opentelemetry::trace::SpanContext, trace_id, IdIs(hex));
  };
  auto SpanId = [](const std::string& hex) {
    return AD_PROPERTY(opentelemetry::trace::SpanContext, span_id, IdIs(hex));
  };
  auto IsSampled = []() {
    return AD_PROPERTY(opentelemetry::trace::SpanContext, IsSampled, true);
  };
  auto expect = [](const auto& request, const auto& m,
                   ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto t = generateLocationTrace(loc);
    EXPECT_THAT(extractParentFromRequest(request), m);
  };

  expect(makeRequestWithTraceparent(TRACEPARENT_SAMPLED),
         testing::Optional(
             testing::AllOf(TraceId(TRACE_ID), SpanId(SPAN_ID), IsSampled())));
  expect(ad_utility::testing::makeGetRequest("/sparql"),
         testing::Eq(std::nullopt));
  // An unknown but well-formed version is accepted.
  expect(makeRequestWithTraceparent(
             "99-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
         testing::Optional(TraceId(TRACE_ID)));
  // The sampling flag is propagated from the input.
  expect(makeRequestWithTraceparent(
             "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"),
         testing::Optional(testing::Not(IsSampled())));
  // Various malformed headers
  for (const std::string_view traceparent :
       // Invalid forma
       {"garbage", "", "00-tooshort-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
        // `ff` is forbidden as version
        "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        // All zeroes is invalid for both trace and span id
        "00-00000000000000000000000000000000-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
        // Version (`zz`) is not hex
        "zz-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}) {
    expect(makeRequestWithTraceparent(traceparent), testing::Eq(std::nullopt));
  }
}
