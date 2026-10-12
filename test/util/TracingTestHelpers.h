// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_TRACINGTESTHELPERS_H
#define QLEVER_TEST_UTIL_TRACINGTESTHELPERS_H

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/exporters/memory/in_memory_span_data.h>
#include <opentelemetry/exporters/memory/in_memory_span_exporter_factory.h>
#include <opentelemetry/nostd/span.h>
#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/span_data.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "./GTestHelpers.h"
#include "util/HashMap.h"

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

  std::shared_ptr<TracerProvider> previousProvider_;
  std::shared_ptr<TextMapPropagator> previousPropagator_;
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
        std::shared_ptr<TracerProvider>{std::move(provider)});
    opentelemetry::context::propagation::GlobalTextMapPropagator::
        SetGlobalPropagator(
            std::make_shared<
                opentelemetry::trace::propagation::HttpTraceContext>());
  }

  ~ScopedInMemoryTracer() {
    opentelemetry::trace::Provider::SetTracerProvider(previousProvider_);
    opentelemetry::context::propagation::GlobalTextMapPropagator::
        SetGlobalPropagator(previousPropagator_);
  }

  ScopedInMemoryTracer(const ScopedInMemoryTracer&) = delete;
  ScopedInMemoryTracer& operator=(const ScopedInMemoryTracer&) = delete;

  // The tracer that tests should create their spans with.
  std::shared_ptr<opentelemetry::trace::Tracer> tracer() const {
    return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
        "qlever.test");
  }

  // All spans that have ended since the last call. Note that this *drains* the
  // underlying buffer, so it must only be called once per assertion block.
  std::vector<std::unique_ptr<SpanData>> spans() const {
    return spanData_->GetSpans();
  }
};

template <typename Id>
std::string traceIdToHex(const Id& id) {
  std::string hex(2 * Id::kSize, '\0');
  id.ToLowerBase16(std::span<char, 2 * Id::kSize>{hex.data(), hex.size()});
  return hex;
}

template <typename T>
auto Attribute(const std::string& key,
               const testing::Matcher<const T&>& valueMatcher) {
  return testing::Pair(key, testing::VariantWith<T>(valueMatcher));
}

template <typename T, typename MatcherT>
testing::Matcher<const opentelemetry::sdk::trace::SpanData&> HasAttribute(
    std::string key, const MatcherT& matcher) {
  return AD_PROPERTY(
      opentelemetry::sdk::trace::SpanData, GetAttributes,
      testing::Contains(testing::Pair(key, testing::VariantWith<T>(matcher))));
}

inline testing::Matcher<
    const std::unique_ptr<opentelemetry::sdk::trace::SpanData>&>
SpanWithName(const std::string& name,
             const testing::Matcher<const opentelemetry::sdk::trace::SpanData&>&
                 m = testing::_) {
  return testing::Pointee(testing::AllOf(
      AD_PROPERTY(opentelemetry::sdk::trace::SpanData, GetName, name), m));
}

MATCHER_P2(
    HasSpan, name, matcher,
    absl::StrCat(
        negation ? "does not have exactly one span named \""
                 : "has exactly one span named \"",
        name, "\" that ",
        testing::DescribeMatcher<const opentelemetry::sdk::trace::SpanData&>(
            matcher))) {
  const opentelemetry::sdk::trace::SpanData* match = nullptr;
  size_t numMatches = 0;
  for (const auto& span : arg) {
    auto spanName = span->GetName();
    if (std::string_view{spanName.data(), spanName.size()} ==
        std::string_view{name}) {
      ++numMatches;
      match = std::to_address(span);
    }
  }
  if (numMatches != 1) {
    *result_listener << "which has " << numMatches << " spans named \"" << name
                     << '"';
    return false;
  }
  *result_listener << "whose span named \"" << name << "\" is one ";
  return testing::ExplainMatchResult(matcher, *match, result_listener);
}

inline testing::Matcher<const opentelemetry::sdk::trace::SpanData&> StatusIs(
    opentelemetry::trace::StatusCode code) {
  return AD_PROPERTY(opentelemetry::sdk::trace::SpanData, GetStatus, code);
}

inline testing::Matcher<const opentelemetry::sdk::trace::SpanData&>
DescriptionIs(const std::string& desc) {
  return AD_PROPERTY(opentelemetry::sdk::trace::SpanData, GetDescription, desc);
}

MATCHER_P(IdIs, hex,
          absl::StrCat(negation ? "is not the id \"" : "is the id \"", hex,
                       "\"")) {
  auto actual = traceIdToHex(arg);
  *result_listener << "which is the id \"" << actual << '"';
  return actual == hex;
}

inline auto TraceIdIs(const std::string& hex) {
  return AD_PROPERTY(opentelemetry::sdk::trace::SpanData, GetTraceId,
                     IdIs(hex));
}
inline auto SpanIdIs(const std::string& hex) {
  return AD_PROPERTY(opentelemetry::sdk::trace::SpanData, GetSpanId, IdIs(hex));
}
inline auto ParentSpanIdIs(const std::string& hex) {
  return AD_PROPERTY(opentelemetry::sdk::trace::SpanData, GetParentSpanId,
                     IdIs(hex));
}

testing::Matcher<const opentelemetry::sdk::trace::SpanData&> Events(
    const auto m) {
  return AD_PROPERTY(opentelemetry::sdk::trace::SpanData, GetEvents, m);
}

inline testing::Matcher<const opentelemetry::sdk::trace::SpanDataEvent&> Event(
    const std::string& name,
    const testing::Matcher<const opentelemetry::sdk::trace::SpanDataEvent&>& m =
        testing::_) {
  return testing::AllOf(
      AD_PROPERTY(opentelemetry::sdk::trace::SpanDataEvent, GetName, name), m);
}

MATCHER_P(AllSpansAreDirectChildrenOfRoot, rootName,
          absl::StrCat(negation ? "one span is not a direct child of \""
                                : "all other spans are direct children of \"",
                       rootName, "\"")) {
  auto rootView = arg | ql::ranges::views::filter([&](const auto& elem) {
                    return elem->GetName() == rootName;
                  });
  auto numRootElements = ql::ranges::distance(rootView);
  if (numRootElements != 1) {
    *result_listener << "which has " << numRootElements
                     << " spans that with the root name \"" << rootName << "\"";
  }
  const auto* root = rootView.begin()->get();

  auto rootSpanId = traceIdToHex(root->GetSpanId());
  return testing::ExplainMatchResult(
      testing::Each(testing::Pointee(testing::AllOf(
          // All spans belong to the same trace as the root trace
          TraceIdIs(traceIdToHex(root->GetTraceId())),
          // Spans are either the root span or a direct child of it
          testing::AnyOf(SpanIdIs(rootSpanId), ParentSpanIdIs(rootSpanId))))),
      arg, result_listener);
}

// Matches a span that starts its own trace, i.e. that has no parent.
inline testing::Matcher<const opentelemetry::sdk::trace::SpanData&>
IsRootSpan() {
  return AD_PROPERTY(opentelemetry::sdk::trace::SpanData, GetParentSpanId,
                     AD_PROPERTY(opentelemetry::trace::SpanId, IsValid, false));
}

// Matches a range of spans in which no two spans belong to the same trace.
MATCHER(SpansAreInDistinctTraces, negation
                                      ? "has two spans in the same trace"
                                      : "has no two spans in the same trace") {
  ad_utility::HashMap<std::string, std::string> spanNameByTraceId;
  for (const auto& span : arg) {
    auto name = span->GetName();
    auto [it, inserted] =
        spanNameByTraceId.emplace(traceIdToHex(span->GetTraceId()),
                                  std::string{name.data(), name.size()});
    if (!inserted) {
      *result_listener << "where the spans \"" << it->second << "\" and \""
                       << std::string_view{name.data(), name.size()}
                       << "\" share the trace id \"" << it->first << '"';
      return false;
    }
  }
  return true;
}

MATCHER_P(Attributes, m, "") {
  return testing::ExplainMatchResult(m, arg.GetAttributes(), result_listener);
}

}  // namespace tracingTestHelpers

#endif  // QLEVER_TEST_UTIL_TRACINGTESTHELPERS_H
