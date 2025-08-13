//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_RUNTIMEPARAMETERS_H
#define QLEVER_RUNTIMEPARAMETERS_H

#include "util/Parameters.h"

inline auto& RuntimeParameters() {
  using ad_utility::detail::parameterShortNames::Bool;
  using ad_utility::detail::parameterShortNames::Double;
  using ad_utility::detail::parameterShortNames::DurationParameter;
  using ad_utility::detail::parameterShortNames::MemorySizeParameter;
  using ad_utility::detail::parameterShortNames::SizeT;
  // NOTE: It is important that the value of the static variable is created by
  // an immediately invoked lambda, otherwise we get really strange segfaults on
  // Clang 16 and 17.
  // TODO<joka921> Figure out whether this is a bug in Clang or whether we
  // clearly misunderstand something about static initialization.
  static ad_utility::Parameters params = []() {
    using namespace std::chrono_literals;
    using namespace ad_utility::memory_literals;
    auto ensureStrictPositivity = [](auto&& parameter) {
      parameter.setParameterConstraint(
          [](std::chrono::seconds value, std::string_view parameterName) {
            if (value <= 0s) {
              throw std::runtime_error{absl::StrCat(
                  "Parameter ", parameterName,
                  " must be strictly positive, was ", value.count(), "s")};
            }
          });
      return AD_FWD(parameter);
    };
    return ad_utility::Parameters{
        // If the time estimate for a sort operation is larger by more than this
        // factor than the remaining time, then the sort is canceled with a
        // timeout exception.
        Double<"sort-estimate-cancellation-factor">{3.0},
        SizeT<"cache-max-num-entries">{1000},
        MemorySizeParameter<"cache-max-size">{30_GB},
        MemorySizeParameter<"cache-max-size-single-entry">{5_GB},
        SizeT<"lazy-index-scan-queue-size">{20},
        SizeT<"lazy-index-scan-num-threads">{10},
        ensureStrictPositivity(
            DurationParameter<std::chrono::seconds, "default-query-timeout">{
                30s}),
        SizeT<"lazy-index-scan-max-size-materialization">{1'000'000},
        Bool<"use-binsearch-transitive-path">{true},
        Bool<"group-by-hash-map-enabled">{false},
        Bool<"group-by-disable-index-scan-optimizations">{false},
        SizeT<"service-max-value-rows">{10'000},
        SizeT<"query-planning-budget">{1500},
        Bool<"throw-on-unbound-variables">{false},
        // Control up until which size lazy results should be cached. Caching
        // does cause significant overhead for this case.
        MemorySizeParameter<"cache-max-size-lazy-result">{5_MB},
        Bool<"websocket-updates-enabled">{true},
        // When the result of an index scan is smaller than a single block, then
        // its size estimate will be the size of the block divided by this
        // value.
        SizeT<"small-index-scan-size-estimate-divisor">{5},
        // Determines whether the cost estimate for a cached subtree should be
        // set to zero in query planning.
        Bool<"zero-cost-estimate-for-cached-subtree">{false},
        // Maximum size for the body of requests that the server will process.
        MemorySizeParameter<"request-body-limit">{100_MB},
        // SERVICE operations are not cached by default, but can be enabled
        // which has the downside that the sibling optimization where VALUES are
        // dynamically pushed into `SERVICE` is no longer used.
        Bool<"cache-service-results">{false},
        // If set to `true`, we expect the contents of URLs loaded via a LOAD to
        // not change over time. This enables caching of LOAD operations.
        Bool<"cache-load-results">{false},
        // If set to `true`, several exceptions will silently be ignored and a
        // dummy result will be returned instead.
        // This mode should only be activated when running the syntax tests of
        // the SPARQL conformance test suite.
        Bool<"syntax-test-mode">{false},
        // If set to `true`, then a division by zero in an expression will lead
        // to an
        // expression error, meaning that the result is undefined. If set to
        // false,
        // the result will be `NaN` or `infinity` respectively.
        Bool<"division-by-zero-is-undef">{true},
        // If set to `true`, the contained `FILTER` expressions in the query
        // try to set and apply a corresponding `PrefilterExpression` (see
        // `PrefilterExpressionIndex.h`) on its variable-related `IndexScan`
        // operation.
        //
        // If set to `false`, the queries `FILTER` expressions omit setting and
        // applying `PrefilterExpression`s. This is useful to set a
        // prefilter-free baseline, or for debugging, as wrong results may be
        // related to the `PrefilterExpression`s.
        Bool<"enable-prefilter-on-index-scans">{true},
        // If set, then unneeded variables will not be emitted as the result of
        // each operation.
        // This makes the queries faster, but leads to more cache misses if e.g.
        // variables in a SELECT clause change
        // between otherwise equal queries.
        Bool<"strip-columns">{false},
        // The maximum number of threads to be used in `SpatialJoinAlgorithms`.
        SizeT<"spatial-join-max-num-threads">{8},
        // The maximum size of the `prefilterBox` for
        // `SpatialJoinAlgorithms::libspatialjoinParse()`.
        SizeT<"spatial-join-prefilter-max-size">{2'500},
    };
  }();
  return params;
}

#endif  // QLEVER_RUNTIMEPARAMETERS_H
