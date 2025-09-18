//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_RUNTIMEPARAMETERS_H
#define QLEVER_RUNTIMEPARAMETERS_H

#include "util/Parameters.h"

DECLARE_PAREMETER(SortEstimateCancellationFactor,
                  "sort-estimate-cancellation-factor");
DECLARE_PAREMETER(CacheMaxNumEntries, "cache-max-num-entries");
DECLARE_PAREMETER(CacheMaxSize, "cache-max-size");
DECLARE_PAREMETER(CacheMaxSizeSingleEntry, "cache-max-size-single-entry");
DECLARE_PAREMETER(DefaultQueryTimeout, "default-query-timeout");
DECLARE_PAREMETER(LazyIndexScanQueueSize, "lazy-index-scan-queue-size");
DECLARE_PAREMETER(LazyIndexScanNumThreads, "lazy-index-scan-num-threads");
DECLARE_PAREMETER(LazyIndexScanMaxSizeMaterialization,
                  "lazy-index-scan-max-size-materialization");
DECLARE_PAREMETER(UseBinsearchTransitivePath, "use-binsearch-transitive-path");
DECLARE_PAREMETER(GroupByHashMapEnabled, "group-by-hash-map-enabled");
DECLARE_PAREMETER(GroupByDisableIndexScanOptimizations,
                  "group-by-disable-index-scan-optimizations");
DECLARE_PAREMETER(ServiceMaxValueRows, "service-max-value-rows");
DECLARE_PAREMETER(QueryPlanningBudget, "query-planning-budget");
DECLARE_PAREMETER(ThrowOnUnboundVariables, "throw-on-unbound-variables");
DECLARE_PAREMETER(CacheMaxSizeLazyResult, "cache-max-size-lazy-result");
DECLARE_PAREMETER(WebsocketUpdatesEnabled, "websocket-updates-enabled");
DECLARE_PAREMETER(SmallIndexScanSizeEstimateDivisor,
                  "small-index-scan-size-estimate-divisor");
DECLARE_PAREMETER(ZeroCostEstimateForCachedSubtree,
                  "zero-cost-estimate-for-cached-subtree");
DECLARE_PAREMETER(RequestBodyLimit, "request-body-limit");
DECLARE_PAREMETER(CacheServiceResults, "cache-service-results");
DECLARE_PAREMETER(CacheLoadResults, "cache-load-results");
DECLARE_PAREMETER(SyntaxTestMode, "syntax-test-mode");
DECLARE_PAREMETER(DivisionByZeroIsUndef, "division-by-zero-is-undef");
DECLARE_PAREMETER(EnablePrefilterOnIndexScans,
                  "enable-prefilter-on-index-scans");
DECLARE_PAREMETER(StripColumnsParameter, "strip-columns");
DECLARE_PAREMETER(SpatialJoinMaxNumThreads, "spatial-join-max-num-threads");
DECLARE_PAREMETER(SpatialJoinPrefilterMaxSize,
                  "spatial-join-prefilter-max-size");
DECLARE_PAREMETER(EnableDistributiveUnion, "enable-distributive-union");

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
        Double<SortEstimateCancellationFactor>{3.0},
        SizeT<CacheMaxNumEntries>{1000},
        MemorySizeParameter<CacheMaxSize>{30_GB},
        MemorySizeParameter<CacheMaxSizeSingleEntry>{5_GB},
        SizeT<LazyIndexScanQueueSize>{20},
        SizeT<LazyIndexScanNumThreads>{10},
        ensureStrictPositivity(
            DurationParameter<std::chrono::seconds, DefaultQueryTimeout>{30s}),
        SizeT<LazyIndexScanMaxSizeMaterialization>{1'000'000},
        Bool<UseBinsearchTransitivePath>{true},
        Bool<GroupByHashMapEnabled>{false},
        Bool<GroupByDisableIndexScanOptimizations>{false},
        SizeT<ServiceMaxValueRows>{10'000},
        SizeT<QueryPlanningBudget>{1500},
        Bool<ThrowOnUnboundVariables>{false},
        // Control up until which size lazy results should be cached. Caching
        // does cause significant overhead for this case.
        MemorySizeParameter<CacheMaxSizeLazyResult>{5_MB},
        Bool<WebsocketUpdatesEnabled>{true},
        // When the result of an index scan is smaller than a single block, then
        // its size estimate will be the size of the block divided by this
        // value.
        SizeT<SmallIndexScanSizeEstimateDivisor>{5},
        // Determines whether the cost estimate for a cached subtree should be
        // set to zero in query planning.
        Bool<ZeroCostEstimateForCachedSubtree>{false},
        // Maximum size for the body of requests that the server will process.
        MemorySizeParameter<RequestBodyLimit>{100_MB},
        // SERVICE operations are not cached by default, but can be enabled
        // which has the downside that the sibling optimization where VALUES are
        // dynamically pushed into `SERVICE` is no longer used.
        Bool<CacheServiceResults>{false},
        // If set to `true`, we expect the contents of URLs loaded via a LOAD to
        // not change over time. This enables caching of LOAD operations.
        Bool<CacheLoadResults>{false},
        // If set to `true`, several exceptions will silently be ignored and a
        // dummy result will be returned instead.
        // This mode should only be activated when running the syntax tests of
        // the SPARQL conformance test suite.
        Bool<SyntaxTestMode>{false},
        // If set to `true`, then a division by zero in an expression will lead
        // to an
        // expression error, meaning that the result is undefined. If set to
        // false,
        // the result will be `NaN` or `infinity` respectively.
        Bool<DivisionByZeroIsUndef>{true},
        // If set to `true`, the contained `FILTER` expressions in the query
        // try to set and apply a corresponding `PrefilterExpression` (see
        // `PrefilterExpressionIndex.h`) on its variable-related `IndexScan`
        // operation.
        //
        // If set to `false`, the queries `FILTER` expressions omit setting and
        // applying `PrefilterExpression`s. This is useful to set a
        // prefilter-free baseline, or for debugging, as wrong results may be
        // related to the `PrefilterExpression`s.
        Bool<EnablePrefilterOnIndexScans>{true},
        // If set, then unneeded variables will not be emitted as the result of
        // each operation.
        // This makes the queries faster, but leads to more cache misses if e.g.
        // variables in a SELECT clause change
        // between otherwise equal queries.
        Bool<StripColumnsParameter>{false},
        // The maximum number of threads to be used in `SpatialJoinAlgorithms`.
        SizeT<SpatialJoinMaxNumThreads>{8},
        // The maximum size of the `prefilterBox` for
        // `SpatialJoinAlgorithms::libspatialjoinParse()`.
        SizeT<SpatialJoinPrefilterMaxSize>{2'500},
        // Push joins into both children of unions if this leads to a cheaper
        // cost-estimate.
        Bool<EnableDistributiveUnion>{true},
    };
  }();
  return params;
}

#endif  // QLEVER_RUNTIMEPARAMETERS_H
