//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_RUNTIMEPARAMETERS_H
#define QLEVER_RUNTIMEPARAMETERS_H

#include "util/Parameters.h"
struct RuntimeParameters {
  using Bool = ad_utility::detail::parameterRuntimeName::Bool;
  using Double = ad_utility::detail::parameterRuntimeName::Double;
  template <typename Duration>
  using Duration =
      ad_utility::detail::parameterRuntimeName::DurationParameter<Duration>;
  using MemorySizeParameter =
      ad_utility::detail::parameterRuntimeName::MemorySizeParameter;
  using SizeT = ad_utility::detail::parameterRuntimeName::SizeT;

  // If set, then unneeded variables will not be emitted as the result of
  // each operation.
  // This makes the queries faster, but leads to more cache misses if e.g.
  // variables in a SELECT clause change
  // between otherwise equal queries.
  Bool stripColumns{false, "strip-columns"};

  // If the time estimate for a sort operation is larger by more than this
  // factor than the remaining time, then the sort is canceled with a
  // timeout exception.
  Double sortEstimateCancellationFactor{3.0,
                                        "sort-estimate-cancellation-factor"};
  SizeT cacheMaxNumEntries{1000, "cache-max-num-entries"};

  MemorySizeParameter cacheMaxSize{ad_utility::MemorySize::gigabytes(30),
                                   "cache-max-size"};
  MemorySizeParameter cacheMaxSizeSingleEntry{
      ad_utility::MemorySize::gigabytes(5), "cache-max-size-single-entry"};
  SizeT lazyIndexScanQueueSize{20, "lazy-index-scan-queue-size"};
  SizeT lazyIndexScanNumThreads{10, "lazy-index-scan-num-threads"};
  Duration<std::chrono::seconds> defaultQueryTimeout{std::chrono::seconds(30),
                                                     "default-query-timeout"};
  SizeT lazyIndexScanMaxSizeMaterialization{
      1'000'000, "lazy-index-scan-max-size-materialization"};
  Bool useBinsearchTransitivePath{true, "use-binsearch-transitive-path"};
  Bool groupByHashMapEnabled{false, "group-by-hash-map-enabled"};
  Bool groupByDisableIndexScanOptimizations{
      false, "group-by-disable-index-scan-optimizations"};
  SizeT serviceMaxValueRows{10'000, "service-max-value-rows"};
  SizeT queryPlanningBudget{1500, "query-planning-budget"};
  Bool throwOnUnboundVariables{false, "throw-on-unbound-variables"};

  // Control up until which size lazy results should be cached. Caching
  // does cause significant overhead for this case.
  MemorySizeParameter cacheMaxSizeLazyResult{
      ad_utility::MemorySize::megabytes(5), "cache-max-size-lazy-result"};
  Bool websocketUpdatesEnabled{true, "websocket-updates-enabled"};
  // When the result of an index scan is smaller than a single block, then
  // its size estimate will be the size of the block divided by this
  // value.
  SizeT smallIndexScanSizeEstimateDivisor{
      5, "small-index-scan-size-estimate-divisor"};
  // Determines whether the cost estimate for a cached subtree should be
  // set to zero in query planning.
  Bool zeroCostEstimateForCachedSubtree{
      false, "zero-cost-estimate-for-cached-subtree"};
  // Maximum size for the body of requests that the server will process.
  MemorySizeParameter requestBodyLimit{ad_utility::MemorySize::gigabytes(1),
                                       "request-body-limit"};
  // SERVICE operations are not cached by default, but can be enabled
  // which has the downside that the sibling optimization where VALUES are
  // dynamically pushed into `SERVICE` is no longer used.
  Bool cacheServiceResults{false, "cache-service-results"};
  // If set to `true`, we expect the contents of URLs loaded via a LOAD to
  // not change over time. This enables caching of LOAD operations.
  Bool cacheLoadResults{false, "cache-load-results"};
  // If set to `true`, several exceptions will silently be ignored and a
  // dummy result will be returned instead.
  // This mode should only be activated when running the syntax tests of
  // the SPARQL conformance test suite.
  Bool syntaxTestMode{false, "syntax-test-mode"};
  // If set to `true`, then a division by zero in an expression will lead
  // to an
  // expression error, meaning that the result is undefined. If set to
  // false,
  // the result will be `NaN` or `infinity` respectively.
  Bool divisionByZeroIsUndef{true, "division-by-zero-is-undef"};
  // If set to `true`, the contained `FILTER` expressions in the query
  // try to set and apply a corresponding `PrefilterExpression` (see
  // `PrefilterExpressionIndex.h`) on its variable-related `IndexScan`
  // operation.
  //
  // If set to `false`, the queries `FILTER` expressions omit setting and
  // applying `PrefilterExpression`s. This is useful to set a
  // prefilter-free baseline, or for debugging, as wrong results may be
  // related to the `PrefilterExpression`s.
  Bool enablePrefilterOnIndexScans{true, "enable-prefilter-on-index-scans"};
  // The maximum number of threads to be used in `SpatialJoinAlgorithms`.
  SizeT spatialJoinMaxNumThreads{8, "spatial-join-max-num-threads"};
  // The maximum size of the `prefilterBox` for
  // `SpatialJoinAlgorithms::libspatialjoinParse()`.
  SizeT spatialJoinPrefilterMaxSize{2'500, "spatial-join-prefilter-max-size"};
  // Push joins into both children of unions if this leads to a cheaper
  // cost-estimate.
  Bool enableDistributiveUnion{true, "enable-distributive-union"};

  std::map<std::string, ad_utility::ParameterBase*> runtimeMap_;

  // Constructor: Add all parameters to the map.
  RuntimeParameters() {
    // Here all of the newly defined parameters have to be added.

    auto add = [this]<typename T>(T& parameter) {
      AD_CONTRACT_CHECK(!runtimeMap_.contains(parameter.name()));
      runtimeMap_[parameter.name()] = &parameter;
    };

    add(stripColumns);
    add(sortEstimateCancellationFactor);
    add(cacheMaxNumEntries);
    add(cacheMaxSize);
    add(cacheMaxSizeSingleEntry);
    add(lazyIndexScanQueueSize);
    add(lazyIndexScanNumThreads);
    add(lazyIndexScanMaxSizeMaterialization);
    add(useBinsearchTransitivePath);
    add(groupByHashMapEnabled);
    add(groupByDisableIndexScanOptimizations);
    add(serviceMaxValueRows);
    add(queryPlanningBudget);
    add(throwOnUnboundVariables);
    add(cacheMaxSizeLazyResult);
    add(websocketUpdatesEnabled);
    add(smallIndexScanSizeEstimateDivisor);
    add(zeroCostEstimateForCachedSubtree);
    add(requestBodyLimit);
    add(cacheServiceResults);
    add(syntaxTestMode);
    add(divisionByZeroIsUndef);
    add(enablePrefilterOnIndexScans);
    add(spatialJoinMaxNumThreads);
    add(spatialJoinPrefilterMaxSize);
    add(enableDistributiveUnion);

    defaultQueryTimeout.setParameterConstraint(
        [](std::chrono::seconds value, std::string_view parameterName) {
          if (value <= std::chrono::seconds{0}) {
            throw std::runtime_error{absl::StrCat(
                "Parameter ", parameterName, " must be strictly positive, was ",
                value.count(), "s")};
          }
        });
  }

  // Obtain a map from parameter names to parameter values.
  // This map only contains strings and is purely for logging
  // to human users.
  [[nodiscard]] ad_utility::HashMap<std::string, std::string> toMap() const;

  //  Set a parameter from a string.
  // Throws if the parameter does not exist or if the value is invalid.
  void setFromString(const std::string& parameterName,
                     const std::string& value);

  // Get all parameter names.
  std::vector<std::string> getKeys() const;

  // no copy and move possibleq
  RuntimeParameters(const RuntimeParameters&) = delete;
  RuntimeParameters& operator=(const RuntimeParameters&) = delete;
  RuntimeParameters(RuntimeParameters&&) = delete;
  RuntimeParameters& operator=(RuntimeParameters&&) = delete;
};

inline ad_utility::Synchronized<RuntimeParameters>& getRuntimeParameters() {
  static ad_utility::Synchronized<RuntimeParameters> value;
  return value;
}

template <auto ParameterPtr, typename ValueType>
void setRuntimeParameter(const ValueType& value) {
  std::invoke(ParameterPtr, *getRuntimeParameters().wlock()).set(value);
}

template <auto ParameterPtr>
auto getRuntimeParameter() {
  auto& parameter{std::invoke(ParameterPtr, *getRuntimeParameters().rlock())};

  return parameter.get();
}

#endif  // QLEVER_RUNTIMEPARAMETERS_H
