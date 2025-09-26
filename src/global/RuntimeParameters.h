//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_RUNTIMEPARAMETERS_H
#define QLEVER_RUNTIMEPARAMETERS_H

#include "util/Parameters.h"

// A set of parameters that can be accessed with a runtime and a compile time
// interface. They are currently managed using a synchronized singleton for the
// complete QLever instance.
struct RuntimeParameters {
  using Bool = ad_utility::detail::parameterShortNames::Bool;
  using Double = ad_utility::detail::parameterShortNames::Double;
  template <typename DurationTp>
  using Duration =
      ad_utility::detail::parameterShortNames::DurationParameter<DurationTp>;
  using MemorySizeParameter =
      ad_utility::detail::parameterShortNames::MemorySizeParameter;
  using SizeT = ad_utility::detail::parameterShortNames::SizeT;

  // ___________________________________________________________________________
  // IMPORTANT NOTE: IF YOU ADD PARAMETERS BELOW, ALSO REGISTER THEM IN THE
  // CONSTRUCTOR, S.T. THEY CAN ALSO BE ACCESSED VIA THE RUNTIME INTERFACE.
  // ___________________________________________________________________________

  // If set, then unneeded variables will not be emitted as the result of
  // each operation.
  // This makes the queries faster, but leads to more cache misses if e.g.
  // variables in a SELECT clause change
  // between otherwise equal queries.
  Bool stripColumns_{false, "strip-columns"};

  // If the time estimate for a sort operation is larger by more than this
  // factor than the remaining time, then the sort is canceled with a
  // timeout exception.
  Double sortEstimateCancellationFactor_{3.0,
                                         "sort-estimate-cancellation-factor"};
  SizeT cacheMaxNumEntries_{1000, "cache-max-num-entries"};

  MemorySizeParameter cacheMaxSize_{ad_utility::MemorySize::gigabytes(30),
                                    "cache-max-size"};
  MemorySizeParameter cacheMaxSizeSingleEntry_{
      ad_utility::MemorySize::gigabytes(5), "cache-max-size-single-entry"};
  SizeT lazyIndexScanQueueSize_{20, "lazy-index-scan-queue-size"};
  SizeT lazyIndexScanNumThreads_{10, "lazy-index-scan-num-threads"};
  Duration<std::chrono::seconds> defaultQueryTimeout_{std::chrono::seconds(30),
                                                      "default-query-timeout"};
  SizeT lazyIndexScanMaxSizeMaterialization_{
      1'000'000, "lazy-index-scan-max-size-materialization"};
  Bool useBinsearchTransitivePath_{true, "use-binsearch-transitive-path"};
  Bool groupByHashMapEnabled_{false, "group-by-hash-map-enabled"};
  Bool groupByDisableIndexScanOptimizations_{
      false, "group-by-disable-index-scan-optimizations"};
  SizeT serviceMaxValueRows_{10'000, "service-max-value-rows"};
  SizeT queryPlanningBudget_{1500, "query-planning-budget"};
  Bool throwOnUnboundVariables_{false, "throw-on-unbound-variables"};

  // Control up until which size lazy results should be cached. Caching
  // does cause significant overhead for this case.
  MemorySizeParameter cacheMaxSizeLazyResult_{
      ad_utility::MemorySize::megabytes(5), "cache-max-size-lazy-result"};
  Bool websocketUpdatesEnabled_{true, "websocket-updates-enabled"};
  // When the result of an index scan is smaller than a single block, then
  // its size estimate will be the size of the block divided by this
  // value.
  SizeT smallIndexScanSizeEstimateDivisor_{
      5, "small-index-scan-size-estimate-divisor"};
  // Determines whether the cost estimate for a cached subtree should be
  // set to zero in query planning.
  Bool zeroCostEstimateForCachedSubtree_{
      false, "zero-cost-estimate-for-cached-subtree"};
  // Maximum size for the body of requests that the server will process.
  MemorySizeParameter requestBodyLimit_{ad_utility::MemorySize::gigabytes(1),
                                        "request-body-limit"};
  // SERVICE operations are not cached by default, but can be enabled
  // which has the downside that the sibling optimization where VALUES are
  // dynamically pushed into `SERVICE` is no longer used.
  Bool cacheServiceResults_{false, "cache-service-results"};
  // If set to `true`, we expect the contents of URLs loaded via a LOAD to
  // not change over time. This enables caching of LOAD operations.
  Bool cacheLoadResults_{false, "cache-load-results"};
  // If set to `true`, several exceptions will silently be ignored and a
  // dummy result will be returned instead.
  // This mode should only be activated when running the syntax tests of
  // the SPARQL conformance test suite.
  Bool syntaxTestMode_{false, "syntax-test-mode"};
  // If set to `true`, then a division by zero in an expression will lead
  // to an
  // expression error, meaning that the result is undefined. If set to
  // false,
  // the result will be `NaN` or `infinity` respectively.
  Bool divisionByZeroIsUndef_{true, "division-by-zero-is-undef"};
  // If set to `true`, the contained `FILTER` expressions in the query
  // try to set and apply a corresponding `PrefilterExpression` (see
  // `PrefilterExpressionIndex.h`) on its variable-related `IndexScan`
  // operation.
  //
  // If set to `false`, the queries `FILTER` expressions omit setting and
  // applying `PrefilterExpression`s. This is useful to set a
  // prefilter-free baseline, or for debugging, as wrong results may be
  // related to the `PrefilterExpression`s.
  Bool enablePrefilterOnIndexScans_{true, "enable-prefilter-on-index-scans"};
  // The maximum number of threads to be used in `SpatialJoinAlgorithms`.
  SizeT spatialJoinMaxNumThreads_{8, "spatial-join-max-num-threads"};
  // The maximum size of the `prefilterBox` for
  // `SpatialJoinAlgorithms::libspatialjoinParse()`.
  SizeT spatialJoinPrefilterMaxSize_{2'500, "spatial-join-prefilter-max-size"};
  // Push joins into both children of unions if this leads to a cheaper
  // cost-estimate.
  Bool enableDistributiveUnion_{true, "enable-distributive-union"};

  // ___________________________________________________________________________
  // IMPORTANT NOTE: IF YOU ADD PARAMETERS ABOVE, ALSO REGISTER THEM IN THE
  // CONSTRUCTOR, S.T. THEY CAN ALSO BE ACCESSED VIA THE RUNTIME INTERFACE.
  // ___________________________________________________________________________

  std::map<std::string, ad_utility::ParameterBase*, std::less<>> runtimeMap_;

  // Constructor: Add all parameters to the map.
  RuntimeParameters();

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

  // Copy and move are disallowed.
  RuntimeParameters(const RuntimeParameters&) = delete;
  RuntimeParameters& operator=(const RuntimeParameters&) = delete;
  RuntimeParameters(RuntimeParameters&&) = delete;
  RuntimeParameters& operator=(RuntimeParameters&&) = delete;
};

// Get synchronized access to the global runtime parameter singleton.
inline ad_utility::Synchronized<RuntimeParameters> globalRuntimeParameters;

// Set a parameter, specified by a pointer-to-member to the `RuntimeParameters`
// struct to the specified `value`, e.g.
// `setRuntimeParameter<&RuntimeParameters::nameOfParameter>(42)`.
template <auto ParameterPtr, typename ValueType>
void setRuntimeParameter(const ValueType& value) {
  std::invoke(ParameterPtr, *globalRuntimeParameters.wlock()).set(value);
}

// Get the current value of the runtime parameter specified by the
// `ParameterPtr` (see `setRuntimeParameter` above for details).
// The value has to be returned as an object, otherwise we might get data races.
// That's why the return type is `auto` and not `decltype(auto)` or `const
// auto&`.
template <auto ParameterPtr>
auto getRuntimeParameter() {
  // It is important that we make the deep copy of the reference that `get()`
  // returns before the lock is released because the `rlock()` object is
  // destroyed. This is achieved by directly returning a copy of the parameter
  // value (the function returns `auto`, see above).
  return std::invoke(ParameterPtr, *globalRuntimeParameters.rlock()).get();
}

#endif  // QLEVER_RUNTIMEPARAMETERS_H
