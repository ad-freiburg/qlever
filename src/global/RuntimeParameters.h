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
        // The maximum number of threads to be used in `SpatialJoinAlgorithms`.
        SizeT<"spatial-join-max-num-threads">{8},
        // The maximum size of the `prefilterBox` for
        // `SpatialJoinAlgorithms::libspatialjoinParse()`.
        SizeT<"spatial-join-prefilter-max-size">{2'500},
        // Push joins into both children of unions if this leads to a cheaper
        // cost-estimate.
        Bool<"enable-distributive-union">{true},
    };
  }();
  return params;
}

struct RuntimeParametersNew {
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
  Bool stripColumns_{false, "strip-columns"};

  // If the time estimate for a sort operation is larger by more than this
  // factor than the remaining time, then the sort is canceled with a
  // timeout exception.
  Double sortEstimateCancellationFactor{3.0,
                                        "sort-estimate-cancellation-factor"};
  SizeT cacheMaxNumEntries{1000, "cache-max-num-entries"};

  // TODO wrap into lambda to get over using operator limitation and use _GB
  // operator
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
  MemorySizeParameter requestBodyLimit{ad_utility::MemorySize::megabytes(100),
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

  RuntimeParametersNew() {
    // Here all of the newly defined parameters have to be added.
    runtimeMap_[stripColumns_.name()] = &stripColumns_;
    runtimeMap_[sortEstimateCancellationFactor.name()] = &stripColumns_;
    runtimeMap_[cacheMaxNumEntries.name()] = &cacheMaxNumEntries;
    runtimeMap_[cacheMaxSize.name()] = &cacheMaxSize;
    runtimeMap_[cacheMaxSizeSingleEntry.name()] = &cacheMaxSizeSingleEntry;
    runtimeMap_[lazyIndexScanQueueSize.name()] = &lazyIndexScanQueueSize;
    runtimeMap_[lazyIndexScanNumThreads.name()] = &lazyIndexScanNumThreads;

    runtimeMap_[lazyIndexScanMaxSizeMaterialization.name()] =
        &lazyIndexScanMaxSizeMaterialization;
    runtimeMap_[useBinsearchTransitivePath.name()] =
        &useBinsearchTransitivePath;
    runtimeMap_[groupByHashMapEnabled.name()] = &groupByHashMapEnabled;
    runtimeMap_[groupByDisableIndexScanOptimizations.name()] =
        &groupByDisableIndexScanOptimizations;
    runtimeMap_[serviceMaxValueRows.name()] = &serviceMaxValueRows;
    runtimeMap_[queryPlanningBudget.name()] = &queryPlanningBudget;
    runtimeMap_[throwOnUnboundVariables.name()] = &throwOnUnboundVariables;
    runtimeMap_[cacheMaxSizeLazyResult.name()] = &cacheMaxSizeLazyResult;

    runtimeMap_[websocketUpdatesEnabled.name()] = &websocketUpdatesEnabled;
    runtimeMap_[smallIndexScanSizeEstimateDivisor.name()] =
        &smallIndexScanSizeEstimateDivisor;
    runtimeMap_[zeroCostEstimateForCachedSubtree.name()] =
        &zeroCostEstimateForCachedSubtree;
    runtimeMap_[requestBodyLimit.name()] = &requestBodyLimit;
    runtimeMap_[cacheServiceResults.name()] = &cacheServiceResults;

    runtimeMap_[syntaxTestMode.name()] = &syntaxTestMode;
    runtimeMap_[divisionByZeroIsUndef.name()] = &divisionByZeroIsUndef;
    runtimeMap_[enablePrefilterOnIndexScans.name()] =
        &enablePrefilterOnIndexScans;
    runtimeMap_[spatialJoinMaxNumThreads.name()] = &spatialJoinMaxNumThreads;
    runtimeMap_[spatialJoinPrefilterMaxSize.name()] =
        &spatialJoinPrefilterMaxSize;
    runtimeMap_[enableDistributiveUnion.name()] = &enableDistributiveUnion;
  }

  // Obtain a map from parameter names to parameter values.
  // This map only contains strings and is purely for logging
  // to human users.
  [[nodiscard]] ad_utility::HashMap<std::string, std::string> toMap() const {
    ad_utility::HashMap<std::string, std::string> result;
    for (const auto& [name, parameter] : runtimeMap_) {
      result[name] = parameter->toString();
    }
    return result;
  }

  void setFromString(const std::string& parameterName,
                     const std::string& value) {
    if (!runtimeMap_.contains(parameterName)) {
      throw std::runtime_error{"No parameter with name " +
                               std::string{parameterName} + " exists"};
    }
    try {
      // Call the virtual set(std::string) function on the
      // correct ParameterBase& in the `_runtimePointers`.
      runtimeMap_.at(parameterName)->setFromString(value);
    } catch (const std::exception& e) {
      throw std::runtime_error("Could not set parameter " +
                               std::string{parameterName} + " to value " +
                               value + ". Exception was: " + e.what());
    }
  }

  template <typename ParameterType>
  void set(const std::string& parameterName,
           const ParameterType::value_type& value) {
    if (!runtimeMap_.contains(parameterName)) {
      throw std::runtime_error{"No parameter with name " +
                               std::string{parameterName} + " exists"};
    }
    try {
      // Call the virtual set(std::string) function on the
      // correct ParameterBase& in the `_runtimePointers`.
      static_cast<ParameterType*>(runtimeMap_.at(parameterName))->set(value);
    } catch (const std::exception& e) {
      throw std::runtime_error("Could not set parameter " +
                               std::string{parameterName} + " to value " +
                               // TODO solve this properly
                               //  ParameterType(value).toString() +
                               ". Exception was: " + e.what());
    }
  }

  template <typename ParameterType>
  ParameterType::value_type get(const std::string& parameterName) const {
    if (!runtimeMap_.contains(parameterName)) {
      throw std::runtime_error{"No parameter with name " +
                               std::string{parameterName} + " exists"};
    }
    return static_cast<ParameterType*>(runtimeMap_.at(parameterName))->get();
  }

  std::vector<std::string> getKeys() const {
    static std::vector<std::string> keys = [this]() {
      std::vector<std::string> result;
      result.reserve(runtimeMap_.size());
      for (const auto& [name, _] : runtimeMap_) {
        result.push_back(name);
      }
      return result;
    }();

    return keys;
  }
  // TODO<BMW> Delete copying and moving (to make the map work)
};

inline ad_utility::Synchronized<RuntimeParametersNew>& runtimeParametersNew() {
  static ad_utility::Synchronized<RuntimeParametersNew> value;
  return value;
}

#endif  // QLEVER_RUNTIMEPARAMETERS_H
