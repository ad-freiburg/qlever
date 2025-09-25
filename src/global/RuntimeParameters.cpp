// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 NN, BMW
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// BMW =  Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "global/RuntimeParameters.h"

#include "backports/algorithm.h"

// _____________________________________________________________________________
RuntimeParameters::RuntimeParameters() {
  // Here all of the newly defined parameters have to be added.

  auto add = [this](auto& parameter) {
    AD_CONTRACT_CHECK(!runtimeMap_.contains(parameter.name()));
    runtimeMap_[parameter.name()] = &parameter;
  };

  add(stripColumns_);
  add(sortEstimateCancellationFactor_);
  add(cacheMaxNumEntries_);
  add(cacheMaxSize_);
  add(cacheMaxSizeSingleEntry_);
  add(lazyIndexScanQueueSize_);
  add(lazyIndexScanNumThreads_);
  add(lazyIndexScanMaxSizeMaterialization_);
  add(useBinsearchTransitivePath_);
  add(groupByHashMapEnabled_);
  add(groupByDisableIndexScanOptimizations_);
  add(serviceMaxValueRows_);
  add(queryPlanningBudget_);
  add(throwOnUnboundVariables_);
  add(cacheMaxSizeLazyResult_);
  add(websocketUpdatesEnabled_);
  add(smallIndexScanSizeEstimateDivisor_);
  add(zeroCostEstimateForCachedSubtree_);
  add(requestBodyLimit_);
  add(cacheServiceResults_);
  add(syntaxTestMode_);
  add(divisionByZeroIsUndef_);
  add(enablePrefilterOnIndexScans_);
  add(spatialJoinMaxNumThreads_);
  add(spatialJoinPrefilterMaxSize_);
  add(enableDistributiveUnion_);

  defaultQueryTimeout_.setParameterConstraint(
      [](std::chrono::seconds value, std::string_view parameterName) {
        if (value <= std::chrono::seconds{0}) {
          throw std::runtime_error{absl::StrCat(
              "Parameter ", parameterName, " must be strictly positive, was ",
              value.count(), "s")};
        }
      });
}

// _____________________________________________________________________________
[[nodiscard]] ad_utility::HashMap<std::string, std::string>
RuntimeParameters::toMap() const {
  ad_utility::HashMap<std::string, std::string> result;

  for (const auto& [name, parameter] : runtimeMap_) {
    result[name] = parameter->toString();
  }

  return result;
}

// _____________________________________________________________________________
void RuntimeParameters::setFromString(const std::string& name,
                                      const std::string& value) {
  if (!runtimeMap_.contains(name)) {
    throw std::runtime_error{"No parameter with name " + std::string{name} +
                             " exists"};
  }
  try {
    runtimeMap_.at(name)->setFromString(value);
  } catch (const std::exception& e) {
    throw std::runtime_error("Could not set parameter " + std::string{name} +
                             " to value " + value +
                             ". Exception was: " + e.what());
  }
}

// _____________________________________________________________________________
std::vector<std::string> RuntimeParameters::getKeys() const {
  static std::vector<std::string> keys =
      ::ranges::to<std::vector>(runtimeMap_ | ql::views::keys);
  return keys;
}
