// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 NN, BMW
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// BMW =  Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "global/RuntimeParameters.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include "backports/algorithm.h"
#include "util/Algorithm.h"

// _____________________________________________________________________________
RuntimeParameters::RuntimeParameters() {
  // Here all of the newly defined parameters have to be added.

  auto add = [this](auto& parameter) {
    AD_CONTRACT_CHECK(!ad_utility::contains(runtimeMap_, parameter.name()));
    runtimeMap_[parameter.name()] = &parameter;
  };

  add(stripColumns_);
  add(memoryForQueries_);
  add(cacheMaxNumEntries_);
  add(cacheMaxSize_);
  add(cacheMaxSizeSingleEntry_);
  add(lazyIndexScanNumBlocksInFlight_);
  add(maxNumThreadsPerQuery_);
  add(rebuildIndexScanNumThreads_);
  add(rebuildPermutationWriterNumThreads_);
  add(rebuildMaxConcurrentPermutationPairs_);
  add(lazyIndexScanMaxSizeMaterialization_);
  add(useBinsearchTransitivePath_);
  add(groupByHashMapEnabled_);
  add(groupByDisableIndexScanOptimizations_);
  add(serviceMaxValueRows_);
  add(serviceMaxRedirects_);
  add(queryPlanningBudget_);
  add(throwOnUnboundVariables_);
  add(cacheMaxSizeLazyResult_);
  add(websocketUpdatesEnabled_);
  add(websocketUpdateInterval_);
  add(smallIndexScanSizeEstimateDivisor_);
  add(zeroCostEstimateForCachedSubtree_);
  add(requestBodyLimit_);
  add(cacheServiceResults_);
  add(cacheLoadResults_);
  add(syntaxTestMode_);
  add(divisionByZeroIsUndef_);
  add(throwOnInvalidWkt_);
  add(enablePrefilterOnIndexScans_);
  add(spatialJoinMaxNumThreads_);
  add(patternTrickNumTasksInFlight_);
  add(sortNumTasksInFlight_);
  add(spatialJoinPrefilterMaxSize_);
  add(spatialJoinTmpDir_);
  add(enableDistributiveUnion_);
  add(treatDefaultGraphAsNamedGraph_);
  add(sparqlResultsJsonWithMeta_);
  add(materializedViewWriterMemory_);
  add(defaultQueryTimeout_);
  add(sortInMemoryThreshold_);
  add(prefilteredOptionalJoin_);
  add(enableMaterializedViewQueryRewrite_);
  add(materializedViewPatternMatchNumAssignments_);
  add(materializedViewPatternMatchNumReplacementPlans_);
  add(serviceAllowedIriPrefixes_);
  add(permutationWriterNumBlocksInFlight_);
  add(vacuumMinimumBlockSize_);
  add(disableCaching_);
  add(logLevel_);
  add(constructDeduplication_);

  // Deprecated aliases (old names of renamed parameters), so that existing
  // configurations keep working.
  aliases_["sparql-results-json-with-time"] = sparqlResultsJsonWithMeta_.name();

  // Propagate runtime log level changes immediately to the global atomic in
  // Log.h. The action fires once immediately on registration, so the atomic is
  // in sync with the parameter default from the start.
  logLevel_.setOnUpdateAction(
      [](LogLevel level) { ad_utility::setRuntimeLogLevel(level); });

  // A constraint that rejects values that are not strictly positive, with a
  // readable error message. Works for integral types and for
  // `std::chrono::seconds`.
  auto mustBeStrictlyPositive = [](auto value, std::string_view parameterName) {
    if (value <= decltype(value){}) {
      auto valueAsString = [&value]() {
        if constexpr (std::is_integral_v<decltype(value)>) {
          return absl::StrCat(value);
        } else {
          return absl::StrCat(std::chrono::seconds{value}.count(), "s");
        }
      }();
      throw std::runtime_error{absl::StrCat("Parameter ", parameterName,
                                            " must be strictly positive, was ",
                                            valueAsString)};
    }
  };
  defaultQueryTimeout_.setParameterConstraint(mustBeStrictlyPositive);
  lazyIndexScanNumBlocksInFlight_.setParameterConstraint(
      mustBeStrictlyPositive);
  maxNumThreadsPerQuery_.setParameterConstraint(mustBeStrictlyPositive);
  permutationWriterNumBlocksInFlight_.setParameterConstraint(
      mustBeStrictlyPositive);
  sortNumTasksInFlight_.setParameterConstraint(mustBeStrictlyPositive);
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
  // Resolve a deprecated alias to the new name, with a warning (so that the
  // configuration gets updated before the alias is removed).
  auto newName = ad_utility::findOptional(aliases_, name);
  if (newName) {
    AD_LOG_WARN << "The runtime parameter \"" << name
                << "\" is deprecated, use \"" << newName.get() << "\" instead"
                << std::endl;
  }

  // Look up the parameter, throw if it doesn't exist.
  auto parameter =
      ad_utility::findOptional(runtimeMap_, newName ? newName.get() : name);
  if (!parameter) {
    throw std::runtime_error{
        "No parameter with name " + std::string{name} +
        " exists. Available parameters are: " + absl::StrJoin(getKeys(), ", ")};
  }
  try {
    parameter.get()->setFromString(value);
  } catch (const std::exception& e) {
    throw std::runtime_error("Could not set parameter " + std::string{name} +
                             " to value " + value +
                             ". Exception was: " + e.what());
  }
}

// _____________________________________________________________________________
void RuntimeParameters::setFromAssignment(const std::string& assignment) {
  auto positionOfEquals = assignment.find('=');
  if (positionOfEquals == std::string::npos) {
    throw std::runtime_error{
        "Expected an assignment of the form <name>=<value>, but got \"" +
        assignment + "\""};
  }
  setFromString(assignment.substr(0, positionOfEquals),
                assignment.substr(positionOfEquals + 1));
}

// _____________________________________________________________________________
void RuntimeParameters::throwIfAliasAndNameAreBothSet(
    const std::function<bool(const std::string&)>& isSet) const {
  for (const auto& [alias, name] : aliases_) {
    if (isSet(alias) && isSet(name)) {
      throw std::runtime_error{absl::StrCat(
          "Specify either the runtime parameter \"", name,
          "\" or its deprecated alias \"", alias, "\", but not both")};
    }
  }
}

// _____________________________________________________________________________
std::vector<std::string> RuntimeParameters::getKeys() const {
  static std::vector<std::string> keys =
      ::ranges::to<std::vector>(runtimeMap_ | ql::views::keys);
  return keys;
}

// _____________________________________________________________________________
std::vector<std::string> RuntimeParameters::getKeysIncludingAliases() const {
  auto keys = getKeys();
  ql::ranges::copy(aliases_ | ql::views::keys, std::back_inserter(keys));
  return keys;
}
