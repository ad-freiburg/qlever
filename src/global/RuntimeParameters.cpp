// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 NN, BMW
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// BMW =  Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "global/RuntimeParameters.h"

#include <absl/strings/str_join.h>

#include <boost/beast/http/status.hpp>

#include "backports/algorithm.h"
#include "backports/reflection.h"
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
  add(rebuildIndexMaxNumThreads_);
  add(rebuildMaxConcurrentPermutationPairs_);
  add(queryQueueMaxWait_);
  add(queryQueueFullStatusCode_);
  add(queryQueueFullRetryAfter_);
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

#ifdef QLEVER_HAS_REFLECTION
  // Check that all the parameters above were added. Without reflection, this
  // is only checked by the hand-written list in `RuntimeParametersTest`.
  static constexpr auto members =
      std::define_static_array(std::meta::nonstatic_data_members_of(
          ^^RuntimeParameters, std::meta::access_context::unchecked()));
  template for (constexpr auto member : members) {
    using Type = typename[:std::meta::type_of(member):];
    if constexpr (std::is_base_of_v<ad_utility::ParameterBase, Type>) {
      const ad_utility::ParameterBase* parameter = &this->[:member:];
      static constexpr std::string_view name = std::meta::identifier_of(member);
      AD_CORRECTNESS_CHECK(
          ad_utility::contains(runtimeMap_ | ql::views::values, parameter),
          "The runtime parameter `", name,
          "` was not registered via `add()` in the constructor of "
          "`RuntimeParameters`");
    }
  }
#endif

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

  // The status code for a rejected query must be an error code that Boost.Beast
  // knows (an unknown code would be sent as status 0).
  queryQueueFullStatusCode_.setParameterConstraint(
      [](size_t value, std::string_view parameterName) {
        namespace http = boost::beast::http;
        auto status = http::int_to_status(static_cast<unsigned>(value));
        auto statusClass = http::to_status_class(status);
        if (status == http::status::unknown ||
            (statusClass != http::status_class::client_error &&
             statusClass != http::status_class::server_error)) {
          throw std::runtime_error{absl::StrCat(
              "Parameter ", parameterName,
              " must be a known HTTP status code of class 4xx or 5xx, was ",
              value)};
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
  if (!ad_utility::contains(runtimeMap_, name)) {
    throw std::runtime_error{
        "No parameter with name " + std::string{name} +
        " exists. Available parameters are: " + absl::StrJoin(getKeys(), ", ")};
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
std::vector<std::string> RuntimeParameters::getKeys() const {
  static std::vector<std::string> keys =
      ::ranges::to<std::vector>(runtimeMap_ | ql::views::keys);
  return keys;
}
