// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "global/RuntimeParameters.h"
#include "util/GTestHelpers.h"

using ::testing::AllOf;
using ::testing::HasSubstr;

// Test setting a runtime parameter from a single `<name>=<value>` string, as
// used by the `--set-runtime-parameter` option of `qlever-server`.
TEST(RuntimeParameters, setFromAssignment) {
  RuntimeParameters params;

  // A valid assignment sets the parameter.
  params.setFromAssignment("default-query-timeout=300s");
  EXPECT_EQ(params.defaultQueryTimeout_.get(), std::chrono::seconds{300});

  // The string is split at the FIRST `=`, so values containing `=` work.
  params.setFromAssignment("default-query-timeout=150s");
  EXPECT_EQ(params.defaultQueryTimeout_.get(), std::chrono::seconds{150});

  // A missing `=` is rejected with a readable message.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("no-equals-sign"),
      HasSubstr("assignment of the form <name>=<value>"));

  // An unknown parameter name is rejected, and the message lists the
  // available parameters.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("no-such-parameter=42"),
      AllOf(HasSubstr("No parameter with name no-such-parameter"),
            HasSubstr("Available parameters are:"),
            HasSubstr("default-query-timeout")));

  // An invalid value for an existing parameter is rejected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("default-query-timeout=banana"),
      HasSubstr("Could not set parameter default-query-timeout"));

  // The use case that prompted this option (see #3031): set the maximal
  // number of redirects for query federation at startup.
  params.setFromAssignment("service-max-redirects=5");
  EXPECT_EQ(params.serviceMaxRedirects_.get(), 5u);
}

// Test that the value `0` is rejected for
// `lazy-index-scan-num-blocks-in-flight` (a lazy scan reads at least one block
// ahead of its consumer).
TEST(RuntimeParameters, lazyIndexScanNumBlocksInFlightIsStrictlyPositive) {
  RuntimeParameters params;
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("lazy-index-scan-num-blocks-in-flight=0"),
      AllOf(HasSubstr("lazy-index-scan-num-blocks-in-flight"),
            HasSubstr("strictly positive")),
      std::runtime_error);
  EXPECT_NO_THROW(
      params.setFromAssignment("lazy-index-scan-num-blocks-in-flight=1"));
  EXPECT_EQ(params.lazyIndexScanNumBlocksInFlight_.get(), 1u);
}

// Test the default of `max-num-threads-per-query` and that the value `0` is
// rejected (a query has to be able to run at least one task).
TEST(RuntimeParameters, maxNumThreadsPerQueryIsStrictlyPositive) {
  RuntimeParameters params;
  EXPECT_EQ(params.maxNumThreadsPerQuery_.get(), 6u);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("max-num-threads-per-query=0"),
      AllOf(HasSubstr("max-num-threads-per-query"),
            HasSubstr("strictly positive")),
      std::runtime_error);
  EXPECT_NO_THROW(params.setFromAssignment("max-num-threads-per-query=1"));
  EXPECT_EQ(params.maxNumThreadsPerQuery_.get(), 1u);
}

// Test the default of `permutation-writer-num-blocks-in-flight` and that the
// value `0` is rejected (a writer has to keep at least one block in flight).
TEST(RuntimeParameters, permutationWriterNumBlocksInFlightIsStrictlyPositive) {
  RuntimeParameters params;
  EXPECT_EQ(params.permutationWriterNumBlocksInFlight_.get(), 4u);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("permutation-writer-num-blocks-in-flight=0"),
      AllOf(HasSubstr("permutation-writer-num-blocks-in-flight"),
            HasSubstr("strictly positive")),
      std::runtime_error);
  EXPECT_NO_THROW(
      params.setFromAssignment("permutation-writer-num-blocks-in-flight=1"));
  EXPECT_EQ(params.permutationWriterNumBlocksInFlight_.get(), 1u);
}

// Test the default of `sort-num-tasks-in-flight` and that the value `0` is
// rejected (unlike for other parameters, `0` does not mean "no limit").
TEST(RuntimeParameters, sortNumTasksInFlightIsStrictlyPositive) {
  RuntimeParameters params;
  EXPECT_EQ(params.sortNumTasksInFlight_.get(), 3u);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("sort-num-tasks-in-flight=0"),
      AllOf(HasSubstr("sort-num-tasks-in-flight"),
            HasSubstr("strictly positive")),
      std::runtime_error);
  EXPECT_NO_THROW(params.setFromAssignment("sort-num-tasks-in-flight=1"));
  EXPECT_EQ(params.sortNumTasksInFlight_.get(), 1u);
}

// Test that `getKeys` and `toMap` (the building blocks of
// `--set-runtime-parameter help`) are consistent with each other.
TEST(RuntimeParameters, getKeysAndToMapAreConsistent) {
  RuntimeParameters params;
  auto keys = params.getKeys();
  auto map = params.toMap();
  EXPECT_FALSE(keys.empty());
  EXPECT_EQ(keys.size(), map.size());
  for (const auto& key : keys) {
    EXPECT_TRUE(map.contains(key)) << key;
  }
}

// Test that every parameter declared in `RuntimeParameters` is registered in
// the constructor, and can thus be set via `setFromString`. If this test fails
// after you added a parameter, add it to the list below AND register it in the
// constructor of `RuntimeParameters`.
//
// NOTE: Without reflection (which only comes with C++26), the parameters have
// to be listed explicitly. The list is complete if the sizes of the listed
// parameters add up to the offset of `runtimeMap_`, which is declared after
// all the parameters.
TEST(RuntimeParameters, allParametersAreRegistered) {
  RuntimeParameters params;
  auto keys = params.getKeys();
  size_t numParameters = 0;
  size_t totalSize = 0;
  auto check = [&](auto& parameter,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    ++numParameters;
    totalSize += sizeof(parameter);
    EXPECT_THAT(keys, ::testing::Contains(parameter.name()));
    EXPECT_NO_THROW(
        params.setFromString(parameter.name(), parameter.toString()))
        << parameter.name();
  };
  check(params.stripColumns_);
  check(params.memoryForQueries_);
  check(params.cacheMaxNumEntries_);
  check(params.cacheMaxSize_);
  check(params.cacheMaxSizeSingleEntry_);
  check(params.lazyIndexScanNumBlocksInFlight_);
  check(params.maxNumThreadsPerQuery_);
  check(params.rebuildIndexScanNumThreads_);
  check(params.rebuildPermutationWriterNumThreads_);
  check(params.rebuildMaxConcurrentPermutationPairs_);
  check(params.lazyIndexScanMaxSizeMaterialization_);
  check(params.useBinsearchTransitivePath_);
  check(params.groupByHashMapEnabled_);
  check(params.groupByDisableIndexScanOptimizations_);
  check(params.serviceMaxValueRows_);
  check(params.serviceMaxRedirects_);
  check(params.queryPlanningBudget_);
  check(params.throwOnUnboundVariables_);
  check(params.cacheMaxSizeLazyResult_);
  check(params.websocketUpdatesEnabled_);
  check(params.websocketUpdateInterval_);
  check(params.smallIndexScanSizeEstimateDivisor_);
  check(params.zeroCostEstimateForCachedSubtree_);
  check(params.requestBodyLimit_);
  check(params.cacheServiceResults_);
  check(params.cacheLoadResults_);
  check(params.syntaxTestMode_);
  check(params.divisionByZeroIsUndef_);
  check(params.throwOnInvalidWkt_);
  check(params.enablePrefilterOnIndexScans_);
  check(params.spatialJoinMaxNumThreads_);
  check(params.patternTrickNumTasksInFlight_);
  check(params.sortNumTasksInFlight_);
  check(params.spatialJoinPrefilterMaxSize_);
  check(params.spatialJoinTmpDir_);
  check(params.enableDistributiveUnion_);
  check(params.treatDefaultGraphAsNamedGraph_);
  check(params.sparqlResultsJsonWithTime_);
  check(params.materializedViewWriterMemory_);
  check(params.defaultQueryTimeout_);
  check(params.sortInMemoryThreshold_);
  check(params.prefilteredOptionalJoin_);
  check(params.enableMaterializedViewQueryRewrite_);
  check(params.materializedViewPatternMatchNumAssignments_);
  check(params.materializedViewPatternMatchNumReplacementPlans_);
  check(params.serviceAllowedIriPrefixes_);
  check(params.permutationWriterNumBlocksInFlight_);
  check(params.vacuumMinimumBlockSize_);
  check(params.disableCaching_);
  check(params.logLevel_);
  check(params.constructDeduplication_);
  EXPECT_EQ(totalSize,
            static_cast<size_t>(reinterpret_cast<char*>(&params.runtimeMap_) -
                                reinterpret_cast<char*>(&params)));
  EXPECT_EQ(numParameters, keys.size());
}
