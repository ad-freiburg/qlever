//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "./util/GTestHelpers.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "util/IndexTestHelpers.h"

// _____________________________________________________________________________
TEST(Permutation, fileNames) {
  using enum Permutation::Enum;
  EXPECT_THAT(Permutation::fileNames(PSO, "foo/index"),
              ::testing::ElementsAre("foo/index.index.pso",
                                     "foo/index.index.pso.meta"));
  EXPECT_THAT(Permutation::fileNames(OSP, "base"),
              ::testing::ElementsAre("base.index.osp", "base.index.osp.meta"));
  // For an internal permutation, the caller appends the infix to the base name.
  EXPECT_THAT(Permutation::fileNames(
                  POS, absl::StrCat("index", QLEVER_INTERNAL_INDEX_INFIX)),
              ::testing::ElementsAre("index.internal.index.pos",
                                     "index.internal.index.pos.meta"));
}

// _____________________________________________________________________________
TEST(Permutation, logRegistrationCanBeDisabled) {
  ENFORCE_LOG_LEVEL_OR_SKIP(INFO);
  std::string basename = gtestCurrentTestName();
  // Build an index on disk. The `Index` object itself is not used, but it has
  // to be kept alive while the permutations below are loaded.
  Index index = ad_utility::testing::makeTestIndex(
      basename, "<a> <b> <c> . <a> <b> <d> . <e> <f> <g> .");

  // Load the `PSO` permutation (including its internal permutation) from disk
  // with the given `logRegistration` and return the log output that this
  // produced.
  auto loadAndCaptureLog = [&basename](bool logRegistration) {
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    Permutation permutation{Permutation::Enum::PSO,
                            ad_utility::makeUnlimitedAllocator<Id>()};
    permutation.loadFromDisk(basename, true, Permutation::Type::NORMAL, {},
                             logRegistration);
    return logStream.str();
  };

  // With `logRegistration` set to `true`, the registration is logged.
  EXPECT_THAT(loadAndCaptureLog(true),
              ::testing::HasSubstr("Registered PSO permutation"));
  // With `logRegistration` set to `false`, neither the permutation itself nor
  // its internal permutation logs its registration.
  EXPECT_THAT(loadAndCaptureLog(false),
              ::testing::Not(::testing::HasSubstr("Registered")));
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
TEST(Permutation, getDistinctCol0Ids) {
  // `getDistinctCol0Ids` is only a thin wrapper around the corresponding
  // function of the `CompressedRelationReader` (which is tested in detail in
  // `CompressedRelationsTest.cpp`), so we only check that all the arguments are
  // forwarded correctly.
  Index index = ad_utility::testing::makeTestIndex(
      gtestCurrentTestName(), "<a> <b> <c> . <a> <b> <d> . <e> <f> <g> .");
  auto sharedSnapshot =
      index.deltaTriplesManager().getCurrentLocatedTriplesSharedState();
  const auto& locatedTriplesState = *sharedSnapshot;
  const Permutation& pso = index.getImpl().PSO();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  ScanSpecification fullScan{std::nullopt, std::nullopt, std::nullopt};

  // Concatenate all the tables that the generator yields.
  auto getDistinctCol0Ids = [&](std::optional<std::vector<Id>> idFilter) {
    IdTable result{1, ad_utility::makeUnlimitedAllocator<Id>()};
    for (const IdTable& table :
         pso.getDistinctCol0Ids(fullScan, false, std::move(idFilter),
                                cancellationHandle, locatedTriplesState)) {
      result.insertAtEnd(table);
    }
    return result;
  };

  // Without a filter, the result has to be the same as that of the eager
  // `getDistinctCol0IdsAndCounts`, whose first column also holds the distinct
  // `col0Id`s.
  IdTable expected = pso.getDistinctCol0IdsAndCounts(cancellationHandle,
                                                     locatedTriplesState, {});
  auto getId = ad_utility::testing::makeGetId(index);
  EXPECT_THAT(getDistinctCol0Ids(std::nullopt).getColumn(0),
              ::testing::ElementsAreArray(expected.getColumn(0)));
  EXPECT_THAT(getDistinctCol0Ids(std::nullopt).getColumn(0),
              ::testing::IsSupersetOf({getId("<b>"), getId("<f>")}));

  // With a filter, only the requested `col0Id`s are returned.
  EXPECT_THAT(getDistinctCol0Ids(std::vector{getId("<b>")}).getColumn(0),
              ::testing::ElementsAre(getId("<b>")));
}
#endif
