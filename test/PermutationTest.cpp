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
#include "util/HashSet.h"
#include "util/IndexTestHelpers.h"
#include "util/StringUtils.h"

// Multi-threaded, to exercise the concurrency of the tested code.
static constexpr auto executor = &ad_utility::testing::multiThreadExecutor;

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

// Test the conversions between the 24 permutations and their names and key
// orders.
TEST(Permutation, namesAndKeyOrders) {
  using P = Permutation;
  // All 24 permutations have distinct names and key orders, and the
  // conversions are inverse to each other.
  ad_utility::HashSet<std::string> names;
  for (auto permutation : P::ALL) {
    auto name = std::string{P::toString(permutation)};
    EXPECT_TRUE(names.insert(name).second) << name;
    EXPECT_EQ(P::fromString(name), permutation);
    EXPECT_EQ(P::fromString(ad_utility::utf8ToLower(name)), permutation);
    EXPECT_EQ(P::fromKeyOrder(P::toKeyOrder(permutation)), permutation);
    // The name lists the columns in the order of the key order, a missing
    // fourth letter means that the graph is last.
    auto keys = P::toKeyOrder(permutation).keys();
    for (size_t i = 0; i < name.size(); ++i) {
      EXPECT_EQ(name.at(i), "SPOG"[keys.at(i)]);
    }
    EXPECT_EQ(P::hasGraphLast(permutation), name.size() == 3);
    EXPECT_EQ(P::hasGraphLast(permutation),
              ad_utility::contains(P::GRAPH_LAST, permutation));
  }
  EXPECT_EQ(names.size(), 24);

  EXPECT_EQ(P::toString(P::PSO), "PSO");
  EXPECT_EQ(P::toString(P::Enum::GSPO), "GSPO");
  EXPECT_THAT(P::toKeyOrder(P::PSO).keys(), ::testing::ElementsAre(1, 0, 2, 3));
  EXPECT_THAT(P::toKeyOrder(P::Enum::OPGS).keys(),
              ::testing::ElementsAre(2, 1, 3, 0));
  // The explicit four-letter names of the permutations with the graph last.
  EXPECT_EQ(P::fromString("PSOG"), P::PSO);
  EXPECT_EQ(P::fromString("ospg"), P::OSP);
  for (std::string_view invalid : {"", "SP", "SPOO", "SPOGG", "XPO", "GSP"}) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        P::fromString(invalid),
        ::testing::HasSubstr("is not the name of a permutation"));
  }
  // The file names of the permutations that do not have the graph last.
  EXPECT_THAT(
      P::fileNames(P::Enum::GSPO, "base"),
      ::testing::ElementsAre("base.index.gspo", "base.index.gspo.meta"));
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
    for (const IdTable& table : pso.getDistinctCol0Ids(
             fullScan, false, std::move(idFilter), cancellationHandle,
             locatedTriplesState, executor())) {
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
