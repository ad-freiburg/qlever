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
  SKIP_IF_LOGLEVEL_IS_LOWER(INFO);
  std::string basename = gtestCurrentTestName();
  // Build an index on disk. The `Index` object itself is not used, but it has
  // to be kept alive while the permutations below are loaded.
  Index index = ad_utility::testing::makeTestIndex(
      basename, "<a> <b> <c> . <a> <b> <d> . <e> <f> <g> .");

  // Load the `PSO` permutation (including its internal permutation) from disk
  // and return the log output that this produced. If `logRegistration` has a
  // value, then it is explicitly passed to `loadFromDisk`, otherwise the
  // default of that argument is used.
  auto loadAndCaptureLog = [&basename](std::optional<bool> logRegistration) {
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    Permutation permutation{Permutation::Enum::PSO,
                            ad_utility::makeUnlimitedAllocator<Id>()};
    if (logRegistration.has_value()) {
      permutation.loadFromDisk(basename, true, Permutation::Type::NORMAL, {},
                               logRegistration.value());
    } else {
      permutation.loadFromDisk(basename, true);
    }
    return logStream.str();
  };

  // By default, the registration is logged.
  EXPECT_THAT(loadAndCaptureLog(std::nullopt),
              ::testing::HasSubstr("Registered PSO permutation"));
  EXPECT_THAT(loadAndCaptureLog(true),
              ::testing::HasSubstr("Registered PSO permutation"));
  // With `logRegistration` set to `false`, neither the permutation itself nor
  // its internal permutation logs its registration.
  EXPECT_THAT(loadAndCaptureLog(false),
              ::testing::Not(::testing::HasSubstr("Registered")));
}
