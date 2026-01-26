// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Claude (Anthropic AI)

#include "index/DeltaTriplesPaths.h"

#include <string>

#include "index/Permutation.h"
#include "util/StringUtils.h"

namespace ad_utility::delta_triples {

// Helper function to convert permutation enum to lowercase string for file
// names.
static std::string permutationToLowercase(Permutation::Enum permutation) {
  auto str = std::string{Permutation::toString(permutation)};
  ad_utility::utf8ToLower(str.data());
  return str;
}

// ____________________________________________________________________________
std::string getDeltaInsertsPath(const std::string& baseDir,
                                Permutation::Enum permutation) {
  return absl::StrCat(baseDir, ".delta-inserts.",
                      permutationToLowercase(permutation));
}

// ____________________________________________________________________________
std::string getDeltaDeletesPath(const std::string& baseDir,
                                Permutation::Enum permutation) {
  return absl::StrCat(baseDir, ".delta-deletes.",
                      permutationToLowercase(permutation));
}

// ____________________________________________________________________________
std::string getDeltaTempInsertsPathconst std::string& baseDir,
                                    Permutation::Enum permutation) {
  return absl::StrCat(baseDir, ".delta-inserts.tmp.",
                      permutationToLowercase(permutation));
}

// ____________________________________________________________________________
std::string getDeltaTempDeletesPath(const std::string& baseDir,
                                    Permutation::Enum permutation) {
  return absl::StrCat(baseDir, ".delta-deletes.tmp.",
                      permutationToLowercase(permutation));
}

}  // namespace ad_utility::delta_triples
