// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Claude (Anthropic AI)

#ifndef QLEVER_SRC_INDEX_DELTATRIPLESPATHS_H
#define QLEVER_SRC_INDEX_DELTATRIPLESPATHS_H

#include <string>

#include "index/Permutation.h"

namespace ad_utility::delta_triples {

// Centralized naming scheme for on-disk delta triple files. This namespace
// provides functions to generate consistent file paths for delta triple
// storage across all permutations.
//
// File naming convention:
// - Inserts: <baseDir>.delta-inserts.<permutation>
// - Deletes: <baseDir>.delta-deletes.<permutation>
// - Temporary files (during rebuild): <baseDir>.delta-inserts.tmp.<permutation>
//
// Example: "index.delta-inserts.pos" for inserted triples in POS permutation.

// Get the file path for on-disk inserted delta triples for the given
// permutation.
std::string getDeltaInsertsPath(const std::string& baseDir,
                                Permutation::Enum permutation);

// Get the file path for on-disk deleted delta triples for the given
// permutation.
std::string getDeltaDeletesPath(const std::string& baseDir,
                                Permutation::Enum permutation);

// Get the temporary file path used during atomic rebuild of inserted delta
// triples.
std::string getDeltaTempInsertsPath(const std::string& baseDir,
                                    Permutation::Enum permutation);

// Get the temporary file path used during atomic rebuild of deleted delta
// triples.
std::string getDeltaTempDeletesPath(const std::string& baseDir,
                                    Permutation::Enum permutation);

}  // namespace ad_utility::delta_triples

#endif  // QLEVER_SRC_INDEX_DELTATRIPLESPATHS_H
