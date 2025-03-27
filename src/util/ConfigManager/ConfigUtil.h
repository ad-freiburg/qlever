// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (June of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <string_view>

namespace ad_utility {
/*
@brief Checks if the given string describes a valid `NAME` in the short hand
grammar.
*/
bool isNameInShortHand(std::string_view str);
}  // namespace ad_utility
