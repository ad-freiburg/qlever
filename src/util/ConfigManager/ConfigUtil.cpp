// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/ConfigManager/ConfigUtil.h"

#include <regex>

#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/ConfigManager/generated/ConfigShorthandParser.h"

namespace ad_utility {
// ____________________________________________________________________________
bool isNameInShortHand(std::string_view str) {
  /*
  TODO We originally did this check directly with ANTLR, in order to keep the
  definition of NAME synchronous with the grammar, but always ran into the same
  bug, regardless of how we did it. Extra parser rule, reading out the tokens
  with parser, reading out the tokens with token stream, etc..
  The tests would always work correctly, but actually using the compiled
  `ConfigManager` would always result in a segmentation fault. We do not know
  the exact cause, but it should be fixed in
  https://github.com/antlr/antlr4/milestone/27
  */
  // Needs to be keeped up to date with the definition of `NAME` in
  // `ConfigShortHand.g4`.
  return std::regex_match(str.data(), std::regex(R"--([a-zA-Z0-9\-_]+)--"));
}
}  // namespace ad_utility
