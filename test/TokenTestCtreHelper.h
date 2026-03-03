// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#ifndef QLEVER_TEST_TOKENTESTCTREHELPER_H
#define QLEVER_TEST_TOKENTESTCTREHELPER_H

#include <string_view>

// All the static functions call ctre::match<correspondingTurtleGrammarRegex>
// This is split up in a separate Header + CPP file, because otherwise the
// compilation of the TokenTest requires an insane amount of RAM
class TokenTestCtreHelper {
 public:
  static bool matchInteger(std::string_view s);
  static bool matchDecimal(std::string_view s);
  static bool matchDouble(std::string_view s);
  static bool matchStringLiteralQuoteString(std::string_view s);
  static bool matchPnCharsBaseString(std::string_view s);
  static bool matchStringLiteralSingleQuoteString(std::string_view s);
  static bool matchStringLiteralLongQuoteString(std::string_view s);
  static bool matchStringLiteralLongSingleQuoteString(std::string_view s);
  static bool matchIriref(std::string_view s);
};

#endif  // QLEVER_TEST_TOKENTESTCTREHELPER_H
