// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

#include <antlr4-runtime.h>

#include "util/MemorySize/MemorySize.h"
#include "util/MemorySize/generated/MemorySizeLanguageLexer.h"
#include "util/MemorySize/generated/MemorySizeLanguageParser.h"

/*
Translate the memory size language to an instance of `MemorySize`.
*/
class MemorySizeParser final {
 public:
  using Parser = MemorySizeLanguageParser;
  using Lexer = MemorySizeLanguageLexer;

  /*
  @brief Parse the given string and create a `MemorySize` object, set to the
  memory size described.

  @param str A string following `./generated/MemorySizeLanguage.g4`.
 */
  static ad_utility::MemorySize parseMemorySize(std::string_view str);

 private:
  static ad_utility::MemorySize visitMemorySizeString(
      Parser::MemorySizeStringContext* context);

  static ad_utility::MemorySize visitPureByteSize(
      Parser::PureByteSizeContext* context);

  static ad_utility::MemorySize visitMemoryUnitSize(
      Parser::MemoryUnitSizeContext* context);
};
