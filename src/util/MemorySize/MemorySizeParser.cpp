// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/MemorySize/MemorySizeParser.h"

#include <string>

#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/StringUtils.h"

using ad_utility::MemorySize;

// _____________________________________________________________________________
MemorySize MemorySizeParser::parseMemorySize(std::string_view str) {
  antlr4::ANTLRInputStream input(str);
  Lexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  Parser parser(&tokens);

  // Get the top node. That is, the node of the first grammar rule.
  MemorySizeLanguageParser::MemorySizeStringContext* memorySizeStringContext{
      parser.memorySizeString()};

  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. Instead we throw a hard coded, general error.
  lexer.removeErrorListeners();
  parser.removeErrorListeners();
  if (parser.getNumberOfSyntaxErrors() > 0uL ||
      lexer.getNumberOfSyntaxErrors() > 0uL) {
    throw std::runtime_error(absl::StrCat(
        "'", str,
        R"(' could not be parsed as a memory size. Examples for valid memory sizes are "4 B", "3.21 MB", "2.392 TB".)"));
  }

  // Our visitor should now convert this to `Memory` with the described memory
  // amount.
  return visitMemorySizeString(memorySizeStringContext);
}
// ____________________________________________________________________________
MemorySize MemorySizeParser::visitMemorySizeString(
    Parser::MemorySizeStringContext* context) {
  if (context->pureByteSize()) {
    return visitPureByteSize(context->pureByteSize());
  } else {
    // Must be a `memoryUnitSize`.
    AD_CORRECTNESS_CHECK(context->memoryUnitSize());
    return visitMemoryUnitSize(context->memoryUnitSize());
  }
}

// ____________________________________________________________________________
MemorySize MemorySizeParser::visitPureByteSize(
    Parser::PureByteSizeContext* context) {
  return MemorySize::bytes(std::stoul(context->UNSIGNED_INTEGER()->getText()));
}

// ____________________________________________________________________________
MemorySize MemorySizeParser::visitMemoryUnitSize(
    Parser::MemoryUnitSizeContext* context) {
  /*
  Create an instance of `MemorySize`.

  @param memoryUnit Which memory unit to use for the creation. Must be one of:
  - 'k' (kB)
  - 'm' (MB)
  - 'g' (GB)
  - 't' (TB)
  @param numUnits Amount of kilobytes, megabytes, etc..
  */
  auto createMemoryInstance = [](char memoryUnit, auto numUnits) {
    switch (memoryUnit) {
      case 'k':
        return MemorySize::kilobytes(numUnits);
      case 'm':
        return MemorySize::megabytes(numUnits);
      case 'g':
        return MemorySize::gigabytes(numUnits);
      case 't':
        return MemorySize::terabytes(numUnits);
      default:
        // Whatever this is, it is false.
        AD_FAIL();
    }
  };

  // Which memory unit are we looking at?
  const char memoryUnitType =
      ad_utility::getLowercase(context->MEMORY_UNIT()->getText()).front();

  // We have to interpret the amount of units.
  if (context->UNSIGNED_INTEGER()) {
    return createMemoryInstance(
        memoryUnitType, std::stoul(context->UNSIGNED_INTEGER()->getText()));
  } else {
    AD_CORRECTNESS_CHECK(context->FLOAT());
    return createMemoryInstance(memoryUnitType,
                                std::stod(context->FLOAT()->getText()));
  }
}
