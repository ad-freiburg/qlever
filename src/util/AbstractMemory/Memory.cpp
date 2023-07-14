// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include <string_view>
#include <tuple>

#include "util/AbstractMemory/Memory.h"
#include "util/AbstractMemory/MemoryDefinitionLanguageVisitor.h"
#include "util/AbstractMemory/generated/MemoryDefinitionLanguageLexer.h"
#include "util/AbstractMemory/generated/MemoryDefinitionLanguageParser.h"
#include "util/antlr/ANTLRErrorHandling.h"

namespace ad_utility {
// _____________________________________________________________________________
Memory& Memory::operator=(const size_t& amountOfMemoryInBytes) {
  memoryInBytes_ = amountOfMemoryInBytes;
  return *this;
}

// _____________________________________________________________________________
Memory& Memory::operator=(size_t&& amountOfMemoryInBytes) {
  memoryInBytes_ = std::move(amountOfMemoryInBytes);
  return *this;
}

// _____________________________________________________________________________
size_t Memory::bytes() const { return memoryInBytes_; }

// Helper function for dividing two instances of `size_t`. Mainly exists for
// code reduction.
static double sizetDivision(const size_t& dividend, const size_t& divisor) {
  auto dv = std::lldiv(dividend, divisor);
  return static_cast<double>(dv.quot) + static_cast<double>(dv.rem) / divisor;
}

// _____________________________________________________________________________
double Memory::kilobytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerKB);
}

// _____________________________________________________________________________
double Memory::megabytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerMB);
}

// _____________________________________________________________________________
double Memory::gigabytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerGB);
}

// _____________________________________________________________________________
double Memory::terabytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerTB);
}

// _____________________________________________________________________________
double Memory::petabytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerPB);
}

// _____________________________________________________________________________
std::string Memory::asString() const {
  // Convert number and memory unit name to the string, we want to return.
  auto toString = [](const auto& number, std::string_view unitName) {
    return absl::StrCat(number, " ", unitName);
  };

  // Just use the first unit, which is bigger/equal to `memoryInBytes_`.
  if (memoryInBytes_ >= numBytesPerPB) {
    return toString(petabytes(), "PB");
  } else if (memoryInBytes_ >= numBytesPerTB) {
    return toString(terabytes(), "TB");
  } else if (memoryInBytes_ >= numBytesPerGB) {
    return toString(gigabytes(), "GB");
  } else if (memoryInBytes_ >= numBytesPerMB) {
    return toString(megabytes(), "MB");
  } else if (memoryInBytes_ >= numBytesPerKB) {
    return toString(kilobytes(), "KB");
  } else {
    // Just return the amount of bytes.
    return toString(memoryInBytes_, "Byte");
  }
}

// _____________________________________________________________________________
void Memory::parse(std::string_view str) {
  antlr4::ANTLRInputStream input(str);
  MemoryDefinitionLanguageLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  MemoryDefinitionLanguageParser parser(&tokens);

  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. We need to turn parse errors into exceptions instead to
  // propagate them to the user.
  ThrowingErrorListener errorListener{};
  parser.removeErrorListeners();
  parser.addErrorListener(&errorListener);
  lexer.removeErrorListeners();
  lexer.addErrorListener(&errorListener);

  // Get the top node. That is, the node of the first grammar rule.
  MemoryDefinitionLanguageParser::MemoryDefinitionStringContext*
      memoryDefinitionStringContext{parser.memoryDefinitionString()};

  // Our visitor should now convert this to an amount of bytes, we can simply
  // assign.
  memoryInBytes_ =
      ToSizeTMemoryDefinitionLanguageVisitor{}.visitMemoryDefinitionString(
          memoryDefinitionStringContext);
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const ad_utility::Memory& mem) {
  os << mem.asString();
  return os;
}

}  // namespace ad_utility
