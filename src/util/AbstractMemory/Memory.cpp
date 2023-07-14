// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include <string_view>
#include <tuple>

#include "util/AbstractMemory/CalculationUtil.h"
#include "util/AbstractMemory/Memory.h"
#include "util/AbstractMemory/MemoryDefinitionLanguageVisitor.h"
#include "util/AbstractMemory/generated/MemoryDefinitionLanguageLexer.h"
#include "util/AbstractMemory/generated/MemoryDefinitionLanguageParser.h"
#include "util/antlr/ANTLRErrorHandling.h"

namespace ad_utility {
// _____________________________________________________________________________
Memory Memory::bytes(size_t numBytes) { return Memory{numBytes}; }

// _____________________________________________________________________________
Memory Memory::kilobytes(size_t numKilobytes) {
  return Memory{convertMemoryUnitsToBytes(numKilobytes, numBytesPerKB)};
}

// _____________________________________________________________________________
Memory Memory::kilobytes(double numKilobytes) {
  return Memory{convertMemoryUnitsToBytes(numKilobytes, numBytesPerKB)};
}

// _____________________________________________________________________________
Memory Memory::megabytes(size_t numMegabytes) {
  return Memory{convertMemoryUnitsToBytes(numMegabytes, numBytesPerMB)};
}

// _____________________________________________________________________________
Memory Memory::megabytes(double numMegabytes) {
  return Memory{convertMemoryUnitsToBytes(numMegabytes, numBytesPerMB)};
}

// _____________________________________________________________________________
Memory Memory::gigabytes(size_t numGigabytes) {
  return Memory{convertMemoryUnitsToBytes(numGigabytes, numBytesPerGB)};
}

// _____________________________________________________________________________
Memory Memory::gigabytes(double numGigabytes) {
  return Memory{convertMemoryUnitsToBytes(numGigabytes, numBytesPerGB)};
}

// _____________________________________________________________________________
Memory Memory::terabytes(size_t numTerabytes) {
  return Memory{convertMemoryUnitsToBytes(numTerabytes, numBytesPerTB)};
}

// _____________________________________________________________________________
Memory Memory::terabytes(double numTerabytes) {
  return Memory{convertMemoryUnitsToBytes(numTerabytes, numBytesPerTB)};
}

// _____________________________________________________________________________
Memory Memory::petabytes(size_t numPetabytes) {
  return Memory{convertMemoryUnitsToBytes(numPetabytes, numBytesPerPB)};
}

// _____________________________________________________________________________
Memory Memory::petabytes(double numPetabytes) {
  return Memory{convertMemoryUnitsToBytes(numPetabytes, numBytesPerPB)};
}

// Helper function for dividing two instances of `size_t`. Mainly exists for
// code reduction.
static double sizetDivision(const size_t& dividend, const size_t& divisor) {
  auto dv = std::lldiv(dividend, divisor);
  return static_cast<double>(dv.quot) + static_cast<double>(dv.rem) / divisor;
}

// _____________________________________________________________________________
size_t Memory::bytes() const { return memoryInBytes_; }

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
Memory Memory::parse(std::string_view str) {
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

  // Our visitor should now convert this to `Memory` with the described memory
  // amount.
  return ToMemoryInstanceMemoryDefinitionLanguageVisitor{}
      .visitMemoryDefinitionString(memoryDefinitionStringContext);
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const ad_utility::Memory& mem) {
  os << mem.asString();
  return os;
}

namespace memory_literals {
// _____________________________________________________________________________
Memory operator""_Byte(unsigned long long int bytes) {
  return Memory::bytes(bytes);
}

// _____________________________________________________________________________
Memory operator""_KB(long double kilobytes) {
  return Memory::kilobytes(static_cast<double>(kilobytes));
}

// _____________________________________________________________________________
Memory operator""_KB(unsigned long long int kilobytes) {
  return Memory::kilobytes(static_cast<size_t>(kilobytes));
}

// _____________________________________________________________________________
Memory operator""_MB(long double megabytes) {
  return Memory::megabytes(static_cast<double>(megabytes));
}

// _____________________________________________________________________________
Memory operator""_MB(unsigned long long int megabytes) {
  return Memory::megabytes(static_cast<size_t>(megabytes));
}

// _____________________________________________________________________________
Memory operator""_GB(long double gigabytes) {
  return Memory::gigabytes(static_cast<double>(gigabytes));
}

// _____________________________________________________________________________
Memory operator""_GB(unsigned long long int gigabytes) {
  return Memory::gigabytes(static_cast<size_t>(gigabytes));
}

// _____________________________________________________________________________
Memory operator""_TB(long double terabytes) {
  return Memory::terabytes(static_cast<double>(terabytes));
}

// _____________________________________________________________________________
Memory operator""_TB(unsigned long long int terabytes) {
  return Memory::terabytes(static_cast<size_t>(terabytes));
}

// _____________________________________________________________________________
Memory operator""_PB(long double petabytes) {
  return Memory::petabytes(static_cast<double>(petabytes));
}

// _____________________________________________________________________________
Memory operator""_PB(unsigned long long int petabytes) {
  return Memory::petabytes(static_cast<size_t>(petabytes));
}
}  // namespace memory_literals

}  // namespace ad_utility
