// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include <string_view>
#include <tuple>

#include "util/MemorySize/MemorySize.h"
#include "util/MemorySize/MemorySizeLanguageVisitor.h"
#include "util/MemorySize/generated/MemorySizeLanguageLexer.h"
#include "util/MemorySize/generated/MemorySizeLanguageParser.h"
#include "util/antlr/ANTLRErrorHandling.h"

namespace ad_utility {
// Just the number of bytes per memory unit.
constexpr size_t numBytesPerKB = ad_utility::pow<size_t>(10, 3);
constexpr size_t numBytesPerMB = ad_utility::pow<size_t>(10, 6);
constexpr size_t numBytesPerGB = ad_utility::pow<size_t>(10, 9);
constexpr size_t numBytesPerTB = ad_utility::pow<size_t>(10, 12);
constexpr size_t numBytesPerPB = ad_utility::pow<size_t>(10, 15);

/*
@brief Calculate the amount of bytes for a given amount of untis and a given
amount of bytes per unit.

@return The amount of bytes. Rounded up, if needed.
*/
template <typename T>
requires std::integral<T> || std::floating_point<T>
size_t constexpr convertMemoryUnitsToBytes(const T& amountOfUnits,
                                           const size_t& numBytesPerUnit) {
  if constexpr (std::is_floating_point_v<T>) {
    // We (maybe) have to round up.
    return static_cast<size_t>(std::ceil(amountOfUnits * numBytesPerUnit));
  } else {
    AD_CORRECTNESS_CHECK(std::is_integral_v<T>);
    return amountOfUnits * numBytesPerUnit;
  }
}

// _____________________________________________________________________________
MemorySize MemorySize::bytes(size_t numBytes) { return MemorySize{numBytes}; }

// _____________________________________________________________________________
MemorySize MemorySize::kilobytes(size_t numKilobytes) {
  return MemorySize{convertMemoryUnitsToBytes(numKilobytes, numBytesPerKB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::kilobytes(double numKilobytes) {
  return MemorySize{convertMemoryUnitsToBytes(numKilobytes, numBytesPerKB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::megabytes(size_t numMegabytes) {
  return MemorySize{convertMemoryUnitsToBytes(numMegabytes, numBytesPerMB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::megabytes(double numMegabytes) {
  return MemorySize{convertMemoryUnitsToBytes(numMegabytes, numBytesPerMB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::gigabytes(size_t numGigabytes) {
  return MemorySize{convertMemoryUnitsToBytes(numGigabytes, numBytesPerGB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::gigabytes(double numGigabytes) {
  return MemorySize{convertMemoryUnitsToBytes(numGigabytes, numBytesPerGB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::terabytes(size_t numTerabytes) {
  return MemorySize{convertMemoryUnitsToBytes(numTerabytes, numBytesPerTB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::terabytes(double numTerabytes) {
  return MemorySize{convertMemoryUnitsToBytes(numTerabytes, numBytesPerTB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::petabytes(size_t numPetabytes) {
  return MemorySize{convertMemoryUnitsToBytes(numPetabytes, numBytesPerPB)};
}

// _____________________________________________________________________________
MemorySize MemorySize::petabytes(double numPetabytes) {
  return MemorySize{convertMemoryUnitsToBytes(numPetabytes, numBytesPerPB)};
}

// Helper function for dividing two instances of `size_t`. Mainly exists for
// code reduction.
static double sizetDivision(const size_t& dividend, const size_t& divisor) {
  auto dv = std::lldiv(dividend, divisor);
  return static_cast<double>(dv.quot) + static_cast<double>(dv.rem) / divisor;
}

// _____________________________________________________________________________
size_t MemorySize::bytes() const { return memoryInBytes_; }

// _____________________________________________________________________________
double MemorySize::kilobytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerKB);
}

// _____________________________________________________________________________
double MemorySize::megabytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerMB);
}

// _____________________________________________________________________________
double MemorySize::gigabytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerGB);
}

// _____________________________________________________________________________
double MemorySize::terabytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerTB);
}

// _____________________________________________________________________________
double MemorySize::petabytes() const {
  return sizetDivision(memoryInBytes_, numBytesPerPB);
}

// _____________________________________________________________________________
std::string MemorySize::asString() const {
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
MemorySize MemorySize::parse(std::string_view str) {
  antlr4::ANTLRInputStream input(str);
  MemorySizeLanguageLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  MemorySizeLanguageParser parser(&tokens);

  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. We need to turn parse errors into exceptions instead to
  // propagate them to the user.
  ThrowingErrorListener errorListener{};
  parser.removeErrorListeners();
  parser.addErrorListener(&errorListener);
  lexer.removeErrorListeners();
  lexer.addErrorListener(&errorListener);

  // Get the top node. That is, the node of the first grammar rule.
  MemorySizeLanguageParser::MemorySizeStringContext* memorySizeStringContext{
      parser.memorySizeString()};

  // Our visitor should now convert this to `Memory` with the described memory
  // amount.
  return ToMemorySizeInstanceMemorySizeLanguageVisitor{}.visitMemorySizeString(
      memorySizeStringContext);
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const ad_utility::MemorySize& mem) {
  os << mem.asString();
  return os;
}

namespace memory_literals {
// _____________________________________________________________________________
MemorySize operator""_Byte(unsigned long long int bytes) {
  return MemorySize::bytes(bytes);
}

// _____________________________________________________________________________
MemorySize operator""_KB(long double kilobytes) {
  return MemorySize::kilobytes(static_cast<double>(kilobytes));
}

// _____________________________________________________________________________
MemorySize operator""_KB(unsigned long long int kilobytes) {
  return MemorySize::kilobytes(static_cast<size_t>(kilobytes));
}

// _____________________________________________________________________________
MemorySize operator""_MB(long double megabytes) {
  return MemorySize::megabytes(static_cast<double>(megabytes));
}

// _____________________________________________________________________________
MemorySize operator""_MB(unsigned long long int megabytes) {
  return MemorySize::megabytes(static_cast<size_t>(megabytes));
}

// _____________________________________________________________________________
MemorySize operator""_GB(long double gigabytes) {
  return MemorySize::gigabytes(static_cast<double>(gigabytes));
}

// _____________________________________________________________________________
MemorySize operator""_GB(unsigned long long int gigabytes) {
  return MemorySize::gigabytes(static_cast<size_t>(gigabytes));
}

// _____________________________________________________________________________
MemorySize operator""_TB(long double terabytes) {
  return MemorySize::terabytes(static_cast<double>(terabytes));
}

// _____________________________________________________________________________
MemorySize operator""_TB(unsigned long long int terabytes) {
  return MemorySize::terabytes(static_cast<size_t>(terabytes));
}

// _____________________________________________________________________________
MemorySize operator""_PB(long double petabytes) {
  return MemorySize::petabytes(static_cast<double>(petabytes));
}

// _____________________________________________________________________________
MemorySize operator""_PB(unsigned long long int petabytes) {
  return MemorySize::petabytes(static_cast<size_t>(petabytes));
}
}  // namespace memory_literals

}  // namespace ad_utility
