// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "util/Memory.h"

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
  return sizetDivision(memoryInBytes_, ad_utility::pow<size_t>(2, 10));
}

// _____________________________________________________________________________
double Memory::megabytes() const {
  return sizetDivision(memoryInBytes_, ad_utility::pow<size_t>(2, 20));
}

// _____________________________________________________________________________
double Memory::gigabytes() const {
  return sizetDivision(memoryInBytes_, ad_utility::pow<size_t>(2, 30));
}

// _____________________________________________________________________________
double Memory::terabytes() const {
  return sizetDivision(memoryInBytes_, ad_utility::pow<size_t>(2, 40));
}

// _____________________________________________________________________________
double Memory::petabytes() const {
  return sizetDivision(memoryInBytes_, ad_utility::pow<size_t>(2, 50));
}

}  // namespace ad_utility
