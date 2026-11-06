// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/NumericRun.h"

#include <cmath>

#include "util/Exception.h"

namespace numericRun {

namespace {
// Return the run of a `Double` with the given `value`.
NumericRun getRunOfDouble(double value) {
  bool isNan = std::isnan(value);
  if (std::signbit(value)) {
    return isNan ? NumericRun::DoubleNegativeNan : NumericRun::DoubleNegative;
  }
  return isNan ? NumericRun::DoublePositiveNan : NumericRun::DoubleNonNegative;
}
}  // namespace

// _____________________________________________________________________________
std::optional<NumericRun> getRunOfId(Id id) {
  switch (id.getDatatype()) {
    case Datatype::Int:
      return id.getInt() >= 0 ? NumericRun::IntNonNegative
                              : NumericRun::IntNegative;
    case Datatype::Double:
      return getRunOfDouble(id.getDouble());
    default:
      return std::nullopt;
  }
}

// _____________________________________________________________________________
Datatype getDatatype(NumericRun run) {
  return run <= NumericRun::IntNegative ? Datatype::Int : Datatype::Double;
}

// _____________________________________________________________________________
std::optional<Datatype> getCommonNumericDatatype(Id first, Id last) {
  auto runOfFirst = getRunOfId(first);
  auto runOfLast = getRunOfId(last);
  if (!runOfFirst.has_value() || !runOfLast.has_value() ||
      getDatatype(runOfFirst.value()) != getDatatype(runOfLast.value())) {
    return std::nullopt;
  }
  return getDatatype(runOfFirst.value());
}

// _____________________________________________________________________________
std::vector<NumericRun> getRunsInInternalOrder(Datatype type) {
  if (type == Datatype::Int) {
    return {NumericRun::IntNonNegative, NumericRun::IntNegative};
  }
  AD_CORRECTNESS_CHECK(type == Datatype::Double);
  return {NumericRun::DoubleNonNegative, NumericRun::DoublePositiveNan,
          NumericRun::DoubleNegative, NumericRun::DoubleNegativeNan};
}

// _____________________________________________________________________________
std::vector<RunWithDirection> getRunsInSemanticOrder(Datatype type,
                                                     bool isDescending) {
  std::vector<RunWithDirection> runs;
  if (type == Datatype::Int) {
    runs = {{NumericRun::IntNegative, false},
            {NumericRun::IntNonNegative, false}};
  } else {
    AD_CORRECTNESS_CHECK(type == Datatype::Double);
    // NOTE: The SPARQL standard does not say where `NaN`s go. Putting them
    // last is how the regular comparator of `ORDER BY` does it (see
    // `OrderByComparator`).
    // NOTE: Negative doubles are the only reversed run: their bit patterns
    // (sign-magnitude) increase with the absolute value, so internally they
    // are sorted from -0 down to -inf.
    runs = {{NumericRun::DoubleNegative, true},
            {NumericRun::DoubleNonNegative, false},
            {NumericRun::DoublePositiveNan, false},
            {NumericRun::DoubleNegativeNan, false}};
  }
  if (isDescending) {
    reverseOrderAndDirection(runs);
  }
  return runs;
}

}  // namespace numericRun
