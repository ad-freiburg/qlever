// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_NUMERICRUN_H
#define QLEVER_SRC_ENGINE_NUMERICRUN_H

#include <cstddef>
#include <optional>
#include <vector>

#include "backports/algorithm.h"
#include "global/Id.h"

// The parts ("runs") into which the internal order of the `Id`s (that is, the
// order by their bits, see `valueIdComparators::compareByBits`) splits the
// `Int`s and the `Double`s. Within each run, the internal order agrees with the
// semantic order of `ORDER BY` (see `OrderByComparator`), up to the direction.
// So a sequence of `Int`s or of `Double`s that is sorted in the internal order
// is brought into the semantic order by rearranging (and possibly reversing)
// its runs, without comparing any values.
//
// The enumerators are in the internal order. Each run is sorted by ascending
// value in the internal order, except for `DoubleNegative`, which is sorted by
// descending value (the magnitude increases with the bits). `ORDER BY` puts all
// `NaN`s after all other doubles. The sign bit of a `NaN` may be set, so there
// are two runs of `NaN`s.
enum class NumericRun {
  IntNonNegative,     // [0 .. max]
  IntNegative,        // [min .. -1]
  DoubleNonNegative,  // [0.0 .. +inf]
  DoublePositiveNan,
  DoubleNegative,  // [-0.0 .. -inf]
  DoubleNegativeNan,
};
// The number of enumerators of `NumericRun`.
constexpr size_t numNumericRuns =
    static_cast<size_t>(NumericRun::DoubleNegativeNan) + 1;

namespace numericRun {

// Return the index of the `run` in `[0, numNumericRuns)`.
constexpr size_t toIndex(NumericRun run) { return static_cast<size_t>(run); }

// Return the run of the `id`, or `std::nullopt` if it is neither an `Int` nor
// a `Double`.
std::optional<NumericRun> getRunOfId(Id id);

// Return the datatype of the values in the `run` (`Int` or `Double`).
Datatype getDatatype(NumericRun run);

// Return the datatype of the `first` and the `last` `Id` of a sequence that is
// sorted in the internal order, if both are `Int`s or both are `Double`s.
// Then all `Id`s of the sequence have that datatype, because the datatype bits
// are the most significant bits of an `Id`. Otherwise return `std::nullopt`.
std::optional<Datatype> getCommonNumericDatatype(Id first, Id last);

// Return the runs of the given numeric datatype in the internal order.
std::vector<NumericRun> getRunsInInternalOrder(Datatype type);

// A run, and whether the semantic order traverses it in reverse internal
// order.
struct RunWithDirection {
  NumericRun run_;
  bool reversed_;
};

// Return the runs of the given numeric datatype in the semantic order of
// `ORDER BY ASC` (`isDescending == false`) or `ORDER BY DESC`, each with the
// direction in which it has to be traversed. For `ASC`, these are the negative
// values first, then the non-negative ones, then the `NaN`s.
std::vector<RunWithDirection> getRunsInSemanticOrder(Datatype type,
                                                     bool isDescending);

// Reverse the order of the `elements` and the direction (`reversed_`) of each
// of them. This turns a traversal in the order of `ORDER BY ASC` into one in
// the order of `ORDER BY DESC`.
template <typename T>
void reverseOrderAndDirection(std::vector<T>& elements) {
  ql::ranges::reverse(elements);
  for (auto& element : elements) {
    element.reversed_ = !element.reversed_;
  }
}

}  // namespace numericRun

#endif  // QLEVER_SRC_ENGINE_NUMERICRUN_H
