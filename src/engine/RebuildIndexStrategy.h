//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_REBUILDINDEXSTRATEGY_H
#define QLEVER_SRC_ENGINE_REBUILDINDEXSTRATEGY_H

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "backports/three_way_comparison.h"

namespace qlever {

// Strategy for automatically rebuilding the index from the current data
// (including updates), configured via the `--rebuild-index-strategy` option of
// `qlever-server`.
//
// The idea is a single threshold on the number of delta triples (inserted plus
// deleted): a rebuild is triggered once the delta triples reach that many. The
// threshold is `fraction` of the number of triples in the current index (so
// that it adapts to the index size), but never smaller than `min` and never
// larger than `max`. In other words, the threshold is
//
//   clamp(fraction * numIndexTriples, min, max)
//
// so `min` avoids frequent rebuilds when the index is still small or empty (a
// fraction of little is little), and `max` bounds the absolute cost of
// querying and updating the delta. Setting `min == max` makes the threshold a
// fixed number of delta triples, independent of the index size.
struct RebuildIndexStrategy {
  // Lower bound on the rebuild threshold: never rebuild before the delta
  // triples reach this many.
  size_t minDeltaTriples_ = 0;

  // Upper bound on the rebuild threshold: always rebuild once the delta
  // triples reach this many.
  size_t maxDeltaTriples_ = 0;

  // The rebuild threshold as a fraction of the number of index triples, before
  // clamping to `[min, max]`.
  double fractionOfIndexTriples_ = 0.0;

  // The number of delta triples at which a rebuild is triggered, given the
  // current number of index triples, see the class comment.
  double rebuildThreshold(size_t numIndexTriples) const {
    return std::clamp(
        fractionOfIndexTriples_ * static_cast<double>(numIndexTriples),
        static_cast<double>(minDeltaTriples_),
        static_cast<double>(maxDeltaTriples_));
  }

  // Decide whether a rebuild should be triggered: `true` iff the number of
  // delta triples has reached `rebuildThreshold`.
  bool shouldTriggerRebuild(size_t numDeltaTriples,
                            size_t numIndexTriples) const {
    return static_cast<double>(numDeltaTriples) >=
           rebuildThreshold(numIndexTriples);
  }

  // Parse the value of the `--rebuild-index-strategy` option:
  // - "manual": returns `std::nullopt`, i.e. rebuilds are only triggered
  //   manually via the `cmd=rebuild-index` HTTP request.
  // - "min:max:fraction": the strategy above, where `min` and `max` are
  //   non-negative numbers of delta triples and `fraction` is a floating-point
  //   number greater than zero (e.g. `10000:1000000:0.1`).
  // Throws `std::runtime_error` for any other value.
  static std::optional<RebuildIndexStrategy> parse(std::string_view strategy) {
    if (strategy == "manual") {
      return std::nullopt;
    }
    std::vector<std::string_view> parts = absl::StrSplit(strategy, ':');
    if (parts.size() != 3) {
      throw std::runtime_error(absl::StrCat(
          "The value \"", strategy,
          "\" is neither \"manual\" nor of the form \"min:max:fraction\""));
    }
    RebuildIndexStrategy result;
    result.minDeltaTriples_ = parseCount(parts[0], "min");
    result.maxDeltaTriples_ = parseCount(parts[1], "max");
    if (!absl::SimpleAtod(parts[2], &result.fractionOfIndexTriples_) ||
        !(result.fractionOfIndexTriples_ > 0.0)) {
      throw std::runtime_error(absl::StrCat(
          "The fraction \"", parts[2],
          "\" in the rebuild index strategy must be a number greater than 0"));
    }
    if (result.minDeltaTriples_ > result.maxDeltaTriples_) {
      throw std::runtime_error(absl::StrCat(
          "In the rebuild index strategy, `min` (", result.minDeltaTriples_,
          ") must not be larger than `max` (", result.maxDeltaTriples_, ")"));
    }
    return result;
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(RebuildIndexStrategy,
                                              minDeltaTriples_,
                                              maxDeltaTriples_,
                                              fractionOfIndexTriples_)

 private:
  // Parse a non-negative number of delta triples, `name` is used in the error
  // message.
  static size_t parseCount(std::string_view number, std::string_view name) {
    size_t result = 0;
    if (!absl::SimpleAtoi(number, &result)) {
      throw std::runtime_error(
          absl::StrCat("The value \"", number, "\" for `", name,
                       "` in the rebuild index strategy is not a "
                       "non-negative number"));
    }
    return result;
  }
};

}  // namespace qlever

#endif  // QLEVER_SRC_ENGINE_REBUILDINDEXSTRATEGY_H
