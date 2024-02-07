//   Copyright 2024, University of Freiburg,
//                   Chair of Algorithms and Data Structures.
//   Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

namespace ad_utility {
struct NoCopyDefaultMove {
  NoCopyDefaultMove() = default;
  NoCopyDefaultMove(const NoCopyDefaultMove&) = delete;
  NoCopyDefaultMove& operator=(const NoCopyDefaultMove&) = delete;

  NoCopyDefaultMove(NoCopyDefaultMove&&) = default;
  NoCopyDefaultMove& operator=(NoCopyDefaultMove&&) = default;
};
}  // namespace ad_utility
