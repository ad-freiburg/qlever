// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "index/LocationTypes.h"

DisableUpdatesOrBlockOffset addOffset(DisableUpdatesOrBlockOffset blockOffset,
                                      size_t delta) {
  if (std::holds_alternative<DisableUpdates>(blockOffset)) {
    return DisableUpdates{};
  } else {
    return std::get<size_t>(blockOffset) + delta;
  }
}

std::ostream& operator<<(
    std::ostream& os, const DisableUpdatesOrBlockOffset& updatesOrBlockOffset) {
  if (holds_alternative<DisableUpdates>(updatesOrBlockOffset)) {
    return os << "Updates disabled";
  } else {
    return os << "Offset(" << get<size_t>(updatesOrBlockOffset) << ")";
  }
}
