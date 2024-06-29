// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <stddef.h>

#include <ostream>
#include <variant>

// TODO<qup42, cpp23> Replace with `std::optional<size_t>` and use `transform`.
struct DisableUpdates {};
// - `size_t` The first block of given contiguous range of blocks is at this
// position in the index.
// - `DisableUpdates` The position of the first block in the index is not known
// and updates therefore cannot be applied.
using DisableUpdatesOrBlockOffset = std::variant<DisableUpdates, size_t>;

DisableUpdatesOrBlockOffset addOffset(DisableUpdatesOrBlockOffset blockOffset,
                                      size_t delta);

std::ostream& operator<<(
    std::ostream& os, const DisableUpdatesOrBlockOffset& updatesOrBlockOffset);
