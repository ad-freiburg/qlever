// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "EdgeInfo.h"

namespace JoinOrdering {

EdgeInfo::EdgeInfo() = default;
// EdgeInfo::EdgeInfo(Direction dir) : direction(dir) {}
EdgeInfo::EdgeInfo(Direction dir, float weight)
    : direction(dir), weight(weight) {}
}  // namespace JoinOrdering
