// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

namespace JoinOrdering {

enum class Direction {
  UNDIRECTED,
  PARENT,
  CHILD,

};

class EdgeInfo {
 public:
  // read from left to right
  // Ra is a dir of Rb
  Direction direction{Direction::UNDIRECTED};
  bool hidden{false};  // instead of erasing

  EdgeInfo();
  explicit EdgeInfo(Direction dir);
};

}  // namespace JoinOrdering
