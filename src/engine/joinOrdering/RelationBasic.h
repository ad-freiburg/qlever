// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once
#include <string>

namespace JoinOrdering {

/**
 * bare-minimum required for a relation to be added to the
 * QueryGraph::add_relation
 */
class RelationBasic {
 public:
  RelationBasic();
  RelationBasic(const RelationBasic& r);
  RelationBasic(std::string label, int cardinality);

  auto operator<=>(const RelationBasic& other) const;
  bool operator==(const RelationBasic& other) const;
  [[nodiscard]] int getCardinality() const;
  [[nodiscard]] std::string getLabel() const;

 private:
  int cardinality{-1};
  std::string label{"R?"};
};

}  // namespace JoinOrdering
