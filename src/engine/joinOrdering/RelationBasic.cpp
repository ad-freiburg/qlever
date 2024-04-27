// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "RelationBasic.h"

#include <utility>

namespace JoinOrdering {

RelationBasic::RelationBasic() = default;
// RelationBasic::RelationBasic(const RelationBasic& r) {
//   this->label = r.label;
//   this->cardinality = r.cardinality;
// }

RelationBasic::RelationBasic(std::string label, int cardinality)
    : cardinality(cardinality), label(std::move(label)) {}
auto RelationBasic::operator<=>(const RelationBasic& other) const = default;

bool RelationBasic::operator==(const RelationBasic& other) const {
  return this->cardinality == other.cardinality && this->label == other.label;
}
int RelationBasic::getCardinality() const { return cardinality; }
std::string RelationBasic::getLabel() const { return label; }
}  // namespace JoinOrdering
