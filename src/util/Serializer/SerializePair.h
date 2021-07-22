//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEPAIR_H
#define QLEVER_SERIALIZEPAIR_H

#include <utility>

namespace ad_utility::serialization {
template <typename Serializer, typename First, typename Second>
void serialize(Serializer& serializer, std::pair<First, Second>& pair) {
  serializer | pair.first;
  serializer | pair.second;
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEPAIR_H
