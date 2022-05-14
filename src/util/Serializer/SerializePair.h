//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEPAIR_H
#define QLEVER_SERIALIZEPAIR_H

#include <utility>

#include "../TypeTraits.h"

namespace ad_utility::serialization {
template <typename Serializer, typename Pair>
requires ad_utility::isInstantiation<std::pair, std::decay_t<Pair>> void
serialize(Serializer& serializer, Pair&& pair) {
  serializer | pair.first;
  serializer | pair.second;
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEPAIR_H
