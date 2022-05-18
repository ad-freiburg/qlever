//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEPAIR_H
#define QLEVER_SERIALIZEPAIR_H

#include <utility>

#include "../TypeTraits.h"
#include "./Serializer.h"

namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_PARTIAL(
    (ad_utility::similarToInstantiation<std::pair, T>)) {
  serializer | arg.first;
  serializer | arg.second;
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEPAIR_H
