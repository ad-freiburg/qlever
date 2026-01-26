//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEPAIR_H
#define QLEVER_SERIALIZEPAIR_H

#include <utility>

#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::similarToInstantiation<T, std::pair>)) {
  serializer | arg.first;
  serializer | arg.second;
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEPAIR_H
