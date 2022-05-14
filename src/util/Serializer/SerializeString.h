//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZESTRING_H
#define QLEVER_SERIALIZESTRING_H

#include <string>

#include "../TypeTraits.h"

namespace ad_utility::serialization {

template <Serializer S>
void serialize(S& serializer,
               ad_utility::SimilarTo<std::string> auto&& string) {
  if constexpr (WriteSerializer<S>) {
    serializer << string.size();
    serializer.serializeBytes(string.data(), string.size());
  } else {
    auto size = string.size();  // just to get the right type
    serializer >> size;
    string.resize(size);
    serializer.serializeBytes(string.data(), string.size());
  }
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZESTRING_H
