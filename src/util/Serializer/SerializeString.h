//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZESTRING_H
#define QLEVER_SERIALIZESTRING_H

#include <string_view>

#include "./SerializeVector.h"

namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT_WRITE(
    (ad_utility::isSimilar<T, std::string_view>)) {
  serializer << arg.size();
  serializer.serializeBytes(arg.data(), arg.size());
}
}  // namespace ad_utility::serialization

// The Strings and Vectors can be serialized in exactly the same way, this
// header just exists for convenience and consistency.

#endif  // QLEVER_SERIALIZESTRING_H
