//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZESTRING_H
#define QLEVER_SERIALIZESTRING_H

#include <cstddef>
#include <string_view>

#include "util/Serializer/SerializeVector.h"

namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT_WRITE(
    (ad_utility::isSimilar<T, std::string_view>)) {
  serializer << arg.size();
  serializer.serializeBytes(arg.data(), arg.size());
}

// Read a string from the serializer without copying it. The returned view
// points into the serializer's internal buffer, so it is only valid as long as
// that buffer lives (and is not moved). Can only be called if a `std::string`
// or `std::string_view` was written to the serializer at its current position.
// Unlike `zeroCopyDeserializeToSpan` (see `SerializeVector.h`), this needs no
// aligned serializer, because the bytes of a string have no alignment
// requirement.
CPP_template(typename S)(requires SpanReadSerializer<S>)
std::string_view zeroCopyDeserializeToStringView(S& serializer) {
  std::size_t size;
  serializer >> size;
  auto bytes = serializer.getSpanToBytes(size);
  AD_CORRECTNESS_CHECK(bytes.size() == size);
  return std::string_view{bytes.data(), size};
}
}  // namespace ad_utility::serialization

// The Strings and Vectors can be serialized in exactly the same way, this
// header just exists for convenience and consistency.

#endif  // QLEVER_SERIALIZESTRING_H
