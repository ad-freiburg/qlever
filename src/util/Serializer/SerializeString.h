//
// Created by johannes on 01.05.21.
//

#ifndef QLEVER_SERIALIZESTRING_H
#define QLEVER_SERIALIZESTRING_H

#include <string>

namespace ad_utility::serialization {

template <typename Serializer>
void serialize(Serializer& serializer, std::string& string) {
  if constexpr (Serializer::IsWriteSerializer) {
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
