//
// Created by johannes on 01.05.21.
//

#ifndef QLEVER_SERIALIZEVECTOR_H
#define QLEVER_SERIALIZEVECTOR_H

#include <vector>

// TODO<joka921> : optimization for trivially serializable types
namespace ad_utility::serialization {
template <typename Serializer, typename T, typename Alloc>
void serialize(Serializer& serializer, std::vector<T, Alloc>& vector) {
  if constexpr (Serializer::IsWriteSerializer) {
    serializer << vector.size();
    if constexpr (std::is_trivially_copyable_v<T>) {
      serializer.serializeBytes(reinterpret_cast<char*>(const_cast<T*>(vector.data())), vector.size() * sizeof(T));
    } else {
      for (const auto& el : vector) {
        serializer << el;
      }
    }
  } else {
    auto size = vector.size();  // just to get the right type
    serializer >> size;
    vector.resize(size);
    if constexpr (std::is_trivially_copyable_v<T>) {
      serializer.serializeBytes(reinterpret_cast<char*>(vector.data()), vector.size() * sizeof(T));
    } else {
      for (size_t i = 0; i < size; ++i) {
        serializer >> vector.back();
      }
    }
  }
}

}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEVECTOR_H
