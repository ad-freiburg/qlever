//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEVECTOR_H
#define QLEVER_SERIALIZEVECTOR_H

#include <string>
#include <type_traits>
#include <vector>

#include "../TypeTraits.h"
#include "./Serializer.h"

namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_PARTIAL_READ(
    (ad_utility::similarToInstantiation<std::vector, T> ||
     ad_utility::similarToInstantiation<std::basic_string, T>)) {
  using V = typename std::decay_t<T>::value_type;
  if constexpr (TriviallySerializable<V>) {
    auto size = arg.size();  // just to get the right type
    serializer >> size;
    arg.resize(size);
    serializer.serializeBytes(reinterpret_cast<char*>(arg.data()),
                              arg.size() * sizeof(V));
  } else {
    auto size = arg.size();  // just to get the right type
    serializer >> size;
    arg.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      arg.emplace_back();
      serializer >> arg.back();
    }
  }
}

AD_SERIALIZE_FUNCTION_PARTIAL_WRITE(
    (ad_utility::similarToInstantiation<std::vector, T> ||
     ad_utility::similarToInstantiation<std::basic_string, T>)) {
  using V = typename std::decay_t<T>::value_type;
  if constexpr (TriviallySerializable<V>) {
    serializer << arg.size();
    serializer.serializeBytes(reinterpret_cast<const char*>(arg.data()),
                              arg.size() * sizeof(V));

  } else {
    serializer << arg.size();
    for (const auto& el : arg) {
      serializer << el;
    }
  }
}

/// Incrementally serialize a std::vector to disk without materializing it.
/// Call `push` for each of the elements that will become part of the vector.
template <typename T, WriteSerializer Serializer>
class VectorIncrementalSerializer {
 private:
  Serializer _serializer;
  uint64_t _startPosition;
  typename std::vector<T>::size_type _size = 0;
  bool _isFinished = false;

 public:
  explicit VectorIncrementalSerializer(Serializer&& serializer)
      : _serializer{std::move(serializer)},
        _startPosition{_serializer.getSerializationPosition()} {
    // `_size` does not have the correct value yet. The correct size will be set
    // in the finish() method.
    _serializer << _size;
  }

  void push(const T& element) {
    _serializer << element;
    _size++;
  }

  void finish() {
    if (_isFinished) {
      return;
    }
    _isFinished = true;
    auto endPosition = _serializer.getSerializationPosition();
    _serializer.setSerializationPosition(_startPosition);
    _serializer << _size;
    _serializer.setSerializationPosition(endPosition);
  }

  Serializer serializer() && {
    finish();
    return std::move(_serializer);
  }

  ~VectorIncrementalSerializer() { finish(); }
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEVECTOR_H
