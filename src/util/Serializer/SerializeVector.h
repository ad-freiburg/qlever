//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEVECTOR_H
#define QLEVER_SERIALIZEVECTOR_H

#include <type_traits>
#include <vector>

namespace ad_utility::serialization {
template <typename Serializer, typename T, typename Alloc>
void serialize(Serializer& serializer, std::vector<T, Alloc>& vector) {
  if constexpr (std::is_trivially_copyable_v<T> &&
                std::is_default_constructible_v<T>) {
    if constexpr (Serializer::IsWriteSerializer) {
      serializer << vector.size();
      serializer.serializeBytes(reinterpret_cast<const char*>(vector.data()),
                                vector.size() * sizeof(T));
    } else {
      auto size = vector.size();  // just to get the right type
      serializer >> size;
      vector.resize(size);
      serializer.serializeBytes(reinterpret_cast<char*>(vector.data()),
                                vector.size() * sizeof(T));
    }

  } else {
    if constexpr (Serializer::IsWriteSerializer) {
      serializer << vector.size();
      for (const auto& el : vector) {
        serializer << el;
      }
    } else {
      auto size = vector.size();  // just to get the right type
      serializer >> size;
      vector.reserve(size);
      for (size_t i = 0; i < size; ++i) {
        vector.emplace_back();
        serializer >> vector.back();
      }
    }
  }
}

/// Incrementally serialize a std::vector to disk without materializing it.
/// Call `push` for each of the elements that will become part of the vector.
template <typename T, typename Serializer>
requires Serializer::IsWriteSerializer class VectorIncrementalSerializer {
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
