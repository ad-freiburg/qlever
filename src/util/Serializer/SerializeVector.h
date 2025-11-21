//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEVECTOR_H
#define QLEVER_SERIALIZEVECTOR_H

#include <cstdint>
#include <string>
#include <vector>

#include "backports/type_traits.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::similarToInstantiation<T, std::vector> ||
     ad_utility::similarToInstantiation<T, std::basic_string>)) {
  using V = typename std::decay_t<T>::value_type;
  auto size = arg.size();  // The value is ignored for `ReadSerializer`s.
  serializer | size;

  if constexpr (ReadSerializer<S>) {
    arg.resize(size);
  }
  if constexpr (TriviallySerializable<V>) {
    using CharPtr = std::conditional_t<ReadSerializer<S>, char*, const char*>;
    serializer.serializeBytes(reinterpret_cast<CharPtr>(arg.data()),
                              arg.size() * sizeof(V));
  } else {
    for (size_t i = 0; i < size; ++i) {
      serializer | arg[i];
    }
  }
}

/// Incrementally serialize a std::vector to disk without materializing it.
/// Call `push` for each of the elements that will become part of the vector.
CPP_template(typename T, typename Serializer)(
    requires WriteSerializer<Serializer>) class VectorIncrementalSerializer {
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
