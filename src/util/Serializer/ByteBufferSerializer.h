//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BYTEBUFFERSERIALIZER_H
#define QLEVER_BYTEBUFFERSERIALIZER_H

#include <vector>

#include "backports/algorithm.h"
#include "backports/type_traits.h"
#include "util/Exception.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility::serialization {
/**
 * Serializer that writes to a buffer of bytes.
 */
class ByteBufferWriteSerializer {
 public:
  using SerializerType = WriteSerializerTag;
  using Storage = std::vector<char>;
  ByteBufferWriteSerializer() = default;
  ByteBufferWriteSerializer(const ByteBufferWriteSerializer&) = delete;
  ByteBufferWriteSerializer& operator=(const ByteBufferWriteSerializer&) =
      delete;
  ByteBufferWriteSerializer(ByteBufferWriteSerializer&&) = default;
  ByteBufferWriteSerializer& operator=(ByteBufferWriteSerializer&&) = default;

  void serializeBytes(const char* bytePointer, size_t numBytes) {
    _data.insert(_data.end(), bytePointer, bytePointer + numBytes);
  }

  void clear() { _data.clear(); }

  const Storage& data() const& noexcept { return _data; }
  Storage&& data() && { return std::move(_data); }
  void reserve(size_t n) { _data.reserve(n); }

 private:
  Storage _data;
};

/**
 * Serializer that reads from a buffer of bytes.
 */
class ByteBufferReadSerializer {
 public:
  using SerializerType = ReadSerializerTag;
  using Storage = std::vector<char>;

  explicit ByteBufferReadSerializer(Storage data) : _data{std::move(data)} {};
  void serializeBytes(char* bytePointer, size_t numBytes) {
    AD_CONTRACT_CHECK(_iterator + numBytes <= _data.end());
    std::copy(_iterator, _iterator + numBytes, bytePointer);
    _iterator += numBytes;
  }

  ByteBufferReadSerializer(const ByteBufferReadSerializer&) noexcept = delete;
  ByteBufferReadSerializer& operator=(const ByteBufferReadSerializer&) = delete;
  ByteBufferReadSerializer(ByteBufferReadSerializer&&) noexcept = default;
  ByteBufferReadSerializer& operator=(ByteBufferReadSerializer&&) noexcept =
      default;

  const Storage& data() const noexcept { return _data; }

 private:
  Storage _data;
  Storage::const_iterator _iterator{_data.begin()};
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_BYTEBUFFERSERIALIZER_H
