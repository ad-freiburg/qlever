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
 * Serializer that writes to a buffer of bytes. The `AlignedSerialization`
 * template parameter controls whether alignment padding is inserted for
 * trivially serializable types (see `alignForType` in `Serializer.h`).
 */
template <bool AlignedSerialization = false>
class ByteBufferWriteSerializerT {
 public:
  using SerializerType = WriteSerializerTag;
  using Storage = std::vector<char>;
  static constexpr bool UsesAlignedSerialization = AlignedSerialization;

  ByteBufferWriteSerializerT() = default;
  ByteBufferWriteSerializerT(const ByteBufferWriteSerializerT&) = delete;
  ByteBufferWriteSerializerT& operator=(const ByteBufferWriteSerializerT&) =
      delete;
  ByteBufferWriteSerializerT(ByteBufferWriteSerializerT&&) = default;
  ByteBufferWriteSerializerT& operator=(ByteBufferWriteSerializerT&&) = default;

  void serializeBytes(const char* bytePointer, size_t numBytes) {
    _data.insert(_data.end(), bytePointer, bytePointer + numBytes);
  }

  void clear() { _data.clear(); }

  const Storage& data() const& noexcept { return _data; }
  Storage&& data() && { return std::move(_data); }
  void reserve(size_t n) { _data.reserve(n); }

  // Get the current write position (number of bytes written so far).
  size_t getCurrentPosition() const { return _data.size(); }

 private:
  Storage _data;
};

/**
 * Serializer that reads from a buffer of bytes. The `AlignedSerialization`
 * template parameter controls whether alignment padding is skipped for
 * trivially serializable types (see `alignForType` in `Serializer.h`).
 */
template <bool AlignedSerialization = false>
class ByteBufferReadSerializerT {
 public:
  using SerializerType = ReadSerializerTag;
  using Storage = std::vector<char>;
  static constexpr bool UsesAlignedSerialization = AlignedSerialization;

  explicit ByteBufferReadSerializerT(Storage data) : _data{std::move(data)} {};
  void serializeBytes(char* bytePointer, size_t numBytes) {
    AD_CONTRACT_CHECK(_iterator + numBytes <= _data.end());
    std::copy(_iterator, _iterator + numBytes, bytePointer);
    _iterator += numBytes;
  }

  ByteBufferReadSerializerT(const ByteBufferReadSerializerT&) noexcept = delete;
  ByteBufferReadSerializerT& operator=(const ByteBufferReadSerializerT&) =
      delete;
  ByteBufferReadSerializerT(ByteBufferReadSerializerT&&) noexcept = default;
  ByteBufferReadSerializerT& operator=(ByteBufferReadSerializerT&&) noexcept =
      default;

  const Storage& data() const noexcept { return _data; }

  // Get the current read position (number of bytes read so far).
  size_t getCurrentPosition() const {
    return static_cast<size_t>(_iterator - _data.begin());
  }

  // Skip the given number of bytes without reading them.
  void skip(size_t numBytes) {
    AD_CONTRACT_CHECK(_iterator + numBytes <= _data.end());
    _iterator += numBytes;
  }

 private:
  Storage _data;
  Storage::const_iterator _iterator{_data.begin()};
};

// Backward-compatible aliases for the default (unaligned) serializers.
using ByteBufferWriteSerializer = ByteBufferWriteSerializerT<false>;
using ByteBufferReadSerializer = ByteBufferReadSerializerT<false>;

// Aligned variants of the byte buffer serializers.
using AlignedByteBufferWriteSerializer = ByteBufferWriteSerializerT<true>;
using AlignedByteBufferReadSerializer = ByteBufferReadSerializerT<true>;

}  // namespace ad_utility::serialization

#endif  // QLEVER_BYTEBUFFERSERIALIZER_H
