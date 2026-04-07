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
    data_.insert(data_.end(), bytePointer, bytePointer + numBytes);
  }

  void clear() { data_.clear(); }

  const Storage& data() const& noexcept { return data_; }
  Storage&& data() && { return std::move(data_); }
  void reserve(size_t n) { data_.reserve(n); }

  // Get the current write position (number of bytes written so far).
  size_t getCurrentPosition() const { return data_.size(); }

 private:
  Storage data_;
};

/**
 * Serializer that reads from a buffer of bytes. The `AlignedSerialization`
 * template parameter controls whether alignment padding is skipped for
 * trivially serializable types (see `alignForType` in `Serializer.h`).
 * The underlying `Storage` can be any random access range over `const char`, in
 * particular `std::vector<char>` and `ql::span<const char>`.
 */
template <bool AlignedSerialization = false,
          typename Storage = std::vector<char>>
class ByteBufferReadSerializerT {
 public:
  static_assert(ql::ranges::random_access_range<Storage>);
  static_assert(
      ql::concepts::same_as<ql::ranges::range_value_t<Storage>, char>);
  using SerializerType = ReadSerializerTag;
  static constexpr bool UsesAlignedSerialization = AlignedSerialization;

 private:
  Storage data_;
  ql::ranges::iterator_t<std::add_const_t<Storage>> iterator_{data_.begin()};

 public:
  explicit ByteBufferReadSerializerT(Storage data) : data_{std::move(data)} {};
  void serializeBytes(char* bytePointer, size_t numBytes) {
    AD_CONTRACT_CHECK(iterator_ + numBytes <= data_.end());
    std::copy(iterator_, iterator_ + numBytes, bytePointer);
    iterator_ += numBytes;
  }

  ByteBufferReadSerializerT(const ByteBufferReadSerializerT&) noexcept = delete;
  ByteBufferReadSerializerT& operator=(const ByteBufferReadSerializerT&) =
      delete;
  ByteBufferReadSerializerT(ByteBufferReadSerializerT&&) noexcept = default;
  ByteBufferReadSerializerT& operator=(ByteBufferReadSerializerT&&) noexcept =
      default;

  const Storage& data() const noexcept { return data_; }

  // Get the current read position (number of bytes read so far).
  size_t getCurrentPosition() const {
    return static_cast<size_t>(iterator_ - data_.begin());
  }

  // Skip the given number of bytes without reading them.
  void skip(size_t numBytes) {
    ensureBytesAvailable(numBytes);
    iterator_ += numBytes;
  }

  // Get a span to the next `numBytes` in the buffer without copying.
  // This enables zero-copy deserialization. The internal iterator is advanced
  // by `numBytes`, so the output is assumed to be already consumed after
  // calling this function.
  ql::span<const char> getSpanToBytes(size_t numBytes) {
    ensureBytesAvailable(numBytes);
    const char* ptr = data_.data() + (iterator_ - data_.begin());
    iterator_ += numBytes;
    return {ptr, numBytes};
  }

 private:
  // Helper function to ensure that at least `numBytes` are available in the
  // buffer.
  void ensureBytesAvailable(size_t numBytes) const {
    if (static_cast<size_t>(data_.end() - iterator_) < numBytes) {
      throw SerializationException{
          "Tried to read/access bytes in ByteBufferReadSerializer but not "
          "enough bytes available"};
    }
  }
};

// Backward-compatible aliases for the default (unaligned) serializers.
using ByteBufferWriteSerializer = ByteBufferWriteSerializerT<false>;
using ByteBufferReadSerializer = ByteBufferReadSerializerT<false>;

// Aligned variants of the byte buffer serializers.
using AlignedByteBufferWriteSerializer = ByteBufferWriteSerializerT<true>;
using AlignedByteBufferReadSerializer = ByteBufferReadSerializerT<true>;

}  // namespace ad_utility::serialization

#endif  // QLEVER_BYTEBUFFERSERIALIZER_H
