//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BYTEBUFFERSERIALIZER_H
#define QLEVER_BYTEBUFFERSERIALIZER_H

#include <absl/base/casts.h>

#include <cstdint>
#include <vector>

#include "backports/algorithm.h"
#include "backports/type_traits.h"
#include "util/AlignedAllocator.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility::serialization {
/**
 * Serializer that writes to a buffer of bytes. The `AlignedSerialization`
 * template parameter controls whether alignment padding is inserted for
 * trivially serializable types (see `alignSerializerForType` in
 * `Serializer.h`).
 */
template <bool usesAlignedSerialization = false>
class ByteBufferWriteSerializerT : public NoCopy {
 public:
  using SerializerType = WriteSerializerTag;
  using Storage =
      std::conditional_t<usesAlignedSerialization,
                         std::vector<char, ad_utility::AlignedAllocator<char>>,
                         std::vector<char>>;
  static constexpr bool UsesAlignedSerialization = usesAlignedSerialization;

  ByteBufferWriteSerializerT() = default;

  void serializeBytes(const char* bytePointer, size_t numBytes) {
    data_.insert(data_.end(), bytePointer, bytePointer + numBytes);
  }

  void clear() { data_.clear(); }

  const Storage& data() const& noexcept { return data_; }
  Storage&& data() && { return std::move(data_); }
  void reserve(size_t n) { data_.reserve(n); }

  // Get the current write position (number of bytes written so far).
  size_t getCurrentPosition() const { return data_.size(); }

  // Overload of `serializeAtPosition` (see `Serializer.h`) for a
  // `ByteBufferWriteSerializerT`. Write the `element` over the bytes that
  // start at `position` (which have to have been written before), without
  // changing the current write position. Use this to fill in a placeholder
  // (for example the size of a block of data, which is only known once that
  // block has been written completely).
  //
  // NOTE: This is a hidden friend (and hence only found via ADL) because it
  // needs access to the buffer, which is not part of the public interface of
  // this class.
  template <typename T>
  friend void serializeAtPosition(ByteBufferWriteSerializerT& serializer,
                                  uint64_t position, const T& element) {
    OverwritingSerializer overwritingSerializer{serializer.data_, position};
    overwritingSerializer << element;
  }

 private:
  // A `WriteSerializer` that does not append to the buffer, but overwrites the
  // bytes that start at a given position (which have to have been written
  // before). Used by `serializeAtPosition` above, so that the patching of a
  // placeholder needs no temporary buffer.
  class OverwritingSerializer {
   public:
    using SerializerType = WriteSerializerTag;
    static constexpr bool UsesAlignedSerialization = usesAlignedSerialization;

    OverwritingSerializer(Storage& data, size_t position)
        : data_{data}, position_{position} {}

    void serializeBytes(const char* bytePointer, size_t numBytes) {
      AD_CONTRACT_CHECK(position_ + numBytes <= data_.size());
      std::copy(bytePointer, bytePointer + numBytes, data_.begin() + position_);
      position_ += numBytes;
    }

    // The position in the buffer at which the next byte will be written. This
    // is required for the alignment handling, see `alignSerializerForType` in
    // `Serializer.h`.
    size_t getCurrentPosition() const { return position_; }

   private:
    Storage& data_;
    size_t position_;
  };

  Storage data_;
};

/**
 * Serializer that reads from a buffer of bytes. The `AlignedSerialization`
 * template parameter controls whether alignment padding is skipped for
 * trivially serializable types (see `alignSerializerForType` in
 * `Serializer.h`). The underlying `Storage` can be any random access range over
 * `const char`, in particular `std::vector<char>` and `ql::span<const char>`.
 */
template <bool AlignedSerialization = false,
          typename Storage = std::vector<char>>
class ByteBufferReadSerializerT : public NoCopy {
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
  explicit ByteBufferReadSerializerT(Storage data) : data_{std::move(data)} {
    if constexpr (AlignedSerialization) {
      AD_CONTRACT_CHECK(
          absl::bit_cast<std::uintptr_t>(data_.data()) %
                  alignof(std::max_align_t) ==
              0,
          "Buffer passed to an aligned read serializer must be aligned to "
          "`alignof(std::max_align_t)`");
    }
  }
  void serializeBytes(char* bytePointer, size_t numBytes) {
    AD_CONTRACT_CHECK(iterator_ + numBytes <= data_.end());
    std::copy(iterator_, iterator_ + numBytes, bytePointer);
    iterator_ += numBytes;
  }

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
using AlignedByteBufferReadSerializer = ByteBufferReadSerializerT<
    true, std::vector<char, ad_utility::AlignedAllocator<char>>>;

static_assert(ZeroCopyReadSerializer<AlignedByteBufferReadSerializer>);

}  // namespace ad_utility::serialization

#endif  // QLEVER_BYTEBUFFERSERIALIZER_H
