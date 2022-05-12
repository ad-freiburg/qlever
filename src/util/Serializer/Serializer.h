// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

// Library for symmetric serialization

#ifndef QLEVER_SERIALIZER_SERIALIZER
#define QLEVER_SERIALIZER_SERIALIZER

#include <algorithm>
#include <type_traits>
#include <vector>

#include "../Exception.h"

namespace ad_utility::serialization {

class SerializationException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

/**
 * Serializer that writes to a buffer of bytes.
 */
class ByteBufferWriteSerializer {
 public:
  constexpr static bool IsWriteSerializer = true;
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

 private:
  Storage _data;
};

/**
 * Serializer that reads from a buffer of bytes.
 */
class ByteBufferReadSerializer {
 public:
  constexpr static bool IsWriteSerializer = false;
  using Storage = std::vector<char>;

  explicit ByteBufferReadSerializer(Storage data) : _data{std::move(data)} {};
  void serializeBytes(char* bytePointer, size_t numBytes) {
    AD_CHECK(_iterator + numBytes <= _data.end());
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

/**
 * Trivial serializer for basic types that simply takes all the bytes
 * from the object as data. Corresponds to a shallow copy (for example,
 * in an std::vector this would not serialize only the metadata, but
 * not the actual data).
 */
template <typename Serializer, typename T>
requires(std::is_arithmetic_v<std::decay_t<T>>) void serialize(
    Serializer& serializer, T&& t) {
  if constexpr (Serializer::IsWriteSerializer) {
    serializer.serializeBytes(reinterpret_cast<const char*>(&t), sizeof(t));
  } else {
    serializer.serializeBytes(reinterpret_cast<char*>(&t), sizeof(t));
  }
}

/**
 * Operator that allows to write something like:
 *
 *   ByteBufferWriteSerializer writer;
 *   int x = 42;
 *   writer | x;
 *   ByteBufferReadSerializer reader(std::move(writer).data());
 *   int y;
 *   reader | y;
 */
template <typename Serializer, typename T>
void operator|(Serializer& serializer, T&& t) {
  serialize(serializer, std::forward<T>(t));
}

// Serialization operator for explicitly writing to a serializer.
template <typename Serializer, typename T>
requires(Serializer::IsWriteSerializer) void operator<<(Serializer& serializer,
                                                        const T& t) {
  serialize(serializer, t);
}

// Serialization operator for explicitly reading from a serializer.
template <typename Serializer, typename T>
requires(!Serializer::IsWriteSerializer) void operator>>(Serializer& serializer,
                                                         T& t) {
  serialize(serializer, t);
}

// Automatically allow serialization from reference to const into a
// writeSerializer. CAREFUL: this does not enforce that the serialization
// functions actually preserve the constness, this has to be made sure by the
// user.
// TODO<joka921> This leads to infinite loops if we use the << operator and have
// no actual serialize-function. Fix this by properly defining const overloads.
template <typename Serializer, typename T>
requires(Serializer::IsWriteSerializer) void serialize(Serializer& serializer,
                                                       const T& t) {
  serialize(serializer, const_cast<T&>(t));
}

}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZER_SERIALIZER
