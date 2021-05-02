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

class ByteBufferWriteSerializer {
 public:
  constexpr static bool IsWriteSerializer = true;
  using Storage = std::vector<char>;
  ByteBufferWriteSerializer() = default;
  ByteBufferWriteSerializer(const ByteBufferWriteSerializer&) = delete;

  void serializeBytes(const char* bytePointer, size_t numBytes) {
    _data.insert(_data.end(), bytePointer, bytePointer + numBytes);
  }

  const Storage& data() const& noexcept { return _data; }
  Storage&& data() && { return std::move(_data); }

 private:
  std::vector<char> _data;
};

class ByteBufferReadSerializer {
 public:
  constexpr static bool IsWriteSerializer = false;
  using Storage = std::vector<char>;

  ByteBufferReadSerializer(Storage data) : _data{std::move(data)} {};
  void serializeBytes(char* bytePointer, size_t numBytes) {
    AD_CHECK(_iterator + numBytes <= _data.end());
    std::copy(_iterator, _iterator + numBytes, bytePointer);
    _iterator += numBytes;
  }

  const Storage& data() const noexcept { return _data; }

 private:
  const Storage _data;
  Storage::const_iterator _iterator{_data.begin()};
};

template <typename T>
static constexpr bool TriviallyCopyable =
    std::is_trivially_copyable_v<std::decay_t<T>>;

template <typename Serializer, typename T,
          typename = std::enable_if_t<TriviallyCopyable<T>>>
void serialize(Serializer& serializer, T&& t) {
  if constexpr (Serializer::IsWriteSerializer) {
    serializer.serializeBytes(reinterpret_cast<const char*>(&t), sizeof(t));
  } else {
    serializer.serializeBytes(reinterpret_cast<char*>(&t), sizeof(t));
  }
}

template <typename Serializer, typename T>
void operator&(Serializer& serializer, T&& t) {
  serialize(serializer, std::forward<T>(t));
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZER_SERIALIZER
