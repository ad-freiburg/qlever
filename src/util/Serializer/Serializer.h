// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

// Library for symmetric serialization

#include <vector>

class ByteBufferWriteSerializer {
  constexpr static bool IsWriteSerializer = true;
  using Storage = std::vector<std::byte>;

 public:
  ByteBufferWriteSerializer(const ByteBufferWriteSerializer&) = delete;

  void serializeBytes(const std::byte* bytePointer, size_t numBytes) {
    std::insert(_data.end(), bytePointer, bytePointer + numBytes);
  }

  const Storage& data() const noexcept { return _data; }
  Storage&& data() && { return std::move(_data); }

 private:
  std::vector<std::byte> _data;
};

class ByteBufferReadSerializer {
  constexpr static bool IsWriteSerializer = false;
  using Storage = std::vector<std::byte>;

 public:
  ByteBufferReadSerializer(Storage data) : _data{std::move(data)} = default;
  void serializeBytes(const std::byte* bytePointer, size_t numBytes) {
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
using TriviallyCopyable = std::is_trivially_copyable_v<T>;

template <typename Serializer, typename T,
          typename = std::enable_if_t<TriviallyCopyable<T>>>
void serialize(Serializer& serializer, T& t) {
  serializer.serializeBytes(&t, sizeof(t));
}

template <typename Serializer, typename T>
void operator|(Serializer& serializer, T& t) {
  serialize(serializer, t);
}
