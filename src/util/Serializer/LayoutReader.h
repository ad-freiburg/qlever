// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_LAYOUTREADER_H
#define QLEVER_SRC_UTIL_SERIALIZER_LAYOUTREADER_H

#include <cstdint>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "backports/three_way_comparison.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeVector.h"

namespace ad_utility::serialization {

// A half-open range `[begin_, end_)` of byte offsets into a serialized
// buffer.
struct ByteRange {
  uint64_t begin_ = 0;
  uint64_t end_ = 0;

  // Return the number of bytes in this range.
  uint64_t size() const { return end_ - begin_; }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(ByteRange, begin_, end_)
};

// A reader for determining the *layout* of a buffer that was written with the
// aligned serialization (see `AlignedByteBufferWriteSerializer`), that is, the
// byte ranges that the serialized objects occupy in the buffer. It wraps the
// zero-copy `ByteBufferReadSerializerT<true, ql::span<const char>>` and
// exposes it via `reader()`, so that any object can be read (and thereby
// skipped) via the regular serialization, while `position()` tells where in
// the buffer the reader currently is. In addition, `readArray` returns the
// byte range of the payload of a serialized array without copying it.
//
// The buffer has to be aligned to `alignof(std::max_align_t)`, like the input
// of every aligned read serializer. All functions throw if the buffer is
// exhausted.
class LayoutReader {
 public:
  using Reader = ByteBufferReadSerializerT<true, ql::span<const char>>;

 private:
  ql::span<const char> buffer_;
  Reader reader_;

 public:
  explicit LayoutReader(ql::span<const char> buffer)
      : buffer_{buffer}, reader_{buffer} {}

  // Return the underlying serializer, e.g. to read an object via the regular
  // serialization (`reader() >> object`), which advances the position.
  Reader& reader() { return reader_; }

  // Return the current position, that is, the offset of the next byte that
  // will be read.
  uint64_t position() const { return reader_.getCurrentPosition(); }

  // Return the size of the complete buffer.
  uint64_t size() const { return buffer_.size(); }

  // Return `true` iff all bytes of the buffer have been consumed.
  bool atEnd() const { return position() == size(); }

  // Read an array of trivially serializable `T`s (that was written as a
  // `std::vector<T>`, a `ql::span<const T>`, or, for `T == char`, a
  // `std::string`) and return the byte range of its payload, that is, of the
  // raw elements, which excludes the preceding size and alignment padding. The
  // payload is aligned to `alignof(T)`.
  template <typename T>
  ByteRange readArray() {
    auto span = zeroCopyDeserializeToSpan<T>(reader_);
    uint64_t begin = static_cast<uint64_t>(
        reinterpret_cast<const char*>(span.data()) - buffer_.data());
    return {begin, begin + span.size() * sizeof(T)};
  }

  // Same as `readArray`, but also return a copy of the elements.
  template <typename T>
  std::pair<ByteRange, std::vector<T>> readArrayWithContents() {
    auto range = readArray<T>();
    const T* ptr = reinterpret_cast<const T*>(buffer_.data() + range.begin_);
    return {range, std::vector<T>(ptr, ptr + range.size() / sizeof(T))};
  }

  // Read and return a single object of type `T` (which has to be default
  // constructible) via the regular serialization.
  template <typename T>
  T readScalar() {
    T value{};
    reader_ >> value;
    return value;
  }
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_LAYOUTREADER_H
