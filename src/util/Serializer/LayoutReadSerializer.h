// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_LAYOUTREADSERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZER_LAYOUTREADSERIALIZER_H

#include <cstdint>
#include <type_traits>

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

// A read serializer for determining the *layout* of a buffer that was written
// with the aligned serialization (see `AlignedByteBufferWriteSerializer`), that
// is, the byte ranges that the serialized objects occupy in the buffer. It is
// the zero-copy `ByteBufferReadSerializerT<true, ql::span<const char>>`, so
// any object can be read (and thereby skipped) via the regular serialization
// (`reader >> object`), and all zero-copy reads (see `ZeroCopyReadSerializer`)
// work on it. In addition, `position()` tells where in the buffer the reader
// currently is, and `rangeOf` converts a zero-copy span into the buffer (e.g.
// the result of `deserializeToSpanOrVector`, see `SerializeVector.h`) to its
// byte range.
//
// A read function can detect this serializer via the `LayoutSerializer`
// concept below, and then record byte ranges instead of (or in addition to)
// constructing the objects, see e.g.
// `SpatialJoinCachedIndex::readFromSerializer`.
//
// The buffer has to be aligned to `alignof(std::max_align_t)`, like the input
// of every aligned read serializer. All functions throw if the buffer is
// exhausted.
class LayoutReadSerializer
    : public ByteBufferReadSerializerT<true, ql::span<const char>> {
 public:
  // The marker that the `LayoutSerializer` concept below checks for.
  static constexpr bool IsLayoutSerializer = true;

 private:
  using Base = ByteBufferReadSerializerT<true, ql::span<const char>>;

 public:
  explicit LayoutReadSerializer(ql::span<const char> buffer) : Base{buffer} {}

  // Return the current position, that is, the offset of the next byte that
  // will be read.
  uint64_t position() const { return getCurrentPosition(); }

  // Return the size of the complete buffer.
  uint64_t size() const { return data().size(); }

  // Return `true` iff all bytes of the buffer have been consumed.
  bool atEnd() const { return position() == size(); }

  // Return the byte range that the `span`, which has to point into the buffer
  // of this serializer (e.g. the result of a zero-copy read), occupies in it.
  template <typename T>
  ByteRange rangeOf(ql::span<const T> span) const {
    uint64_t begin = static_cast<uint64_t>(
        reinterpret_cast<const char*>(span.data()) - data().data());
    return {begin, begin + span.size() * sizeof(T)};
  }

  // Call `f()` (which typically reads from this serializer) and return the
  // byte range that it consumed, that is, from the position before the call
  // to the position after it.
  template <typename F>
  ByteRange recordRange(const F& f) {
    uint64_t begin = position();
    f();
    return {begin, position()};
  }

  // Read and return a single object of type `T` (which has to be default
  // constructible) via the regular serialization.
  template <typename T>
  T readScalar() {
    T value{};
    *this >> value;
    return value;
  }
};

// Implementation of the `LayoutSerializer` concept below.
namespace detail {
template <typename S, typename = void>
struct IsLayoutSerializerImpl : std::false_type {};

template <typename S>
struct IsLayoutSerializerImpl<
    S, std::enable_if_t<std::decay_t<S>::IsLayoutSerializer>> : std::true_type {
};
}  // namespace detail

// Concept for a serializer that only determines the layout of a buffer (see
// `LayoutReadSerializer`). A read function that is instantiated with such a
// serializer may record byte ranges instead of constructing objects.
template <typename S>
CPP_concept LayoutSerializer = detail::IsLayoutSerializerImpl<S>::value;

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_LAYOUTREADSERIALIZER_H
