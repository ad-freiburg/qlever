// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_CHUNKEDREADSERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZER_CHUNKEDREADSERIALIZER_H

#include <algorithm>
#include <cstddef>
#include <cstring>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/span.h"
#include "util/NoCopyNoMove.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility::serialization {

// The concept for the ranges that a `ChunkedReadSerializer` reads from: an
// input range of chunks of bytes, like the body of an HTTP response.
template <typename Range>
CPP_concept RangeOfByteChunks =
    ql::ranges::input_range<Range> &&
    ql::concepts::convertible_to<ql::ranges::range_reference_t<Range>,
                                 ql::span<const std::byte>>;

// A `ReadSerializer` that reads from a range of chunks of bytes (see
// `RangeOfByteChunks`). The bytes are copied chunk-wise, and a single value
// can be split across several chunks. The range is only referenced, so it has
// to outlive the serializer.
CPP_template(typename Range)(requires RangeOfByteChunks<Range>)
class ChunkedReadSerializer : public NoCopyNoMove {
 public:
  using SerializerType = ReadSerializerTag;

 private:
  ql::ranges::iterator_t<Range> it_;
  ql::ranges::sentinel_t<Range> end_;
  // The part of `*it_` that has not been read yet. NOTE: `it_` is only
  // advanced when `*it_` has been read completely, as the chunk might point
  // into a buffer that is reused for the next chunk (as for a generator).
  ql::span<const std::byte> current_;

  // Advance to the next non-empty chunk if `current_` has been read
  // completely. Return false if the range is exhausted.
  bool fillCurrent() {
    while (current_.empty() && it_ != end_) {
      ++it_;
      if (it_ != end_) {
        current_ = *it_;
      }
    }
    return !current_.empty();
  }

 public:
  explicit ChunkedReadSerializer(Range& range)
      : it_{ql::ranges::begin(range)}, end_{ql::ranges::end(range)} {
    if (it_ != end_) {
      current_ = *it_;
    }
  }

  // Copy the next `numBytes` bytes to `bytePointer`. Throw if the range ends
  // before all of them have been read.
  void serializeBytes(char* bytePointer, size_t numBytes) {
    while (numBytes > 0) {
      if (!fillCurrent()) {
        throw SerializationException{
            "The range of chunks that is read by a `ChunkedReadSerializer` "
            "ended unexpectedly"};
      }
      size_t numBytesFromCurrent = std::min(numBytes, current_.size());
      std::memcpy(bytePointer, current_.data(), numBytesFromCurrent);
      current_ = current_.subspan(numBytesFromCurrent);
      bytePointer += numBytesFromCurrent;
      numBytes -= numBytesFromCurrent;
    }
  }

  // Return true if all the bytes of the range have been read.
  bool isExhausted() { return !fillCurrent(); }
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_CHUNKEDREADSERIALIZER_H
