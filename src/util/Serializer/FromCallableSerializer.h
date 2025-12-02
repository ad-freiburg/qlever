// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZERFROMCALLABLESERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZERFROMCALLABLESERIALIZER_H

#include "backports/concepts.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility::serialization {
// A serializer that lifts a simple callable (called the `ReadFunction`) to a
// fully blown `ReadSerializer`. The `ReadFunction` has two arguments, a `char*`
// (the target), and a `size_t` (the number of bytes to read). The semantics of
// the `ReadFunction` are "read `numBytes` bytes from the source represented by
// the `ReadFunction` and store them at the `target`. An example for a
// `ReadFunction` is a lambda that captures a network stream, and reads from
// that stream in its `operator()`.
CPP_template(typename ReadFunction)(
    requires ql::concepts::invocable<ReadFunction, char*,
                                     size_t>) class ReadViaCallableSerializer {
 public:
  using SerializerType = ReadSerializerTag;

 private:
  // The `semiregular_box` adds the move-assignment operator in case the
  // `ReadFunction` is a lambda.
  ::ranges::semiregular_box<ReadFunction> readFunction_;

 public:
  // Construct from the `readFunction` (see above for details).
  explicit ReadViaCallableSerializer(ReadFunction readFunction)
      : readFunction_{std::move(readFunction)} {};

  // The main serialization function, simply delegates to the `readFunction_`.
  void serializeBytes(char* bytePointer, size_t numBytes) {
    readFunction_(bytePointer, numBytes);
  }

  // Serializers are move-only types.
  ReadViaCallableSerializer(const ReadViaCallableSerializer&) = delete;
  ReadViaCallableSerializer& operator=(const ReadViaCallableSerializer&) =
      delete;
  ReadViaCallableSerializer(ReadViaCallableSerializer&&) = default;
  ReadViaCallableSerializer& operator=(ReadViaCallableSerializer&&) = default;
};
}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZERFROMCALLABLESERIALIZER_H
