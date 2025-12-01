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
// A serializer, that lifts a callable (that will become the `serializeBytes`
// function, to a fully blown `ReadSerializer`, that can be used within the
// serialization framework.
CPP_template(typename F)(requires ql::concepts::invocable<
                         F, char*, size_t>) class ReadViaCallableSerializer {
 public:
  using SerializerType = ReadSerializerTag;

 private:
  ::ranges::semiregular_box<F> readFunction_;

 public:
  // Construct from the `readFunction`.
  explicit ReadViaCallableSerializer(F readFunction)
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
