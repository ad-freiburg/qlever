// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_SERIALIZEVARIANT_H
#define QLEVER_SRC_UTIL_SERIALIZER_SERIALIZEVARIANT_H

#include <cstdint>
#include <variant>

#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

// Serialization for `std::variant<Ts...>`: the index of the alternative that
// the variant currently holds (as a `uint64_t`), followed by the alternative
// itself. Every alternative has to be serializable and (for reading) default
// constructible.
//
// NOTE: An index that is out of range is cheap to detect and therefore throws.
// This does not make the deserialization of arbitrary input safe in general
// (the alternative itself may still be garbage), it only guarantees that the
// variant never ends up holding an alternative that was not written.
namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::similarToInstantiation<T, std::variant>)) {
  using V = std::decay_t<T>;
  static constexpr size_t numAlternatives = std::variant_size_v<V>;
  if constexpr (ReadSerializer<S>) {
    uint64_t index = 0;
    serializer >> index;
    AD_CONTRACT_CHECK(index < numAlternatives,
                      "Tried to deserialize a `std::variant` with the out of "
                      "range index ",
                      index, " (the variant has ", numAlternatives,
                      " alternatives)");
    ad_utility::RuntimeValueToCompileTimeValueVi<numAlternatives - 1>(
        index, [&serializer, &arg](auto indexVi) {
          serializer >> arg.template emplace<decltype(indexVi)::value>();
        });
  } else {
    // A `valueless_by_exception` variant holds no alternative at all, so there
    // is nothing that could be written (and read back).
    AD_CONTRACT_CHECK(!arg.valueless_by_exception(),
                      "Tried to serialize a `std::variant` that is "
                      "`valueless_by_exception`");
    serializer << static_cast<uint64_t>(arg.index());
    auto writeAlternative = [&serializer](const auto& alternative) {
      serializer << alternative;
    };
    std::visit(writeAlternative, arg);
  }
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_SERIALIZEVARIANT_H
