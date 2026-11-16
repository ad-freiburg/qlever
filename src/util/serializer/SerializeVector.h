//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEVECTOR_H
#define QLEVER_SERIALIZEVECTOR_H

#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/ContainersWithAllocator.h"
#include "util/NoCopyNoMove.h"
#include "util/TypeTraits.h"
#include "util/UniqueCleanup.h"
#include "util/Views.h"
#include "util/serializer/Serializer.h"

namespace ad_utility::serialization {
namespace detail {
// Serialize the elements of the contiguous `range` (for example a
// `std::vector`, a `std::string` or a `ql::span`) without its size: their bytes
// for a trivially serializable value type, else one element after the other.
template <typename S, typename Range>
void serializeElements(S& serializer, Range& range) {
  using V = ql::ranges::range_value_t<Range>;
  if constexpr (TriviallySerializable<V>) {
    using CharPtr = std::conditional_t<ReadSerializer<S>, char*, const char*>;
    alignSerializerForType<V>(serializer);
    serializer.serializeBytes(
        reinterpret_cast<CharPtr>(ql::ranges::data(range)),
        ql::ranges::size(range) * sizeof(V));
  } else {
    for (auto&& element : range) {
      serializer | element;
    }
  }
}

// Read the `size` elements of type `V` that `serializeElements` wrote and
// discard them.
template <typename V, typename S>
void discardElements(S& serializer, size_t size) {
  if constexpr (TriviallySerializable<V>) {
    alignSerializerForType<V>(serializer);
  }
  [[maybe_unused]] V discarded;
  for ([[maybe_unused]] auto i : ad_utility::integerRange(size)) {
    serializer | discarded;
  }
}
}  // namespace detail

AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::similarToInstantiation<T, std::vector> ||
     ad_utility::similarToInstantiation<T, qlm::vector> ||
     ad_utility::similarToInstantiation<T, std::basic_string>)) {
  using V = typename std::decay_t<T>::value_type;
  auto size = arg.size();  // The value is ignored for `ReadSerializer`s.
  serializer | size;

  if constexpr (ReadSerializer<S>) {
    checkAllocation(serializer, size, sizeof(V));
    arg.resize(size);
  }
  detail::serializeElements(serializer, arg);
}

// Serialization for `ql::span`. When writing to a `span` from a serializer, the
// span has to have the correct size, else an exception will be thrown.
// Note 1: In that case, the serializer behaves as if the span was read, so the
// contents of the span are lost, but elements following the `span` can still be
// deserialized.
// Note 2: To mitigate this issue, it is much safer to deserialize to
// a `std::vector`, as serializing from a `span` but deserializing to
// a `vector` works because those types share the same serialization format.
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT((ad_utility::SimilarToSpan<T>)) {
  using V = typename std::decay_t<T>::value_type;
  auto size = arg.size();  // The value is ignored for `ReadSerializer`s.
  serializer | size;

  if constexpr (ReadSerializer<S>) {
    if (arg.size() != size) {
      // The size does not match, we consume the complete `span` into the void
      // and then throw an exception.
      detail::discardElements<V>(serializer, size);
      throw std::runtime_error{
          "To serialize into a span, the span must be properly sized in "
          "advance. Note: "
          "the span with the non-matching size has been consumed from the "
          "serializer, "
          "and can no longer be retrieved."};
    }
  }
  detail::serializeElements(serializer, arg);
}

namespace detail {
// The `size` trivially copyable values of type `T` that follow in the
// `serializer`, without copying them (see `zeroCopyDeserializeToSpan`).
CPP_template(typename T, typename S)(requires ZeroCopyReadSerializer<S> &&
                                           TriviallySerializable<T>)
ql::span<const T> zeroCopyDeserializeElements(S& serializer, size_t size) {
  alignSerializerForType<T>(serializer);
  auto bytes = serializer.getSpanToBytes(size * sizeof(T));
  AD_CORRECTNESS_CHECK(bytes.size() == size * sizeof(T));
  AD_CORRECTNESS_CHECK(
      reinterpret_cast<std::uintptr_t>(bytes.data()) % alignof(T) == 0);
  // TODO<C++23> Technically this is undefined behavior for types other than
  // `char` without calling `start_lifetime_as_array` or memcopying the data,
  // But no compiler implements this as of now, and it has been working in
  // practice forever.
  return ql::span<const T>{reinterpret_cast<const T*>(bytes.data()), size};
}
}  // namespace detail

// Read a span of trivially copyable values from the serializer without copying
// them. The pointer of the span will point into the serializers internal
// buffer. Can only be called if a `span` or `vector` of the same type was
// written to the serializer at its current position using a serializer
// that implements aligned serialization.
CPP_template(typename T, typename S)(requires ZeroCopyReadSerializer<S> &&
                                           TriviallySerializable<T>)
ql::span<const T> zeroCopyDeserializeToSpan(S& serializer) {
  std::size_t size;
  serializer >> size;
  return detail::zeroCopyDeserializeElements<T>(serializer, size);
}

/// Incrementally serialize a std::vector to disk without materializing it.
/// Call `push` for each of the elements that will become part of the vector.
CPP_template(typename T, typename Serializer)(
    requires WriteSerializer<Serializer>)
class VectorIncrementalSerializer : public ad_utility::NoCopy {
 private:
  using SizeType = typename std::vector<T>::size_type;
  struct State {
    Serializer serializer_;
    uint64_t startPosition_;
    SizeType size_ = 0;
  };

  // Write the final size to the header. Runs on destruction and when the
  // serializer is overwritten, unless `finish()` was called before.
  struct Finisher {
    void operator()(State&& state) const {
      serializeAtPosition(state.serializer_, state.startPosition_, state.size_);
    }
  };
  // NOTE: This class is move-only. Because of this `UniqueCleanup`, the
  // implicit move operations are correct: A moved-from serializer doesn't write
  // anything on destruction, and a move assignment first finishes the
  // overwritten serializer.
  ad_utility::unique_cleanup::UniqueCleanup<State, Finisher> state_;

 public:
  explicit VectorIncrementalSerializer(Serializer&& serializer)
      : state_{initialize(std::move(serializer)), Finisher{}} {}

  void push(const T& element) {
    state_->serializer_ << element;
    state_->size_++;
  }

  void finish() { std::move(state_).runNowIfActive(); }

  Serializer serializer() && {
    finish();
    return std::move(state_->serializer_);
  }

 private:
  // Write a placeholder for the size, which is set by the `Finisher`.
  static State initialize(Serializer&& serializer) {
    uint64_t startPosition = serializer.getSerializationPosition();
    serializer << SizeType{0};
    alignSerializerForType<T>(serializer);
    return State{std::move(serializer), startPosition};
  }
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEVECTOR_H
