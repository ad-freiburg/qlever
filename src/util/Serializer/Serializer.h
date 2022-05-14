// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

// Library for symmetric serialization.

#ifndef QLEVER_SERIALIZER_SERIALIZER
#define QLEVER_SERIALIZER_SERIALIZER

#include "../Forward.h"
#include "../TypeTraits.h"
namespace ad_utility::serialization {

class SerializationException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

/// Concepts for serializer types.

/// A `WriteSerializer` can write a span of bytes to some resource.
template <typename S>
concept WriteSerializer = requires(S s, const char* ptr, size_t numBytes) {
  {
    typename S::IsWriteSerializer {}
  }
  ->std::same_as<std::true_type>;
  {s.serializeBytes(ptr, numBytes)};
};

/// A `ReadSerializer` can read to a span of bytes from some resource.
template <typename S>
concept ReadSerializer = requires(S s, char* ptr, size_t numBytes) {
  {
    typename S::IsWriteSerializer {}
  }
  ->std::same_as<std::false_type>;
  {s.serializeBytes(ptr, numBytes)};
};

/// A `Serializer` is either a `WriteSerializer` or a `ReadSerializer`
template <typename S>
concept Serializer = WriteSerializer<S> || ReadSerializer<S>;

/**
 * Operator that allows to write something like:
 *
 *   ByteBufferWriteSerializer writer;
 *   int x = 42;
 *   writer | x;
 *   ByteBufferReadSerializer reader(std::move(writer).data());
 *   int y;
 *   reader | y;
 *
 *   For the element `t` of type `T` on the right hand side, either a static
 * function T::serialize(serializer, AD_FWD(t)); or a free function
 *   serialize(serializer, AD_FWD(t));
 *   must be available.
 *
 *   If both cases apply, the serialization fails with a `static_assert`. To
 * automatically create all overloads of `serialize` for reading from a `const
 * T&` and writing to a `T&` and to avoid that types that are implicitly
 * convertible to `T` use `T`'s serialization function, the following syntax can
 * be used:
 *
 *   void serialize(auto&& serializer, ad_utility::SimilarTo<T> auto&& t) {
 *     serializer | t._someMember;
 *     serializer | t._someOtherMember;
 *     // ...
 *   }
 *
 */
template <typename Serializer, typename T>
void operator|(Serializer& serializer, T&& t) {
  static constexpr bool hasStaticSerialize = requires() {
    std::decay_t<T>::serialize(serializer, AD_FWD(t));
  };

  static constexpr bool hasFreeSerialize = requires() {
    serialize(serializer, AD_FWD(t));
  };

  if constexpr (hasStaticSerialize) {
    std::decay_t<T>::serialize(serializer, AD_FWD(t));
    static_assert(!hasFreeSerialize,
                  "Encountered a type for which a free and a static "
                  "serialization function are present, this is not supported");
  } else if constexpr (hasFreeSerialize) {
    serialize(serializer, AD_FWD(t));
  } else {
    static_assert(ad_utility::alwaysFalse<T>,
                  "Tried to use operator| on a serializer and a type T that "
                  "has neither a static nor a free serialize function");
  }
}

/// Serialization operator for explicitly writing to a serializer.
void operator<<(WriteSerializer auto& serializer, const auto& t) {
  serializer | t;
}

/// Serialization operator for explicitly reading from a serializer.
void operator>>(ReadSerializer auto& serializer, auto& t) { serializer | t; }

/// Arithmetic types (the builtins like int, char, double) can be trivially
/// serialized by just copying the bits (see below).
/// NOTE: We cannot put this AFTER the definition of
/// `isTrivalSerializationAllowed`, otherwise we will get a compiler error on
/// g++.
/// TODO<joka921> Find out, if this is a compiler bug and file a report.
template <typename T>
requires std::is_arithmetic_v<std::decay_t<T>> std::true_type
allowTrivialSerialization(T) {
  return {};
}

/**
 * Types for which a function `allowTrivialSerialization(t)` exists and which
 * are trivially copyable can be serialized by simply copying the bytes. To make
 * a user-defined type which is trivially copyable also trivially serializable
 * declare (no need to define it) this function in the same namespace as the
 * type or even as a friend function, for example:
 * struct X { int x; };
 * void allowTrivialSerialization(X);
 *
 * struct Y {
 *   int y;
 *   friend void allowTrivialSerialization(Y);
 * };
 *
 * Note that this will also enable trivial serialization for types that are
 * trivially copyable and implicitly convertible to `X` or `Y`. If this behavior
 * is not desired, you can use the following pattern:
 *
 * struct Z { int z; };
 * void allowTrivialSerialization(std::same_as<Z> auto);
 */
template <typename T>
concept TriviallySerializable = requires(T t) {
  allowTrivialSerialization(t);
}
&&std::is_trivially_copyable_v<std::decay_t<T>>;

/// Serialize function for `TriviallySerializable` types that works by simply
/// serializing the binary object representation.
template <Serializer S, TriviallySerializable T>
void serialize(S& serializer, T&& t) {
  if constexpr (WriteSerializer<S>) {
    serializer.serializeBytes(reinterpret_cast<const char*>(&t), sizeof(t));
  } else {
    static_assert(ReadSerializer<S>);
    serializer.serializeBytes(reinterpret_cast<char*>(&t), sizeof(t));
  }
}

}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZER_SERIALIZER
