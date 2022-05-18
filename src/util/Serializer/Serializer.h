// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

// Library for symmetric serialization.

#ifndef QLEVER_SERIALIZER_SERIALIZER
#define QLEVER_SERIALIZER_SERIALIZER

#include "../Forward.h"
#include "../TypeTraits.h"

/**
 * \file Serializer.h
 * \brief Defines a generic and extendable framework for consistent
 * serialization of arbitrary types.
 *
 * Serialization is defined in terms of `Serializer` types that can either write
 * to (`WriteSerializer`) or read from (`ReadSerializer`) a resource like a
 * buffer, file, network connection etc. This framework predefines serializers
 * for byte buffers (in ByteBufferSerializer.h) and files (FileSerializer.h). To
 * write a custom serializer that fulfills the `WriteSerializer` or
 * `ReadSerializer` concept (see below) and is defined in the
 * `ad_utility::serialization` namespace. The latter point is important to make
 * argument-dependendent lookup word. A type `T` is called "serializable" if it
 * can be serialized to or from a `Serializer`. A type is serializable to or
 * from a certain serializer, if the call `serialize(serializer, t);` exists and
 * is found. `serialize` functions are predefined for builtin arithmetic types
 * (in this header) and for several STL types (SerializeVector.h,
 * SerializePair.h, etc.).
 *
 * To make a custom type serializable you have to define the `serialize`
 * function for this type either in the same namespace as the type or in the
 * namespace `ad_utility::serialization`. The latter case can be used to make
 * additional STL types serializable (inserting functions into `std` is
 * forbidden in general). `serialize` functions should be defined using the
 * `AD_SERIALIZE_FUNCTION` macro and its variants that can be found in this
 * file. These macros take care of several pitfalls which one might fall into
 * when defining the serialize functions manually (SFINAE, constness, etc.).
 *
 * For types that can be serialized by simply copying their bytes, you can allow
 * trivial serialization (see below for details) instead of writing the
 * `serialize` function.
 *
 * This file also defines the syntactic sugar `serializer | t` as an equivalent
 * of `serialize(serializer, t)`.
 *
 * For an example usage of this serialization framework see the unit tests in
 * `SerializerTest.h`;
 */
namespace ad_utility::serialization {

class SerializationException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

/// Concepts for serializer types.

/// A `WriteSerializer` can write from a span of bytes to some resource and has
/// a public alias `using IsWriteSerializer = std::true_type'`.
template <typename S>
concept WriteSerializer = requires(S s, const char* ptr, size_t numBytes) {
  {
    typename std::decay_t<S>::IsWriteSerializer {}
  }
  ->std::same_as<std::true_type>;
  {s.serializeBytes(ptr, numBytes)};
};

/// A `ReadSerializer` can read to span of bytes from some resource and has a
/// public alias `using IsWriteSerializer = std::false_type'`.
template <typename S>
concept ReadSerializer = requires(S s, char* ptr, size_t numBytes) {
  {
    typename std::decay_t<S>::IsWriteSerializer {}
  }
  ->std::same_as<std::false_type>;
  {s.serializeBytes(ptr, numBytes)};
};

/// A `Serializer` is either a `WriteSerializer` or a `ReadSerializer`
template <typename S>
concept Serializer = WriteSerializer<S> || ReadSerializer<S>;

/// If we try to reference from a const object or reference, the serializer must
/// be a `WriteSerializer`. The following type trait can be used to check this
/// constraint at compile time.
template <Serializer S, typename T>
static constexpr bool SerializerMatchesConstness =
    WriteSerializer<S> || !std::is_const_v<std::remove_reference_t<T>>;

/**
 * This macro automatically creates a free serialization function for a type.
 * Example usage: struct X { int a; int b;
 * }
 *
 * AD_SERIALIZE_FUNCTION(X) {
 *   serializer | arg.a;  // serialize the `a` member of an 'X'
 *   serializer | arg.b
 * }
 *
 * Inside of the body there are variables `serializer` and `arg` (the element to
 * be serialized). The `serialize` function created by this macro is found by
 * ADL and used by this framework if the following conditions apply:
 * - The macro is placed in the same namespace as `T` or in
 * `ad_utility::serialization`
 * - The first argument to `serialize` fulfills the `Serializer` concept.
 * - The second argument to `serialize` is a `T`, `T&`, `const T&`, `T&&` etc.
 * - The serializer is a `WriteSerializer` or the `arg` is not const.
 */
#define AD_SERIALIZE_FUNCTION(T)                                            \
  template <ad_utility::serialization::Serializer S,                        \
            ad_utility::SimilarTo<T> U>                                     \
  requires ad_utility::serialization::SerializerMatchesConstness<S, U> void \
  serialize(S& serializer, U&& arg)

/// Similar to `AD_SERIALIZE_FUNCTION` but defines a `friend` function to also
/// access private members of a class. It is possible to declare the friend
/// using `AD_SERIALIZE_FRIEND_FUNCTION(Type);` and to define it outside of the
/// class with `AD_SERIALIZE_FUNCTION(Type){...}`
#define AD_SERIALIZE_FRIEND_FUNCTION(T)                           \
  template <ad_utility::serialization::Serializer S,              \
            ad_utility::SimilarTo<T> U>                           \
  requires ad_utility::serialization::SerializerMatchesConstness< \
      S, U> friend void                                           \
  serialize(S& serializer, U&& arg)

/**
 * Define `serialize` functions for all types that fulfill the `Constraint`.
 * Inside the `Constraint` you have access to the types `S` (the serializer) and
 * `T` (the type to be serialized). CAVEAT: `T` might be `Type&`, `Type&&`,
 * `const Type&&` so you might need to use `std::decay_t`,
 * `std::remove_reference_t` or other type traits inside the constraint. The
 * same holds in principle for the serializer type `S` but the concepcts
 * `Serializer` , `WriteSerializer` and `ReadSerializer` are also true for
 * references to serializers. For an example usage see `SerializePair.h`
 */
#define AD_SERIALIZE_FUNCTION_PARTIAL(Constraint)                \
  template <ad_utility::serialization::Serializer S, typename T> \
  requires(Constraint) void serialize(S& serializer, T&& arg)

/// Similar to `AD_SERIALIZE_FUNCTION_PARTIAL` but only for `WriteSerializer`s.
/// For an exmple usage see `SerializeVector.h`
#define AD_SERIALIZE_FUNCTION_PARTIAL_WRITE(Constraint)               \
  template <ad_utility::serialization::WriteSerializer S, typename T> \
  requires(Constraint) void serialize(S& serializer, T&& arg)

/// Similar to `AD_SERIALIZE_FUNCTION_PARTIAL` but only for `ReadSerializer`s.
/// For an exmple usage see `SerializeVector.h`
#define AD_SERIALIZE_FUNCTION_PARTIAL_READ(Constraint)               \
  template <ad_utility::serialization::ReadSerializer S, typename T> \
  requires(Constraint) void serialize(S& serializer, T&& arg)

/**
 * Operator that allows the short hand notation
 * template <typename Serializer, typename T>
 */
void operator|(Serializer auto& serializer, auto&& t) {
  serialize(serializer, AD_FWD(t));
}

/// Serialization operator for explicitly writing to a serializer.
void operator<<(WriteSerializer auto& serializer, const auto& t) {
  serializer | t;
}

/// Serialization operator for explicitly reading from a serializer.
void operator>>(ReadSerializer auto& serializer, auto&& t) { serializer | t; }

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
