// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

// Library for symmetric serialization.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_SERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZER_SERIALIZER_H

#include <stdexcept>

#include "util/Forward.h"
#include "util/TypeTraits.h"

/**
 * \file Serializer.h
 * \brief Defines a generic and extendable framework for consistent
 * serialization of arbitrary types.
 *
 * Serialization is defined in terms of `Serializer` types that can either write
 * to (`WriteSerializer`) or read from (`ReadSerializer`) a resource like a
 * buffer, file, network connection etc. This framework predefines serializers
 * for byte buffers (in ByteBufferSerializer.h) and files (FileSerializer.h). To
 * write a custom serializer, make it fulfill the `WriteSerializer` or
 * `ReadSerializer` concept (see below) and define in the
 * `ad_utility::serialization` namespace. The namespace is important to make
 * argument-dependent lookup work. A type is serializable to or
 * from a certain serializer, if the call `serialize(serializer, t)` exists and
 * is found by the argument-dependent lookup. There are predefined `serialize`
 * functions for the builtin arithmetic types (in this header) and for several
 * STL types (SerializeVector.h, SerializePair.h, etc.).
 *
 * To make a custom type serializable, you have to define the `serialize`
 * function for this type either in the same namespace as the type or in the
 * namespace `ad_utility::serialization`. The latter namespace can be used to
 * make additional STL types serializable (inserting functions into `std` is
 * forbidden in general). The `serialize` functions should be defined using the
 * `AD_SERIALIZE_FUNCTION` macro and its variants, which can be found in this
 * file. These macros take care of several pitfalls, which one might fall into
 * when defining the serialize functions manually (SFINAE, constness, etc.).
 *
 * For types that can be serialized by simply copying their bytes, you can allow
 * trivial serialization (see below for details) instead of writing the
 * `serialize` function.
 *
 * This file also defines the syntactic sugar `serializer | t` as an equivalent
 * form of `serialize(serializer, t)`.
 *
 * For an example usage of this serialization framework see the unit tests in
 * `SerializerTest.h`.
 */
namespace ad_utility::serialization {

class SerializationException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

/// Concepts for serializer types.
class WriteSerializerTag {};
class ReadSerializerTag {};

// A `WriteSerializer` can write from a span of bytes to some resource and has
// a public member type `using SerializerType = WriteSerializerTag`.
template <typename S>
CPP_requires(writeSerializerRequires,
             requires(S s, const char* ptr, size_t numBytes)(
                 s.serializeBytes(ptr, numBytes),
                 typename std::decay_t<S>::SerializerType{},
                 concepts::requires_<ql::concepts::same_as<
                     typename S::SerializerType, WriteSerializerTag>>));

template <typename S>
CPP_concept WriteSerializer = CPP_requires_ref(writeSerializerRequires, S);

// A `ReadSerializer` can read to span of bytes from some resource and has a
// public alias `using SerializerType = ReadSerializerTag'`.
template <typename S>
CPP_requires(readSerializerRequires,
             requires(S s, char* ptr, size_t numBytes)(
                 s.serializeBytes(ptr, numBytes),
                 typename std::decay_t<S>::SerializerType{},
                 concepts::requires_<ql::concepts::same_as<
                     typename S::SerializerType, ReadSerializerTag>>));

template <typename S>
CPP_concept ReadSerializer = CPP_requires_ref(readSerializerRequires, S);

/// A `Serializer` is either a `WriteSerializer` or a `ReadSerializer`
template <typename S>
CPP_concept Serializer = WriteSerializer<S> || ReadSerializer<S>;

/// If we try to serialize from a const object or reference, the serializer must
/// be a `WriteSerializer`. The following type trait can be used to check this
/// constraint at compile time.
CPP_template(typename S, typename T)(
    requires Serializer<S>) static constexpr bool SerializerMatchesConstness =
    WriteSerializer<S> || !std::is_const_v<std::remove_reference_t<T>>;

/**
 * This macro automatically creates a free (non-member) serialization function
 * for a type. Example usage:
 *
 * struct X {
 *   int a;
 *   int b;
 * }
 *
 * AD_SERIALIZE_FUNCTION(X) {
 *   serializer | arg.a;  // serialize the `a` member of an 'X'
 *   serializer | arg.b
 * }
 *
 * Inside of the body there are variables `serializer` and `arg` (the element to
 * be serialized). The `serialize` function created by this macro is found by
 * argument-dependent lookup and used by this framework if the following
 * conditions apply:
 * - The macro is placed in the same namespace as `T` or in
 * `ad_utility::serialization`
 * - The first argument to `serialize` fulfills the `Serializer` concept.
 * - The second argument to `serialize` is a `T`, `T&`, `const T&`, `T&&` etc.
 * - If the `arg` is const then the serializer is a `WriteSerializer`.
 */
#define AD_SERIALIZE_FUNCTION(T)                                             \
  CPP_template(typename S, typename U)(                                      \
      requires ad_utility::serialization::Serializer<S> CPP_and              \
          ad_utility::SimilarTo<U, T>                                        \
              CPP_and ad_utility::serialization::SerializerMatchesConstness< \
                  S, U>) void                                                \
  serialize(S& serializer, U&& arg)

/// Similar to `AD_SERIALIZE_FUNCTION` but defines a `friend` function to also
/// access private members of a class. It is possible to declare the friend
/// using `AD_SERIALIZE_FRIEND_FUNCTION(Type);` and to define it outside of the
/// class with `AD_SERIALIZE_FUNCTION(Type){...}`
#define AD_SERIALIZE_FRIEND_FUNCTION(T)                                        \
  CPP_variadic_template(typename S, typename U)(                               \
      requires ad_utility::serialization::Serializer<S> &&                     \
      ad_utility::SimilarTo<U, T> &&                                           \
      ad_utility::serialization::SerializerMatchesConstness<S, U>) friend void \
  serialize(S& serializer, U&& arg)

/**
 * Define `serialize` functions for all types that fulfill the `Constraint`.
 * Inside the `Constraint` you have access to the types `S` (the serializer) and
 * `T` (the type to be serialized).
 * CAVEAT: `T` might be `Type&`, `Type&&`,
 * `const Type&&` so you might need to use `std::decay_t`,
 * `std::remove_reference_t` or other type traits inside the constraint. The
 * same holds in principle for the serializer type `S` but the concepcts
 * `Serializer` , `WriteSerializer` and `ReadSerializer` are also true for
 * references to serializers. For an example usage see `SerializePair.h`.
 */
#define AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(Constraint)       \
  CPP_template(typename S, typename T)(                         \
      requires ad_utility::serialization::Serializer<S> CPP_and \
          Constraint) void                                      \
  serialize(S& serializer, T&& arg)

/// Similar to `AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT` but only for
/// `WriteSerializer`s. For an example usage see `SerializeVector.h`
#define AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT_WRITE(Constraint)      \
  CPP_template(typename S, typename T)(                              \
      requires ad_utility::serialization::WriteSerializer<S> CPP_and \
          Constraint) void                                           \
  serialize(S& serializer, T&& arg)

/// Similar to `AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT` but only for
/// `ReadSerializer`s. For an example usage see `SerializeVector.h`
#define AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT_READ(Constraint)      \
  CPP_template(typename S, typename T)(                             \
      requires ad_utility::serialization::ReadSerializer<S> CPP_and \
          Constraint) void                                          \
  serialize(S& serializer, T&& arg)

/**
 * Operator that allows the short hand notation
 * `serializer | t`
 * instead of
 * `serialize(serializer, t)`
 */
CPP_template(typename S, typename T)(requires Serializer<S>) void operator|(
    S& serializer, T&& t) {
  serialize(serializer, AD_FWD(t));
}

/// Serialization operator for explicitly writing to a serializer.
CPP_template(typename S, typename T)(requires WriteSerializer<S>) void
operator<<(S& serializer, const T& t) {
  serializer | t;
}

/// Serialization operator for explicitly reading from a serializer.
CPP_template(typename S, typename T)(requires ReadSerializer<S>) void
operator>>(S& serializer, T&& t) {
  serializer | t;
}

/// An empty struct that isn't needed by the users of this serialization
/// framework. It is used internally to make argument-dependent lookup work.
struct TrivialSerializationHelperTag {};
/**
 * Types T for which a function `allowTrivialSerialization(T,
 * TrivialSerializationHelperTag)` exists and that are trivially copyable can be
 * serialized by simply copying the bytes. To make a user-defined type which is
 * trivially copyable also trivially serializable, declare (no need to define
 * it) this function in the same namespace as the type, as a friend function, or
 * in the namespace `ad_utility::serialization`. Types in the global namespace
 * have to define `allowTrivialSerialization` as a friend or in
 * `ad_utility::serialization` because of the argument-dependent lookup rules.
 * If you want to break the dependencies between your types and this header, you
 * can also define the second parameter to be templated.
 * Note: In addition to the cases described above, all `enum` types are
 * implicitly trivially serializable.
 *
 * For example, one can equivalently write one of the following two:
 *
 * struct X {
 *   int x;
 * };
 * void allowTrivialSerialization(X, auto);
 *
 * struct Y {
 *   int y;
 *   friend void allowTrivialSerialization(Y, auto);
 * };
 *
 * Note that this will also enable trivial serialization for types that are
 * trivially copyable and implicitly convertible to `X` or `Y`. If this behavior
 * is not desired, you can use the following pattern:
 *
 * struct Z {
 *   int z;
 * };
 * void allowTrivialSerialization(std::same_as<Z> auto, auto);
 */
template <typename T>
CPP_requires(
    triviallySerializableRequires,
    requires(T t, TrivialSerializationHelperTag tag)(
        // The `TrivialSerializationHelperTag` lets the argument-dependent
        // lookup also find the `allowTrivialSerialization` function if it is
        // defined in the `ad::serialization` namespace.
        allowTrivialSerialization(t, tag)));

template <typename T>
CPP_concept TriviallySerializable =
    CPP_requires_ref(triviallySerializableRequires, T) &&
    std::is_trivially_copyable_v<std::decay_t<T>>;

/// Serialize function for `TriviallySerializable` types that works by simply
/// serializing the binary object representation.
CPP_template(typename S, typename T)(
    requires Serializer<S> CPP_and
        TriviallySerializable<T>) void serialize(S& serializer, T&& t) {
  if constexpr (WriteSerializer<S>) {
    serializer.serializeBytes(reinterpret_cast<const char*>(&t), sizeof(t));
  } else {
    static_assert(ReadSerializer<S>);
    serializer.serializeBytes(reinterpret_cast<char*>(&t), sizeof(t));
  }
}

/// Arithmetic types (the builtins like int, char, double), as well as enums can
/// be trivially serialized.
CPP_template(typename T,
             typename U)(requires(std::is_arithmetic_v<std::decay_t<T>> ||
                                  std::is_enum_v<std::decay_t<T>>))
    [[maybe_unused]] std::true_type allowTrivialSerialization(T, U) {
  return {};
}

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_SERIALIZER_H
