// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_ENCODEDIRISCHEME_H
#define QLEVER_SRC_INDEX_ENCODEDIRISCHEME_H

#include <absl/container/inlined_vector.h>
#include <absl/numeric/bits.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <typeindex>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Synchronized.h"
#include "util/json.h"

namespace qlever {

class EncodedIriScheme;

// `EncodedIriScheme`s are always handled via a `shared_ptr` to a `const`
// object, because they are immutable and shared between all the copies of an
// `EncodedIriManager`.
using EncodedIriSchemePtr = std::shared_ptr<const EncodedIriScheme>;

// Interface for user-defined schemes that encode IRIs directly into an `Id`
// (see `EncodedIriManager.h` for the general mechanism). While the built-in
// prefixes can only encode IRIs that consist of a fixed prefix followed by a
// sequence of decimal digits, a scheme may encode arbitrarily structured IRIs,
// for example `<somePrefix://num_123_anotherNum_24>`, where only the `123` and
// the `24` are load-bearing.
//
// A scheme reserves `numTags()` of the tags (prefix indices) of the
// `EncodedIriManager`; they are called the "local tags" of the scheme and are
// numbered `[0, numTags())`. The manager maps them to a contiguous range of its
// own tags. Which of its local tags a scheme uses for a given IRI is entirely
// up to the scheme; a typical use case are IRIs with the same prefix but
// different internal structure (see the tests for an example).
//
// The contract for implementations is the following:
//
// 1. All member functions must be `const`, deterministic, and thread-safe.
//    In particular, `encode` is called concurrently by all the parser threads
//    during index building, and by all the query threads at query time.
// 2. `encode` and `decode` must be inverse to each other, and the same must
//    hold for all future runs of QLever that load an index built with this
//    scheme (see `toJson` below).
// 3. The payload returned by `encode` must fit into the lowest
//    `numPayloadBits()` bits. The manager left-aligns it within the bits that
//    are available for the payload.
// 4. Ideally the encoding is order-preserving, meaning that the order of the
//    payloads is the same as the lexicographic order of the encoded IRIs. This
//    is not required for correctness (`Join`, `GROUP BY`, `DISTINCT`, etc. only
//    require a consistent order, see `LocalVocabEntry.cpp`), but if it is
//    violated, then `ORDER BY` and range filters on the affected IRIs follow
//    the order of the encoding and not the lexicographic order. The helpers in
//    the `encodedIri` namespace at the bottom of this file encode decimal
//    digits in an order-preserving way.
// 5. All IRIs that a scheme can encode must start with one of the literal
//    prefixes reported by `prefixes()`. None of these prefixes may be a prefix
//    of any other prefix or scheme prefix used by the same manager.
class EncodedIriScheme {
 public:
  // The numbers that are encoded in a single IRI, see `decodeNumbers`.
  using Numbers = absl::InlinedVector<uint64_t, 4>;

  // The result of a successful encoding: the local tag (in `[0, numTags())`)
  // and the payload (in the lowest `numPayloadBits()` bits).
  using TagAndPayload = std::pair<size_t, uint64_t>;

  // The JSON key under which `toJsonWithName` stores the `name` of the scheme.
  static constexpr const char* nameKey_ = "scheme-name";

  virtual ~EncodedIriScheme() = default;

  // The name of the scheme. It has to be unique among all the schemes that are
  // used together, and stable across QLever versions, because it is stored in
  // the index and used to look up the scheme in the
  // `EncodedIriSchemeRegistry` (see below) when the index is loaded again.
  virtual std::string name() const = 0;

  // The literal prefixes (without angle brackets, e.g. `http://example.org/`)
  // of all the IRIs that this scheme can encode. Used to dispatch to this
  // scheme, and to determine the position of the tags of this scheme in the
  // global order of all tags.
  virtual std::vector<std::string> prefixes() const = 0;

  // The number of tags that this scheme reserves. Must be at least one.
  virtual size_t numTags() const = 0;

  // The number of bits that this scheme uses for its payload. Must be at most
  // the number of payload bits of the manager (52 for the default
  // `EncodedIriManager`).
  virtual size_t numPayloadBits() const = 0;

  // Try to encode the given IRI (with angle brackets, e.g.
  // `<http://example.org/12>`). Return `std::nullopt` if this scheme cannot
  // encode the IRI; it is then stored in the vocabulary as usual. Note that
  // this function is only called if the IRI starts with one of the `prefixes`.
  virtual std::optional<TagAndPayload> encode(
      std::string_view iriWithAngleBrackets) const = 0;

  // The inverse of `encode`: convert the `localTag` and the `payload` back to
  // the IRI (with angle brackets).
  virtual std::string decode(size_t localTag, uint64_t payload) const = 0;

  // Extract the raw numbers that make up the payload, in the order in which
  // they appear in the IRI. For example, for
  // `<somePrefix://num_123_anotherNum_24>` this should return `{123, 24}`.
  virtual Numbers decodeNumbers(size_t localTag, uint64_t payload) const = 0;

  // Serialize the scheme to JSON. The result has to contain all the
  // information that the static `fromJson` function of the concrete class
  // needs to restore an equivalent scheme. It must not use the key `nameKey_`,
  // which is added by `toJsonWithName` below. Schemes without any
  // configuration can simply return an empty JSON object.
  virtual nlohmann::json toJson() const = 0;

  // The `toJson` of the concrete scheme, with the `name` added under the key
  // `nameKey_`. This is what is stored in the index.
  nlohmann::json toJsonWithName() const;

  // The inverse of `toJsonWithName`: look up the stored name in the
  // `EncodedIriSchemeRegistry` and dispatch to the `fromJson` function of the
  // corresponding class. Throw if no scheme with that name is registered in
  // this process (see `registerEncodedIriScheme` below).
  static EncodedIriSchemePtr fromJsonWithName(const nlohmann::json& j);
};

// The global registry of all the `EncodedIriScheme` classes that are known to
// this process. It is used to restore the schemes of an index that is loaded
// from disk. Classes are added to it via `registerEncodedIriScheme` below.
class EncodedIriSchemeRegistry {
 public:
  // A function that restores a scheme from the JSON that was written by
  // `EncodedIriScheme::toJson`.
  using Factory = std::function<EncodedIriSchemePtr(const nlohmann::json&)>;

 private:
  // The registered classes, by name. The `type_index` is stored to detect
  // whether the same class or a different class is registered twice.
  struct Entry {
    std::type_index type_;
    Factory factory_;
  };
  ad_utility::Synchronized<ad_utility::HashMap<std::string, Entry>> schemes_;

 public:
  // Get the single global instance.
  static EncodedIriSchemeRegistry& get();

  // Register the class `type` under the given `name`. Registering the same
  // class under the same name twice is a no-op, registering a different class
  // under a name that is already taken throws.
  void add(std::string name, std::type_index type, Factory factory);

  // Get the factory for the given `name`, or `std::nullopt` if no scheme with
  // that name is registered.
  std::optional<Factory> getFactory(const std::string& name) const;

  // The names of all registered schemes, sorted. Used for error messages.
  std::vector<std::string> registeredNames() const;
};

// Register the `EncodedIriScheme` class `T` in the global registry, such that
// indices that were built using `T` can be loaded again. `T` has to provide
// the following two static functions:
//
//   static std::string T::schemeName();
//   static EncodedIriSchemePtr T::fromJson(const nlohmann::json&);
//
// where `schemeName()` returns the same name as the virtual `name()`, and
// `fromJson` is the inverse of the virtual `toJson`.
//
// NOTE: This function has to be called before an index that uses `T` is
// loaded. Calling it twice for the same class is a no-op.
template <typename T>
void registerEncodedIriScheme() {
  EncodedIriSchemeRegistry::get().add(
      std::string{T::schemeName()}, std::type_index{typeid(T)},
      [](const nlohmann::json& j) -> EncodedIriSchemePtr {
        return T::fromJson(j);
      });
}

}  // namespace qlever

// Convenience macro for `qlever::registerEncodedIriScheme<Type>()`, to be used
// at namespace scope in the `.cpp` file of the scheme.
//
// NOTE: If the scheme lives in a static library, then the linker may discard
// the translation unit with the registration, unless something else from the
// same translation unit is used. In that case, call
// `qlever::registerEncodedIriScheme<Type>()` explicitly during the startup of
// your application.
#define AD_ENCODED_IRI_SCHEME_CONCAT_IMPL(a, b) a##b
#define AD_ENCODED_IRI_SCHEME_CONCAT(a, b) \
  AD_ENCODED_IRI_SCHEME_CONCAT_IMPL(a, b)
#define AD_REGISTER_ENCODED_IRI_SCHEME(...)                        \
  [[maybe_unused]] static const bool AD_ENCODED_IRI_SCHEME_CONCAT( \
      encodedIriSchemeRegistration_, __LINE__) = [] {              \
    ::qlever::registerEncodedIriScheme<__VA_ARGS__>();             \
    return true;                                                   \
  }()

namespace qlever::encodedIri {

// The number of bits that are used to encode a single decimal digit.
static constexpr size_t NibbleSize = 4;

// Encode the decimal digits in `numberStr` into the lowest `numBits` bits of
// the result. Each digit `i` is encoded as the nibble `i + 1` (such that the
// padding nibble `0` is smaller than any digit), and the nibbles are stored
// left-aligned within the `numBits` bits. This makes the encoding
// order-preserving: the order of the encodings is the same as the
// lexicographic order of the digit strings. `numBits` has to be a multiple of
// `NibbleSize`, and `numberStr` may consist of at most `numBits / NibbleSize`
// digits (which the caller has to check, e.g. via `fitsIntoNibbles`).
constexpr uint64_t encodeDecimalNibbles(std::string_view numberStr,
                                        size_t numBits) {
  AD_CONTRACT_CHECK(numBits % NibbleSize == 0 && numBits <= 64);
  AD_CONTRACT_CHECK(numberStr.size() <= numBits / NibbleSize);

  uint64_t result = 0;
  // Compute the starting shift (for the first digit).
  uint64_t shift = numBits - NibbleSize;
  for (const char digitChar : numberStr) {
    // Deliberately encode `[0, ..., 9]` as `[1, ..., A]`, so that the padding
    // nibble `0` is smaller than any valid digit encoding.
    uint8_t digit = static_cast<uint8_t>(digitChar - '0') + 1;
    result |= static_cast<uint64_t>(digit) << shift;
    shift -= NibbleSize;
  }
  return result;
}

// Return true if and only if `numberStr` (which may only consist of digits)
// can be encoded into `numBits` bits by `encodeDecimalNibbles`.
constexpr bool fitsIntoNibbles(std::string_view numberStr, size_t numBits) {
  return numberStr.size() <= numBits / NibbleSize;
}

// Helper for decoding numbers. Call `processDigit` for every digit (from high
// to low) in the decoded representation of `encoded`, which has to be an
// encoding into `numBits` bits (see `encodeDecimalNibbles`).
template <typename F>
void decodeDecimalNibblesHelper(F processDigit, uint64_t encoded,
                                size_t numBits) {
  size_t numDigits = numBits / NibbleSize;
  size_t shift = numBits - NibbleSize;
  auto numTrailingZeros = static_cast<size_t>(absl::countr_zero(encoded));
  size_t numTrailingZeroNibbles = numTrailingZeros / NibbleSize;
  size_t len = numTrailingZeroNibbles >= numDigits
                   ? 0
                   : numDigits - numTrailingZeroNibbles;
  for (size_t i = 0; i < len; ++i) {
    processDigit(((encoded >> shift) & 0xF) - 1);
    shift -= NibbleSize;
  }
}

// The inverse of `encodeDecimalNibbles`. The digits are appended to `result`.
inline void decodeDecimalNibbles(std::string& result, uint64_t encoded,
                                 size_t numBits) {
  decodeDecimalNibblesHelper(
      [&result](auto digit) {
        result.push_back(static_cast<char>(digit + '0'));
      },
      encoded, numBits);
}

// Overload of `decodeDecimalNibbles` that returns the decoded digits as a
// number.
inline uint64_t decodeDecimalNibblesToNumber(uint64_t encoded, size_t numBits) {
  uint64_t result = 0;
  decodeDecimalNibblesHelper(
      [&result](auto digit) {
        result *= 10;
        result += digit;
      },
      encoded, numBits);
  return result;
}

}  // namespace qlever::encodedIri

#endif  // QLEVER_SRC_INDEX_ENCODEDIRISCHEME_H
