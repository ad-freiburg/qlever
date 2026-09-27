// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H
#define QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string_view>

#include "backports/span.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "util/Exception.h"

namespace ql::engine {

// Forward declarations
struct TypeFormatDescriptor;

// Function pointer signature for single-pass term formatting.
using TermFormatterFn = char* (*)(ValueId id, std::string_view rawTerm,
                                  char* out, std::string_view prefix,
                                  std::string_view suffix) noexcept;

// _____________________________________________________________________________
// 16-entry lookup table descriptor mapping a 4-bit Datatype tag to delimiters
// and a fast, branchless formatting function pointer.
struct TypeFormatDescriptor {
  std::string_view prefix_{""};
  std::string_view suffix_{""};
  TermFormatterFn formatFn_{nullptr};

  constexpr TypeFormatDescriptor() = default;

  constexpr TypeFormatDescriptor(std::string_view prefix,
                                 std::string_view suffix,
                                 TermFormatterFn formatFn) noexcept
      : prefix_{prefix}, suffix_{suffix}, formatFn_{formatFn} {}
};

namespace detail {

// Format a double into `[out, last)`, returning the one-past-the-end pointer.
// Floating-point `std::to_chars` is only available on macOS 13.3 and later,
// but QLever still targets macOS 11.0, so fall back to `snprintf` on Apple
// platforms.
inline char* formatDoubleValue(char* out, char* last, double value) noexcept {
#if defined(__APPLE__)
  const int numChars =
      std::snprintf(out, static_cast<size_t>(last - out), "%.17g", value);
  return numChars > 0 ? out + numChars : out;
#else
  auto [ptr, ec] = std::to_chars(out, last, value);
  (void)ec;
  return ptr;
#endif
}

// Fast branchless copy for terms with opening and closing delimiters.
inline char* formatTermWithDelimiters(ValueId, std::string_view rawTerm,
                                      char* out, std::string_view prefix,
                                      std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  std::memcpy(out, rawTerm.data(), rawTerm.size());
  out += rawTerm.size();
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Fast branchless formatter for integer values.
inline char* formatInteger(ValueId id, std::string_view, char* out,
                           std::string_view prefix,
                           std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  auto [ptr, ec] = std::to_chars(out, out + 24, id.getInt());
  (void)ec;
  out = ptr;
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Fast branchless formatter for double values.
inline char* formatDouble(ValueId id, std::string_view, char* out,
                          std::string_view prefix,
                          std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  out = formatDoubleValue(out, out + 32, id.getDouble());
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Branchless boolean lookup table.
inline constexpr std::array<std::string_view, 2> kBoolStrings{"false", "true"};

// Fast branchless formatter for boolean values.
inline char* formatBoolean(ValueId id, std::string_view, char* out,
                           std::string_view prefix,
                           std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  const std::string_view val = kBoolStrings[static_cast<size_t>(id.getBool())];
  std::memcpy(out, val.data(), val.size());
  out += val.size();
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Fast branchless formatter for blank node indices.
inline char* formatBlankNode(ValueId id, std::string_view, char* out,
                             std::string_view prefix,
                             std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  auto [ptr, ec] = std::to_chars(out, out + 24, id.getBlankNodeIndex().get());
  (void)ec;
  out = ptr;
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Fast branchless formatter for date values.
inline char* formatDate(ValueId id, std::string_view, char* out,
                        std::string_view prefix,
                        std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  auto [str, type] = id.getDate().toStringAndType();
  (void)type;
  std::memcpy(out, str.data(), str.size());
  out += str.size();
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Fast branchless formatter for GeoPoint values.
inline char* formatGeoPoint(ValueId id, std::string_view, char* out,
                            std::string_view prefix,
                            std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  auto [str, type] = id.getGeoPoint().toStringAndType();
  (void)type;
  std::memcpy(out, str.data(), str.size());
  out += str.size();
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// No-op formatter for undefined or unmapped datatype slots.
inline char* formatUndefined(ValueId, std::string_view, char* out,
                             std::string_view, std::string_view) noexcept {
  return out;
}

// Builds the default 16-entry lookup table for standard RDF N-Triples export.
constexpr std::array<TypeFormatDescriptor, 16> makeDefaultLut() {
  std::array<TypeFormatDescriptor, 16> lut{};
  for (size_t i = 0; i < 16; ++i) {
    lut[i] = TypeFormatDescriptor{"", "", &formatUndefined};
  }

  // 0: Undefined
  lut[static_cast<size_t>(Datatype::Undefined)] =
      TypeFormatDescriptor{"", "", &formatUndefined};

  // 1: Bool
  lut[static_cast<size_t>(Datatype::Bool)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#boolean>", &formatBoolean};

  // 2: Int
  lut[static_cast<size_t>(Datatype::Int)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#integer>", &formatInteger};

  // 3: Double
  lut[static_cast<size_t>(Datatype::Double)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#double>", &formatDouble};

  // VocabIndex (IRI by default when formatting raw names)
  lut[static_cast<size_t>(Datatype::VocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  // LocalVocabIndex
  lut[static_cast<size_t>(Datatype::LocalVocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  // SecondaryVocabIndex: same vocabulary-term treatment as the main
  // vocabularies (an unmapped slot would silently emit nothing).
  lut[static_cast<size_t>(Datatype::SecondaryVocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  // TextRecordIndex
  lut[static_cast<size_t>(Datatype::TextRecordIndex)] =
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters};

  // Date
  lut[static_cast<size_t>(Datatype::Date)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", &formatDate};

  // GeoPoint
  lut[static_cast<size_t>(Datatype::GeoPoint)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      &formatGeoPoint};

  // WordVocabIndex
  lut[static_cast<size_t>(Datatype::WordVocabIndex)] =
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters};

  // BlankNodeIndex
  lut[static_cast<size_t>(Datatype::BlankNodeIndex)] =
      TypeFormatDescriptor{"_:bn", "", &formatBlankNode};

  // EncodedVal
  lut[static_cast<size_t>(Datatype::EncodedVal)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  return lut;
}

// Builds the 16-entry lookup table for Turtle export (compact
// literals/numbers).
constexpr std::array<TypeFormatDescriptor, 16> makeTurtleLut() {
  std::array<TypeFormatDescriptor, 16> lut{};
  for (size_t i = 0; i < 16; ++i) {
    lut[i] = TypeFormatDescriptor{"", "", &formatUndefined};
  }

  lut[static_cast<size_t>(Datatype::Undefined)] =
      TypeFormatDescriptor{"", "", &formatUndefined};
  lut[static_cast<size_t>(Datatype::Bool)] =
      TypeFormatDescriptor{"", "", &formatBoolean};
  lut[static_cast<size_t>(Datatype::Int)] =
      TypeFormatDescriptor{"", "", &formatInteger};
  lut[static_cast<size_t>(Datatype::Double)] =
      TypeFormatDescriptor{"", "", &formatDouble};
  lut[static_cast<size_t>(Datatype::VocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::LocalVocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::SecondaryVocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::TextRecordIndex)] =
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::Date)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", &formatDate};
  lut[static_cast<size_t>(Datatype::GeoPoint)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      &formatGeoPoint};
  lut[static_cast<size_t>(Datatype::WordVocabIndex)] =
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::BlankNodeIndex)] =
      TypeFormatDescriptor{"_:bn", "", &formatBlankNode};
  lut[static_cast<size_t>(Datatype::EncodedVal)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  return lut;
}

// Builds the 16-entry lookup table for raw vocabulary entries (where terms
// already contain quotes/delimiters).
constexpr std::array<TypeFormatDescriptor, 16> makeRawVocabLut() {
  std::array<TypeFormatDescriptor, 16> lut{};
  for (size_t i = 0; i < 16; ++i) {
    lut[i] = TypeFormatDescriptor{"", "", &formatUndefined};
  }

  lut[static_cast<size_t>(Datatype::Undefined)] =
      TypeFormatDescriptor{"", "", &formatUndefined};
  lut[static_cast<size_t>(Datatype::Bool)] =
      TypeFormatDescriptor{"", "", &formatBoolean};
  lut[static_cast<size_t>(Datatype::Int)] =
      TypeFormatDescriptor{"", "", &formatInteger};
  lut[static_cast<size_t>(Datatype::Double)] =
      TypeFormatDescriptor{"", "", &formatDouble};
  lut[static_cast<size_t>(Datatype::VocabIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::LocalVocabIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::SecondaryVocabIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::TextRecordIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::Date)] =
      TypeFormatDescriptor{"", "", &formatDate};
  lut[static_cast<size_t>(Datatype::GeoPoint)] =
      TypeFormatDescriptor{"", "", &formatGeoPoint};
  lut[static_cast<size_t>(Datatype::WordVocabIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::BlankNodeIndex)] =
      TypeFormatDescriptor{"_:bn", "", &formatBlankNode};
  lut[static_cast<size_t>(Datatype::EncodedVal)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};

  return lut;
}

}  // namespace detail

// Global constexpr lookup tables.
inline constexpr auto kDefaultTypeFormatLut = detail::makeDefaultLut();
inline constexpr auto kTurtleTypeFormatLut = detail::makeTurtleLut();
inline constexpr auto kRawVocabTypeFormatLut = detail::makeRawVocabLut();

// _____________________________________________________________________________
// Deep branchless type dispatcher module.
//
// Eliminates branch mispredictions during export loops by indexing directly
// into a 16-entry constexpr lookup table using the 4-bit ValueId datatype tag.
class BranchlessTypeDispatcher {
 public:
  using LookupTable = std::array<TypeFormatDescriptor, 16>;

  // ___________________________________________________________________________
  // Format a single RDF term branchlessly into `out`.
  // Precondition: `out` must point to sufficient pre-allocated memory.
  // Returns: Pointer past the last byte written.
  static inline char* dispatchTermFormat(
      ValueId id, std::string_view rawTerm, char* out,
      const LookupTable& lut = kDefaultTypeFormatLut) {
    AD_CONTRACT_CHECK(out != nullptr);
    const uint8_t typeTag =
        static_cast<uint8_t>(id.getBits() >> ValueId::numDataBits) & 0x0F;
    const auto& desc = lut[typeTag];
    AD_CONTRACT_CHECK(desc.formatFn_ != nullptr);
    return desc.formatFn_(id, rawTerm, out, desc.prefix_, desc.suffix_);
  }

  // ___________________________________________________________________________
  // Batch format a contiguous slice of terms branchlessly.
  // Preconditions: `ids` and `rawTerms` must have identical lengths, and `out`
  // must be non-null.
  // Returns: Total number of bytes written.
  static inline size_t dispatchBatchTermFormat(
      ql::span<const ValueId> ids, ql::span<const std::string_view> rawTerms,
      char* out, const LookupTable& lut = kDefaultTypeFormatLut) {
    AD_CONTRACT_CHECK(ids.size() == rawTerms.size());
    AD_CONTRACT_CHECK(out != nullptr || ids.empty());
    if (ids.empty()) {
      return 0;
    }

    char* curr = out;
    const size_t numTerms = ids.size();
    for (size_t i = 0; i < numTerms; ++i) {
      curr = dispatchTermFormat(ids[i], rawTerms[i], curr, lut);
    }
    return static_cast<size_t>(curr - out);
  }

  // Access to built-in lookup tables.
  [[nodiscard]] static constexpr const LookupTable& defaultLut() noexcept {
    return kDefaultTypeFormatLut;
  }

  [[nodiscard]] static constexpr const LookupTable& turtleLut() noexcept {
    return kTurtleTypeFormatLut;
  }

  [[nodiscard]] static constexpr const LookupTable& rawVocabLut() noexcept {
    return kRawVocabTypeFormatLut;
  }
};

}  // namespace ql::engine

#endif  // QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H
