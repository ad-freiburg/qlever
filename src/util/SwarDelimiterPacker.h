// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SWARDELIMITERPACKER_H
#define QLEVER_SRC_UTIL_SWARDELIMITERPACKER_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include "util/Exception.h"

namespace ad_utility {

// _____________________________________________________________________________
// Helper to pack up to 8 characters from a string view into a 64-bit unsigned
// integer using little-endian byte ordering.
//
// The emitted byte sequence in memory is little-endian by construction: byte
// `i` of the input lands in bits `[8*i, 8*i+8)` and is stored via `memcpy`
// from the integer. Correct output therefore requires a little-endian host
// (all QLever targets: x86-64, ARM64).
[[nodiscard]] constexpr uint64_t packDelimPattern(
    std::string_view sv) noexcept {
  uint64_t val = 0;
  const size_t limit = sv.size() < 8 ? sv.size() : 8;
  for (size_t i = 0; i < limit; ++i) {
    val |=
        (static_cast<uint64_t>(static_cast<unsigned char>(sv[i])) << (i * 8));
  }
  return val;
}

// _____________________________________________________________________________
// Helper to pack up to 4 characters into a 32-bit unsigned integer.
[[nodiscard]] constexpr uint32_t packDelimPattern32(
    std::string_view sv) noexcept {
  uint32_t val = 0;
  const size_t limit = sv.size() < 4 ? sv.size() : 4;
  for (size_t i = 0; i < limit; ++i) {
    val |=
        (static_cast<uint32_t>(static_cast<unsigned char>(sv[i])) << (i * 8));
  }
  return val;
}

// _____________________________________________________________________________
// Helper to pack up to 2 characters into a 16-bit unsigned integer.
[[nodiscard]] constexpr uint16_t packDelimPattern16(
    std::string_view sv) noexcept {
  uint16_t val = 0;
  const size_t limit = sv.size() < 2 ? sv.size() : 2;
  for (size_t i = 0; i < limit; ++i) {
    val |=
        (static_cast<uint16_t>(static_cast<unsigned char>(sv[i])) << (i * 8));
  }
  return val;
}

// Forward declaration
class SwarDelimiterPacker;

// _____________________________________________________________________________
// Strongly-typed immutable descriptor for a packed delimiter pattern.
// Holds the 64-bit packed bit pattern and the exact byte length (0 to 8).
struct PackedDelimiter {
  uint64_t pattern_ = 0;
  uint8_t len_ = 0;

  constexpr PackedDelimiter() noexcept = default;

  constexpr PackedDelimiter(uint64_t pattern, uint8_t len) noexcept
      : pattern_(pattern), len_(len) {}

  constexpr explicit PackedDelimiter(std::string_view sv) noexcept
      : pattern_(packDelimPattern(sv)),
        len_(static_cast<uint8_t>(sv.size() <= 8 ? sv.size() : 8)) {}

  [[nodiscard]] constexpr uint64_t pattern() const noexcept { return pattern_; }
  [[nodiscard]] constexpr size_t len() const noexcept { return len_; }
  [[nodiscard]] constexpr size_t size() const noexcept { return len_; }
  [[nodiscard]] constexpr bool empty() const noexcept { return len_ == 0; }

  // Manual equality, kept C++17-compatible: a defaulted `operator==` and
  // the rewritten `operator!=` it synthesizes require C++20, but QLever
  // still builds in C++17 mode, see `cpp-17-libqlever.yml`).
  constexpr bool operator==(const PackedDelimiter& other) const noexcept {
    return pattern_ == other.pattern_ && len_ == other.len_;
  }
  constexpr bool operator!=(const PackedDelimiter& other) const noexcept {
    return !(*this == other);
  }

  // Unpack delimiter sequence into an std::string
  [[nodiscard]] std::string toString() const {
    std::string s;
    s.resize(len_);
    for (size_t i = 0; i < len_; ++i) {
      s[i] = static_cast<char>((pattern_ >> (i * 8)) & 0xFF);
    }
    return s;
  }
};

// _____________________________________________________________________________
// SIMD Within A Register (SWAR) Delimiter Packer:
// Packs common RDF syntax delimiter sequences into 64-bit unsigned integers
// and performs branchless unaligned 64-bit store intrinsics.
class SwarDelimiterPacker {
 public:
  // ___________________________________________________________________________
  // Core Store Intrinsics:
  // Copies exactly `len` bytes of `delimPattern` into `out` and advances the
  // pointer by `len` bytes. Safe for exact-fit buffers: unlike the
  // compile-time fixed-length overload below, this dynamic overload never
  // writes past `out + len`. Preconditions:
  // - `out` must not be nullptr.
  // - `out` must point to a buffer with at least `len` bytes of writable
  //   capacity.
  // - `len` must be <= 8.
  //
  // Callers that can guarantee 8 bytes of writable capacity and need the
  // single unaligned 64-bit store should use the fixed-length template
  // overload (or an explicit 8-byte `memcpy`) instead.
  [[nodiscard]] static inline char* writeDelim64(char* out,
                                                 uint64_t delimPattern,
                                                 size_t len) {
    AD_CONTRACT_CHECK(out != nullptr);
    AD_CONTRACT_CHECK(len <= 8);
    std::memcpy(out, &delimPattern, len);
    return out + len;
  }

  // Compile-time fixed-length overload for maximum compiler optimization.
  // Performs a single unaligned 64-bit store (e.g. `mov [rdi], rsi`) with
  // zero branching. Requires 8 bytes of writable capacity at `out`, even
  // when `Len < 8` (the trailing bytes are scratch overwritten by the wide
  // store); use the dynamic overload for exact-fit buffers.
  template <size_t Len>
  [[nodiscard]] static inline char* writeDelim64(char* out,
                                                 uint64_t delimPattern) {
    static_assert(Len <= 8, "SWAR delimiter length must be <= 8 bytes");
    AD_CONTRACT_CHECK(out != nullptr);
    std::memcpy(out, &delimPattern, sizeof(uint64_t));
    return out + Len;
  }

  // Write a strongly typed PackedDelimiter. Copies exactly `delim.len()`
  // bytes; safe for exact-fit buffers.
  [[nodiscard]] static inline char* writeDelim(char* out,
                                               const PackedDelimiter& delim) {
    return writeDelim64(out, delim.pattern(), delim.len());
  }

  // 32-bit store intrinsic (copies exactly `len` <= 4 bytes, advances by
  // `len`). Safe for exact-fit buffers.
  [[nodiscard]] static inline char* writeDelim32(char* out,
                                                 uint32_t delimPattern,
                                                 size_t len) {
    AD_CONTRACT_CHECK(out != nullptr);
    AD_CONTRACT_CHECK(len <= 4);
    std::memcpy(out, &delimPattern, len);
    return out + len;
  }

  // 16-bit store intrinsic (copies exactly `len` <= 2 bytes, advances by
  // `len`). Safe for exact-fit buffers.
  [[nodiscard]] static inline char* writeDelim16(char* out,
                                                 uint16_t delimPattern,
                                                 size_t len) {
    AD_CONTRACT_CHECK(out != nullptr);
    AD_CONTRACT_CHECK(len <= 2);
    std::memcpy(out, &delimPattern, len);
    return out + len;
  }

  // Compile-time pattern generator for arbitrary delimiter strings <= 8 bytes
  [[nodiscard]] static constexpr uint64_t pack(std::string_view sv) noexcept {
    return packDelimPattern(sv);
  }

  // ___________________________________________________________________________
  // TSV Delimiter Bit Patterns:
  static constexpr uint64_t TSV_TAB = packDelimPattern("\t");      // '\t' (1B)
  static constexpr uint64_t TSV_NEWLINE = packDelimPattern("\n");  // '\n' (1B)
  static constexpr uint64_t TSV_CRLF = packDelimPattern("\r\n");  // "\r\n" (2B)
  static constexpr uint64_t TSV_TAB_TAB =
      packDelimPattern("\t\t");  // "\t\t" (2B)
  static constexpr uint64_t TSV_TAB_NEWLINE =
      packDelimPattern("\t\n");  // "\t\n" (2B)
  static constexpr uint64_t TSV_TAB_CRLF =
      packDelimPattern("\t\r\n");  // "\t\r\n" (3B)

  // CSV Delimiter Bit Patterns:
  static constexpr uint64_t CSV_COMMA = packDelimPattern(",");     // ',' (1B)
  static constexpr uint64_t CSV_NEWLINE = packDelimPattern("\n");  // '\n' (1B)
  static constexpr uint64_t CSV_CRLF = packDelimPattern("\r\n");  // "\r\n" (2B)
  static constexpr uint64_t CSV_QUOTE = packDelimPattern("\"");   // '"' (1B)
  static constexpr uint64_t CSV_QUOTE_COMMA_QUOTE =
      packDelimPattern("\",\"");  // "\",\"" (3B)
  static constexpr uint64_t CSV_COMMA_QUOTE =
      packDelimPattern(",\"");  // ",\"" (2B)
  static constexpr uint64_t CSV_QUOTE_COMMA =
      packDelimPattern("\",");  // "\"," (2B)
  static constexpr uint64_t CSV_QUOTE_NEWLINE =
      packDelimPattern("\"\n");  // "\"\n" (2B)
  static constexpr uint64_t CSV_QUOTE_CRLF =
      packDelimPattern("\"\r\n");  // "\"\r\n" (3B)
  static constexpr uint64_t CSV_NEWLINE_QUOTE =
      packDelimPattern("\n\"");  // "\n\"" (2B)
  static constexpr uint64_t CSV_CRLF_QUOTE =
      packDelimPattern("\r\n\"");  // "\r\n\"" (3B)

  // Turtle & N-Triples Delimiter Bit Patterns:
  static constexpr uint64_t SPACE = packDelimPattern(" ");  // ' ' (1B)
  static constexpr uint64_t DOT_NEWLINE =
      packDelimPattern(" .\n");  // " .\n" (3B)
  static constexpr uint64_t DOT_CRLF =
      packDelimPattern(" .\r\n");  // " .\r\n" (4B)
  static constexpr uint64_t SHORT_DOT_NEWLINE =
      packDelimPattern(".\n");  // ".\n" (2B)
  static constexpr uint64_t SHORT_DOT_CRLF =
      packDelimPattern(".\r\n");                                // ".\r\n" (3B)
  static constexpr uint64_t IRI_OPEN = packDelimPattern("<");   // '<' (1B)
  static constexpr uint64_t IRI_CLOSE = packDelimPattern(">");  // '>' (1B)
  static constexpr uint64_t IRI_CLOSE_SPACE =
      packDelimPattern("> ");  // "> " (2B)
  static constexpr uint64_t IRI_CLOSE_SPACE_OPEN =
      packDelimPattern("> <");  // "> <" (3B)
  static constexpr uint64_t IRI_CLOSE_SPACE_QUOTE =
      packDelimPattern("> \"");  // "> \"" (3B)
  static constexpr uint64_t IRI_CLOSE_DOT_NEWLINE =
      packDelimPattern("> .\n");  // "> .\n" (4B)
  static constexpr uint64_t IRI_CLOSE_DOT_CRLF =
      packDelimPattern("> .\r\n");  // "> .\r\n" (5B)
  static constexpr uint64_t LITERAL_QUOTE = packDelimPattern("\"");  // '"' (1B)
  static constexpr uint64_t LITERAL_QUOTE_SPACE =
      packDelimPattern("\" ");  // "\" " (2B)
  static constexpr uint64_t LITERAL_QUOTE_SPACE_OPEN =
      packDelimPattern("\" <");  // "\" <" (3B)
  static constexpr uint64_t LITERAL_QUOTE_DOT_NEWLINE =
      packDelimPattern("\" .\n");  // "\" .\n" (4B)
  static constexpr uint64_t LITERAL_QUOTE_DOT_CRLF =
      packDelimPattern("\" .\r\n");  // "\" .\r\n" (5B)
  static constexpr uint64_t SEMICOLON_SPACE =
      packDelimPattern("; ");                                      // "; " (2B)
  static constexpr uint64_t COMMA_SPACE = packDelimPattern(", ");  // ", " (2B)

  // ___________________________________________________________________________
  // High-Level Pre-configured RDF Triple Transitions:
  // Subject IRI closing to Predicate IRI opening: "> <" (3B)
  static constexpr PackedDelimiter TRIPLE_S_TO_P_IRI{IRI_CLOSE_SPACE_OPEN, 3};
  // Predicate IRI closing to Object IRI opening: "> <" (3B)
  static constexpr PackedDelimiter TRIPLE_P_TO_O_IRI{IRI_CLOSE_SPACE_OPEN, 3};
  // Predicate IRI closing to Object Literal opening: "> \"" (3B)
  static constexpr PackedDelimiter TRIPLE_P_TO_O_LIT{IRI_CLOSE_SPACE_QUOTE, 3};
  // Object IRI closing to Triple terminator: "> .\n" (4B)
  static constexpr PackedDelimiter TRIPLE_O_IRI_END{IRI_CLOSE_DOT_NEWLINE, 4};
  // Object Literal closing quote to Triple terminator: "\" .\n" (4B)
  static constexpr PackedDelimiter TRIPLE_O_LIT_END{LITERAL_QUOTE_DOT_NEWLINE,
                                                    4};
  // Raw Object (blank node / numeric / boolean) to Triple terminator: " .\n"
  // (3B)
  static constexpr PackedDelimiter TRIPLE_O_RAW_END{DOT_NEWLINE, 3};

  // ___________________________________________________________________________
  // TSV Packed Delimiters:
  static constexpr PackedDelimiter DELIM_TSV_TAB{TSV_TAB, 1};
  static constexpr PackedDelimiter DELIM_TSV_NEWLINE{TSV_NEWLINE, 1};
  static constexpr PackedDelimiter DELIM_TSV_CRLF{TSV_CRLF, 2};
  static constexpr PackedDelimiter DELIM_TSV_TAB_TAB{TSV_TAB_TAB, 2};
  static constexpr PackedDelimiter DELIM_TSV_TAB_NEWLINE{TSV_TAB_NEWLINE, 2};

  // ___________________________________________________________________________
  // CSV Packed Delimiters:
  static constexpr PackedDelimiter DELIM_CSV_COMMA{CSV_COMMA, 1};
  static constexpr PackedDelimiter DELIM_CSV_NEWLINE{CSV_NEWLINE, 1};
  static constexpr PackedDelimiter DELIM_CSV_CRLF{CSV_CRLF, 2};
  static constexpr PackedDelimiter DELIM_CSV_QUOTE{CSV_QUOTE, 1};
  static constexpr PackedDelimiter DELIM_CSV_QUOTE_COMMA_QUOTE{
      CSV_QUOTE_COMMA_QUOTE, 3};
  static constexpr PackedDelimiter DELIM_CSV_COMMA_QUOTE{CSV_COMMA_QUOTE, 2};
  static constexpr PackedDelimiter DELIM_CSV_QUOTE_COMMA{CSV_QUOTE_COMMA, 2};
  static constexpr PackedDelimiter DELIM_CSV_QUOTE_NEWLINE{CSV_QUOTE_NEWLINE,
                                                           2};
  static constexpr PackedDelimiter DELIM_CSV_QUOTE_CRLF{CSV_QUOTE_CRLF, 3};
  static constexpr PackedDelimiter DELIM_CSV_NEWLINE_QUOTE{CSV_NEWLINE_QUOTE,
                                                           2};

  // ___________________________________________________________________________
  // Turtle & N-Triples Packed Delimiters:
  static constexpr PackedDelimiter DELIM_SPACE{SPACE, 1};
  static constexpr PackedDelimiter DELIM_DOT_NEWLINE{DOT_NEWLINE, 3};
  static constexpr PackedDelimiter DELIM_DOT_CRLF{DOT_CRLF, 4};
  static constexpr PackedDelimiter DELIM_IRI_OPEN{IRI_OPEN, 1};
  static constexpr PackedDelimiter DELIM_IRI_CLOSE{IRI_CLOSE, 1};
  static constexpr PackedDelimiter DELIM_IRI_CLOSE_SPACE{IRI_CLOSE_SPACE, 2};
  static constexpr PackedDelimiter DELIM_LITERAL_QUOTE{LITERAL_QUOTE, 1};
  static constexpr PackedDelimiter DELIM_LITERAL_QUOTE_SPACE{
      LITERAL_QUOTE_SPACE, 2};
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SWARDELIMITERPACKER_H
