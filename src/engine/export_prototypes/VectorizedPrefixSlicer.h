// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

// SSE2 is implied by x86-64, but on 32-bit x86 it must be queried explicitly:
// `__i386__`/`_M_IX86` alone do not guarantee it (same gating as in
// `util/FastIntToString.h`).
#if defined(__x86_64__) || defined(_M_X64) || defined(__SSE2__)
#include <emmintrin.h>
#define QLEVER_SLICER_X86 1
#endif

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "backports/span.h"
#include "util/Exception.h"

namespace qlever::export_pipeline {

// _____________________________________________________________________________
// Standard known IRI prefix IDs for high-throughput single-instruction
// emission.
enum class WellKnownPrefixId : uint8_t {
  WikidataEntity = 0,  // http://www.wikidata.org/entity/
  WikidataDirectProp,  // http://www.wikidata.org/prop/direct/
  RdfSyntax,           // http://www.w3.org/1999/02/22-rdf-syntax-ns#
  RdfsSchema,          // http://www.w3.org/2000/01/rdf-schema#
  OwlOntology,         // http://www.w3.org/2002/07/owl#
  SchemaOrg,           // http://schema.org/
  XmlSchema,           // http://www.w3.org/2001/XMLSchema#
  Count
};

// _____________________________________________________________________________
// Vectorized Prefix Table: Aligns common IRI prefixes to 16-byte boundaries so
// they can be copied into chunk buffers using 1-3 SSE2/AVX instructions.
class VectorizedPrefixTable {
 public:
  struct PrefixEntry {
    alignas(16) char data[48];
    size_t length;
  };

 private:
  std::array<PrefixEntry, static_cast<size_t>(WellKnownPrefixId::Count)>
      entries_{};

 public:
  VectorizedPrefixTable() {
    initEntry(WellKnownPrefixId::WikidataEntity,
              "http://www.wikidata.org/entity/");
    initEntry(WellKnownPrefixId::WikidataDirectProp,
              "http://www.wikidata.org/prop/direct/");
    initEntry(WellKnownPrefixId::RdfSyntax,
              "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    initEntry(WellKnownPrefixId::RdfsSchema,
              "http://www.w3.org/2000/01/rdf-schema#");
    initEntry(WellKnownPrefixId::OwlOntology, "http://www.w3.org/2002/07/owl#");
    initEntry(WellKnownPrefixId::SchemaOrg, "http://schema.org/");
    initEntry(WellKnownPrefixId::XmlSchema,
              "http://www.w3.org/2001/XMLSchema#");
  }

  // ___________________________________________________________________________
  // Number of bytes `writePrefixFast` stores for a prefix of `length` bytes:
  // the vector stores cover whole 16-byte blocks.
  [[nodiscard]] static constexpr size_t storeSize(size_t length) {
    return (length + 15) / 16 * 16;
  }

  // ___________________________________________________________________________
  // Write a well-known prefix into `out` using 128-bit vector stores and
  // return the number of valid bytes (`entry.length`). The stores cover whole
  // 16-byte blocks, so `out` must hold `storeSize(entry.length)` bytes (at
  // most 48); a smaller `out` throws instead of overflowing.
  [[nodiscard]] size_t writePrefixFast(WellKnownPrefixId id,
                                       ql::span<char> out) const {
    const auto index = static_cast<size_t>(id);
    AD_CONTRACT_CHECK(index < entries_.size());
    const auto& entry = entries_[index];
    AD_CONTRACT_CHECK(out.size() >= storeSize(entry.length));
#ifdef QLEVER_SLICER_X86
    const __m128i* src = reinterpret_cast<const __m128i*>(entry.data);
    __m128i* dst = reinterpret_cast<__m128i*>(out.data());

    if (entry.length <= 16) {
      _mm_storeu_si128(dst, _mm_load_si128(src));
    } else if (entry.length <= 32) {
      _mm_storeu_si128(dst, _mm_load_si128(src));
      _mm_storeu_si128(dst + 1, _mm_load_si128(src + 1));
    } else {
      _mm_storeu_si128(dst, _mm_load_si128(src));
      _mm_storeu_si128(dst + 1, _mm_load_si128(src + 1));
      _mm_storeu_si128(dst + 2, _mm_load_si128(src + 2));
    }
#else
    std::memcpy(out.data(), entry.data, entry.length);
#endif
    return entry.length;
  }

  // ___________________________________________________________________________
  // Returns the static singleton instance.
  static const VectorizedPrefixTable& instance() {
    static const VectorizedPrefixTable table;
    return table;
  }

 private:
  void initEntry(WellKnownPrefixId id, std::string_view prefix) {
    const auto index = static_cast<size_t>(id);
    AD_CONTRACT_CHECK(index < entries_.size());
    auto& e = entries_[index];
    AD_CONTRACT_CHECK(prefix.size() <= sizeof(e.data));
    std::memset(e.data, 0, sizeof(e.data));
    std::memcpy(e.data, prefix.data(), prefix.size());
    e.length = prefix.size();
  }
};

}  // namespace qlever::export_pipeline
