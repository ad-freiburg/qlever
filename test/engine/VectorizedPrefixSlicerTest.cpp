// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <string_view>
#include <utility>

#include "engine/export_prototypes/VectorizedPrefixSlicer.h"

namespace {

using qlever::export_pipeline::VectorizedPrefixTable;
using qlever::export_pipeline::WellKnownPrefixId;

// Every well-known prefix round-trips byte-identically through the vector
// stores. The scratch buffer is 48 bytes: the stores cover whole 16-byte
// blocks, so the buffer must hold the length rounded up to a multiple of
// 16, not just the valid length.
TEST(VectorizedPrefixSlicerTest, AllPrefixesRoundTrip) {
  const std::array<std::pair<WellKnownPrefixId, std::string_view>, 7> cases = {{
      {WellKnownPrefixId::WikidataEntity, "http://www.wikidata.org/entity/"},
      {WellKnownPrefixId::WikidataDirectProp,
       "http://www.wikidata.org/prop/direct/"},
      {WellKnownPrefixId::RdfSyntax,
       "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
      {WellKnownPrefixId::RdfsSchema, "http://www.w3.org/2000/01/rdf-schema#"},
      {WellKnownPrefixId::OwlOntology, "http://www.w3.org/2002/07/owl#"},
      {WellKnownPrefixId::SchemaOrg, "http://schema.org/"},
      {WellKnownPrefixId::XmlSchema, "http://www.w3.org/2001/XMLSchema#"},
  }};
  const auto& table = VectorizedPrefixTable::instance();
  for (const auto& [id, expected] : cases) {
    std::array<char, 48> buffer{};
    size_t written = table.writePrefixFast(id, buffer);
    EXPECT_EQ(written, expected.size());
    EXPECT_EQ(std::string_view(buffer.data(), written), expected);
  }
}

// A buffer sized for the valid length only (not the rounded-up store size)
// is rejected instead of being overwritten, and so is an out-of-range id.
TEST(VectorizedPrefixSlicerTest, TooSmallBufferAndInvalidIdThrow) {
  const auto& table = VectorizedPrefixTable::instance();
  // "http://schema.org/" has 18 bytes, the stores cover 32.
  std::array<char, 18> exact{};
  EXPECT_THROW(static_cast<void>(
                   table.writePrefixFast(WellKnownPrefixId::SchemaOrg, exact)),
               ad_utility::Exception);
  std::array<char, 32> rounded{};
  EXPECT_EQ(table.writePrefixFast(WellKnownPrefixId::SchemaOrg, rounded), 18u);
  EXPECT_THROW(static_cast<void>(
                   table.writePrefixFast(WellKnownPrefixId::Count, rounded)),
               ad_utility::Exception);
  static_assert(VectorizedPrefixTable::storeSize(18) == 32);
  static_assert(VectorizedPrefixTable::storeSize(32) == 32);
  static_assert(VectorizedPrefixTable::storeSize(33) == 48);
}

}  // namespace
