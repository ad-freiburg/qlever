// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_ENGINE_BINARY_EXPORT_BINARYEXPORTTESTHELPERS_H
#define QLEVER_TEST_ENGINE_BINARY_EXPORT_BINARYEXPORTTESTHELPERS_H

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "../../util/IndexTestHelpers.h"
#include "../ValuesForTesting.h"
#include "engine/QueryExecutionTree.h"
#include "engine/binary_export/BinaryExport.h"
#include "global/Id.h"
#include "parser/SelectClause.h"

// Helpers for the tests of QLever's binary export format.
namespace binaryExportTestHelpers {
using namespace qlever::binary_export;

// The `Id` with which the exporter refers to the `i`-th transmitted string of
// a batch, see `StringMapping`.
inline Id transmitted(size_t i) {
  return Id::makeFromLocalVocabIndex(
      reinterpret_cast<LocalVocabIndex>(i << Id::numDatatypeBits));
}

// A test index that encodes the IRIs with one of the `prefixes` directly in
// the `Id`.
inline QueryExecutionContext* qecWithEncodedPrefixes(
    std::vector<std::string> prefixes = {"http://example.org/"}) {
  ad_utility::testing::TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets = std::move(prefixes);
  return ad_utility::testing::getQec(config);
}

// The plain prefix pattern for `prefix`, as used by `qecWithEncodedPrefixes`.
inline encodedIri::Pattern prefixPattern(std::string prefix) {
  return encodedIri::plainPrefixPattern(std::move(prefix),
                                        EncodedIriManager::NumBitsEncoding);
}

// Add `word` to the `localVocab` and return its `Id`.
inline Id localVocabId(LocalVocab& localVocab, std::string word,
                       const QueryExecutionContext& qec) {
  return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
      LocalVocabEntry::fromStringRepresentation(std::move(word),
                                                qec.getLocalVocabContext())));
}

// The bytes of the trivially copyable `value`.
template <typename T>
std::string bytesOf(const T& value) {
  return std::string{reinterpret_cast<const char*>(&value), sizeof(value)};
}

// The bytes of the `ids`.
inline std::string bytesOf(const std::vector<Id>& ids) {
  std::string result;
  for (Id id : ids) {
    result.append(bytesOf(id.getBits()));
  }
  return result;
}

// A string as serialized by QLever's serialization framework.
inline std::string serialized(std::string_view string) {
  return absl::StrCat(bytesOf(string.size()), string);
}

// A vector of strings as serialized by QLever's serialization framework.
inline std::string serialized(const std::vector<std::string>& strings) {
  std::string result = bytesOf(strings.size());
  for (const auto& string : strings) {
    result.append(serialized(string));
  }
  return result;
}

// A batch consists of the IDs, followed by a marker (which has the size of an
// `Id`) and the strings.
inline void expectBatch(
    std::string_view batch, const std::vector<Id>& ids,
    const std::vector<std::string>& strings,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  auto idBytes = bytesOf(ids);
  auto stringBytes = serialized(strings);
  ASSERT_EQ(batch.size(), idBytes.size() + sizeof(Id::T) + stringBytes.size());
  EXPECT_EQ(batch.substr(0, idBytes.size()), idBytes);
  EXPECT_EQ(batch.substr(idBytes.size() + sizeof(Id::T)), stringBytes);
}

// Export the `varNames` of the result of `qet` and return all the bytes.
inline std::string exportBytes(const QueryExecutionTree& qet,
                               const std::vector<std::string>& varNames,
                               LimitOffsetClause limitAndOffset = {}) {
  parsedQuery::SelectClause selectClause;
  selectClause.setSelected(ad_utility::transform(
      varNames, [](const std::string& name) { return Variable{name}; }));
  std::string result;
  for (std::string_view chunk : exportAsQLeverBinary(
           qet, selectClause, limitAndOffset,
           std::make_shared<ad_utility::CancellationHandle<>>())) {
    result.append(chunk);
  }
  return result;
}

// A `QueryExecutionTree` with the result `table` and the `variables`.
inline QueryExecutionTree makeQet(
    QueryExecutionContext* qec, IdTable table,
    std::vector<std::optional<Variable>> variables) {
  return QueryExecutionTree{
      qec, std::make_shared<ValuesForTesting>(qec, std::move(table),
                                              std::move(variables))};
}

// Return the result of `f`, which is called while `GeoPoint`s use the
// `encoding` (the default encoding is restored afterwards).
template <typename F>
auto withGeoPointEncoding(GeoPointEncodingEnum encoding, F f) {
  absl::Cleanup resetEncoding{
      []() { GeoPoint::setEncoding(GeoPointEncodingEnum::ZOrder); }};
  GeoPoint::setEncoding(encoding);
  return f();
}
}  // namespace binaryExportTestHelpers

#endif  // QLEVER_TEST_ENGINE_BINARY_EXPORT_BINARYEXPORTTESTHELPERS_H
