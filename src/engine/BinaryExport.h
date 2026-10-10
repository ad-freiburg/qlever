// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BINARY_EXPORT_H
#define QLEVER_SRC_ENGINE_BINARY_EXPORT_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/StringMapping.h"
#include "global/Id.h"
#include "index/Index.h"
#include "parser/SelectClause.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
#include "util/Serializer/FromCallableSerializer.h"
#include "util/http/HttpClient.h"
#include "util/stream_generator.h"

namespace qlever::binary_export {

// Convert `originalId`, which might contain references into this process'
// memory space to an id that completely inlines a value, or one that only has
// a reference to the passed `stringMapping`.
Id toExportableId(Id originalId, const LocalVocab& localVocab,
                  StringMapping& stringMapping);

// Export the result of `qet` in QLever's binary format, which is used for the
// efficient transfer of results between two QLever instances (see
// `ad_utility::MediaType::binaryQleverExport`). The format consists of a
// header (see `writeHeader` in the `.cpp` file), followed by the IDs of the
// rows (as produced by `toExportableId`), which are interrupted by batches of
// the strings that the non-trivial IDs refer to (see `StringMapping`). The
// format always ends with such a batch. If there are no columns, the header is
// followed by the number of rows only.
ad_utility::streams::stream_generator exportAsQLeverBinary(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset,
    ad_utility::SharedCancellationHandle cancellationHandle);

// Static helper functions for testing
class BinaryExportHelpers {
 public:
  // Read a value of type T from an iterator range.
  template <typename T, typename It, typename End>
  static T read(It& it, const End& end) {
    T buffer;
    std::span bufferView{reinterpret_cast<char*>(&buffer), sizeof(T)};
    for (char& byte : bufferView) {
      if (it == end) {
        throw std::runtime_error{"Stream ended unexpectedly."};
      }
      byte = static_cast<char>(*it);
      ++it;
    }
    return buffer;
  }

  template <typename It, typename End>
  struct IteratorReader {
    It it;
    End end;

    void operator()(char* target, size_t numBytes) {
      for (size_t i = 0; i < numBytes; ++i) {
        AD_CORRECTNESS_CHECK(it != end);
        *target = static_cast<char>(*it);
        ++it, ++target;
      }
    }
  };

  // Read a vector of strings from an iterator range.
  template <typename It, typename End>
  static std::vector<std::string> readVectorOfStrings(It& it, const End& end) {
    std::vector<std::string> transmittedStrings;
    IteratorReader<It, End> reader{it, end};
    ad_utility::serialization::ReadViaCallableSerializer serializer{
        std::ref(reader)};
    serializer >> transmittedStrings;
    it = reader.it;
    return transmittedStrings;
  }

  static std::string writeVectorOfStrings(
      const std::vector<std::string>& strings) {
    std::string result;
    result.reserve(strings.size() * 100);
    auto write = [&result](const char* src, size_t numBytes) {
      result.insert(result.end(), src, src + numBytes);
    };
    ad_utility::serialization::WriteViaCallableSerializer writer{write};
    writer << strings;
    return result;
  }

  // Convert the IDs of `result`, starting at row `dirtyIndex`, from the
  // representation of the exporting instance to IDs of the local instance
  // `qec`. IDs that refer to strings are replaced by the corresponding entries
  // of `transmittedStrings`, encoded IRIs are remapped using `prefixMapping`
  // (see `getPrefixMapping`) or decoded using the remote `prefixes`, blank
  // nodes are replaced by new local blank nodes (consistently via
  // `blankNodeMapping`), and `GeoPoint`s are converted to the local encoding.
  // New entries are added to `vocab`.
  static void rewriteVocabIds(
      IdTable& result, const size_t dirtyIndex,
      const QueryExecutionContext& qec, LocalVocab& vocab,
      const std::vector<std::string>& transmittedStrings,
      const ad_utility::HashMap<uint8_t, uint8_t>& prefixMapping,
      const std::vector<encodedIri::Pattern>& prefixes,
      ad_utility::HashMap<Id::T, Id>& blankNodeMapping,
      GeoPointEncodingEnum remoteGeoPointEncoding);

  // Get mapping from remote prefixes to local prefixes.
  static ad_utility::HashMap<uint8_t, uint8_t> getPrefixMapping(
      const QueryExecutionContext& qec,
      const std::vector<encodedIri::Pattern>& prefixes);
};

// Read the result that the exporting instance sent in the binary format (see
// `exportAsQLeverBinary`) from the `response`. The columns of the result are
// the `expectedVariables` (names with a leading `?`) in this order; a
// variable that the response doesn't contain is undefined in all rows.
Result importBinaryHttpResponse(bool requestLaziness,
                                HttpOrHttpsResponse response,
                                const QueryExecutionContext& qec,
                                std::vector<std::string> expectedVariables,
                                std::vector<ColumnIndex> resultSortedOn);
}  // namespace qlever::binary_export

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_H

#endif
