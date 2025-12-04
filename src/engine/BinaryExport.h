// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_BINARY_EXPORT_H
#define QLEVER_SRC_ENGINE_BINARY_EXPORT_H

#include "engine/StringMapping.h"
#include "global/Id.h"
#include "index/Index.h"
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

ad_utility::streams::stream_generator exportAsQLeverBinary(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset,
    ad_utility::SharedCancellationHandle cancellationHandle);

// Static helper functions for testing
class BinaryExportHelpers {
 public:
  // Return true iff the value can be serialized without a vocab entry.
  static bool isTrivial(Id id);

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
    ad_utility::serialization::ReadViaCallableSerializer serializer{std::ref(reader)};
    serializer >> transmittedStrings;
    it = reader.it;
    return transmittedStrings;
  }

  static std::string writeVectorOfStrings(const std::vector<std::string>& strings) {
    std::string result;
    result.reserve(strings.size() * 100);
    auto write = [&result](const char* src, size_t numBytes) {
      result.insert(result.end(), src, src + numBytes);
    };
    ad_utility::serialization::WriteViaCallableSerializer writer{write};
    writer << strings;
    return result;
  }

  // Rewrite vocab IDs in result table using transmitted strings.
  static void rewriteVocabIds(IdTable& result, size_t dirtyIndex,
                               const QueryExecutionContext& qec,
                               LocalVocab& vocab,
                               const std::vector<std::string>& transmittedStrings);

  // Convert raw ID bits to a proper Id, handling encoded values.
  static Id toIdImpl(const QueryExecutionContext& qec,
                     const std::vector<std::string>& prefixes,
                     const ad_utility::HashMap<uint8_t, uint8_t>& prefixMapping,
                     LocalVocab& vocab, Id::T bits);

  // Get mapping from remote prefixes to local prefixes.
  static ad_utility::HashMap<uint8_t, uint8_t> getPrefixMapping(
      const QueryExecutionContext& qec,
      const std::vector<std::string>& prefixes);
};


// _____________________________________________________________________________
Result importBinaryHttpResponse(bool requestLaziness,
                                HttpOrHttpsResponse response,
                                const QueryExecutionContext& qec,
                                std::vector<ColumnIndex> resultSortedOn);
}  // namespace qlever::binary_export

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_H
