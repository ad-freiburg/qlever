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

#include <concepts>
#include <iterator>
#include <type_traits>

#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/StringMapping.h"
#include "global/Id.h"
#include "index/Index.h"
#include "parser/SelectClause.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
#include "util/Serializer/FromCallableSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/http/HttpClient.h"
#include "util/stream_generator.h"

namespace qlever::binary_export {

// Convert `originalId`, which might contain references into this process'
// memory space to an id that completely inlines a value, or one that only has
// a reference to the passed `stringMapping`.
Id toExportableId(Id originalId, StringMapping& stringMapping);

// The exporter sends the IDs of the rows in batches. Each batch is followed by
// the strings that its IDs refer to (see `StringMapping`). A batch ends after
// the first row with which its IDs take up at least `minNumIdBytesPerBatch`
// bytes, or its (distinct) strings take up at least `minNumStringBytesPerBatch`
// bytes. These are no upper limits: a batch is at least as large as the last
// row (with all of its strings), which can be arbitrarily large (for example a
// single WKT literal of a large `MULTIPOLYGON` can take up tens of megabytes).
// The minimum for the strings is much larger than the one for the IDs, such
// that also batches with large strings consist of several rows, which
// benefit from the deduplication of the strings within a batch.
// TODO<joka921> These values are arbitrary.
inline constexpr size_t minNumIdBytesPerBatch = 1 << 20;
inline constexpr size_t minNumStringBytesPerBatch = 16 << 20;

// The minimum sizes of an `ExportBatch`, see above. They can be changed for
// testing.
struct ExportBatchSizes {
  size_t minNumIdBytes_ = minNumIdBytesPerBatch;
  size_t minNumStringBytes_ = minNumStringBytesPerBatch;
};

// A batch of the binary export (see above and `exportAsQLeverBinary`). The IDs
// of the rows are written to a buffer, which is returned together with the
// strings that the IDs refer to by `finish`, such that each batch can be
// yielded at once.
class ExportBatch {
  ExportBatchSizes sizes_;
  StringMapping stringMapping_;
  // The IDs of the rows of this batch.
  std::string buffer_;
  uint64_t numRows_ = 0;

 public:
  // The strings are resolved using the `index`.
  explicit ExportBatch(const Index& index, ExportBatchSizes sizes = {})
      : sizes_{sizes}, stringMapping_{index} {}

  // Append the IDs of the row `row` of `idTable` (only the `columns` in this
  // order) to the batch, see `toExportableId`.
  void addRow(const IdTableView<0>& idTable, size_t row,
              ql::span<const ColumnIndex> columns);

  // Return true if the batch has reached one of its minimum `sizes_`, such
  // that it should be finished.
  bool shouldFinish() const;

  // Return the bytes of the batch, which consist of the IDs of its rows,
  // followed by a marker and the strings that the IDs refer to. Start a new
  // (empty) batch.
  std::string finish();

  uint64_t numRows() const { return numRows_; }
};

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

// A type with the size of a single byte, like `char` or `std::byte`. The
// binary format is read from iterators over such types.
template <typename T>
concept SingleByte = sizeof(T) == 1 && std::is_trivially_copyable_v<T>;

// An iterator over single bytes, see above.
template <typename It>
concept ByteIterator =
    std::input_iterator<It> && SingleByte<std::iter_value_t<It>>;

// Helper functions for the binary export and import. They are publicly
// accessible, such that they can be tested individually.
class BinaryExportHelpers {
 public:
  // A callable (as required by `ReadViaCallableSerializer`) that reads the
  // bytes from the range `[it, end)` and advances `it` accordingly. Throws if
  // the range ends before all the requested bytes have been read.
  // TODO<joka921> The bytes are read one by one, which is rather inefficient.
  // Reading whole blocks (e.g. the spans from which the input is joined, see
  // `importBinaryGenerator`) would be faster.
  template <ByteIterator It, std::sentinel_for<It> End>
  struct IteratorReader {
    It it;
    End end;

    void operator()(char* target, size_t numBytes) {
      for (size_t i = 0; i < numBytes; ++i) {
        if (it == end) {
          throw std::runtime_error{"Stream ended unexpectedly."};
        }
        *target = static_cast<char>(*it);
        ++it, ++target;
      }
    }
  };

  // Read a value of type `T` (using QLever's serialization framework) from the
  // range `[it, end)` and advance `it` by the number of bytes read.
  template <typename T, ByteIterator It, std::sentinel_for<It> End>
  static T read(It& it, const End& end) {
    IteratorReader<It, End> reader{std::move(it), end};
    ad_utility::serialization::ReadViaCallableSerializer serializer{
        std::ref(reader)};
    T value;
    serializer >> value;
    it = std::move(reader.it);
    return value;
  }

  // Serialize the `strings` (via QLever's serialization framework) into a
  // single string of bytes, which can be read by
  // `read<std::vector<std::string>>`.
  static std::string writeVectorOfStrings(
      const std::vector<std::string>& strings) {
    // The serialization of a vector or a string consists of its size followed
    // by its elements.
    size_t totalSize = sizeof(size_t);
    for (const auto& string : strings) {
      totalSize += sizeof(size_t) + string.size();
    }
    std::string result;
    result.reserve(totalSize);
    auto write = [&result](const char* src, size_t numBytes) {
      result.append(src, numBytes);
    };
    ad_utility::serialization::WriteViaCallableSerializer writer{write};
    writer << strings;
    AD_EXPENSIVE_CHECK(result.size() == totalSize);
    return result;
  }
};

// Convert the IDs that the exporting instance sent (see `toExportableId`) to
// IDs of the local instance. This is the counterpart of `toExportableId` and
// `StringMapping` on the importing side. In addition to the strings, it also
// handles all the other IDs that depend on the instance: encoded IRIs (whose
// patterns might differ), blank nodes, and `GeoPoint`s (whose encoding might
// differ). The strings and the IRIs that can't be encoded with the same local
// pattern are converted like any other input (see `toValueId`), so they become
// encoded IRIs or IDs of the vocabularies of the local index if possible, and
// are added to the local vocab (see `takeLocalVocab`) otherwise.
class IdRewriter {
  const QueryExecutionContext* qec_;
  // The patterns of the encoded IRIs of the exporting instance, and the
  // mapping from their indices to the indices of the equal local patterns.
  std::vector<encodedIri::Pattern> remotePatterns_;
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping_;
  GeoPointEncodingEnum remoteGeoPointEncoding_;
  // The same remote blank node is always replaced by the same local one.
  ad_utility::HashMap<Id::T, Id> blankNodeMapping_;
  // The entries that are not contained in the local index.
  LocalVocab localVocab_;
  // The local IDs of the strings of the current batch (see `startBatch`), and
  // of the encoded IRIs that were decoded in the current batch.
  std::vector<Id> transmittedIds_;
  ad_utility::HashMap<Id::T, Id> decodedIris_;

 public:
  IdRewriter(const QueryExecutionContext& qec,
             std::vector<encodedIri::Pattern> remotePatterns,
             GeoPointEncodingEnum remoteGeoPointEncoding);

  // Start a new batch, the IDs of which refer to the `transmittedStrings`. Each
  // of the strings is converted once, all the IDs that refer to it are then
  // replaced by the result of that conversion.
  void startBatch(std::vector<std::string> transmittedStrings);

  // Rewrite all the IDs of the `table`, starting at row `firstRow`.
  void rewriteRows(IdTable& table, size_t firstRow);

  // Rewrite a single `Id`, see the functions below.
  Id operator()(Id id);

  // Rewrite an `Id` of type `EncodedVal`. If its pattern also exists locally,
  // only the index of the pattern is changed, otherwise the IRI is decoded and
  // converted (at most once per batch).
  Id rewriteEncodedVal(Id id);

  // Replace a remote blank node by a local one (the same for all occurrences).
  Id rewriteBlankNode(Id id);

  // Replace an `Id` that refers to one of the transmitted strings of the
  // current batch (see `startBatch`).
  Id rewriteString(Id id) const;

  // Convert a `GeoPoint` from the remote to the local encoding.
  Id rewriteGeoPoint(Id id) const;

  // Return the local vocab that contains the entries of the rewritten IDs.
  // The local vocab is then reset, but it still contains the blank nodes (as
  // the next batches might contain the same blank nodes again).
  LocalVocab takeLocalVocab();

  // Get the mapping from the indices of the remote `patterns` to the indices
  // of the equal local patterns of `qec`. Remote patterns that don't exist
  // locally are not contained in the mapping.
  static ad_utility::HashMap<uint8_t, uint8_t> getPrefixMapping(
      const QueryExecutionContext& qec,
      const std::vector<encodedIri::Pattern>& patterns);
};

// Read the result that the exporting instance sent in the binary format (see
// `exportAsQLeverBinary`) from the `response`. The columns of the result are
// the `expectedVariables` (names with a leading `?`) in this order; a
// variable that the response doesn't contain is undefined in all rows. The
// `resultSortedOn` are the columns by which the result is sorted (as in the
// constructor of `Result`). They are not checked, the caller has to make sure
// that the exporting instance sends the result in this order.
//
// The strings (literals and IRIs) of the result, as well as the IRIs that are
// encoded with a pattern that doesn't exist in the local index, are converted
// like any other input (see `toValueId`). That is, they become encoded IRIs or
// IDs of the vocabularies of the local index if possible, and are only stored
// in the `LocalVocab` of the result otherwise.
Result importBinaryHttpResponse(bool requestLaziness,
                                HttpOrHttpsResponse response,
                                const QueryExecutionContext& qec,
                                std::vector<std::string> expectedVariables,
                                std::vector<ColumnIndex> resultSortedOn);
}  // namespace qlever::binary_export

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_H

#endif
