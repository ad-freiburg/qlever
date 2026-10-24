// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BINARY_EXPORT_H
#define QLEVER_SRC_ENGINE_BINARY_EXPORT_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <algorithm>
#include <cstring>
#include <type_traits>

#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/StringMapping.h"
#include "global/Id.h"
#include "index/Index.h"
#include "parser/SelectClause.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
#include "util/MemorySize/MemorySize.h"
#include "util/http/HttpClient.h"
#include "util/stream_generator.h"

namespace qlever::binary_export {

// The exporter sends the IDs of the rows in batches. Each batch is followed by
// the strings that its IDs refer to (see `StringMapping`). A batch ends after
// the first row with which its IDs take up at least
// `defaultMinNumIdBytesPerBatch` bytes, or its (distinct) strings take up at
// least `defaultMinNumStringBytesPerBatch` bytes.
//
// NOTE 1: These are no upper limits. A batch is at least as large as the last
// row (with all of its strings), which can be arbitrarily large (for example a
// single WKT literal of a large `MULTIPOLYGON` can take up tens of megabytes).
//
// NOTE 2: The minimum for the strings is much larger than the one for the IDs,
// such that also batches with large strings consist of several rows, which
// benefit from the deduplication of the strings within a batch.
//
// TODO<joka921> These values are arbitrary.
inline constexpr size_t defaultMinNumIdBytesPerBatch = 1 << 20;
inline constexpr size_t defaultMinNumStringBytesPerBatch = 16 << 20;

// The minimum sizes of an `ExportBatch`, see above. They can be changed for
// testing.
struct ExportBatchSizes {
  size_t minNumIdBytes_ = defaultMinNumIdBytesPerBatch;
  size_t minNumStringBytes_ = defaultMinNumStringBytesPerBatch;
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
  // order) to the batch, see `toExportableId` in the `.cpp` file.
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
// header (see `Header` in the `.cpp` file), followed by the IDs of the
// rows (as produced by `toExportableId` in the `.cpp` file), which are
// interrupted by batches of the strings that the non-trivial IDs refer to (see
// `StringMapping`). The format always ends with such a batch. Only the selected
// variables that are bound in the result get a column, the header lists the
// others as unbound. If there are no columns, the header is followed by the
// number of rows only.
ad_utility::streams::stream_generator exportAsQLeverBinary(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset,
    ad_utility::SharedCancellationHandle cancellationHandle);

// Reads the bytes of the binary format from a range of byte spans, like the
// body of an HTTP response. The bytes are copied block-wise, and a value can
// be split across several spans.
template <typename Range>
class ByteSpanReader {
  ql::ranges::iterator_t<Range> it_;
  ql::ranges::sentinel_t<Range> end_;
  // The part of `*it_` that has not been read yet. NOTE: `it_` is only
  // advanced when `*it_` has been read completely, as the span might point
  // into a buffer that is reused for the next span (as for a generator).
  ql::span<const std::byte> current_;

  // Advance to the next non-empty span if `current_` has been read completely.
  // Return false if the range is exhausted.
  bool fillCurrent() {
    while (current_.empty() && it_ != end_) {
      ++it_;
      if (it_ != end_) {
        current_ = *it_;
      }
    }
    return !current_.empty();
  }

 public:
  explicit ByteSpanReader(Range& range)
      : it_{ql::ranges::begin(range)}, end_{ql::ranges::end(range)} {
    if (it_ != end_) {
      current_ = *it_;
    }
  }

  // Copy the next `numBytes` bytes to `target`. Throw if the range ends before
  // all of them have been read.
  void operator()(char* target, size_t numBytes) {
    while (numBytes > 0) {
      if (!fillCurrent()) {
        throw std::runtime_error{"Stream ended unexpectedly."};
      }
      size_t numBytesFromCurrent = std::min(numBytes, current_.size());
      std::memcpy(target, current_.data(), numBytesFromCurrent);
      current_ = current_.subspan(numBytesFromCurrent);
      target += numBytesFromCurrent;
      numBytes -= numBytesFromCurrent;
    }
  }

  // Return true if all the bytes have been read.
  bool atEnd() { return !fillCurrent(); }
};

// Helper functions for the binary export and import. They are publicly
// accessible, such that they can be tested individually.
class BinaryExportHelpers {
 public:
  // Read a value of the trivially copyable type `T` from the `reader` (see
  // `ByteSpanReader`).
  template <typename T, typename Reader>
  static T read(Reader& reader) {
    static_assert(std::is_trivially_copyable_v<T>);
    T value;
    reader(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
  }

  // Read a string that was serialized via QLever's serialization framework
  // (its size, followed by its characters) from the `reader`. Throw if it is
  // larger than `maxNumBytes`, before any memory is allocated for it.
  template <typename Reader>
  static std::string readString(Reader& reader, size_t maxNumBytes) {
    auto size = read<size_t>(reader);
    throwIfTooLarge(size, maxNumBytes);
    std::string result(size, '\0');
    reader(result.data(), size);
    return result;
  }

  // Read a vector of strings as written by `appendVectorOfStrings` from the
  // `reader`. Throw if the strings (including the `std::string` objects
  // themselves) take up more than `maxNumBytes`.
  template <typename Reader>
  static std::vector<std::string> readVectorOfStrings(Reader& reader,
                                                      size_t maxNumBytes) {
    auto size = read<size_t>(reader);
    throwIfTooLarge(size, maxNumBytes / sizeof(std::string));
    size_t remainingBytes = maxNumBytes - size * sizeof(std::string);
    std::vector<std::string> result;
    result.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      result.push_back(readString(reader, remainingBytes));
      remainingBytes -= result.back().size();
    }
    return result;
  }

  // Return the number of bytes of the serialization of the `strings`, see
  // `appendVectorOfStrings`.
  static size_t serializedSize(const std::vector<std::string>& strings);

  // Append the serialization of the `strings` (via QLever's serialization
  // framework) to the `target`. To avoid reallocations, the caller can reserve
  // the `serializedSize` beforehand.
  static void appendVectorOfStrings(std::string& target,
                                    const std::vector<std::string>& strings);

 private:
  // Throw if `size` is larger than `maxSize`, which protects against
  // allocating huge amounts of memory for an invalid result.
  static void throwIfTooLarge(size_t size, size_t maxSize);
};

// Convert the IDs that the exporting instance sent (see `toExportableId` in the
// `.cpp` file) to IDs of the local instance. This is the counterpart of
// `toExportableId` and `StringMapping` on the importing side. In addition to
// the strings, it also handles all the other IDs that depend on the instance:
// encoded IRIs (whose patterns might differ), blank nodes, and `GeoPoint`s
// (whose encoding might differ). The strings and the IRIs that can't be
// encoded with the same local pattern are converted like any other input (see
// `toValueId`), so they become encoded IRIs or IDs of the vocabularies of the
// local index if possible, and are added to the local vocab (see
// `takeLocalVocab`) otherwise.
class IdRewriter {
  const QueryExecutionContext* qec_;
  // The patterns of the encoded IRIs of the exporting instance, and the
  // mapping from their indices to the indices of the equal local patterns.
  std::vector<encodedIri::Pattern> remotePatterns_;
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping_;
  GeoPointEncodingEnum remoteGeoPointEncoding_;
  // The same remote blank node is always replaced by the same local one. The
  // local blank nodes are allocated in `blankNodeVocab_`, which is merged into
  // the `LocalVocab` of each batch (see `takeLocalVocab`) and only contains
  // blank nodes. That way, the batches don't keep each other's words alive.
  ad_utility::HashMap<Id::T, Id> blankNodeMapping_;
  LocalVocab blankNodeVocab_;
  // The entries of the current batch that are not contained in the local
  // index.
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

  // Return the local vocab that contains the entries of the rewritten IDs
  // (including all the blank nodes so far, as the next batches might contain
  // the same blank nodes again). The local vocab is then reset.
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
// the `expectedVariables` (names with a leading `?`) in this order, and the
// response has to contain exactly these variables (in any order). The
// `resultSortedOn` are the columns by which the result is sorted (as in the
// constructor of `Result`). They are not checked, the caller has to make sure
// that the exporting instance sends the result in this order. A single string,
// as well as the strings of a single batch, may take up at most
// `maxStringBatchSize`, which protects against allocating huge amounts of
// memory for an invalid response.
//
// The strings (literals and IRIs) of the result, as well as the IRIs that are
// encoded with a pattern that doesn't exist in the local index, are converted
// like any other input (see `toValueId`). That is, they become encoded IRIs or
// IDs of the vocabularies of the local index if possible, and are only stored
// in the `LocalVocab` of the result otherwise.
Result importBinaryHttpResponse(
    bool requestLaziness, HttpOrHttpsResponse response,
    const QueryExecutionContext& qec,
    std::vector<std::string> expectedVariables,
    std::vector<ColumnIndex> resultSortedOn,
    ad_utility::MemorySize maxStringBatchSize,
    ad_utility::SharedCancellationHandle cancellationHandle);

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_H
