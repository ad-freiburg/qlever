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

#include "engine/QueryExecutionTree.h"
#include "engine/StringMapping.h"
#include "global/Id.h"
#include "index/Index.h"
#include "parser/SelectClause.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
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

// Helper functions for the binary export. They are publicly accessible, such
// that they can be tested individually.
class BinaryExportHelpers {
 public:
  // Return the number of bytes of the serialization of the `strings`, see
  // `appendVectorOfStrings`.
  static size_t serializedSize(const std::vector<std::string>& strings);

  // Append the serialization of the `strings` (via QLever's serialization
  // framework) to the `target`. To avoid reallocations, the caller can reserve
  // the `serializedSize` beforehand.
  static void appendVectorOfStrings(std::string& target,
                                    const std::vector<std::string>& strings);
};

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_H
