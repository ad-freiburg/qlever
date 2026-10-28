// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/binary_export/BinaryImport.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <optional>

#include "backports/algorithm.h"
#include "engine/binary_export/BinaryFormat.h"
#include "engine/binary_export/IdRewriter.h"
#include "util/Algorithm.h"
#include "util/Generator.h"
#include "util/HashSet.h"
#include "util/Serializer/AllocationLimitedReadSerializer.h"
#include "util/Serializer/ChunkedReadSerializer.h"

namespace qlever::binary_export {

using CancellationHandle = ad_utility::SharedCancellationHandle;
namespace serialization = ad_utility::serialization;

// Read the special case of a result without columns, for which only the number
// of rows is exported (see `exportNumRows` in `BinaryExport.cpp`), from the
// `serializer`. Return a table with `numColumns` columns (all of which are
// unbound) and that number of rows.
template <typename Serializer>
static IdTable importNumRows(Serializer& serializer, size_t numColumns,
                             const QueryExecutionContext& qec) {
  auto numRows = serialization::read<uint64_t>(serializer);
  IdTable result{numColumns, qec.getAllocator()};
  result.resize(numRows);
  for (auto column : result.getColumns()) {
    ql::ranges::fill(column, Id::makeUndefined());
  }
  return result;
}

// For each of the `expectedVariables`, the index of the column of the
// response that contains it (`std::nullopt` if it is unbound). Throw if the
// response doesn't contain exactly the `expectedVariables`.
static std::vector<std::optional<size_t>> getRemoteColumns(
    const std::vector<std::string>& expectedVariables, const Header& header) {
  const auto& remoteVariables = header.variableNames_;
  auto allRemoteVariables = remoteVariables;
  ql::ranges::copy(header.unboundVariableNames_,
                   std::back_inserter(allRemoteVariables));
  using Set = ad_utility::HashSet<std::string>;
  if (Set{allRemoteVariables.begin(), allRemoteVariables.end()} !=
      Set{expectedVariables.begin(), expectedVariables.end()}) {
    throw std::runtime_error{absl::StrCat(
        "The result in QLever's binary export format contains the variables \"",
        absl::StrJoin(allRemoteVariables, " "), "\", but the variables \"",
        absl::StrJoin(expectedVariables, " "), "\" were expected")};
  }
  return ad_utility::transform(
      expectedVariables,
      [&remoteVariables](const auto& variable) -> std::optional<size_t> {
        auto it = ql::ranges::find(remoteVariables, variable);
        if (it == remoteVariables.end()) {
          return std::nullopt;
        }
        return static_cast<size_t>(it - remoteVariables.begin());
      });
}

// Read the remaining IDs of a row of the response (the `firstId` has already
// been read) from the `serializer` into the `rowBuffer` (which has one
// entry per column of the response), and append the row to the `table`
// (which has one column per entry of `remoteColumns`, see `getRemoteColumns`).
template <typename Serializer>
static void readRow(Serializer& serializer, Id::T firstId,
                    const std::vector<std::optional<size_t>>& remoteColumns,
                    std::vector<Id::T>& rowBuffer, IdTable& table) {
  rowBuffer.at(0) = firstId;
  serializer.serializeBytes(reinterpret_cast<char*>(rowBuffer.data() + 1),
                            (rowBuffer.size() - 1) * sizeof(Id::T));
  table.emplace_back();
  size_t rowIndex = table.size() - 1;
  for (size_t col = 0; col < remoteColumns.size(); ++col) {
    table(rowIndex, col) =
        remoteColumns[col].has_value()
            ? Id::fromBits(rowBuffer[remoteColumns[col].value()])
            : Id::makeUndefined();
  }
}

// Core coroutine that reads the binary response and yields one
// `IdTableVocabPair` per vocab batch (or a single one if `yieldOnce` is set).
// The columns of the result are the `expectedVariables` in this order, see
// `getRemoteColumns`.
static Result::Generator importBinaryGenerator(
    HttpOrHttpsResponse response, const QueryExecutionContext& qec,
    std::vector<std::string> expectedVariables, bool yieldOnce,
    size_t maxStringBatchSize, CancellationHandle cancellationHandle) {
  serialization::ChunkedReadSerializer serializer{response.body_};
  auto header = Header::read(serializer, maxStringBatchSize);
  auto remoteColumns = getRemoteColumns(expectedVariables, header);
  size_t numColumns = expectedVariables.size();

  // Special case 0 columns: yield a single pair with the correct row count.
  if (header.variableNames_.empty()) {
    co_yield Result::IdTableVocabPair{
        importNumRows(serializer, numColumns, qec), LocalVocab{}};
    co_return;
  }

  IdRewriter rewriter{qec, std::move(header.patterns_),
                      header.geoPointEncoding_};
  IdTable currentBatch{numColumns, qec.getAllocator()};
  // The rows before this index have already been rewritten.
  size_t firstRowToRewrite = 0;
  std::vector<Id::T> rowBuffer(header.variableNames_.size());

  while (!serializer.isExhausted()) {
    cancellationHandle->throwIfCancelled();
    auto firstId = serialization::read<Id::T>(serializer);
    if (firstId != vocabMarker) {
      readRow(serializer, firstId, remoteColumns, rowBuffer, currentBatch);
      continue;
    }
    // The end of a batch, rewrite its rows using the strings that follow.
    rewriter.startBatch(
        serialization::readWithAllocationLimit<std::vector<std::string>>(
            serializer, maxStringBatchSize));
    rewriter.rewriteRows(currentBatch, firstRowToRewrite);
    firstRowToRewrite = currentBatch.size();
    if (!yieldOnce) {
      co_yield Result::IdTableVocabPair{std::move(currentBatch),
                                        rewriter.takeLocalVocab()};
      currentBatch = IdTable{numColumns, qec.getAllocator()};
      firstRowToRewrite = 0;
    }
  }
  // The exporter always ends with a vocab, so all rows have been converted.
  if (firstRowToRewrite != currentBatch.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format ended unexpectedly"};
  }
  if (yieldOnce) {
    co_yield Result::IdTableVocabPair{std::move(currentBatch),
                                      rewriter.takeLocalVocab()};
  }
}

// _____________________________________________________________________________
Result importBinaryHttpResponse(bool requestLaziness,
                                HttpOrHttpsResponse response,
                                const QueryExecutionContext& qec,
                                std::vector<std::string> expectedVariables,
                                std::vector<ColumnIndex> resultSortedOn,
                                ad_utility::MemorySize maxStringBatchSize,
                                CancellationHandle cancellationHandle) {
  auto generator = importBinaryGenerator(
      std::move(response), qec, std::move(expectedVariables), !requestLaziness,
      maxStringBatchSize.getBytes(), std::move(cancellationHandle));

  if (requestLaziness) {
    return Result{std::move(generator), std::move(resultSortedOn)};
  } else {
    auto [idTable, localVocab] =
        ad_utility::getSingleElement(std::move(generator));
    return Result{std::move(idTable), std::move(resultSortedOn),
                  std::move(localVocab)};
  }
}

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
