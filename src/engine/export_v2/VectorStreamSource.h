// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_VECTORSTREAMSOURCE_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_VECTORSTREAMSOURCE_H

#include <cstddef>
#include <functional>
#include <optional>
#include <utility>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/Result.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ql::engine::export_v2 {

// A positive number of rows in one output chunk.
class RowsPerChunk {
 public:
  explicit RowsPerChunk(size_t value) : value_{value} {
    AD_CONTRACT_CHECK(value > 0);
  }

  [[nodiscard]] size_t value() const noexcept { return value_; }

 private:
  size_t value_;
};

struct VectorStreamConfig {
  RowsPerChunk rowsPerChunk = RowsPerChunk{8192};
};

// Keep rows whose ID in `column` equals `value`. Several filters are
// conjoined.
struct EqualityFilter {
  ColumnIndex column;
  Id value;
};

// Rechunk lazy QLever result blocks and push them into a synchronous sink.
// The source is stateless, so concurrent and nested calls are independent.
//
// Each sink argument owns the local vocabulary entries referenced by its IDs.
// It is valid only until the sink returns. A sink that retains data must clone
// both `idTable_` and `localVocab_`.
class VectorStreamSource {
 public:
  explicit VectorStreamSource(VectorStreamConfig config = {})
      : config_{std::move(config)} {}

  [[nodiscard]] const VectorStreamConfig& config() const noexcept {
    return config_;
  }

  // `blocks` must be a range of `Result::IdTableVocabPair` objects. The first
  // block fixes the schema and allocator. Empty input and an empty final chunk
  // produce no sink invocation.
  template <typename Range, typename Sink>
  void run(Range&& blocks, Sink&& sink,
           ql::span<const EqualityFilter> filters = {}) const {
    std::optional<Result::IdTableVocabPair> output;
    bool currentVocabIsMerged = false;

    auto flush = [&]() {
      AD_CORRECTNESS_CHECK(output.has_value());
      if (output->idTable_.empty()) {
        return;
      }
      std::invoke(sink, std::as_const(*output));
      output->idTable_.clear();
      output->localVocab_ = LocalVocab{};
      currentVocabIsMerged = false;
    };

    for (auto&& block : blocks) {
      const auto& inputTable = block.idTable_;
      if (!output.has_value()) {
        output.emplace(
            IdTable{inputTable.numColumns(), inputTable.getAllocator()},
            LocalVocab{});
        output->idTable_.reserve(config_.rowsPerChunk.value());
      }

      AD_CONTRACT_CHECK(inputTable.numColumns() ==
                        output->idTable_.numColumns());
      for (const auto& filter : filters) {
        AD_CONTRACT_CHECK(filter.column < inputTable.numColumns());
      }

      for (const auto& row : inputTable) {
        if (!passesFilters(row, filters)) {
          continue;
        }
        if (!currentVocabIsMerged) {
          output->localVocab_.mergeWith(block.localVocab_);
          currentVocabIsMerged = true;
        }
        output->idTable_.push_back(row);
        if (output->idTable_.numRows() == config_.rowsPerChunk.value()) {
          flush();
        }
      }
      currentVocabIsMerged = false;
    }

    if (output.has_value() && !output->idTable_.empty()) {
      flush();
    }
  }

 private:
  VectorStreamConfig config_;

  template <typename Row>
  [[nodiscard]] static bool passesFilters(
      const Row& row, ql::span<const EqualityFilter> filters) noexcept {
    return ql::ranges::all_of(filters, [&row](const EqualityFilter& filter) {
      return row[filter.column] == filter.value;
    });
  }
};

}  // namespace ql::engine::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_VECTORSTREAMSOURCE_H
