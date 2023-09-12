//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_IDTABLECOMPRESSEDWRITER_H
#define QLEVER_IDTABLECOMPRESSEDWRITER_H

#include <algorithm>
#include <execution>
#include <future>
#include <queue>
#include <ranges>

#include "engine/CallFixedSize.h"
#include "engine/idTable/IdTable.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"

class IdTableCompressedWriter {
  struct BlockMetadata {
    size_t compressedSize_;
    size_t uncompressedSize_;
    size_t offsetInFile_;
  };

 private:
  std::string filename_;
  ad_utility::Synchronized<ad_utility::File, std::shared_mutex> file_{filename_,
                                                                      "w+"};
  // For each IdTable for each column for each block store the metadata.
  using ColumnMetadata = std::vector<BlockMetadata>;
  std::vector<ColumnMetadata> blocksPerColumn_;
  std::vector<size_t> startOfSingleIdTables;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  ad_utility::MemorySize blockSize_ = ad_utility::MemorySize::megabytes(4ul);

 public:
  explicit IdTableCompressedWriter(std::string filename, size_t numCols,
                                   ad_utility::AllocatorWithLimit<Id> allocator)
      : filename_{std::move(filename)},
        blocksPerColumn_(numCols),
        allocator_{std::move(allocator)} {}
  ~IdTableCompressedWriter() {
    file_.wlock()->close();
    // TODO<joka921> Get it back in.
    // ad_utility::deleteFile(filename_);
  }

  const auto& allocator() const { return allocator_; }
  size_t numColumns() const { return blocksPerColumn_.size(); }

  void writeIdTable(const IdTable& table) {
    AD_CONTRACT_CHECK(table.numColumns() == numColumns());
    size_t bs = blockSize_.getBytes() / sizeof(Id);
    AD_CONTRACT_CHECK(bs > 0);
    startOfSingleIdTables.push_back(blocksPerColumn_.at(0).size());
    std::vector<std::future<void>> compressColumFutures;
    for (auto i : std::views::iota(0u, numColumns())) {
      compressColumFutures.push_back(
          std::async(std::launch::async, [this, i, bs, &table]() {
            auto& blockMetadata = blocksPerColumn_.at(i);
            decltype(auto) column = table.getColumn(i);
            for (size_t lower = 0; lower < column.size(); lower += bs) {
              size_t upper = std::min(lower + bs, column.size());
              auto sizeInBytes = (upper - lower) * sizeof(Id);
              auto compressed = ZstdWrapper::compress(
                  const_cast<Id*>(column.data()) + lower, sizeInBytes);
              size_t offset = 0;
              {
                auto lck = file_.wlock();
                offset = lck->tell();
                lck->write(compressed.data(), compressed.size());
              }
              blockMetadata.push_back({compressed.size(), sizeInBytes, offset});
            }
          }));
    }
    for (auto& fut : compressColumFutures) {
      fut.get();
    }
  }

 private:
  template <size_t N = 0>
  cppcoro::generator<typename IdTableStatic<N>::const_row_reference>
  makeGeneratorForRows(size_t index) {
    // TODO<joka921, GCC13> This whole function can be
    // std::views::join(std::ranges::owning_view{makeGeneratorForIdTable<N>(index)});
    for (auto& block : makeGeneratorForIdTable<N>(index)) {
      for (typename IdTableStatic<N>::const_row_reference row : block) {
        co_yield row;
      }
    }
  }

 public:
  template <size_t N = 0>
  std::vector<cppcoro::generator<const IdTableStatic<N>>> getAllGenerators() {
    file_.wlock()->flush();
    std::vector<cppcoro::generator<const IdTableStatic<N>>> result;
    result.reserve(startOfSingleIdTables.size());
    for (auto i : std::views::iota(0u, startOfSingleIdTables.size())) {
      result.push_back(makeGeneratorForIdTable<N>(i));
    }
    return result;
  }
  template <size_t N = 0>
  auto getAllRowGenerators() {
    file_.wlock()->flush();
    std::vector<decltype(makeGeneratorForRows<N>(0))> result;
    result.reserve(startOfSingleIdTables.size());
    for (auto i : std::views::iota(0u, startOfSingleIdTables.size())) {
      result.push_back(makeGeneratorForRows<N>(i));
    }
    return result;
  }

 private:
  template <size_t NumCols = 0>
  cppcoro::generator<const IdTableStatic<NumCols>> makeGeneratorForIdTable(
      size_t index) {
    using Table = IdTableStatic<NumCols>;
    auto firstBlock = startOfSingleIdTables.at(index);
    auto lastBlock = index + 1 < startOfSingleIdTables.size()
                         ? startOfSingleIdTables.at(index + 1)
                         : blocksPerColumn_.at(0).size();

    auto readBlock = [this](size_t blockIdx) {
      Table block{numColumns(), allocator_};
      block.reserve(blockSize_.getBytes() / sizeof(Id));
      size_t size =
          blocksPerColumn_.at(0).at(blockIdx).uncompressedSize_ / sizeof(Id);
      block.resize(size);
      std::vector<std::future<void>> readColumnFutures;
      for (auto i : std::views::iota(0u, numColumns())) {
        readColumnFutures.push_back(
            std::async(std::launch::async, [&block, this, i, blockIdx]() {
              decltype(auto) col = block.getColumn(i);
              const auto& metaData = blocksPerColumn_.at(i).at(blockIdx);
              std::vector<char> compressed;
              compressed.resize(metaData.compressedSize_);
              // TODO<joka921> Check that we have decompressed the correct
              // sizes.
              file_.wlock()->read(compressed.data(), metaData.compressedSize_,
                                  metaData.offsetInFile_);
              // TODO<joka921> also do this here.
              ZstdWrapper::decompressToBuffer(compressed.data(),
                                              compressed.size(), col.data(),
                                              metaData.uncompressedSize_);
            }));
      }
      for (auto& fut : readColumnFutures) {
        fut.get();
      }
      return block;
    };

    std::future<Table> fut;

    for (size_t blockIdx = firstBlock; blockIdx < lastBlock; ++blockIdx) {
      std::optional<Table> table;
      if (fut.valid()) {
        table = fut.get();
      }
      fut = std::async(std::launch::async, readBlock, blockIdx);
      if (table.has_value()) {
        co_yield table.value();
      }
    }
    // Last block.
    if (fut.valid()) {
      auto table = fut.get();
      co_yield table;
    }
  }
};

template <typename Comp, size_t NumStaticCols>
class ExternalIdTableSorter {
 private:
  IdTableStatic<NumStaticCols> currentBlock_;
  size_t blocksize_;
  IdTableCompressedWriter writer_;
  Comp comp_{};
  std::future<void> fut_;

 public:
  explicit ExternalIdTableSorter(std::string filename, size_t numCols,
                                 size_t memoryInBytes,
                                 ad_utility::AllocatorWithLimit<Id> allocator)
      : currentBlock_{allocator},
        blocksize_{memoryInBytes / numCols / sizeof(Id)},
        writer_{std::move(filename), numCols, allocator} {
    if constexpr (NumStaticCols == 0) {
      currentBlock_.setNumColumns(numCols);
    }
    currentBlock_.reserve(blocksize_);
  }

 private:
  void pushBlock(IdTableStatic<NumStaticCols> block) {
#ifdef USE_PARALLEL
    ad_utility::parallel_sort(block.begin(), block.end(), comp_);
#else
    std::ranges::sort(block, comp_);
#endif

    if (fut_.valid()) {
      fut_.get();
    }
    fut_ = std::async(std::launch::async,
                      [block = std::move(block), this]() mutable {
                        writer_.writeIdTable(std::move(block).toDynamic());
                      });
  }

 public:
  void pushRow(const auto& row) {
    currentBlock_.push_back(row);
    if (currentBlock_.size() >= blocksize_) {
      // TODO<joka921> Also do this in parallel.
      pushBlock(std::move(currentBlock_));
      // TODO<joka921> The move operations of the IdTable currently have wrong
      // semantics as they don't restore the empty column vectors.
      currentBlock_ = IdTableStatic<NumStaticCols>(writer_.allocator());
      currentBlock_.reserve(blocksize_);
    }
  }
  void push(const auto& row) { pushRow(row); }

  /// Return a lambda that takes a `ValueType` and calls `push` for that value.
  /// Note that `this` is captured by reference
  auto makePushCallback() {
    return [this](auto&& value) { pushRow(AD_FWD(value)); };
  }

  cppcoro::generator<
      const typename IdTableStatic<NumStaticCols>::const_row_reference>
  sortedView() {
    for (const auto& block : sortedBlocks()) {
      for (const auto& row : block) {
        co_yield row;
      }
    }
  }
  cppcoro::generator<const IdTableStatic<NumStaticCols>> sortedBlocks() {
    pushBlock(std::move(currentBlock_));
    if (fut_.valid()) {
      fut_.get();
    }
    auto rowGenerators = writer_.getAllRowGenerators<NumStaticCols>();
    using P = std::pair<decltype(rowGenerators[0].begin()),
                        decltype(rowGenerators[0].end())>;
    auto projection = [](const auto& el) -> decltype(auto) {
      return *el.first;
    };
    // TODO<joka921> Debug why we can't use `std::ranges::min_element` here.
    auto comp = [&, this](const auto& a, const auto& b) {
      return comp_(projection(a), projection(b));
    };
    std::vector<P> pq;

    for (auto& gen : rowGenerators) {
      pq.emplace_back(gen.begin(), gen.end());
    }
    std::make_heap(pq.begin(), pq.end(), comp);
    // TODO<joka921> yield in parallael.
    IdTableStatic<NumStaticCols> result(writer_.numColumns(),
                                        writer_.allocator());
    result.reserve(blocksize_);
    while (!pq.empty()) {
      std::pop_heap(pq.begin(), pq.end(), comp);
      auto& min = pq.back();
      result.push_back(*min.first);
      ++(min.first);
      if (min.first == min.second) {
        pq.pop_back();
      } else {
        std::push_heap(pq.begin(), pq.end(), comp);
      }
      if (result.size() >= blocksize_) {
        co_yield result;
        // TODO<joka921> prepare the next block in parallel
        result.clear();
      }
    }
    co_yield result;
  }
};

#endif  // QLEVER_IDTABLECOMPRESSEDWRITER_H
