//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_IDTABLECOMPRESSEDWRITER_H
#define QLEVER_IDTABLECOMPRESSEDWRITER_H

#include <algorithm>
#include <future>
#include <queue>
#include <ranges>

#include "absl/strings/str_cat.h"
#include "engine/CallFixedSize.h"
#include "engine/idTable/IdTable.h"
#include "util/AsyncStream.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Views.h"
#include "util/http/beast.h"

namespace ad_utility {

using namespace ad_utility::memory_literals;
// A class that stores a sequence of `IdTable`s in a file. Each `IdTable` is
// compressed blockwise. Typically, the blocksize is much smaller than the size
// of a single IdTable, such that there are multiple blocks per IdTable. This is
// an important building block for an external merge sort implementation where
// we want very large presorted `IdTables` over which we need to incrementally
// iterate (hence the smaller blocks for compression). These tables all have the
// same number of columns, so they can be thought of as large blocks of a very
// large `IdTable` which is formed by the concatenation of the single tables.
class IdTableCompressedWriter {
  // Metadata for a compressed block of bytes. A block is a contiguous part of a
  // column of an `IdTable`.
  struct CompressedBlockMetadata {
    // The sizes are in Bytes.
    size_t compressedSize_;
    size_t uncompressedSize_;
    size_t offsetInFile_;
  };

 private:
  // The filename and actual file to which the `IdTable` is written .
  std::string filename_;
  ad_utility::Synchronized<ad_utility::File, std::shared_mutex> file_{filename_,
                                                                      "w+"};
  // For a single column, the concatenation of the blocks for that column of all
  // `IdTables`.
  using ColumnMetadata = std::vector<CompressedBlockMetadata>;

  // The `ColumnMetadata` of each column.
  std::vector<ColumnMetadata> blocksPerColumn_;
  // For each contained `IdTable` contains the index in the `ColumnMetadata`
  // where the blocks of this table begin.
  std::vector<size_t> startOfSingleIdTables_;

  ad_utility::AllocatorWithLimit<Id> allocator_;
  // Each column of each `IdTable` will be split up into blocks of this size and
  // then separately compressed and stored. Has to be chosen s.t. it is much
  // smaller than the size of the single `IdTables` and  large enough to make
  // the used compression algorithm work well.
  ad_utility::MemorySize blockSizeUncompressed_ = 4_MB;

 public:
  // Constructor. The file at `filename` will be overwritten. Each of the
  // `IdTables` that will be passed in has to have exactly `numCols` columns.
  explicit IdTableCompressedWriter(
      std::string filename, size_t numCols,
      ad_utility::AllocatorWithLimit<Id> allocator,
      ad_utility::MemorySize blockSizeUncompressed = 4_MB)
      : filename_{std::move(filename)},
        blocksPerColumn_(numCols),
        allocator_{std::move(allocator)},
        blockSizeUncompressed_(blockSizeUncompressed) {}

  // Destructor. Deletes the stored file.
  ~IdTableCompressedWriter() {
    file_.wlock()->close();
    ad_utility::deleteFile(filename_);
  }

  // Simple getters for the stored allocator and the number of columns;
  const auto& allocator() const { return allocator_; }
  size_t numColumns() const { return blocksPerColumn_.size(); }
  const MemorySize& blockSizeUncompressed() const {
    return blockSizeUncompressed_;
  }

  // Store an `idTable`.
  void writeIdTable(const IdTable& table) {
    AD_CONTRACT_CHECK(table.numColumns() == numColumns());
    size_t blockSize = blockSizeUncompressed_.getBytes() / sizeof(Id);
    AD_CONTRACT_CHECK(blockSize > 0);
    startOfSingleIdTables_.push_back(blocksPerColumn_.at(0).size());
    // The columns are compressed and stored in parallel.
    // TODO<joka921> Use parallelism per block instead of per column (more
    // fine-grained) but only once we have a reasonable abstraction for
    // parallelism.
    std::vector<std::future<void>> compressColumFutures;
    for (auto i : std::views::iota(0u, numColumns())) {
      compressColumFutures.push_back(
          std::async(std::launch::async, [this, i, blockSize, &table]() {
            auto& blockMetadata = blocksPerColumn_.at(i);
            decltype(auto) column = table.getColumn(i);
            // TODO<C++23> Use `std::views::chunk`
            for (size_t lower = 0; lower < column.size(); lower += blockSize) {
              size_t upper = std::min(lower + blockSize, column.size());
              auto thisBlockSizeUncompressed = (upper - lower) * sizeof(Id);
              auto compressed =
                  ZstdWrapper::compress(const_cast<Id*>(column.data()) + lower,
                                        thisBlockSizeUncompressed);
              size_t offset = 0;
              {
                auto lck = file_.wlock();
                offset = lck->tell();
                lck->write(compressed.data(), compressed.size());
              }
              blockMetadata.push_back(
                  {compressed.size(), thisBlockSizeUncompressed, offset});
            }
          }));
    }
    for (auto& fut : compressColumFutures) {
      fut.get();
    }
  }

  // Return a vector of generators where the `i-th` generator generates the
  // `i-th` IdTable that was stored. The IdTables are yielded in (smaller)
  // blocks which are `IdTables` themselves.
  template <size_t N = 0>
  std::vector<cppcoro::generator<const IdTableStatic<N>>> getAllGenerators() {
    file_.wlock()->flush();
    std::vector<cppcoro::generator<const IdTableStatic<N>>> result;
    result.reserve(startOfSingleIdTables_.size());
    for (auto i : std::views::iota(0u, startOfSingleIdTables_.size())) {
      result.push_back(makeGeneratorForIdTable<N>(i));
    }
    return result;
  }

  // Return a vector of generators where the `i-th` generator generates the
  // `i-th` IdTable that was stored. The IdTables are yielded row by row.
  template <size_t N = 0>
  auto getAllRowGenerators() {
    file_.wlock()->flush();
    std::vector<decltype(makeGeneratorForRows<N>(0))> result;
    result.reserve(startOfSingleIdTables_.size());
    for (auto i : std::views::iota(0u, startOfSingleIdTables_.size())) {
      result.push_back(makeGeneratorForRows<N>(i));
    }
    return result;
  }

  template <size_t N = 0>
  auto getGeneratorForAllRows() {
    return std::views::join(ad_utility::OwningView{getAllRowGenerators<N>()});
  }

  // Clear the underlying file and completely reset the data structure s.t. it
  // can be reused.
  void clear() {
    file_.wlock()->close();
    ad_utility::deleteFile(filename_);
    file_.wlock()->open(filename_, "w+");
    std::ranges::for_each(blocksPerColumn_, [](auto& block) { block.clear(); });
    startOfSingleIdTables_.clear();
  }

 private:
  // Get the row generator for a single IdTable, specified by the `index`.
  template <size_t N = 0>
  auto makeGeneratorForRows(size_t index) {
    return ad_utility::OwningView{makeGeneratorForIdTable<N>(index)} |
           std::views::join;
  }

  // Get the block generator for a single IdTable, specified by the `index`.
  template <size_t NumCols = 0>
  cppcoro::generator<const IdTableStatic<NumCols>> makeGeneratorForIdTable(
      size_t index) {
    using Table = IdTableStatic<NumCols>;
    auto firstBlock = startOfSingleIdTables_.at(index);
    auto lastBlock = index + 1 < startOfSingleIdTables_.size()
                         ? startOfSingleIdTables_.at(index + 1)
                         : blocksPerColumn_.at(0).size();

    // Lambda that decompresses the block at the given `blockIdx`. The
    // individual columns are decompressed concurrently.
    auto readBlock = [this](size_t blockIdx) {
      Table block{numColumns(), allocator_};
      block.reserve(blockSizeUncompressed_.getBytes() / sizeof(Id));
      size_t blockSize =
          blocksPerColumn_.at(0).at(blockIdx).uncompressedSize_ / sizeof(Id);
      block.resize(blockSize);
      std::vector<std::future<void>> readColumnFutures;
      for (auto i : std::views::iota(0u, numColumns())) {
        readColumnFutures.push_back(
            std::async(std::launch::async, [&block, this, i, blockIdx]() {
              decltype(auto) col = block.getColumn(i);
              const auto& metaData = blocksPerColumn_.at(i).at(blockIdx);
              std::vector<char> compressed;
              compressed.resize(metaData.compressedSize_);
              auto numBytesRead = file_.wlock()->read(compressed.data(),
                                                      metaData.compressedSize_,
                                                      metaData.offsetInFile_);
              AD_CORRECTNESS_CHECK(numBytesRead >= 0 &&
                                   static_cast<size_t>(numBytesRead) ==
                                       metaData.compressedSize_);
              auto numBytesDecompressed = ZstdWrapper::decompressToBuffer(
                  compressed.data(), compressed.size(), col.data(),
                  metaData.uncompressedSize_);
              AD_CORRECTNESS_CHECK(numBytesDecompressed ==
                                   metaData.uncompressedSize_);
            }));
      }
      for (auto& fut : readColumnFutures) {
        fut.get();
      }
      return block;
    };

    std::future<Table> fut;

    // Yield one block after the other. While one block is yielded the next
    // block is already read concurrently.
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
    AD_CORRECTNESS_CHECK(fut.valid());
    auto table = fut.get();
    co_yield table;
  }
};

// This class allows the external and compressed storing of an `IdTable` that is
// too large to be stored in RAM. `NumStaticCols == 0` means that the IdTable is
// stored dynamically (see `IdTable.h` and `CallFixedSize.h` for details). The
// interface is as follows: First there is one call to `push` for each row of
// the IdTable, and then there is one single call to `getRows` which yields a
// generator that yields the rows that have previously been pushed.
template <size_t NumStaticCols>
class ExternalIdTableCompressor {
 protected:
  using MemorySize = ad_utility::MemorySize;
  // Used to aggregate rows for the next block.
  IdTableStatic<NumStaticCols> currentBlock_;
  // For statistical reasons
  size_t numElementsPushed_ = 0;
  // The number of columns of the `IdTable`. Might be different
  // from `NumStaticCols` when dynamic tables (NumStaticCols == 0) are used;
  size_t numColumns_;
  // The maximum amount of memory that this class can use.
  MemorySize memory_;
  // The number of rows per block in the first phase.
  // The division by two is there because we store two blocks at the same time:
  // One that is currently being sorted and written to disk in the background,
  // and one that is used to collect rows in the calls to `push`.
  size_t blocksize_{memory_.getBytes() / (numColumns_ * sizeof(Id) * 2)};
  IdTableCompressedWriter writer_;
  // Used to compress and write a block on a background thread while we can
  // already collect the rows for the next block.
  std::future<void> compressAndWriteFuture_;

 public:
  // Constructor.
  explicit ExternalIdTableCompressor(
      std::string filename, size_t numCols, size_t memoryInBytes,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = 4_MB)
      : currentBlock_{allocator},
        numColumns_{numCols},
        memory_{MemorySize::bytes(memoryInBytes)},
        writer_{std::move(filename), numCols, allocator, blocksizeCompression} {
    currentBlock_.setNumColumns(numCols);
    currentBlock_.reserve(blocksize_);
    AD_CONTRACT_CHECK(NumStaticCols == 0 || NumStaticCols == numCols);
  }

  // When we have a static number of columns, then the `numCols` argument to the
  // constructor is redundant.
  explicit ExternalIdTableCompressor(
      std::string filename, size_t memoryInBytes,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = 4_MB) requires(NumStaticCols > 0)
      : ExternalIdTableCompressor(std::move(filename), NumStaticCols,
                                  memoryInBytes, std::move(allocator),
                                  blocksizeCompression) {}

  size_t size() const { return numElementsPushed_; }

 public:
  // Add a single row to the input. The type of `row` needs to be something that
  // can be `push_back`ed to a `IdTable`.
  void push(const auto& row) requires requires { currentBlock_.push_back(row); }
  {
    ++numElementsPushed_;
    currentBlock_.push_back(row);
    if (currentBlock_.size() >= blocksize_) {
      // TODO<joka921> Also do this in parallel.
      pushBlock(std::move(currentBlock_));
      // TODO<joka921> The move operations of the `IdTable` currently have wrong
      // semantics as they don't restore the empty column vectors.
      currentBlock_ =
          IdTableStatic<NumStaticCols>(numColumns_, writer_.allocator());
      currentBlock_.reserve(blocksize_);
    }
  }

  // Return a lambda that takes a `ValueType` and calls `push` for that value.
  // Note that `this` is captured by reference
  auto makePushCallback() {
    return [self = this](auto&& value) { self->push(AD_FWD(value)); };
  }

  // Transition from the input phase, where `push()` can be called, to the
  // output phase and return a generator that yields the sorted elements. This
  // function must be called exactly once.
  auto getRows() { return getBlocks(); }

  // Delete the underlying file and reset the sorter. May only be called if no
  // active `getBlocks()` generator that has not been fully iterated over is
  // currently active, else the behavior is undefined.
  void clear() {
    currentBlock_ =
        IdTableStatic<NumStaticCols>(numColumns_, writer_.allocator());
    numElementsPushed_ = 0;
    if (compressAndWriteFuture_.valid()) {
      compressAndWriteFuture_.get();
    }
    writer_.clear();
  }

 private:
  // Transition from the input phase, where `push()` may be called, to the
  // output phase and return a generator that yields the sorted elements. This
  // function may be called exactly once.
  auto getBlocks() {
    pushBlock(std::move(currentBlock_));
    currentBlock_ =
        IdTableStatic<NumStaticCols>(numColumns_, writer_.allocator());
    currentBlock_.setNumColumns(numColumns_);
    if (compressAndWriteFuture_.valid()) {
      compressAndWriteFuture_.get();
    }
    return writer_.getGeneratorForAllRows<NumStaticCols>();
  }

  void pushBlock(IdTableStatic<NumStaticCols> block) {
    if (compressAndWriteFuture_.valid()) {
      compressAndWriteFuture_.get();
    }
    if (block.empty()) {
      return;
    }
    // TODO<joka921> We probably need one thread for sorting and one for writing
    // for the maximal performance. But this requires more work for a general
    // synchronization framework.
    compressAndWriteFuture_ = std::async(
        std::launch::async, [block = std::move(block), this]() mutable {
          writer_.writeIdTable(std::move(block).toDynamic());
        });
  }
};

// This class allows the external (on-disk) sorting of an `IdTable` that is too
// large to be stored in RAM. `NumStaticCols == 0` means that the IdTable is
// stored dynamically (see `IdTable.h` and `CallFixedSize.h` for details). The
// interface is as follows: First there is one call to `push` for each row of
// the IdTable, and then there is one single call to `getRows` which yields a
// generator that yields the sorted rows one by one.

// When using very small block sizes in unit tests, then sometimes there are
// false positives in the memory limit mechanism, so setting the following
// variable to `true` allows to disable the memory limit.
inline bool EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;
template <typename Comparator, size_t NumStaticCols>
class ExternalIdTableSorter {
 private:
  using MemorySize = ad_utility::MemorySize;
  // Used to aggregate rows for the next block.
  IdTableStatic<NumStaticCols> currentBlock_;
  // For statistical reasons
  size_t numElementsPushed_ = 0;
  // The number of columns of the `IdTable`s to be sorted. Might be different
  // from `NumStaticCols` when dynamic tables (NumStaticCols == 0) are used;
  size_t numColumns_;
  // The maximum amount of memory that this sorter can use.
  MemorySize memory_;
  // The number of rows per block in the first phase.
  // The division by two is there because we store two blocks at the same time:
  // One that is currently being sorted and written to disk in the background,
  // and one that is used to collect rows in the calls to `push`.
  size_t blocksize_{memory_.getBytes() / (numColumns_ * sizeof(Id) * 2)};
  IdTableCompressedWriter writer_;
  Comparator comp_{};
  // Used to compress and write a block on a background thread while we can
  // already collect the rows for the next block.
  std::future<void> sortAndWriteFuture_;

  // Track if we are currently in the merging phase.
  std::atomic<bool> mergeIsActive_ = false;

  // The maximal blocksize in the output phase.
  MemorySize maxOutputBlocksize = 1_GB;
  // The number of merged blocks that are buffered during the
  //  output phase.
  int numBufferedOutputBlocks = 4;

 public:
  // Constructor.
  explicit ExternalIdTableSorter(std::string filename, size_t numCols,
                                 size_t memoryInBytes,
                                 ad_utility::AllocatorWithLimit<Id> allocator,
                                 MemorySize blocksizeCompression = 4_MB)
      : currentBlock_{allocator},
        numColumns_{numCols},
        memory_{MemorySize::bytes(memoryInBytes)},
        writer_{std::move(filename), numCols, allocator, blocksizeCompression} {
    currentBlock_.setNumColumns(numCols);
    currentBlock_.reserve(blocksize_);
    AD_CONTRACT_CHECK(NumStaticCols == 0 || NumStaticCols == numCols);
  }

  // When we have a static number of columns, then the `numCols` argument to the
  // constructor is redundant.
  explicit ExternalIdTableSorter(std::string filename, size_t memoryInBytes,
                                 ad_utility::AllocatorWithLimit<Id> allocator,
                                 MemorySize blocksizeCompression = 4_MB)
      requires(NumStaticCols > 0)
      : ExternalIdTableSorter(std::move(filename), NumStaticCols, memoryInBytes,
                              std::move(allocator), blocksizeCompression) {}

 public:
  // Add a single row to the input. The type of `row` needs to be something that
  // can be `push_back`ed to a `IdTable`.
  void push(const auto& row) requires requires { currentBlock_.push_back(row); }
  {
    ++numElementsPushed_;
    currentBlock_.push_back(row);
    if (currentBlock_.size() >= blocksize_) {
      // TODO<joka921> Also do this in parallel.
      pushBlock(std::move(currentBlock_));
      // TODO<joka921> The move operations of the `IdTable` currently have wrong
      // semantics as they don't restore the empty column vectors.
      currentBlock_ =
          IdTableStatic<NumStaticCols>(numColumns_, writer_.allocator());
      currentBlock_.reserve(blocksize_);
    }
  }

  // Return a lambda that takes a `ValueType` and calls `push` for that value.
  // Note that `this` is captured by reference
  auto makePushCallback() {
    return [self = this](auto&& value) { self->push(AD_FWD(value)); };
  }

  // Transition from the input phase, where `push()` can be called, to the
  // output phase and return a generator that yields the sorted elements. This
  // function must be called exactly once.
  cppcoro::generator<
      const typename IdTableStatic<NumStaticCols>::const_row_reference>
  sortedView() {
    size_t numYielded = 0;
    mergeIsActive_.store(true);
    for (const auto& block : ad_utility::streams::runStreamAsync(
             sortedBlocks(), std::max(1, numBufferedOutputBlocks - 2))) {
      for (typename IdTableStatic<NumStaticCols>::const_row_reference row :
           block) {
        ++numYielded;
        co_yield row;
      }
    }
    AD_CORRECTNESS_CHECK(numYielded == numElementsPushed_);
    mergeIsActive_.store(false);
  }

  // Delete the underlying file and reset the sorter. May only be called if no
  // active `getBlocks()` generator that has not been fully iterated over is
  // currently active, else an exception will be thrown.
  void clear() {
    if (mergeIsActive_.load()) {
      throw std::runtime_error{
          "Calling `clear` on an an `ExternalIdTableSorter` that is currently "
          "been iterated over is forbidden."};
    }
    currentBlock_ =
        IdTableStatic<NumStaticCols>(numColumns_, writer_.allocator());
    numElementsPushed_ = 0;
    if (sortAndWriteFuture_.valid()) {
      sortAndWriteFuture_.get();
    }
    writer_.clear();
  }

 private:
  // Transition from the input phase, where `push()` may be called, to the
  // output phase and return a generator that yields the sorted elements. This
  // function may be called exactly once.
  cppcoro::generator<IdTableStatic<NumStaticCols>> sortedBlocks() {
    // Optimization for very small inputs, just sort the input.
    if (!sortAndWriteFuture_.valid()) {
      AD_CORRECTNESS_CHECK(numElementsPushed_ == currentBlock_.size());
      sortBlockInPlace(currentBlock_);
      co_yield currentBlock_;
      co_return;
    }
    pushBlock(std::move(currentBlock_));
    currentBlock_ =
        IdTableStatic<NumStaticCols>(numColumns_, writer_.allocator());
    currentBlock_.setNumColumns(numColumns_);
    if (sortAndWriteFuture_.valid()) {
      sortAndWriteFuture_.get();
    }
    auto rowGenerators = writer_.getAllRowGenerators<NumStaticCols>();

    auto requiredMemoryForInputBlocks =
        rowGenerators.size() * numColumns_ * writer_.blockSizeUncompressed();
    const size_t blockSizeOutput = [&]() -> size_t {
      if (EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING) {
        // For unit tests, always yield 5 outputs at once.
        return 5;
      } else {
        if (requiredMemoryForInputBlocks >= memory_) {
          throw std::runtime_error{absl::StrCat(
              "Insufficient memory for merging ", rowGenerators.size(),
              " blocks. Please increase the memory settings")};
        }
        using namespace ad_utility::memory_literals;
        // Don't use a too large output size.
        auto blockSizeOutputMemory = std::min(
            (memory_ - requiredMemoryForInputBlocks) / numBufferedOutputBlocks,
            maxOutputBlocksize);

        size_t blockSizeForOutput =
            blockSizeOutputMemory.getBytes() / (sizeof(Id) * numColumns_);
        if (blockSizeForOutput <= 100) {
          throw std::runtime_error{absl::StrCat(
              "Insufficient memory for merging ", rowGenerators.size(),
              " blocks. Please increase the memory settings")};
        }
        return blockSizeForOutput;
      }
    }();

    using P = std::pair<decltype(rowGenerators[0].begin()),
                        decltype(rowGenerators[0].end())>;
    auto projection = [](const auto& el) -> decltype(auto) {
      return *el.first;
    };
    // NOTE: We have to switch the arguments, because the heap operations by
    // default order descending...
    auto comp = [&, this](const auto& a, const auto& b) {
      return comp_(projection(b), projection(a));
    };
    std::vector<P> pq;

    for (auto& gen : rowGenerators) {
      pq.emplace_back(gen.begin(), gen.end());
    }
    std::make_heap(pq.begin(), pq.end(), comp);
    IdTableStatic<NumStaticCols> result(writer_.numColumns(),
                                        writer_.allocator());
    result.reserve(blockSizeOutput);
    size_t numPopped = 0;
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
      if (result.size() >= blockSizeOutput) {
        numPopped += result.numRows();
        co_yield result;
        // The `result` will be moved away, so we have to reset it again.
        result = IdTableStatic<NumStaticCols>(writer_.numColumns(),
                                              writer_.allocator());
        result.reserve(blockSizeOutput);
      }
    }
    numPopped += result.numRows();
    co_yield result;
    AD_CORRECTNESS_CHECK(numPopped == numElementsPushed_);
  }

  void sortBlockInPlace(IdTableStatic<NumStaticCols>& block) const {
#ifdef _PARALLEL_SORT
    ad_utility::parallel_sort(block.begin(), block.end(), comp_);
#else
    std::ranges::sort(block, comp_);
#endif
  }
  void pushBlock(IdTableStatic<NumStaticCols> block) {
    if (sortAndWriteFuture_.valid()) {
      sortAndWriteFuture_.get();
    }
    if (block.empty()) {
      return;
    }
    // TODO<joka921> We probably need one thread for sorting and one for writing
    // for the maximal performance. But this requires more work for a general
    // synchronization framework.
    sortAndWriteFuture_ = std::async(
        std::launch::async, [block = std::move(block), this]() mutable {
          sortBlockInPlace(block);
          writer_.writeIdTable(std::move(block).toDynamic());
        });
  }
};
}  // namespace ad_utility

#endif  // QLEVER_IDTABLECOMPRESSEDWRITER_H
