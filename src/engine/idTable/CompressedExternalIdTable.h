// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_COMPRESSEDEXTERNALIDTABLE_H
#define QLEVER_COMPRESSEDEXTERNALIDTABLE_H

#include <absl/strings/str_cat.h>

#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/thread_pool.hpp>
#include <condition_variable>
#include <exception>
#include <future>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

#include "backports/algorithm.h"
#include "engine/CallFixedSize.h"
#include "engine/idTable/ExternalIdTableSorterMergeConfig.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/RowMajorIdTable.h"
#include "engine/idTable/RowMajorMergeBlock.h"
#include "global/RuntimeParameters.h"
#include "util/AsyncStream.h"
#include "util/CancellationHandle.h"
#include "util/CompressedBlockFile.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/File.h"
#include "util/GlobalExecutor.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/PostAndGetFuture.h"
#include "util/TransparentFunctors.h"
#include "util/Views.h"
#include "util/blockSort/BlockIndirectSort.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

namespace ad_utility {

namespace compressedExternalIdTable::detail {
template <typename B, typename R>
CPP_requires(HasPushBackRequires, requires(B& b, const R& r)(b.push_back(r)));

template <typename B, typename R>
CPP_concept HasPushBack = CPP_requires_ref(HasPushBackRequires, B, R);
}  // namespace compressedExternalIdTable::detail

using namespace ad_utility::memory_literals;

// The default size for compressed blocks in the following classes.
static constexpr ad_utility::MemorySize DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE =
    500_kB;

// A class that stores a sequence of `IdTable`s in a file. Each `IdTable` is
// compressed blockwise. Typically, the blocksize is much smaller than the size
// of a single `IdTable`, such that there are multiple blocks per `IdTable`.
// This is an important building block for an external merge sort implementation
// where we want very large pre-sorted `IdTables` over which we need to
// incrementally iterate (hence the smaller blocks for compression). These
// tables all have the same number of columns, so they can be thought of as
// large blocks of a very large `IdTable` which is formed by the concatenation
// of the single tables.
class CompressedExternalIdTableWriter {
 private:
  // Metadata for a compressed block of bytes. A block is a contiguous part of a
  // column of an `IdTable`.
  struct CompressedBlockMetadata {
    // The sizes are in Bytes.
    size_t compressedSize_;
    size_t uncompressedSize_;
    size_t offsetInFile_;
  };

  // The filename and actual file to which the `IdTable` is written .
  std::string filename_;
  // The offset at which the next block is written. The blocks are written
  // with the positioned `File::write`, so a thread only has to reserve its
  // range here, see `writeBytes`.
  std::atomic<off_t> nextOffset_{0};
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

  // For each block (indexed exactly like the `ColumnMetadata` of a single
  // column), the first and the last row of that block, stored as
  // `[first_0, last_0, first_1, ...]`. The block boundaries are identical for
  // all columns (see `writeIdTable`), so a single vector suffices. This
  // metadata is used by the merge phase (see
  // `util/parallelBlockMerge/ParallelBlockMerge.h`) to split the runs into
  // disjoint value ranges that can be merged independently. It lives in RAM
  // only and is never serialized, so adding it does not change the index
  // format.
  std::vector<IdTable::row_type> firstAndLastRowPerBlock_;

  ad_utility::AllocatorWithLimit<Id> allocator_;
  // Each column of each `IdTable` will be split up into blocks of this size and
  // then separately compressed and stored. Has to be chosen s.t. it is much
  // smaller than the size of the single `IdTables` and  large enough to make
  // the used compression algorithm work well.
  ad_utility::MemorySize blockSizeUncompressed_ =
      DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE;

  // How the blocks of the columns are compressed, see `compressAndWriteColumn`
  // and `decompressColumnInto`. `NO_BLOCK_COMPRESSION` stores them
  // uncompressed. This is a property of the whole file (and not of a single
  // block), so the reading side simply knows it and it is never stored.
  CompressedBlockFile::CompressionLevel compressionLevel_ = ZSTD_DEFAULT_LEVEL;

  // Keep track of the number of active output generators to detect whether we
  // are currently reading from the file and it is thus unsafe to add to the
  // contents.
  // NOTE: This is an atomic, because it is also decremented from the callbacks
  // that are attached to the lifetime of the generators, which typically run on
  // background threads.
  std::atomic<size_t> numActiveGenerators_{0};

 public:
  // Constructor. The file at `filename` will be overwritten. Each of the
  // `IdTables` that will be passed in has to have exactly `numCols` columns.
  explicit CompressedExternalIdTableWriter(
      std::string filename, size_t numCols,
      ad_utility::AllocatorWithLimit<Id> allocator,
      ad_utility::MemorySize blockSizeUncompressed =
          DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
      CompressedBlockFile::CompressionLevel compressionLevel =
          ZSTD_DEFAULT_LEVEL)
      : filename_{std::move(filename)},
        blocksPerColumn_(numCols),
        allocator_{std::move(allocator)},
        blockSizeUncompressed_(blockSizeUncompressed),
        compressionLevel_{compressionLevel} {}

  // Destructor. Deletes the stored file.
  ~CompressedExternalIdTableWriter() {
    file_.wlock()->close();
    ad_utility::deleteFile(filename_);
  }

  // Simple getters for the stored allocator and the number of columns;
  const auto& allocator() const { return allocator_; }
  size_t numColumns() const { return blocksPerColumn_.size(); }
  // The name of the file that the `IdTable`s are written to. Other temporary
  // files of the same sorter derive their names from it.
  const std::string& filename() const { return filename_; }
  const MemorySize& blockSizeUncompressed() const {
    return blockSizeUncompressed_;
  }

  // Store an `idTable`.
  void writeIdTable(const IdTable& table) {
    AD_CONTRACT_CHECK(table.numColumns() == numColumns());
    const BlockLayout layout = prepareWrite(
        table.numRows(),
        [&table](size_t row) { return IdTable::row_type{table[row]}; });
    // NOTE: The unit of parallelism is a single column of a single block, see
    // `compressAndWriteBlockOfColumn`.
    runTasksInParallel(layout.numBlocks_ * numColumns(), [this, &table, layout](
                                                             size_t taskIdx) {
      compressAndWriteBlockOfColumn(table, taskIdx / numColumns(),
                                    taskIdx % numColumns(), layout);
    });
  }

  // Store a row-major `table` (see `RowMajorIdTable.h`). The resulting blocks,
  // and hence the whole file, are exactly the same as for the column-major
  // `writeIdTable` above: each block is transposed into a scratch buffer right
  // before it is compressed, see `transposeCompressAndWriteBlock`.
  template <size_t NumCols>
  void writeRowMajorIdTable(const RowMajorIdTable<NumCols>& table) {
    AD_CONTRACT_CHECK(NumCols == numColumns());
    const BlockLayout layout =
        prepareWrite(table.numRows(), [&table](size_t row) {
          return rowMajorIdTable::toDynamicRow<NumCols>(table[row]);
        });
    // NOTE: In contrast to the column-major case, the unit of parallelism is a
    // whole block, because the transposition of a block has to happen for all
    // of its columns at once, see `transposeCompressAndWriteBlock`.
    runTasksInParallel(
        layout.numBlocks_, [this, &table, layout](size_t blockIdx) {
          transposeCompressAndWriteBlock(table, blockIdx, layout);
        });
  }

 private:
  // Where the blocks of the table that is currently being written live in the
  // `blocksPerColumn_`, how many of them there are, and how many rows each of
  // them has (the last one may have fewer).
  struct BlockLayout {
    size_t firstBlockIdx_;
    size_t numBlocks_;
    size_t blockSize_;
  };

  // Make room for the blocks of a new table with `numRows` rows and store the
  // first and the last row of each of them, which `getRow(rowIdx)` returns.
  // This is everything that the two `write...IdTable` functions above share;
  // the layout of the table itself only matters for the compression.
  //
  // NOTE: The first and the last row of a block have to be stored here and not
  // inside the per-column tasks of `writeIdTable`, because each of those tasks
  // only sees a single column.
  template <typename GetRow>
  BlockLayout prepareWrite(size_t numRows, const GetRow& getRow) {
    if (numActiveGenerators_ != 0) {
      AD_THROW(
          "Trying to call `writeIdTable` on an "
          "`CompressedExternalIdTableWriter` that is currently being iterated "
          "over");
    }
    size_t blockSize = blockSizeUncompressed_.getBytes() / sizeof(Id);
    AD_CONTRACT_CHECK(blockSize > 0);
    size_t firstBlockIdx = blocksPerColumn_.at(0).size();
    startOfSingleIdTables_.push_back(firstBlockIdx);
    for (size_t lower = 0; lower < numRows; lower += blockSize) {
      size_t upper = std::min(lower + blockSize, numRows);
      firstAndLastRowPerBlock_.push_back(getRow(lower));
      firstAndLastRowPerBlock_.push_back(getRow(upper - 1));
    }
    size_t numBlocks = (numRows + blockSize - 1) / blockSize;
    // Make room for the metadata of the new blocks. The tasks of the callers
    // then only *assign* to those elements (each task to an element of its
    // own), so that no synchronization is needed for the metadata.
    for (auto& blockMetadata : blocksPerColumn_) {
      blockMetadata.resize(firstBlockIdx + numBlocks);
    }
    return {firstBlockIdx, numBlocks, blockSize};
  }
  // Compress the part of the `columnIdx`-th column of the `table` that belongs
  // to the block with index `blockIdx` (counted relative to the `table`), write
  // it to the file, and store the resulting metadata. The `firstBlockIdx` is
  // the index that the first block of the `table` has in the `blocksPerColumn_`
  // (see `writeIdTable`).
  //
  // This function may be called concurrently for arbitrary combinations of
  // `blockIdx` and `columnIdx`: the file is written under an exclusive lock
  // (which is not a bottleneck, because it is only held for the `write` itself
  // and not for the compression), and the metadata of each block is stored in
  // an element of its own, which `writeIdTable` has allocated beforehand.
  void compressAndWriteBlockOfColumn(const IdTable& table, size_t blockIdx,
                                     size_t columnIdx,
                                     const BlockLayout& layout) {
    decltype(auto) column = table.getColumn(columnIdx);
    size_t lower = blockIdx * layout.blockSize_;
    size_t upper = std::min(lower + layout.blockSize_, column.size());
    AD_CORRECTNESS_CHECK(lower < upper);
    compressAndWriteColumn(column.data() + lower, upper - lower,
                           layout.firstBlockIdx_ + blockIdx, columnIdx);
  }

  // Transpose the block with the (table-relative) index `blockIdx` of the
  // row-major `table` into a scratch buffer and compress and write each of its
  // columns from there, see `compressAndWriteColumn`. The scratch buffer holds
  // a single block (`blockSize_` rows of `NumCols` columns), so at most one of
  // them per thread of the pool is alive at any time.
  template <size_t NumCols>
  void transposeCompressAndWriteBlock(const RowMajorIdTable<NumCols>& table,
                                      size_t blockIdx,
                                      const BlockLayout& layout) {
    size_t lower = blockIdx * layout.blockSize_;
    size_t upper = std::min(lower + layout.blockSize_, table.numRows());
    AD_CORRECTNESS_CHECK(lower < upper);
    size_t numRows = upper - lower;
    // The columns of the block, stored one after the other.
    ad_utility::UninitializedVector<Id> scratch(NumCols * numRows);
    rowMajorIdTable::ColumnPointers<NumCols> columns{};
    for (size_t columnIdx = 0; columnIdx < NumCols; ++columnIdx) {
      columns[columnIdx] = scratch.data() + columnIdx * numRows;
    }
    rowMajorIdTable::transposeToColumnMajor<NumCols>(table.data() + lower,
                                                     numRows, columns);
    for (size_t columnIdx = 0; columnIdx < NumCols; ++columnIdx) {
      compressAndWriteColumn(columns[columnIdx], numRows,
                             layout.firstBlockIdx_ + blockIdx, columnIdx);
    }
  }

  // Compress `numRows` contiguous `Id`s of the column `columnIdx` of the block
  // with the given global index, write them to the file, and store the
  // resulting metadata.
  void compressAndWriteColumn(const Id* column, size_t numRows,
                              size_t globalBlockIdx, size_t columnIdx) {
    auto uncompressedSize = numRows * sizeof(Id);
    auto writeBytes = [this](const void* data, size_t numBytes) {
      // Reserve a range of the file and then write to it with the positioned
      // `File::write`, which only needs a shared lock. Several threads
      // therefore write concurrently, instead of queueing for the exclusive
      // lock that a write at the shared file position would need. Only the
      // positioned `read` and `write` are used on this file.
      auto offset = nextOffset_.fetch_add(static_cast<off_t>(numBytes));
      auto numBytesWritten = file_.rlock()->write(data, numBytes, offset);
      AD_CORRECTNESS_CHECK(numBytesWritten == static_cast<ssize_t>(numBytes),
                           "Writing a block to a temporary file failed");
      return static_cast<size_t>(offset);
    };
    if (!compressionLevel_.has_value()) {
      blocksPerColumn_.at(columnIdx).at(globalBlockIdx) =
          CompressedBlockMetadata{uncompressedSize, uncompressedSize,
                                  writeBytes(column, uncompressedSize)};
      return;
    }
    auto compressed = ZstdWrapper::compress(column, uncompressedSize,
                                            compressionLevel_.value());
    blocksPerColumn_.at(columnIdx).at(globalBlockIdx) = CompressedBlockMetadata{
        compressed.size(), uncompressedSize,
        writeBytes(compressed.data(), compressed.size())};
  }

  // Run the `numTasks` tasks that compress the blocks of a single table and
  // write them to the file (`runTask(taskIdx)` runs a single one of them) on
  // the global thread pool (see `util/GlobalExecutor.h`). Return only when all
  // of them are done, rethrowing the first exception that any of them has
  // thrown.
  //
  // NOTE: For the column-major case the tasks are per block *and* column (and
  // not per column, as they used to be), because the columns of a single table
  // are typically few, while its blocks are many. The tasks are therefore small
  // enough to keep all the threads of the pool busy until the very end of the
  // table. They are also claimed in the order of the blocks (and within a
  // block, in the order of the columns), such that the parts that are later
  // read together (see `readBlockSequential`) tend to end up close to each
  // other in the file.
  //
  // NOTE: The calling thread doesn't only wait for the pool, but also works on
  // the blocks itself. That way this function makes progress even if all the
  // threads of the pool are currently busy, so that it can safely be called
  // from a thread that one of those threads is (indirectly) waiting for. This
  // is exactly the case for the dedicated thread on which the
  // `CompressedExternalIdTableBase` writes its blocks, see
  // `CompressedExternalIdTableBase::transformAndWriteBlock`.
  template <typename RunTask>
  void runTasksInParallel(size_t numTasks, const RunTask& runTask) {
    if (numTasks == 0) {
      return;
    }
    // The index of the next task that has not been claimed by a worker yet.
    std::atomic<size_t> nextTaskIdx = 0;
    std::mutex exceptionMutex;
    std::exception_ptr firstException;

    // Claim and run tasks until there are none left. Each task compresses and
    // writes a single block of a single column.
    auto worker = [&]() {
      try {
        while (true) {
          size_t taskIdx = nextTaskIdx.fetch_add(1);
          if (taskIdx >= numTasks) {
            return;
          }
          runTask(taskIdx);
        }
      } catch (...) {
        // Stop handing out tasks; the exception is rethrown by the caller as
        // soon as all the workers have finished.
        nextTaskIdx.store(numTasks);
        std::lock_guard lock{exceptionMutex};
        if (!firstException) {
          firstException = std::current_exception();
        }
      }
    };

    // Run the workers, one of them in the calling thread (see the NOTE above).
    size_t numWorkers =
        std::min(numTasks, ad_utility::globalExecutorNumThreads());
    std::vector<std::future<void>> workerFutures;
    workerFutures.reserve(numWorkers - 1);
    for ([[maybe_unused]] size_t i : ql::views::iota(size_t{1}, numWorkers)) {
      workerFutures.push_back(
          ad_utility::postAndGetFuture(ad_utility::globalExecutor(), worker));
    }
    worker();
    // NOTE: The `worker` never throws, so none of the `get()` calls does. We
    // therefore always wait for *all* the workers, which we have to, because
    // they use references to local variables of this function.
    for (auto& future : workerFutures) {
      future.get();
    }
    if (firstException) {
      std::rethrow_exception(firstException);
    }
  }

 public:
  // Return a vector of generators where the `i-th` generator generates the
  // `i-th` IdTable that was stored. The IdTables are yielded in (smaller)
  // blocks which are `IdTables` themselves.
  template <size_t N = 0>
  std::vector<InputRangeTypeErased<IdTableStatic<N>>> getAllGenerators() {
    file_.wlock()->flush();
    std::vector<InputRangeTypeErased<IdTableStatic<N>>> result;
    result.reserve(startOfSingleIdTables_.size());
    for (auto i : ql::views::iota(0u, startOfSingleIdTables_.size())) {
      result.push_back(makeGeneratorForIdTable<N>(i));
    }
    return result;
  }

  // The number of `IdTable`s (= presorted runs) that have been written.
  size_t numIdTables() const { return startOfSingleIdTables_.size(); }

  // The number of blocks of the `IdTable` with the given index.
  size_t numBlocksOfIdTable(size_t idTableIdx) const {
    return endBlockOfIdTable(idTableIdx) -
           startOfSingleIdTables_.at(idTableIdx);
  }

  // The number of rows in the given block of the given `IdTable`.
  size_t numRowsInBlock(size_t idTableIdx, size_t blockIdx) const {
    return numRowsInBlock(globalBlockIdx(idTableIdx, blockIdx));
  }

  // The first row of the given block of the given `IdTable`.
  const IdTable::row_type& firstRowOfBlock(size_t idTableIdx,
                                           size_t blockIdx) const {
    return firstAndLastRowPerBlock_.at(2 *
                                       globalBlockIdx(idTableIdx, blockIdx));
  }

  // The last row of the given block of the given `IdTable`.
  const IdTable::row_type& lastRowOfBlock(size_t idTableIdx,
                                          size_t blockIdx) const {
    return firstAndLastRowPerBlock_.at(
        2 * globalBlockIdx(idTableIdx, blockIdx) + 1);
  }

  // Read and decompress the given block of the given `IdTable`. Thread-safe:
  // the columns are decompressed sequentially, so that this can be called
  // concurrently from many threads without spawning threads of its own.
  template <size_t N = 0>
  IdTableStatic<N> readBlockOfIdTable(size_t idTableIdx,
                                      size_t blockIdx) const {
    return readBlockSequential<N>(globalBlockIdx(idTableIdx, blockIdx));
  }

  // Like `readBlockOfIdTable`, but return the block row-major (see
  // `RowMajorIdTable.h`). The block is stored column-major, so its columns are
  // decompressed into a scratch buffer, from which the block is then transposed
  // in one cache-friendly pass. Thread-safe for the same reason as
  // `readBlockOfIdTable`.
  template <size_t NumCols>
  RowMajorIdTable<NumCols> readBlockOfIdTableRowMajor(size_t idTableIdx,
                                                      size_t blockIdx) const {
    AD_CONTRACT_CHECK(NumCols == numColumns());
    size_t globalIdx = globalBlockIdx(idTableIdx, blockIdx);
    size_t numRows = numRowsInBlock(globalIdx);
    // The columns of the block, stored one after the other.
    ad_utility::UninitializedVector<Id> scratch(NumCols * numRows);
    rowMajorIdTable::ConstColumnPointers<NumCols> columns{};
    for (size_t columnIdx = 0; columnIdx < NumCols; ++columnIdx) {
      Id* column = scratch.data() + columnIdx * numRows;
      decompressColumnInto(globalIdx, columnIdx, column);
      columns[columnIdx] = column;
    }
    RowMajorIdTable<NumCols> block{allocator_};
    block.resize(numRows);
    rowMajorIdTable::transposeToRowMajor<NumCols>(columns, numRows,
                                                  block.data());
    return block;
  }

  // Register a reader that accesses the blocks directly (via
  // `readBlockOfIdTable`), such that the writer knows that it is currently
  // being read from, see `numActiveGenerators_`.
  void registerActiveReader() { ++numActiveGenerators_; }

  // Unregister a reader that was previously registered via
  // `registerActiveReader`.
  void unregisterActiveReader() noexcept { --numActiveGenerators_; }

  // Flush the underlying file, such that all written blocks become readable.
  void flush() { file_.wlock()->flush(); }

  // Clear the underlying file and completely reset the data structure s.t. it
  // can be reused.
  void clear() {
    if (numActiveGenerators_ > 0) {
      AD_THROW(
          "Trying to call `writeIdTable` on an "
          "`CompressedExternalIdTableWriter` that is currently being iterated "
          "over");
    }
    file_.wlock()->close();
    ad_utility::deleteFile(filename_);
    file_.wlock()->open(filename_, "w+");
    nextOffset_.store(0);
    ql::ranges::for_each(blocksPerColumn_, [](auto& block) { block.clear(); });
    startOfSingleIdTables_.clear();
    firstAndLastRowPerBlock_.clear();
  }

 private:
  // The total number of blocks that were written so far. All the columns are
  // split into blocks at exactly the same row boundaries, so the number of
  // blocks of the first column is the number of blocks of the whole writer.
  size_t numBlocks() const {
    return blocksPerColumn_.empty() ? 0 : blocksPerColumn_.at(0).size();
  }

  // The number of rows in the block with the given global index.
  size_t numRowsInBlock(size_t blockIdx) const {
    return blocksPerColumn_.at(0).at(blockIdx).uncompressedSize_ / sizeof(Id);
  }

  // The index (in the `ColumnMetadata` of a single column) of the first block
  // that does NOT belong to the `IdTable` with the given index anymore. This
  // mirrors the logic in `makeGeneratorForIdTable`.
  size_t endBlockOfIdTable(size_t idTableIdx) const {
    AD_CONTRACT_CHECK(idTableIdx < startOfSingleIdTables_.size());
    return idTableIdx + 1 < startOfSingleIdTables_.size()
               ? startOfSingleIdTables_.at(idTableIdx + 1)
               : numBlocks();
  }

  // Translate the index of a block that is local to the `IdTable` with the
  // given index into the corresponding global block index (which is the index
  // into the `ColumnMetadata` of a single column and, times two, into
  // `firstAndLastRowPerBlock_`).
  size_t globalBlockIdx(size_t idTableIdx, size_t blockIdx) const {
    size_t global = startOfSingleIdTables_.at(idTableIdx) + blockIdx;
    AD_CONTRACT_CHECK(global < endBlockOfIdTable(idTableIdx));
    return global;
  }

  // Get the block generator for a single IdTable, specified by the `index`.
  template <size_t NumCols = 0>
  InputRangeTypeErased<IdTableStatic<NumCols>> makeGeneratorForIdTable(
      size_t index) {
    size_t firstBlock = startOfSingleIdTables_.at(index);
    size_t lastBlock{index + 1 < startOfSingleIdTables_.size()
                         ? startOfSingleIdTables_.at(index + 1)
                         : blocksPerColumn_.at(0).size()};
    auto readBlocks = ql::views::iota(firstBlock, lastBlock) |
                      ql::views::transform([this](auto blockIdx) {
                        return this->template readBlock<NumCols>(blockIdx);
                      });
    ++numActiveGenerators_;
    auto callback = [this]() noexcept { --numActiveGenerators_; };
    using namespace ad_utility;
    return InputRangeTypeErased{CallbackOnEndView(
        bufferedAsyncView(std::move(readBlocks), 1), callback)};
  }

  // Read and decompress column `columnIdx` of the block at `blockIdx` into
  // `block`. This is the shared per-column kernel used by both `readBlock` and
  // `readBlockSequential`. May be called concurrently for distinct `columnIdx`
  // values on the same `block` (as done by `readBlock`), and also concurrently
  // for the same `columnIdx` from several threads, because it only takes a
  // shared lock on the file (`File::read` with an explicit offset is `pread`).
  template <size_t NumCols = 0>
  void decompressColumnIntoBlock(size_t blockIdx, size_t columnIdx,
                                 IdTableStatic<NumCols>& block) const {
    decltype(auto) col = block.getColumn(columnIdx);
    decompressColumnInto(blockIdx, columnIdx, col.data());
  }

  // Read and decompress column `columnIdx` of the block at `blockIdx` into
  // `target`, which has to have room for the whole column of that block (see
  // `numRowsInBlock`). This is the kernel that all the reading functions of
  // this class share; see `decompressColumnIntoBlock` for the thread safety.
  void decompressColumnInto(size_t blockIdx, size_t columnIdx,
                            Id* target) const {
    const auto& metaData = blocksPerColumn_.at(columnIdx).at(blockIdx);
    auto readBytes = [this, &metaData](void* buffer) {
      auto numBytesRead = file_.rlock()->read(buffer, metaData.compressedSize_,
                                              metaData.offsetInFile_);
      AD_CORRECTNESS_CHECK(numBytesRead >= 0 &&
                           static_cast<size_t>(numBytesRead) ==
                               metaData.compressedSize_);
    };
    if (!compressionLevel_.has_value()) {
      // NOTE: An uncompressed column is read straight into the `target`, so
      // this path needs neither an intermediate buffer nor a copy.
      AD_CORRECTNESS_CHECK(metaData.compressedSize_ ==
                           metaData.uncompressedSize_);
      readBytes(target);
      return;
    }
    std::vector<char> compressed(metaData.compressedSize_);
    readBytes(compressed.data());
    auto numBytesDecompressed =
        ZstdWrapper::decompressToBuffer(compressed.data(), compressed.size(),
                                        target, metaData.uncompressedSize_);
    AD_CORRECTNESS_CHECK(numBytesDecompressed == metaData.uncompressedSize_);
  }

  // Allocate and size an IdTableStatic for the block at `blockIdx`.
  template <size_t NumCols = 0>
  IdTableStatic<NumCols> makeBlock(size_t blockIdx) const {
    IdTableStatic<NumCols> block{numColumns(), allocator_};
    block.resize(numRowsInBlock(blockIdx));
    return block;
  }

  // Decompresses the block at the given `blockIdx`. The individual columns are
  // decompressed concurrently.
  template <size_t NumCols = 0>
  IdTableStatic<NumCols> readBlock(size_t blockIdx) const {
    auto block = makeBlock<NumCols>(blockIdx);
    std::vector<std::future<void>> readColumnFutures;
    for (auto i : ql::views::iota(0u, numColumns())) {
      readColumnFutures.push_back(
          std::async(std::launch::async, [this, i, blockIdx, &block]() {
            this->template decompressColumnIntoBlock<NumCols>(blockIdx, i,
                                                              block);
          }));
    }
    for (auto& fut : readColumnFutures) {
      fut.get();
    }
    return block;
  }

  // Like `readBlock`, but decompresses columns sequentially rather than in
  // parallel. This avoids per-block thread creation, making it suitable both
  // for use inside a single persistent background thread (e.g.
  // `runStreamAsync`) and for the many threads of the merge phase (see
  // `readBlockOfIdTable`).
  template <size_t NumCols = 0>
  IdTableStatic<NumCols> readBlockSequential(size_t blockIdx) const {
    auto block = makeBlock<NumCols>(blockIdx);
    for (auto i : ql::views::iota(0u, numColumns())) {
      decompressColumnIntoBlock<NumCols>(blockIdx, i, block);
    }
    return block;
  }

 public:
  // Read all blocks as a single `InputRangeTypeErased<IdTableStatic<N>>` via
  // one background thread. This creates a constant number of threads regardless
  // of the number of stored blocks. Columns are decompressed sequentially
  // within a block; the single background thread already provides concurrency
  // with the consumer.
  //
  // TODO<joka921> This function is only used by the unused
  // `CompressedExternalIdTable`. Remove it together with that class.
  template <size_t N = 0>
  InputRangeTypeErased<IdTableStatic<N>> getBlockStream() {
    file_.wlock()->flush();
    CachingTransformInputRange readBlocks{
        ql::views::iota(size_t{0}, numBlocks()), [this](size_t blockIdx) {
          return this->template readBlockSequential<N>(blockIdx);
        }};
    ++numActiveGenerators_;
    auto callback = [this]() noexcept { --numActiveGenerators_; };
    // Queue size 2 keeps the producer one block ahead of the consumer.
    return ad_utility::streams::runStreamAsync(
        CallbackOnEndView{std::move(readBlocks), std::move(callback)}, 2);
  }
};

// A callback that pushes complete blocks (instead of single rows) into a
// `CompressedExternalIdTableBase` (see `makePushBlockCallback` below). This is
// a named type and not a lambda, such that callers that accept both per-row
// and per-block callbacks can tell the two apart (see e.g. `liftCallback` in
// `IndexImpl.cpp`).
template <typename Table>
struct PushBlockCallback {
  Table* table_;

  template <typename Block>
  void operator()(const Block& block) const {
    table_->pushBlock(block);
  }
};

// The conversion between the memory limit and the number of rows per block of
// a `CompressedExternalIdTableBase` (see below). These functions have rather
// general names, but are tied to that class, hence the dedicated namespace.
namespace compressedExternalIdTable {

// The amount of memory that a `CompressedExternalIdTableBase` with
// `numColumns` columns requires per row of its block size. The factor of two is
// there because we store two blocks at the same time: One that is currently
// being sorted and written to disk in the background, and one that is used to
// collect rows in the calls to `push`.
inline size_t blockMemoryPerRow(size_t numColumns) {
  return numColumns * sizeof(Id) * 2;
}

// The number of rows per block that a `CompressedExternalIdTableBase` with
// `numColumns` columns uses for the given `memory` limit.
inline size_t blocksizeForMemory(MemorySize memory, size_t numColumns) {
  return memory.getBytes() / blockMemoryPerRow(numColumns);
}

// The inverse of `blocksizeForMemory`: the memory limit for which a
// `CompressedExternalIdTableBase` with `numColumns` columns uses exactly
// `blocksize` rows per block.
inline MemorySize memoryForBlocksize(size_t blocksize, size_t numColumns) {
  return MemorySize::bytes(blocksize * blockMemoryPerRow(numColumns));
}

// Whether the transformation of a single block (for the
// `CompressedExternalIdTableSorter` this is the sort of that block) may use
// more than one thread, see `CompressedExternalIdTableBase::transformBlock`.
enum struct Parallelism { Allowed, Disallowed };

// The number of rows below which the last (and typically incomplete) block of
// the input phase is transformed in the calling thread and with a single
// thread, instead of being handed to the dedicated background thread, see
// `CompressedExternalIdTableBase::transformAndPushLastBlock`. For a block that
// small, the thread hop and the setup of a parallel sort cost more than they
// buy, especially as the caller immediately waits for the result anyway.
constexpr inline size_t MAX_ROWS_FOR_SEQUENTIAL_LAST_BLOCK = 100'000;

// The largest number of columns for which the row-major mode (see
// `SortBlockBuffer` below) is available if the number of columns is only known
// at runtime. It is the maximum of the `callFixedSize` mechanism (see
// `CallFixedSize.h`), which is what turns such a runtime number into the
// compile-time number of columns of a `RowMajorIdTable`.
constexpr inline int MAX_NUM_COLUMNS_ROW_MAJOR =
    DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE;

// Whether the external sorters store the rows of a block row-major, see
// `SortBlockBuffer` below. Every sorter reads this once, when it is
// constructed, so changing the underlying runtime parameter only affects the
// sorters that are created afterwards.
inline bool rowMajorModeIsEnabled() {
  return getRuntimeParameter<&RuntimeParameters::externalSorterRowMajor_>();
}

// Whether the row-major mode is available for a table with `NumStaticCols`
// statically known and `numColumns` actual columns. A number of columns that is
// only known at runtime has to be within the reach of `callFixedSize`, see
// `MAX_NUM_COLUMNS_ROW_MAJOR`.
template <size_t NumStaticCols>
bool rowMajorModeIsSupported(size_t numColumns) {
  if (NumStaticCols > 0) {
    return numColumns == NumStaticCols;
  }
  return numColumns > 0 &&
         numColumns <= static_cast<size_t>(MAX_NUM_COLUMNS_ROW_MAJOR);
}

namespace detail {

// The `std::variant` of the `RowMajorIdTable`s that a sorter with
// `NumStaticCols` statically known columns may use: exactly one alternative if
// that number is known at compile time, and one per supported number of columns
// otherwise, see `MAX_NUM_COLUMNS_ROW_MAJOR`.
template <typename Sequence>
struct RowMajorTableVariantFromSequence;

// ___________________________________________________________________________
template <size_t... Is>
struct RowMajorTableVariantFromSequence<std::index_sequence<Is...>> {
  using type = std::variant<RowMajorIdTable<Is + 1>...>;
};

// ___________________________________________________________________________
template <size_t NumStaticCols>
struct RowMajorTableVariant {
  using type = std::variant<RowMajorIdTable<NumStaticCols>>;
};

// ___________________________________________________________________________
template <>
struct RowMajorTableVariant<0> {
  using type = typename RowMajorTableVariantFromSequence<
      std::make_index_sequence<MAX_NUM_COLUMNS_ROW_MAJOR>>::type;
};

// ___________________________________________________________________________
template <size_t NumStaticCols>
using RowMajorTableVariantT =
    typename RowMajorTableVariant<NumStaticCols>::type;

// Create the alternative of the variant that matches the `numColumns`, which
// `rowMajorModeIsSupported` has to have accepted.
template <size_t NumStaticCols>
RowMajorTableVariantT<NumStaticCols> makeRowMajorTable(
    size_t numColumns, const AllocatorWithLimit<Id>& allocator) {
  using Variant = RowMajorTableVariantT<NumStaticCols>;
  AD_CONTRACT_CHECK(rowMajorModeIsSupported<NumStaticCols>(numColumns));
  if constexpr (NumStaticCols > 0) {
    return Variant{RowMajorIdTable<NumStaticCols>{allocator}};
  } else {
    return ad_utility::callFixedSizeVi<MAX_NUM_COLUMNS_ROW_MAJOR>(
        static_cast<int>(numColumns),
        [&allocator](auto numColumnsVi) -> Variant {
          constexpr size_t N =
              static_cast<size_t>(decltype(numColumnsVi)::value);
          if constexpr (N == 0) {
            // `callFixedSize` maps a number of columns that is out of its reach
            // to zero, which the check above has already excluded.
            AD_FAIL();
          } else {
            return Variant{RowMajorIdTable<N>{allocator}};
          }
        });
  }
}

// Convert a table with `I` statically known columns to a table with `N`
// statically known columns, where `N` is either `I` itself or `0` (meaning that
// the number of columns is only known at runtime). This is a no-op in the
// former and a cheap move of the columns in the latter case.
template <size_t N, int I>
IdTableStatic<N> toOutputTable(IdTableStatic<I> table) {
  if constexpr (static_cast<int>(N) == I) {
    return table;
  } else {
    static_assert(N == 0);
    return std::move(table).toDynamic();
  }
}

}  // namespace detail

}  // namespace compressedExternalIdTable

// The in-memory buffer in which a `CompressedExternalIdTableBase` (see below)
// collects the rows of a single block, sorts them, and hands them to its
// `CompressedExternalIdTableWriter`.
//
// It stores those rows in one of two layouts, which is decided once, when the
// buffer is created:
// * Column-major (in an `IdTableStatic`), which is the layout of both the input
//   and the file, so that neither `pushBlock` nor the writing of a block has to
//   touch the data at all.
// * Row-major (in a `RowMajorIdTable`), which makes the *sorting* of a block
//   much cheaper, because a row is then a single dense `std::array` instead of
//   one `Id` in each of `numColumns` far apart columns. The price is one
//   transposition when the rows come in (in `push` and `insertAtEnd`) and one
//   when they are written (per compressed block, see
//   `CompressedExternalIdTableWriter::writeRowMajorIdTable`), so that the file
//   is exactly the same in both layouts.
//
// Which of the two is used is a runtime decision (see `rowMajorModeIsEnabled`),
// so both alternatives are members of this class and exactly one of them is
// engaged. The row-major one additionally is a `std::variant`, because the
// number of columns of a `RowMajorIdTable` is a compile-time constant, which
// for a table with a dynamic number of columns is obtained via `callFixedSize`,
// see `compressedExternalIdTable::detail::makeRowMajorTable`.
template <size_t NumStaticCols>
class SortBlockBuffer {
 public:
  using ColumnMajor = IdTableStatic<NumStaticCols>;

 private:
  using RowMajorVariant =
      compressedExternalIdTable::detail::RowMajorTableVariantT<NumStaticCols>;

  AllocatorWithLimit<Id> allocator_;
  size_t numColumns_;
  // Exactly one of the two is engaged, see the class comment above.
  std::optional<ColumnMajor> columnMajor_;
  std::optional<RowMajorVariant> rowMajor_;

 public:
  // Construct an empty buffer. If `rowMajor` is true (which requires
  // `compressedExternalIdTable::rowMajorModeIsSupported`), then the rows are
  // stored row-major.
  SortBlockBuffer(size_t numColumns, AllocatorWithLimit<Id> allocator,
                  bool rowMajor)
      : allocator_{std::move(allocator)}, numColumns_{numColumns} {
    if (rowMajor) {
      rowMajor_ =
          compressedExternalIdTable::detail::makeRowMajorTable<NumStaticCols>(
              numColumns_, allocator_);
    } else {
      columnMajor_.emplace(numColumns_, allocator_);
    }
  }

  // Simple getters.
  bool isRowMajor() const { return rowMajor_.has_value(); }
  size_t numColumns() const { return numColumns_; }
  const AllocatorWithLimit<Id>& allocator() const { return allocator_; }

  // Call the `function` with the underlying table, which is either the
  // column-major `IdTableStatic` or one of the `RowMajorIdTable` alternatives.
  // This is how the transformation of a block (for the sorter: the sort)
  // reaches the rows, see `BlockSorter`.
  template <typename F>
  decltype(auto) visit(F&& function) {
    if (columnMajor_.has_value()) {
      return function(columnMajor_.value());
    }
    return std::visit(AD_FWD(function), rowMajor_.value());
  }

  // The number of rows.
  size_t numRows() const {
    if (columnMajor_.has_value()) {
      return columnMajor_.value().numRows();
    }
    return std::visit([](const auto& table) { return table.numRows(); },
                      rowMajor_.value());
  }
  size_t size() const { return numRows(); }
  bool empty() const { return numRows() == 0; }

  // Remove all the rows, but keep the memory that is already allocated.
  void clear() {
    visit([](auto& table) { table.clear(); });
  }

  // Make room for `numRows` rows.
  void reserve(size_t numRows) {
    visit([numRows](auto& table) { table.reserve(numRows); });
  }

  // Make this buffer hold exactly `numRows` rows. Rows that are added by this
  // are not initialized (the underlying storage uses a
  // `default_init_allocator`), and rows that are removed by it keep their
  // memory, so that growing to the same size again is free. This is what makes
  // the concurrent filling of a buffer possible, see
  // `CompressedExternalIdTableBase::pushBlockConcurrently`.
  void resize(size_t numRows) {
    visit([numRows](auto& table) { table.resize(numRows); });
  }

  // Copy the rows `[beginRow, endRow)` of the column-major `table` into the
  // already existing rows of this buffer that start at `targetRow`,
  // transposing them if this buffer is row-major. In contrast to `insertAtEnd`
  // below this doesn't change the size of this buffer, so several such copies
  // into disjoint target ranges may run concurrently, see
  // `CompressedExternalIdTableBase::pushBlockConcurrently`.
  CPP_template(typename Table)(requires IdTableLike<Table>) void insertAt(
      const Table& table, size_t beginRow, size_t endRow, size_t targetRow) {
    AD_CONTRACT_CHECK(beginRow <= endRow && endRow <= table.numRows());
    const size_t numNewRows = endRow - beginRow;
    AD_CONTRACT_CHECK(targetRow + numNewRows <= numRows());
    if (columnMajor_.has_value()) {
      auto& target = columnMajor_.value();
      for (size_t col = 0; col < numColumns_; ++col) {
        auto source = table.getColumn(col).subspan(beginRow, numNewRows);
        auto destination = target.getColumn(col).subspan(targetRow, numNewRows);
        ql::ranges::copy(source, destination.begin());
      }
      return;
    }
    std::visit(
        [&table, beginRow, endRow, targetRow](auto& rows) {
          rows.writeTransposedAt(table, beginRow, endRow, targetRow);
        },
        rowMajor_.value());
  }

  // Append a single row, which may be anything that can be `push_back`ed to an
  // `IdTable` (in particular a row reference of one).
  template <typename R>
  void push_back(const R& row) {
    visit([&row](auto& table) { table.push_back(row); });
  }

  // Append the rows `[beginRow, endRow)` of the column-major `table`,
  // transposing them if this buffer is row-major.
  CPP_template(typename Table)(requires IdTableLike<Table>) void insertAtEnd(
      const Table& table, size_t beginRow, size_t endRow) {
    if (columnMajor_.has_value()) {
      columnMajor_.value().insertAtEnd(table, beginRow, endRow);
      return;
    }
    std::visit(
        [&table, beginRow, endRow](auto& rows) {
          rows.appendTransposed(table, beginRow, endRow);
        },
        rowMajor_.value());
  }

  // Write the contents of this buffer to the `writer` and clear it afterwards,
  // keeping its memory for the next block. The file is column-major in both
  // cases, see `CompressedExternalIdTableWriter::writeRowMajorIdTable`.
  void writeToAndClear(CompressedExternalIdTableWriter& writer) {
    if (columnMajor_.has_value()) {
      // NOTE: The round trip via the dynamic table moves the columns and
      // therefore keeps their memory, and so does the `clear()`, so that the
      // buffer still has the capacity that the next block needs.
      IdTable dynamicBlock = std::move(columnMajor_).value().toDynamic();
      writer.writeIdTable(dynamicBlock);
      dynamicBlock.clear();
      columnMajor_.emplace(
          std::move(dynamicBlock)
              .template toStatic<static_cast<int>(NumStaticCols)>());
      return;
    }
    std::visit(
        [&writer](auto& rows) {
          writer.writeRowMajorIdTable(rows);
          rows.clear();
        },
        rowMajor_.value());
  }

  // Return the contents of this buffer as a column-major table, transposing
  // them if this buffer is row-major. This is only needed for the inputs that
  // are so small that they never reach the file at all, see
  // `CompressedExternalIdTableSorter::sortedBlocks`.
  ColumnMajor extractColumnMajor() && {
    if (columnMajor_.has_value()) {
      return std::move(columnMajor_).value();
    }
    return copyToColumnMajor();
  }

  // Like `extractColumnMajor`, but leave this buffer untouched.
  ColumnMajor copyToColumnMajor() const {
    if (columnMajor_.has_value()) {
      return columnMajor_.value().clone();
    }
    const auto& allocator = allocator_;
    return std::visit(
        [&allocator](const auto& rows) -> ColumnMajor {
          return compressedExternalIdTable::detail::toOutputTable<
              NumStaticCols>(rows.toColumnMajor(allocator));
        },
        rowMajor_.value());
  }

  // Append the rows `[beginRow, endRow)` of this buffer to the column-major
  // `target`.
  void appendRowsTo(ColumnMajor& target, size_t beginRow, size_t endRow) const {
    if (columnMajor_.has_value()) {
      target.insertAtEnd(columnMajor_.value(), beginRow, endRow);
      return;
    }
    std::visit(
        [&target, beginRow, endRow](const auto& rows) {
          rows.appendToColumnMajor(target, beginRow, endRow);
        },
        rowMajor_.value());
  }
};

// An input policy for `ad_utility::parallelBlockMerge` that reads the blocks of
// the `IdTable`s (= presorted runs) stored in a
// `CompressedExternalIdTableWriter`. The `Element` is a dynamic, owning `Row`,
// which can be compared against the (proxy) row references of an
// `IdTableStatic<NumStaticCols>` because all comparators used in QLever are
// templated on both of their argument types.
//
// The `BlockType` decides in which layout the merge sees the blocks. It is
// either the column-major `IdTableStatic` (the default) or the
// `RowMajorMergeBlock`, which the merge reads *and* writes row-major, so that
// every row that is merged touches a single cache line instead of one per
// column. The runs themselves are stored column-major in both cases, so in the
// row-major case a block is transposed when it is read, see
// `CompressedExternalIdTableWriter::readBlockOfIdTableRowMajor`.
//
// The class registers itself as an active reader of the `writer` for its whole
// lifetime (see `CompressedExternalIdTableWriter::registerActiveReader`), such
// that writing to the `writer` while a merge is running correctly throws.
template <size_t NumStaticCols,
          typename BlockType = IdTableStatic<NumStaticCols>>
class CompressedIdTableRunsInput : public ad_utility::NoCopy {
 public:
  using Block = BlockType;
  using Element = IdTable::row_type;
  using value_type = typename Block::value_type;
  // Whether the blocks of the merge are row-major, see the class comment above.
  static constexpr bool isRowMajor =
      !std::is_same_v<Block, IdTableStatic<NumStaticCols>>;

 private:
  // The `writer` that stores the runs. It is `nullptr` if and only if this
  // object was moved from.
  CompressedExternalIdTableWriter* writer_;

 public:
  // Construct from the `writer`, which has to outlive this object. Flush the
  // `writer`, such that all blocks that were written so far can be read again.
  explicit CompressedIdTableRunsInput(CompressedExternalIdTableWriter& writer)
      : writer_{&writer} {
    writer_->flush();
    writer_->registerActiveReader();
  }

  // The class owns the registration as an active reader, so it must not be
  // copied (hence the `NoCopy` base class). It has to be movable, because
  // `parallelBlockMergeToRange` takes its input by value, and the move has to
  // be written by hand, because it has to reset the source.
  CompressedIdTableRunsInput(CompressedIdTableRunsInput&& other) noexcept
      : writer_{std::exchange(other.writer_, nullptr)} {}
  CompressedIdTableRunsInput& operator=(
      CompressedIdTableRunsInput&& other) noexcept {
    std::swap(writer_, other.writer_);
    return *this;
  }

  // ________________________________________________________________________
  ~CompressedIdTableRunsInput() {
    if (writer_ != nullptr) {
      writer_->unregisterActiveReader();
    }
  }

  // ________________________________________________________________________
  size_t numRuns() const { return writer_->numIdTables(); }

  // ________________________________________________________________________
  size_t numBlocks(size_t run) const {
    return writer_->numBlocksOfIdTable(run);
  }

  // ________________________________________________________________________
  size_t numElementsInBlock(size_t run, size_t block) const {
    return writer_->numRowsInBlock(run, block);
  }

  // ________________________________________________________________________
  const Element& firstElement(size_t run, size_t block) const {
    return writer_->firstRowOfBlock(run, block);
  }

  // ________________________________________________________________________
  const Element& lastElement(size_t run, size_t block) const {
    return writer_->lastRowOfBlock(run, block);
  }

  // Read and decompress a single block. This is the only function that performs
  // I/O; it is thread-safe, because it only takes a shared lock on the
  // underlying file.
  Block getBlock(size_t run, size_t block) const {
    if constexpr (isRowMajor) {
      return Block{writer_->template readBlockOfIdTableRowMajor<NumStaticCols>(
          run, block)};
    } else {
      return writer_->template readBlockOfIdTable<NumStaticCols>(run, block);
    }
  }

  // ________________________________________________________________________
  Block makeEmptyBlock() const {
    if constexpr (isRowMajor) {
      return Block{writer_->allocator()};
    } else {
      return Block{writer_->numColumns(), writer_->allocator()};
    }
  }

  // ________________________________________________________________________
  template <typename R>
  void appendToBlock(Block& block, R&& row) const {
    block.push_back(row);
  }

  // The memory of a single row, which is one `Id` per column.
  template <typename R>
  MemorySize memorySizeOfElement([[maybe_unused]] const R& row) const {
    return MemorySize::bytes(writer_->numColumns() * sizeof(Id));
  }
};

// Make a mismatch with the `InputConcept` a clear compile error.
static_assert(parallelBlockMerge::InputConcept<CompressedIdTableRunsInput<0>>);
static_assert(parallelBlockMerge::InputConcept<CompressedIdTableRunsInput<3>>);
static_assert(parallelBlockMerge::InputConcept<
              CompressedIdTableRunsInput<3, RowMajorMergeBlock<3>>>);

// The common base implementation of `CompressedExternalIdTable` and
// `CompressedExternalIdTableSorter` (see below). It is implemented as a mixin
// class.
CPP_class_template(size_t NumStaticCols,
                   typename BlockTransformation = ad_utility::Noop)(requires(
    ql::concepts::invocable<
        BlockTransformation,
        SortBlockBuffer<NumStaticCols>&>)) class CompressedExternalIdTableBase {
 public:
  using value_type = typename IdTableStatic<NumStaticCols>::row_type;
  using reference = typename IdTableStatic<NumStaticCols>::row_reference;
  using const_reference =
      typename IdTableStatic<NumStaticCols>::const_row_reference;
  using MemorySize = ad_utility::MemorySize;
  // The buffer in which the rows of the next block are aggregated, and which
  // also decides whether they are stored column-major or row-major, see
  // `SortBlockBuffer`.
  using Buffer = SortBlockBuffer<NumStaticCols>;

 protected:
  // Used to aggregate rows for the next block.
  Buffer currentBlock_;
  // For statistical reasons
  size_t numElementsPushed_ = 0;
  size_t numBlocksPushed_ = 0;
  // The number of columns of the `IdTable`. Might be different
  // from `NumStaticCols` when dynamic tables (NumStaticCols == 0) are used;
  size_t numColumns_;

  // The maximum amount of memory that this class can use.
  MemorySize memory_;

  // The number of rows per block in the first phase.
  size_t blocksize_{
      compressedExternalIdTable::blocksizeForMemory(memory_, numColumns_)};
  CompressedExternalIdTableWriter writer_;

  // The dedicated thread on which the blocks are transformed (for the
  // `CompressedExternalIdTableSorter` this means: sorted), compressed, and
  // written to the `writer_` in the background, see `transformAndWriteBlock`.
  // A single thread suffices, because there is always at most one such task in
  // flight: `transformAndWriteBlock` waits for the previous one before it posts
  // the next one.
  //
  // NOTE: The pool is declared before the `compressAndWriteFuture_`, such that
  // it is destroyed (and its thread joined) only after that future is gone.
  // The destructor additionally waits for the task explicitly, see there.
  boost::asio::thread_pool blockWritePool_{1};

  // NOTE: The background task hands the block buffer that it is done with back
  // via this future, so that the next block can reuse its memory instead of
  // allocating (and faulting in) a buffer of its own, see
  // `transformAndWriteBlock`.
  std::future<Buffer> compressAndWriteFuture_;

  // If the `compressAndWriteFuture_` is currently active, wait for its
  // computation to be completed and return the block buffer that the background
  // task has given back (empty, but with its memory still allocated). Else do
  // nothing and return `std::nullopt`.
  std::optional<Buffer> waitForFuture() {
    if (compressAndWriteFuture_.valid()) {
      return compressAndWriteFuture_.get();
    }
    return std::nullopt;
  }

  // Store the `future` inside the `compressAndWriteFuture_`. This trivial
  // wrapper can be used to inject more detailed logging when analyzing the
  // control flow of this class or when fixing bugs.
  void setFuture(std::future<Buffer> future) {
    AD_CORRECTNESS_CHECK(!compressAndWriteFuture_.valid());
    compressAndWriteFuture_ = std::move(future);
  }

  // Run the `function` on the `blockWritePool_` and wait for its completion,
  // rethrowing the exception that it has thrown (if any). Use this for work
  // that logically belongs to the background task of `transformAndWriteBlock`,
  // but has to be finished before the calling function returns. Running it on
  // the dedicated thread instead of simply calling it here guarantees that
  // *all* parallel invocations of the `blockTransformation_` (for the
  // `CompressedExternalIdTableSorter` this is the parallel sort of a block) are
  // made from one and the same thread. The strictly sequential invocations of
  // `transformAndPushLastBlock` are exempt from that, see there.
  template <typename Function>
  void runOnBlockWriteThreadAndWait(Function function) {
    ad_utility::postAndGetFuture(blockWritePool_.get_executor(),
                                 std::move(function))
        .get();
  }

  // The state of the concurrent pushing of blocks, see
  // `pushBlockConcurrently`. While that mode is active, the `currentBlock_` is
  // resized to a complete block up front, and `numRowsReserved_` (and not the
  // size of the buffer) is the number of rows that have actually been pushed.
  // All three members are protected by the `concurrentPushMutex_`, and the
  // condition variable is notified whenever one of them changes.
  std::mutex concurrentPushMutex_;
  std::condition_variable concurrentPushCv_;
  bool blockIsResizedForConcurrentPush_ = false;
  // The number of rows of the `currentBlock_` that have been handed out to
  // pushers, which is also the position at which the next pusher may copy.
  size_t numRowsReserved_ = 0;
  // The number of pushers that currently copy their rows into the
  // `currentBlock_`. The block may only be written (and the mode may only be
  // left) once this has dropped to zero.
  size_t numOutstandingCopies_ = 0;

  // Flag that is `true` if this is the first iteration over the table, and
  // `false` if there has already been a previous iteration.
  std::atomic<bool> isFirstIteration_ = true;

  // Flag used for correctness checking that `transformAndPushLastBlock` is only
  // called once.
  std::atomic<bool> transformAndPushWasCalled_ = false;

  [[no_unique_address]] BlockTransformation blockTransformation_{};

 public:
  // The destructor must wait for any pending async task before members are
  // destroyed. Without this, `blockTransformation_` (declared after
  // `compressAndWriteFuture_`) is destroyed first, and the still-running
  // async thread accesses freed memory via `this->blockTransformation_`.
  ~CompressedExternalIdTableBase() { waitForFuture(); }

  explicit CompressedExternalIdTableBase(
      std::string filename, size_t numCols, ad_utility::MemorySize memory,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
      BlockTransformation blockTransformation = {})
      : currentBlock_{numCols, allocator,
                      compressedExternalIdTable::rowMajorModeIsEnabled() &&
                          compressedExternalIdTable::rowMajorModeIsSupported<
                              NumStaticCols>(numCols)},
        numColumns_{numCols},
        memory_{memory},
        writer_{std::move(filename), numCols, allocator, blocksizeCompression,
                compressedExternalIdTable::sorterCompressionLevels()
                    .presortedRuns_},
        blockTransformation_{blockTransformation} {
    this->currentBlock_.reserve(blocksize_);
    AD_CONTRACT_CHECK(NumStaticCols == 0 || NumStaticCols == numCols);
  }
  // Add a single row to the input. The type of `row` needs to be something that
  // can be `push_back`ed to a `IdTable`.
  CPP_template(typename R)(
      requires compressedExternalIdTable::detail::HasPushBack<
          decltype(currentBlock_), R>) void push(const R& row) {
    finishConcurrentPushes();
    ++numElementsPushed_;
    currentBlock_.push_back(row);
    if (currentBlock_.size() >= blocksize_) {
      writeCurrentBlockAndRecycleBuffer();
    }
  }

  // Add all rows of the `table` (which has to be some kind of `IdTable`) to
  // the input. This is much more efficient than calling `push` for each of the
  // rows, because the `IdTable`s are stored column-based: Each column of the
  // `table` is copied contiguously into the corresponding column of the
  // internal buffer, instead of scattering each single row over all the
  // columns. The `table` may be arbitrarily large, it is automatically split
  // into blocks. The resulting blocks are exactly the same as if `push` had
  // been called for each row individually.
  CPP_template(typename Table)(requires IdTableLike<Table>) void pushBlock(
      const Table& table) {
    finishConcurrentPushes();
    AD_CONTRACT_CHECK(table.numColumns() == numColumns_);
    const size_t numRows = table.numRows();
    numElementsPushed_ += numRows;
    size_t numPushed = 0;
    while (numPushed < numRows) {
      // Note: `blocksize_` may be zero for very small memory limits (which
      // only happens in unit tests), so we always insert at least one row to
      // guarantee progress.
      size_t remainingSpace = blocksize_ > currentBlock_.numRows()
                                  ? blocksize_ - currentBlock_.numRows()
                                  : 1;
      size_t numToPush = std::min(remainingSpace, numRows - numPushed);
      currentBlock_.insertAtEnd(table, numPushed, numPushed + numToPush);
      numPushed += numToPush;
      if (currentBlock_.numRows() >= blocksize_) {
        writeCurrentBlockAndRecycleBuffer();
      }
    }
  }

  // A variant of `pushBlock` above that may be called concurrently from
  // several threads. Its purpose is that the copying of the rows (which for
  // large blocks is by far the most expensive part of a push) happens in
  // parallel instead of being serialized by a lock.
  //
  // This works as follows: The `currentBlock_` is resized to a complete block
  // up front, so that a pusher only has to *reserve* the range of rows into
  // which it then copies. The reserving is what the lock is held for; it is a
  // handful of integer operations, so the next pusher can start its copy
  // almost immediately. The only point at which the pushers have to be
  // synchronized is when a block is full and has to be handed to the sorting
  // machinery: the pusher that fills the block up waits for all the copies
  // that are still outstanding before it writes it.
  //
  // NOTE: The rows of a single `table` are not necessarily contiguous in the
  // resulting blocks, and the blocks contain the rows of the concurrent pushes
  // in an arbitrary order. Only use this for inputs that are sorted (or
  // otherwise reordered) afterwards anyway, which for the
  // `CompressedExternalIdTableSorter` is always the case.
  CPP_template(typename Table)(
      requires IdTableLike<Table>) void pushBlockConcurrently(const Table&
                                                                  table) {
    AD_CONTRACT_CHECK(table.numColumns() == numColumns_);
    const size_t numRows = table.numRows();
    size_t numPushed = 0;
    while (numPushed < numRows) {
      size_t targetRow = 0;
      size_t numToPush = 0;
      {
        std::unique_lock lock{concurrentPushMutex_};
        prepareBlockForConcurrentPush(lock);
        targetRow = numRowsReserved_;
        numToPush = std::min(concurrentPushBlocksize() - targetRow,
                             numRows - numPushed);
        numRowsReserved_ += numToPush;
        numElementsPushed_ += numToPush;
        ++numOutstandingCopies_;
      }
      // NOTE: This is the expensive part, and it deliberately runs without the
      // lock being held, so that it runs concurrently with the copies of the
      // other pushers.
      currentBlock_.insertAt(table, numPushed, numPushed + numToPush,
                             targetRow);
      numPushed += numToPush;
      {
        std::lock_guard lock{concurrentPushMutex_};
        AD_CORRECTNESS_CHECK(numOutstandingCopies_ > 0);
        --numOutstandingCopies_;
      }
      concurrentPushCv_.notify_all();
    }
  }

  // Leave the mode of `pushBlockConcurrently` above, if it is currently
  // active: Wait for all outstanding copies and shrink the `currentBlock_`
  // back to the rows that were actually pushed, so that all the other
  // functions of this class see the buffer in its normal state again. This is
  // called by all those functions, so that `pushBlockConcurrently` may be
  // freely mixed with them.
  void finishConcurrentPushes() {
    std::unique_lock lock{concurrentPushMutex_};
    if (!blockIsResizedForConcurrentPush_) {
      return;
    }
    concurrentPushCv_.wait(lock,
                           [this]() { return numOutstandingCopies_ == 0; });
    currentBlock_.resize(numRowsReserved_);
    blockIsResizedForConcurrentPush_ = false;
    numRowsReserved_ = 0;
  }

  // ___________________________________________________________________
  size_t size() const { return numElementsPushed_; }

  // Return a lambda that takes a `ValueType` and calls `push` for that value.
  auto makePushCallback() {
    return [self = this](auto&& value) { self->push(AD_FWD(value)); };
  }

  // Return a callback that takes a complete block and calls `pushBlock` for
  // it. Prefer this over `makePushCallback` above whenever complete blocks are
  // available, because pushing a complete block is much more efficient than
  // pushing its rows one by one (see `pushBlock` above).
  PushBlockCallback<CompressedExternalIdTableBase> makePushBlockCallback() {
    return {this};
  }

  // Delete the underlying file and reset the sorter. May only be called if no
  // active `getBlocks()` generator that has not been fully iterated over is
  // currently active, else an exception is thrown by the underlying
  // `CompressedExternalIdTableWriter`.
  void clear() {
    finishConcurrentPushes();
    resetCurrentBlock(false);
    numElementsPushed_ = 0;
    waitForFuture();
    writer_.clear();
    numBlocksPushed_ = 0;
    isFirstIteration_ = true;
    transformAndPushWasCalled_ = false;
  }

 protected:
  // The number of rows of a complete block in the mode of
  // `pushBlockConcurrently`. It is the `blocksize_`, except that a
  // `blocksize_` of zero (which only happens for the very small memory limits
  // of some unit tests) is rounded up to one, so that every push makes
  // progress.
  size_t concurrentPushBlocksize() const {
    return std::max<size_t>(blocksize_, 1);
  }

  // Make sure that the `currentBlock_` is resized to a complete block and has
  // room for at least one more row, writing it out if it is already full. The
  // `lock` (which has to be held on the `concurrentPushMutex_`) is temporarily
  // released while waiting for the outstanding copies of the other pushers.
  // See `pushBlockConcurrently` above.
  void prepareBlockForConcurrentPush(std::unique_lock<std::mutex>& lock) {
    if (!blockIsResizedForConcurrentPush_) {
      numRowsReserved_ = currentBlock_.numRows();
      currentBlock_.resize(concurrentPushBlocksize());
      blockIsResizedForConcurrentPush_ = true;
    }
    while (numRowsReserved_ >= concurrentPushBlocksize()) {
      if (numOutstandingCopies_ > 0) {
        // Note: The predicate of the enclosing loop is rechecked after the
        // wait, because another pusher may have written the block in the
        // meantime, in which case there is nothing left to do here.
        concurrentPushCv_.wait(lock);
        continue;
      }
      // The block is exactly full and nobody is copying into it anymore, so it
      // can be written. The next block is then again resized up front.
      writeCurrentBlockAndRecycleBuffer();
      currentBlock_.resize(concurrentPushBlocksize());
      numRowsReserved_ = 0;
    }
  }

  // Clear the current block. If `reserve` is `true`, we subsequently also
  // reserve the `blocksize_`.
  void resetCurrentBlock(bool reserve) {
    currentBlock_.clear();
    if (reserve) {
      currentBlock_.reserve(blocksize_);
    }
  }

  // Apply the `blockTransformation_` to the `block`. A transformation that can
  // make use of several threads (for the `CompressedExternalIdTableSorter` this
  // is the sort of the block) does so only if the `parallelism` is `Allowed`.
  void transformBlock(Buffer& block,
                      compressedExternalIdTable::Parallelism parallelism) {
    if constexpr (std::is_invocable_v<BlockTransformation&, Buffer&,
                                      compressedExternalIdTable::Parallelism>) {
      blockTransformation_(block, parallelism);
    } else {
      blockTransformation_(block);
    }
  }

  // Asynchronously compress the `block` and write it to the underlying
  // `writer_`. Before compressing, apply the transformation that is specified
  // by the `Impl` via the `transformBlock` function.
  //
  // Return the block buffer of the *previous* such task (empty, but with its
  // memory still allocated), or `std::nullopt` if there was no previous task.
  // Reusing that buffer for the next block is what keeps the number of block
  // buffers that are ever allocated at two, see
  // `writeCurrentBlockAndRecycleBuffer`.
  std::optional<Buffer> transformAndWriteBlock(Buffer block) {
    auto recycledBlock = waitForFuture();
    if (block.empty()) {
      if (numBlocksPushed_ > 0) {
        // NOTE: In `transformAndPushLastBlock` we assert that if at least one
        // block has been pushed, then `compressAndWriteFuture_` is valid.
        // Therefore, we have to set a valid future here, even if it does
        // nothing.
        setFuture(ad_utility::makeReadyFuture(std::move(block)));
      }
      return recycledBlock;
    }
    ++numBlocksPushed_;
    setFuture(ad_utility::postAndGetFuture(
        blockWritePool_.get_executor(),
        [block = std::move(block), this]() mutable -> Buffer {
          transformBlock(block,
                         compressedExternalIdTable::Parallelism::Allowed);
          // NOTE: Writing the block also clears it, but keeps its memory, so
          // the buffer that we give back already has the capacity that the next
          // block needs, see `SortBlockBuffer::writeToAndClear`.
          block.writeToAndClear(this->writer_);
          return std::move(block);
        }));
    return recycledBlock;
  }

  // Hand the `currentBlock_` to the background thread (see
  // `transformAndWriteBlock`) and make the buffer that the *previous*
  // background task has given back the new `currentBlock_`. Only the very first
  // block has no such buffer to reuse and therefore has to allocate one, so
  // that in total exactly two block buffers are allocated: the one that the
  // background thread is working on, and the one that `push` fills.
  void writeCurrentBlockAndRecycleBuffer() {
    auto recycledBlock = transformAndWriteBlock(std::move(currentBlock_));
    if (recycledBlock.has_value()) {
      currentBlock_ = std::move(recycledBlock).value();
    }
    resetCurrentBlock(true);
  }

  // Return `true` if the last block of the input phase is small enough to be
  // transformed sequentially in the calling thread, see
  // `compressedExternalIdTable::MAX_ROWS_FOR_SEQUENTIAL_LAST_BLOCK`.
  bool lastBlockIsTransformedSequentially() const {
    return currentBlock_.numRows() <
           compressedExternalIdTable::MAX_ROWS_FOR_SEQUENTIAL_LAST_BLOCK;
  }

  // If there is less than one complete block (meaning that the number of calls
  // to `push` was `< blocksize_`), apply the transformation to `currentBlock_`
  // and return `false`. Else, write the `currentBlock_` via
  // `transformAndWriteBlock`, block until the writing is actually finished,
  // and return `true`. Using this function allows for an efficient usage of
  // this class for very small inputs.
  bool transformAndPushLastBlock() {
    finishConcurrentPushes();
    if (!isFirstIteration_) {
      return numBlocksPushed_ != 0;
    }
    AD_CORRECTNESS_CHECK(!transformAndPushWasCalled_.exchange(true));

    // If we have pushed at least one (complete) block, then the last future
    // from pushing a block is still in flight. If we have never pushed a block,
    // then also the future cannot be valid.
    AD_CORRECTNESS_CHECK(
        (numBlocksPushed_ == 0) != compressAndWriteFuture_.valid(), [this]() {
          return absl::StrCat(
              "numBlocksPushed: ", numBlocksPushed_,
              ", futureIsValid: ", compressAndWriteFuture_.valid());
        });
    // Optimization for inputs that are smaller than the blocksize, do not use
    // the external file, but simply sort and return the single block.
    if (numBlocksPushed_ == 0) {
      AD_CORRECTNESS_CHECK(this->numElementsPushed_ ==
                           this->currentBlock_.size());
      if (lastBlockIsTransformedSequentially()) {
        transformBlock(this->currentBlock_,
                       compressedExternalIdTable::Parallelism::Disallowed);
      } else {
        runOnBlockWriteThreadAndWait([this]() {
          transformBlock(this->currentBlock_,
                         compressedExternalIdTable::Parallelism::Allowed);
        });
      }
      return false;
    }
    // The last block is the remainder of the input and therefore typically much
    // smaller than the previous ones. If it is small enough, then transforming
    // it sequentially in the calling thread is cheaper than handing it to the
    // background thread, because we have to wait for the result right away.
    if (lastBlockIsTransformedSequentially()) {
      waitForFuture();
      transformAndWriteLastBlockInCallingThread();
      return true;
    }
    transformAndWriteBlock(std::move(this->currentBlock_));
    resetCurrentBlock(false);
    waitForFuture();
    return true;
  }

  // Transform the `currentBlock_` sequentially and write it to the `writer_`,
  // both in the calling thread. May only be called when the background task is
  // not running, see `transformAndPushLastBlock`, which is the only caller.
  //
  // NOTE: In contrast to `transformAndWriteBlock`, this leaves the
  // `compressAndWriteFuture_` invalid although it may increase the
  // `numBlocksPushed_`. That is fine, because the invariant that ties the two
  // together is checked only once, at the beginning of
  // `transformAndPushLastBlock`, and therefore before this function runs.
  void transformAndWriteLastBlockInCallingThread() {
    AD_CORRECTNESS_CHECK(!compressAndWriteFuture_.valid());
    if (currentBlock_.empty()) {
      return;
    }
    ++numBlocksPushed_;
    transformBlock(currentBlock_,
                   compressedExternalIdTable::Parallelism::Disallowed);
    currentBlock_.writeToAndClear(writer_);
  }
};

// This class allows the external and compressed storing of an `IdTable` that is
// too large to be stored in RAM. `NumStaticCols == 0` means that the `IdTable`
// is stored dynamically (see `IdTable.h` and `CallFixedSize.h` for details).
// The interface is as follows: First there is one call to `push` for each row
// of the `IdTable`, and then there is one single call to `getRows` which yields
// a generator that yields the rows that have previously been pushed.
//
// TODO<joka921> This class is unused (outside of its own unit tests).
// Remove it.
template <size_t NumStaticCols>
class CompressedExternalIdTable
    : public CompressedExternalIdTableBase<NumStaticCols> {
 private:
  using Base = CompressedExternalIdTableBase<NumStaticCols>;

  using MemorySize = ad_utility::MemorySize;

 public:
  // Constructor.
  explicit CompressedExternalIdTable(
      std::string filename, size_t numCols, ad_utility::MemorySize memory,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE)
      : Base{std::move(filename), numCols, memory, std::move(allocator),
             blocksizeCompression} {}

  // When we have a static number of columns, then the `numCols` argument to the
  // constructor is redundant.
  CPP_member explicit CPP_ctor(CompressedExternalIdTable)(
      std::string filename, ad_utility::MemorySize memory,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE)(
      requires(NumStaticCols > 0))
      : CompressedExternalIdTable(std::move(filename), NumStaticCols, memory,
                                  std::move(allocator), blocksizeCompression) {}

  // Transition from the input phase, where `push()` may be called, to the
  // output phase and return a generator that yields the elements of the
  // `IdTable` in the order that they were `push`ed. This function may be
  // called exactly once.
  auto getRows() {
    using namespace ad_utility;
    using Block = IdTableStatic<NumStaticCols>;
    // Both branches return the same type via this helper.
    auto joinBlocks = [](InputRangeTypeErased<Block> stream) {
      return ql::views::join(OwningViewNoConst{std::move(stream)});
    };
    if (!this->transformAndPushLastBlock()) {
      // Single block: wrap currentBlock_ as a one-element block stream.
      return joinBlocks(
          InputRangeTypeErased<Block>{lazySingleValueRange([this]() {
            return std::move(this->currentBlock_).extractColumnMajor();
          })});
    }
    this->transformAndWriteBlock(std::move(this->currentBlock_));
    this->resetCurrentBlock(false);
    this->waitForFuture();
    // Stream all blocks through a single background thread (O(1) threads total
    // regardless of block count) with sequential column decompression.
    return joinBlocks(this->writer_.template getBlockStream<NumStaticCols>());
  }
};

// A virtual base class for the `CompressedExternalIdTableSorter` (see below)
// that type-erases the used comparator as well as the statically known number
// of columns. The interface only deals in blocks, so that the costs of the
// virtual calls and the checking of the correct number of columns disappear.
class CompressedExternalIdTableSorterTypeErased {
 public:
  // Push a complete block at once.
  virtual void pushBlock(const IdTableStatic<0>& block) = 0;
  // Push a complete block given as a non-owning view at once.
  virtual void pushBlock(const IdTableView<0>& block) = 0;
  // Get the sorted output after all blocks have been pushed. If `blocksize ==
  // nullopt`, the size of the returned blocks will be chosen automatically.
  virtual ad_utility::InputRangeTypeErased<IdTableStatic<0>> getSortedOutput(
      std::optional<size_t> blocksize = std::nullopt) = 0;

  // Clear the complete sorter s.t. it can be reused. This deletes the contents
  // of the underlying file. Note:  We need a name that is distinct from `clear`
  // because of name collisions in the multiple inheritance of the
  // implementation.
  virtual void clearUnderlying() = 0;
  virtual ~CompressedExternalIdTableSorterTypeErased() = default;
};

// This class allows the external (on-disk) sorting of an `IdTable` that is too
// large to be stored in RAM. `NumStaticCols == 0` means that the IdTable is
// stored dynamically (see `IdTable.h` and `CallFixedSize.h` for details). The
// interface is as follows: First there is one call to `push` for each row of
// the IdTable, and then there is one single call to `sortedView` which yields a
// generator that yields the sorted rows one by one.

// When using very small block sizes in unit tests, then sometimes there are
// false positives in the memory limit mechanism, so setting the following
// variable to `true` allows to disable the memory limit.
inline std::atomic<bool>
    EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;

// Sort the rows of a single block, given as the `range` of those rows (which is
// either the column-major `IdTableStatic` or the `RowMajorIdTable` of a
// `SortBlockBuffer`). The `parallelism` argument specifies whether the sort may
// use several threads, see `CompressedExternalIdTableBase::transformBlock`.
//
// The parallel case sorts on the global thread pool, such that the block sort
// uses the same threads (and hence obeys the same parallelism setting) as the
// other phases of the index build, see `util/GlobalExecutor.h`.
//
// NOTE: The parallel sort blocks the calling thread until it is complete, so it
// must not be called from a thread of the global thread pool itself. It isn't:
// the parallel invocations of this function all come from the dedicated thread
// of the `blockWritePool_` of the sorter, see
// `CompressedExternalIdTableBase::runOnBlockWriteThreadAndWait`.
template <typename Range, typename Comparator>
void sortBlockRange(Range& block, const Comparator& comparator,
                    compressedExternalIdTable::Parallelism parallelism) {
  if (parallelism == compressedExternalIdTable::Parallelism::Allowed) {
    ad_utility::blockSort::blockIndirectSort(
        ql::ranges::subrange{std::begin(block), std::end(block)}, comparator,
        static_cast<uint32_t>(ad_utility::globalExecutorNumThreads()),
        ad_utility::globalExecutor());
    return;
  }
  ql::ranges::sort(std::begin(block), std::end(block), comparator);
}

// The implementation of sorting a single block of a `SortBlockBuffer`, no
// matter in which of its two layouts that block currently is, see
// `sortBlockRange` above.
template <typename Comparator>
struct BlockSorter {
  [[no_unique_address]] Comparator comparator_{};
  template <typename T>
  void operator()(T& block,
                  compressedExternalIdTable::Parallelism parallelism =
                      compressedExternalIdTable::Parallelism::Allowed) {
    block.visit([this, parallelism](auto& rows) {
      sortBlockRange(rows, comparator_, parallelism);
    });
  }
};
// Deduction guide for the implicit aggregate initialization (its "constructor")
// in the aggregate above. Is actually not needed in C++20, but GCC 11 requires
// it.
template <typename Comparator>
BlockSorter(Comparator) -> BlockSorter<Comparator>;

template <typename Comparator, size_t NumStaticCols>
class CompressedExternalIdTableSorter
    : public CompressedExternalIdTableBase<NumStaticCols,
                                           BlockSorter<Comparator>>,
      public CompressedExternalIdTableSorterTypeErased {
 private:
  using Base =
      CompressedExternalIdTableBase<NumStaticCols, BlockSorter<Comparator>>;
  [[no_unique_address]] Comparator comparator_{};
  // Track if we are currently in the merging phase.
  std::atomic<bool> mergeIsActive_ = false;

  // The maximal blocksize in the output phase.
  MemorySize maxOutputBlocksize_ = 1_GB;
  // The number of merged blocks that are buffered during the output phase. It
  // is the number of output blocks that the memory accounting of the merge
  // phase reserves memory for on the consumer side (see
  // `compressedExternalIdTable::computeMergePhaseParameters`), and how it is
  // split between the read-ahead of the consumer, the read-ahead of the spill
  // files and the two blocks that are always in the consumer's hands is decided
  // by `compressedExternalIdTable::makeMergeOptions`, see there.
  int numBufferedOutputBlocks_ = 12;

  // See the `moveResultOnMerge()` getter function for documentation.
  bool moveResultOnMerge_ = true;

  // The executor on which the merge phase runs, together with the number of
  // threads that run it.
  //
  // NOTE: The default executor is the process-wide shared thread pool (see
  // `ad_utility::globalExecutor`), so the assumed parallelism has to be the
  // size of exactly that pool and not the number of hardware threads. The two
  // differ as soon as the pool was sized explicitly, for example via the
  // `--num-threads` option of the index builder.
  boost::asio::any_io_executor mergeExecutor_ =
      compressedExternalIdTable::defaultSorterMergeExecutor();
  size_t mergeParallelism_ = ad_utility::globalExecutorNumThreads();

  // Set as soon as the warning about a reduced parallelism (see
  // `warnIfParallelismIsReduced`) was logged, such that it is logged at most
  // once per sorter.
  std::atomic<bool> reducedParallelismWasLogged_ = false;

  // The number of merge phases that were started so far, which is what makes
  // the names of the spill files of a merge phase unique, see
  // `compressedExternalIdTable::makeSpillFilename`.
  std::atomic<size_t> numMergePhases_ = 0;

  // See `setMergeSpillCompression`. The default comes from the runtime
  // parameter `external-sorter-compression-level` and is read once, when this
  // sorter is constructed.
  CompressedBlockFile::CompressionLevel mergeSpillCompression_ =
      compressedExternalIdTable::sorterCompressionLevels().mergePhaseSpill_;

 public:
  // Constructor.
  CompressedExternalIdTableSorter(
      std::string filename, size_t numCols, ad_utility::MemorySize memory,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
      Comparator comparator = {})
      : Base{std::move(filename),
             numCols,
             memory,
             std::move(allocator),
             blocksizeCompression,
             BlockSorter{comparator}},
        comparator_{comparator} {}

  // When we have a static number of columns, then the `numCols` argument to the
  // constructor is redundant.
  CPP_member CPP_ctor(CompressedExternalIdTableSorter)(
      std::string filename, ad_utility::MemorySize memory,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
      Comparator comp = {})(requires(NumStaticCols > 0))
      : CompressedExternalIdTableSorter(std::move(filename), NumStaticCols,
                                        memory, std::move(allocator),
                                        blocksizeCompression, comp) {}

  // Explicitly inherit the `push` function, such that we can use it unqualified
  // within this class.
  using Base::push;

  // Set the executor on which the merge phase runs, together with the number of
  // threads that run that executor. Use this to share a thread pool with other
  // tasks, or to pin the parallelism in tests and benchmarks. A `parallelism`
  // of one means "merge serially in the consuming thread", in which case the
  // `executor` is never used at all.
  //
  // IMPORTANT: The `executor` must not be run by the thread that consumes the
  // sorted output, see `parallelBlockMerge::parallelBlockMergeToRange`.
  void setMergeExecutor(boost::asio::any_io_executor executor,
                        size_t parallelism) {
    AD_CONTRACT_CHECK(parallelism > 0);
    mergeExecutor_ = std::move(executor);
    mergeParallelism_ = parallelism;
  }

  // Set how the merge phase stores the output blocks that it spills (see
  // `makeBlockStorageFactory`): a ZSTD compression level, or
  // `NO_BLOCK_COMPRESSION` to store them uncompressed. Use this to trade the
  // CPU that the compression costs against the bytes that the spill file
  // occupies, see `compressedExternalIdTable::MERGE_PHASE_SPILL_COMPRESSION`
  // for the reasoning behind the built-in default. The default of this sorter
  // comes from the runtime parameter `external-sorter-compression-level`, so
  // this setter is only needed to override that per sorter.
  void setMergeSpillCompression(
      CompressedBlockFile::CompressionLevel compression) {
    mergeSpillCompression_ = compression;
  }

  // If set to `false` then the sorted result can be extracted multiple times.
  // If set to `true` then the result is moved out and unusable after the first
  // merge. In that case an exception will be thrown at the start of the second
  // merge.
  // Note: This mechanism gives a performance advantage for very small inputs
  // that can be completely sorted in RAM. In that case we can avoid a copy of
  // the sorted result.
  bool& moveResultOnMerge() {
    AD_CONTRACT_CHECK(this->isFirstIteration_);
    return moveResultOnMerge_;
  }

  // Transition from the input phase, where `push()` can be called, to the
  // output phase and return a generator that yields the sorted elements one by
  // one. Either this function or the following function must be called exactly
  // once.
  auto sortedView() { return ql::views::join(getSortedBlocks()); }

  // Similar to `sortedView` (see above), but the elements are yielded in
  // blocks. The size of the blocks is `blocksize` if specified, otherwise it
  // will be automatically determined from the given memory limit.
  CPP_template(size_t N = NumStaticCols)(requires(N == NumStaticCols || N == 0))
      ad_utility::InputRangeTypeErased<IdTableStatic<N>> getSortedBlocks(
          std::optional<size_t> blocksize = std::nullopt) {
    // If we move the result out, there must only be a single merge phase.
    AD_CONTRACT_CHECK(this->isFirstIteration_ || !this->moveResultOnMerge_);
    AD_CONTRACT_CHECK(!mergeIsActive_.load());
    mergeIsActive_.store(true);

    // NOTE: The blocks are read ahead by the merge itself (see
    // `numBufferedOutputBlocks_` and
    // `parallelBlockMerge::MergeOptions::numPrefetchedOutputBlocks`), so no
    // asynchronous stream is needed on top of it.
    using namespace ad_utility;
    return InputRangeTypeErased{
        CallbackOnEndView{sortedBlocks<N>(blocksize), [&, this]() noexcept {
                            this->isFirstIteration_ = false;
                            mergeIsActive_.store(false);
                          }}};
  }

  // The implementation of the type-erased interface. Push a complete block at
  // once.
  void pushBlock(const IdTableStatic<0>& block) override {
    Base::pushBlock(block);
  }

  // The implementation of the type-erased interface. Push a complete block
  // given as a non-owning view at once.
  void pushBlock(const IdTableView<0>& block) override {
    Base::pushBlock(block);
  }

  // The implementation of the type-erased interface. Get the sorted blocks as
  // dynamic IdTables.
  ad_utility::InputRangeTypeErased<IdTableStatic<0>> getSortedOutput(
      std::optional<size_t> blocksize) override {
    return sortedBlocks<0>(blocksize);
  }

 private:
  // Return a lazy range that yields the blocks of the `merged` range and, on
  // natural exhaustion, checks that the total number of yielded rows is exactly
  // the number of rows that were pushed. The check deliberately happens while
  // pulling the blocks and not in a destructor or a `CallbackOnEndView`,
  // because several callers (for example `Sort` with `requestLaziness`) abandon
  // the range early, and the check must not fire in that case.
  template <size_t N>
  auto checkedMergeResult(
      ad_utility::InputRangeTypeErased<IdTableStatic<N>> merged) const {
    using LoopControl = ad_utility::LoopControl<IdTableStatic<N>>;
    return ad_utility::InputRangeFromLoopControlGet{
        [blocks = std::move(merged), sorter = this,
         numPopped = size_t{0}]() mutable {
          auto block = blocks.get();
          if (!block.has_value()) {
            AD_CORRECTNESS_CHECK(
                numPopped == sorter->numElementsPushed_, [&numPopped, sorter] {
                  return absl::StrCat(
                      "numPopped: ", numPopped,
                      "num elements pushed:", sorter->numElementsPushed_);
                });
            return LoopControl::makeBreak();
          }
          numPopped += block.value().numRows();
          return LoopControl::yieldValue(std::move(block.value()));
        }};
  }

  void clearUnderlying() override { this->clear(); }
  // Transition from the input phase, where `push()` may be called, to the
  // output phase and return an input range that yields the sorted elements.
  // This function may be called exactly once.
  CPP_template(size_t N = NumStaticCols)(requires(N == NumStaticCols || N == 0))
      ad_utility::InputRangeTypeErased<IdTableStatic<N>> sortedBlocks(
          std::optional<size_t> blocksize = std::nullopt) {
    if (!this->transformAndPushLastBlock()) {
      // There was only one block, return it. If a blocksize was explicitly
      // requested for the output, and the single block is larger than this
      // blocksize, we manually have to split it into chunks.
      auto& block = this->currentBlock_;
      const auto blocksizeOutput = blocksize.value_or(block.numRows());
      if (block.numRows() <= blocksizeOutput) {
        using namespace ad_utility;
        namespace detail = compressedExternalIdTable::detail;
        return block.empty()
                   ? InputRangeTypeErased{ql::views::empty<IdTableStatic<N>>}
                   : InputRangeTypeErased{
                         lazySingleValueRange([this]() -> IdTableStatic<N> {
                           if (this->moveResultOnMerge_) {
                             return detail::toOutputTable<N>(
                                 std::move(this->currentBlock_)
                                     .extractColumnMajor());
                           } else {
                             return detail::toOutputTable<N>(
                                 this->currentBlock_.copyToColumnMajor());
                           }
                         })};
      }
      namespace rv = ::ranges::views;
      auto chunked =
          rv::chunk(rv::iota(size_t{0}, block.numRows()), blocksizeOutput) |
          rv::transform([&](const auto& chunk) {
            auto chunkStart = *chunk.begin();
            auto chunkSize = ::ranges::size(chunk);
            auto curBlock = IdTableStatic<NumStaticCols>(
                this->numColumns_, this->writer_.allocator());
            block.appendRowsTo(curBlock, chunkStart, chunkStart + chunkSize);
            return compressedExternalIdTable::detail::toOutputTable<N>(
                std::move(curBlock));
          });
      return ad_utility::InputRangeTypeErased(std::move(chunked));
    }

    // Merge the presorted runs (which live compressed in the `writer_`) in
    // parallel, see `util/parallelBlockMerge/ParallelBlockMerge.h`.
    const auto config = makeMergePhaseConfig(blocksize);
    const auto parameters =
        compressedExternalIdTable::computeMergePhaseParameters(config);
    warnIfParallelismIsReduced(parameters);
    if (this->currentBlock_.isRowMajor()) {
      return mergeRowMajor<N>(config, parameters);
    }
    return mergeRuns<N, IdTableStatic<N>, N>(config, parameters);
  }

  // Merge the presorted runs with the blocks in the row-major layout, see
  // `RowMajorMergeBlock`. The number of columns of such a block is a
  // compile-time constant, which for a sorter with a dynamic number of columns
  // is obtained via `callFixedSize`; the runs of such a sorter are therefore
  // merged as blocks of `I` columns and only the resulting blocks are converted
  // back to the dynamic output type, see `detail::toOutputTable`.
  CPP_template(size_t N)(requires(N == NumStaticCols || N == 0))
      ad_utility::InputRangeTypeErased<IdTableStatic<N>> mergeRowMajor(
          const compressedExternalIdTable::MergePhaseConfig& config,
          const compressedExternalIdTable::MergePhaseParameters& parameters) {
    if constexpr (NumStaticCols > 0) {
      return mergeRuns<N, RowMajorMergeBlock<NumStaticCols>, NumStaticCols>(
          config, parameters);
    } else {
      using Result = ad_utility::InputRangeTypeErased<IdTableStatic<N>>;
      return ad_utility::callFixedSizeVi<
          compressedExternalIdTable::MAX_NUM_COLUMNS_ROW_MAJOR>(
          static_cast<int>(this->numColumns_),
          [this, &config, &parameters](auto numColumnsVi) -> Result {
            constexpr size_t I =
                static_cast<size_t>(decltype(numColumnsVi)::value);
            if constexpr (I == 0) {
              // Excluded by `rowMajorModeIsSupported`, which the buffer of the
              // input phase has already checked.
              AD_FAIL();
            } else {
              return mergeRuns<N, RowMajorMergeBlock<I>, I>(config, parameters);
            }
          });
    }
  }

  // Merge the presorted runs as blocks of type `Block` with `I` columns and
  // yield the result as blocks with `N` statically known columns. The two
  // layouts of a block differ only in the `Block` type, see
  // `CompressedIdTableRunsInput`.
  template <size_t N, typename Block, size_t I>
  ad_utility::InputRangeTypeErased<IdTableStatic<N>> mergeRuns(
      const compressedExternalIdTable::MergePhaseConfig& config,
      const compressedExternalIdTable::MergePhaseParameters& parameters) {
    // The block storage spells the default, column-major block type as `void`,
    // see `CompressedIdTableBlockStorage`.
    using StorageBlock =
        std::conditional_t<std::is_same_v<Block, IdTableStatic<I>>, void,
                           Block>;
    auto merged =
        parallelBlockMerge::parallelBlockMergeToRange</*moveElements=*/true>(
            mergeExecutor_, CompressedIdTableRunsInput<I, Block>{this->writer_},
            this->comparator_,
            makeBlockStorageFactory<I, StorageBlock>(parameters),
            compressedExternalIdTable::makeMergeOptions(config, parameters),
            // NOTE: The sorter has no cancellation handle of its own, and the
            // merge requires one that is not `nullptr`, so this is a fresh
            // handle that is never cancelled.
            std::make_shared<ad_utility::CancellationHandle<>>());
    return ad_utility::InputRangeTypeErased{
        checkedMergeResult<N>(toOutputBlocks<N, Block, I>(std::move(merged)))};
  }

  // Turn the blocks of the merge into the output blocks of this sorter. For the
  // column-major layout this is a no-op, and for the row-major one it is a
  // cheap move: such a block is already column-major when it arrives here,
  // because it was either spilled to disk (and read back column-major) or
  // transposed on a worker thread of the merge while it waited in the block
  // storage, see the FINALIZATION note at `CompressedIdTableBlockStorage`. The
  // transposition is only ever done here for a block storage that does not
  // finalize its blocks, which the merge phase of this sorter does not use.
  template <size_t N, typename Block, size_t I>
  ad_utility::InputRangeTypeErased<IdTableStatic<N>> toOutputBlocks(
      ad_utility::InputRangeTypeErased<Block> merged) const {
    namespace detail = compressedExternalIdTable::detail;
    if constexpr (std::is_same_v<Block, IdTableStatic<N>>) {
      return merged;
    } else {
      using LoopControl = ad_utility::LoopControl<IdTableStatic<N>>;
      return ad_utility::InputRangeTypeErased<IdTableStatic<N>>{
          ad_utility::InputRangeFromLoopControlGet{
              [blocks = std::move(merged),
               allocator = this->writer_.allocator()]() mutable {
                auto block = blocks.get();
                if (!block.has_value()) {
                  return LoopControl::makeBreak();
                }
                return LoopControl::yieldValue(detail::toOutputTable<N>(
                    std::move(block).value().toColumnMajor(allocator)));
              }}};
    }
  }

  // The factory for the intermediate storage of the output blocks of the merge
  // phase, see `compressedExternalIdTable::makeMergePhaseBlockStorageFactory`.
  // How many of those blocks a chunk may buffer before it starts spilling is
  // part of the `parameters` that the memory limit was split into, see
  // `compressedExternalIdTable::numBufferedOutputBlocksPerChunk`.
  template <size_t N, typename Block = void>
  auto makeBlockStorageFactory(
      const compressedExternalIdTable::MergePhaseParameters& parameters) {
    return compressedExternalIdTable::makeMergePhaseBlockStorageFactory<N,
                                                                        Block>(
        mergeExecutor_,
        compressedExternalIdTable::makeSpillFilename(
            this->writer_.filename(), numMergePhases_.fetch_add(1)),
        this->writer_.allocator(), parameters.numBufferedBlocksPerChunk_,
        mergeSpillCompression_);
  }

  // The configuration from which the parameters of the merge phase are derived,
  // see `compressedExternalIdTable::computeMergePhaseParameters`.
  compressedExternalIdTable::MergePhaseConfig makeMergePhaseConfig(
      std::optional<size_t> blocksize) const {
    compressedExternalIdTable::MergePhaseConfig config;
    config.numRuns_ = this->writer_.numIdTables();
    config.numColumns_ = this->numColumns_;
    config.memoryLimit_ = this->memory_;
    config.inputBlockSize_ = this->writer_.blockSizeUncompressed();
    config.numBufferedOutputBlocks_ =
        static_cast<size_t>(numBufferedOutputBlocks_);
    config.maxOutputBlockSize_ = maxOutputBlocksize_;
    config.parallelism_ = mergeParallelism_;
    config.outputBlockSizeOverride_ = blocksize;
    config.ignoreMemoryLimit_ =
        EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING;
    return config;
  }

  // Warn (once per sorter) if the memory limit forces the merge phase to use
  // less parallelism than the merge executor offers.
  void warnIfParallelismIsReduced(
      const compressedExternalIdTable::MergePhaseParameters& parameters) {
    if (parameters.numChunksInFlight_ >= mergeParallelism_ ||
        reducedParallelismWasLogged_.exchange(true)) {
      return;
    }
    AD_LOG_WARN << "The merge phase of the external sorter can only merge "
                << parameters.numChunksInFlight_
                << " chunks concurrently instead of the " << mergeParallelism_
                << " chunks that the available parallelism offers, because "
                   "of the memory limit of "
                << this->memory_.asString()
                << ". Increasing the memory limit will speed up the merge."
                << std::endl;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_COMPRESSEDEXTERNALIDTABLE_H
