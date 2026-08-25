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
#include <future>
#include <utility>

#include "backports/algorithm.h"
#include "engine/CallFixedSize.h"
#include "engine/idTable/CompressedIdTableBlocks.h"
#include "engine/idTable/IdTable.h"
#include "util/AsyncStream.h"
#include "util/CompressedBlockFile.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/MemorySize/MemorySize.h"
#include "util/TransparentFunctors.h"
#include "util/Views.h"
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
  // The file to which the `IdTable`s are written. The file is deleted together
  // with this object, see `CompressedBlockFile`.
  ad_utility::CompressedBlockFile file_;

  // The number of columns that each of the stored `IdTable`s has.
  size_t numColumns_;

  // The metadata of all the stored blocks, in the order in which they were
  // written. Each entry holds one compressed byte range per column.
  std::vector<compressedIdTable::BlockMetadata> blocks_;

  // For each contained `IdTable` contains the index in `blocks_` where the
  // blocks of this table begin.
  std::vector<size_t> startOfSingleIdTables_;

  // For each block (indexed exactly like `blocks_`), the first and the last row
  // of that block, stored as `[first_0, last_0, first_1, ...]`.
  // The block boundaries are identical for all columns (see `writeIdTable`), so
  // a single vector suffices. This metadata is used by the merge phase (see
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
          DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE)
      : file_{std::move(filename)},
        numColumns_{numCols},
        allocator_{std::move(allocator)},
        blockSizeUncompressed_(blockSizeUncompressed) {}

  // Simple getters for the stored allocator and the number of columns;
  const auto& allocator() const { return allocator_; }
  size_t numColumns() const { return numColumns_; }
  const MemorySize& blockSizeUncompressed() const {
    return blockSizeUncompressed_;
  }

  // Store an `idTable`.
  void writeIdTable(const IdTable& table) {
    if (numActiveGenerators_ != 0) {
      AD_THROW(
          "Trying to call `writeIdTable` on an "
          "`CompressedExternalIdTableWriter` that is currently being iterated "
          "over");
    }
    AD_CONTRACT_CHECK(table.numColumns() == numColumns());
    size_t blockSize = blockSizeUncompressed_.getBytes() / sizeof(Id);
    AD_CONTRACT_CHECK(blockSize > 0);
    size_t firstNewBlock = blocks_.size();
    startOfSingleIdTables_.push_back(firstNewBlock);
    size_t numNewBlocks = (table.numRows() + blockSize - 1) / blockSize;
    blocks_.resize(firstNewBlock + numNewBlocks);
    // Set up the metadata of the new blocks and store the first and the last
    // row of each of them. This has to happen serially (and not inside the
    // per-column tasks below), because each of those tasks only sees a single
    // column. Note that an empty `table` consistently leads to zero blocks,
    // both here and in the loop below.
    for (size_t lower = 0; lower < table.numRows(); lower += blockSize) {
      size_t upper = std::min(lower + blockSize, table.numRows());
      auto& block = blocks_.at(firstNewBlock + lower / blockSize);
      block.numRows_ = upper - lower;
      block.columns_.resize(numColumns_);
      firstAndLastRowPerBlock_.emplace_back(table[lower]);
      firstAndLastRowPerBlock_.emplace_back(table[upper - 1]);
    }
    // The columns are compressed and stored in parallel. This is free of data
    // races: Each task only assigns to the element for its own column of the
    // `columns_` of the new blocks, so the tasks write to pairwise disjoint
    // elements of vectors that are never reallocated while the tasks run (all
    // the required elements were already created by the serial loop above).
    // TODO<joka921> Use parallelism per block instead of per column (more
    // fine-grained) but only once we have a reasonable abstraction for
    // parallelism.
    std::vector<std::future<void>> compressColumFutures;
    for (auto i : ql::views::iota(0u, numColumns())) {
      compressColumFutures.push_back(std::async(
          std::launch::async, [this, i, blockSize, firstNewBlock, &table]() {
            // TODO<C++23> Use `ql::views::chunkd`
            for (size_t lower = 0; lower < table.numRows();
                 lower += blockSize) {
              size_t upper =
                  std::min<size_t>(lower + blockSize, table.numRows());
              blocks_[firstNewBlock + lower / blockSize].columns_[i] =
                  compressedIdTable::writeColumn(file_, table, i, lower, upper);
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
  std::vector<InputRangeTypeErased<IdTableStatic<N>>> getAllGenerators() {
    file_.flush();
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
    return blocks_.at(globalBlockIdx(idTableIdx, blockIdx)).numRows_;
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
    return compressedIdTable::readBlock<N>(
        file_, blocks_.at(globalBlockIdx(idTableIdx, blockIdx)), allocator_);
  }

  // Register a reader that accesses the blocks directly (via
  // `readBlockOfIdTable`), such that the writer knows that it is currently
  // being read from, see `numActiveGenerators_`.
  void registerActiveReader() { ++numActiveGenerators_; }

  // Unregister a reader that was previously registered via
  // `registerActiveReader`.
  void unregisterActiveReader() noexcept { --numActiveGenerators_; }

  // Flush the underlying file, such that all written blocks become readable.
  void flush() { file_.flush(); }

  // Clear the underlying file and completely reset the data structure s.t. it
  // can be reused.
  void clear() {
    if (numActiveGenerators_ > 0) {
      AD_THROW(
          "Trying to call `writeIdTable` on an "
          "`CompressedExternalIdTableWriter` that is currently being iterated "
          "over");
    }
    file_.clear();
    blocks_.clear();
    startOfSingleIdTables_.clear();
    firstAndLastRowPerBlock_.clear();
  }

 private:
  // The index (in `blocks_`) of the first block that does NOT belong to the
  // `IdTable` with the given index anymore. This mirrors the logic in
  // `makeGeneratorForIdTable`.
  size_t endBlockOfIdTable(size_t idTableIdx) const {
    AD_CONTRACT_CHECK(idTableIdx < startOfSingleIdTables_.size());
    return idTableIdx + 1 < startOfSingleIdTables_.size()
               ? startOfSingleIdTables_.at(idTableIdx + 1)
               : blocks_.size();
  }

  // Translate the index of a block that is local to the `IdTable` with the
  // given index into the corresponding global block index (which is the index
  // into `blocks_` and `firstAndLastRowPerBlock_`).
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
                         : blocks_.size()};
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

  // Read and decompress the block with the given global index (which is the
  // index into `blocks_`). The individual columns are decompressed
  // concurrently, which is safe because they are disjoint parts of the `block`,
  // see `compressedIdTable::readColumnIntoBlock`.
  template <size_t NumCols = 0>
  IdTableStatic<NumCols> readBlock(size_t blockIdx) {
    const auto& metadata = blocks_.at(blockIdx);
    auto block = compressedIdTable::makeBlock<NumCols>(metadata, allocator_);
    std::vector<std::future<void>> readColumnFutures;
    for (auto i : ql::views::iota(0u, numColumns())) {
      readColumnFutures.push_back(
          std::async(std::launch::async, [this, i, &metadata, &block]() {
            compressedIdTable::readColumnIntoBlock<NumCols>(file_, metadata, i,
                                                            block);
          }));
    }
    for (auto& fut : readColumnFutures) {
      fut.get();
    }
    return block;
  }

 public:
  // Read all blocks as a single `InputRangeTypeErased<IdTableStatic<N>>` via
  // one background thread. This creates a constant number of threads regardless
  // of the number of stored blocks. Columns are decompressed sequentially
  // within a block; the single background thread already provides concurrency
  // with the consumer.
  template <size_t N = 0>
  InputRangeTypeErased<IdTableStatic<N>> getBlockStream() {
    file_.flush();
    CachingTransformInputRange readBlocks{
        ql::views::iota(size_t{0}, blocks_.size()), [this](size_t blockIdx) {
          return compressedIdTable::readBlock<N>(file_, blocks_.at(blockIdx),
                                                 allocator_);
        }};
    ++numActiveGenerators_;
    auto callback = [this]() noexcept { --numActiveGenerators_; };
    // Queue size 2 keeps the producer one block ahead of the consumer.
    return ad_utility::streams::runStreamAsync(
        CallbackOnEndView{std::move(readBlocks), std::move(callback)}, 2);
  }
};

// An input policy for `ad_utility::parallelBlockMerge` that reads the blocks of
// the `IdTable`s (= presorted runs) stored in a
// `CompressedExternalIdTableWriter`. The `Key` is a dynamic, owning `Row`,
// which can be compared against the (proxy) row references of an
// `IdTableStatic<NumStaticCols>` because all comparators used in QLever are
// templated on both of their argument types.
//
// The class registers itself as an active reader of the `writer` for its whole
// lifetime (see `CompressedExternalIdTableWriter::registerActiveReader`), such
// that writing to the `writer` while a merge is running correctly throws.
template <size_t NumStaticCols>
class CompressedIdTableRunsInput {
 public:
  using Block = IdTableStatic<NumStaticCols>;
  using Key = IdTable::row_type;
  using value_type = typename Block::row_type;

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
  // copied. It has to be movable, because `parallelBlockMergeToRange` takes its
  // input by value.
  CompressedIdTableRunsInput(const CompressedIdTableRunsInput&) = delete;
  CompressedIdTableRunsInput& operator=(const CompressedIdTableRunsInput&) =
      delete;
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
  const Key& firstKey(size_t run, size_t block) const {
    return writer_->firstRowOfBlock(run, block);
  }

  // ________________________________________________________________________
  const Key& lastKey(size_t run, size_t block) const {
    return writer_->lastRowOfBlock(run, block);
  }

  // Read and decompress a single block. This is the only function that performs
  // I/O; it is thread-safe, because it only takes a shared lock on the
  // underlying file.
  Block readBlock(size_t run, size_t block) const {
    return writer_->template readBlockOfIdTable<NumStaticCols>(run, block);
  }

  // ________________________________________________________________________
  Block makeEmptyBlock() const {
    return Block{writer_->numColumns(), writer_->allocator()};
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

// Make a mismatch with the `BlockedRunsInput` concept a clear compile error.
static_assert(
    parallelBlockMerge::BlockedRunsInput<CompressedIdTableRunsInput<0>>);
static_assert(
    parallelBlockMerge::BlockedRunsInput<CompressedIdTableRunsInput<3>>);

// The common base implementation of `CompressedExternalIdTable` and
// `CompressedExternalIdTableSorter` (see below). It is implemented as a mixin
// class.
CPP_class_template(size_t NumStaticCols,
                   typename BlockTransformation = ad_utility::Noop)(requires(
    ql::concepts::invocable<
        BlockTransformation,
        IdTableStatic<NumStaticCols>&>)) class CompressedExternalIdTableBase {
 public:
  using value_type = typename IdTableStatic<NumStaticCols>::row_type;
  using reference = typename IdTableStatic<NumStaticCols>::row_reference;
  using const_reference =
      typename IdTableStatic<NumStaticCols>::const_row_reference;
  using MemorySize = ad_utility::MemorySize;

 protected:
  // Used to aggregate rows for the next block.
  IdTableStatic<NumStaticCols> currentBlock_;
  // For statistical reasons
  size_t numElementsPushed_ = 0;
  size_t numBlocksPushed_ = 0;
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
  CompressedExternalIdTableWriter writer_;
  std::future<void> compressAndWriteFuture_;

  // If the `compressAndWriteFuture_` is currently active, wait for its
  // computation to be completed, else do nothing.
  void waitForFuture() {
    if (compressAndWriteFuture_.valid()) {
      compressAndWriteFuture_.get();
    }
  }

  // Store the `future` inside the `compressAndWriteFuture_`. This trivial
  // wrapper can be used to inject more detailed logging when analyzing the
  // control flow of this class or when fixing bugs.
  void setFuture(std::future<void> future) {
    AD_CORRECTNESS_CHECK(!compressAndWriteFuture_.valid());
    compressAndWriteFuture_ = std::move(future);
  }

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
      : currentBlock_{numCols, allocator},
        numColumns_{numCols},
        memory_{memory},
        writer_{std::move(filename), numCols, allocator, blocksizeCompression},
        blockTransformation_{blockTransformation} {
    this->currentBlock_.reserve(blocksize_);
    AD_CONTRACT_CHECK(NumStaticCols == 0 || NumStaticCols == numCols);
  }
  // Add a single row to the input. The type of `row` needs to be something that
  // can be `push_back`ed to a `IdTable`.
  CPP_template(typename R)(
      requires compressedExternalIdTable::detail::HasPushBack<
          decltype(currentBlock_), R>) void push(const R& row) {
    ++numElementsPushed_;
    currentBlock_.push_back(row);
    if (currentBlock_.size() >= blocksize_) {
      pushBlock(std::move(currentBlock_));
      resetCurrentBlock(true);
    }
  }

  // ___________________________________________________________________
  size_t size() const { return numElementsPushed_; }

  // Return a lambda that takes a `ValueType` and calls `push` for that value.
  auto makePushCallback() {
    return [self = this](auto&& value) { self->push(AD_FWD(value)); };
  }

  // Delete the underlying file and reset the sorter. May only be called if no
  // active `getBlocks()` generator that has not been fully iterated over is
  // currently active, else an exception is thrown by the underlying
  // `CompressedExternalIdTable`.
  void clear() {
    resetCurrentBlock(false);
    numElementsPushed_ = 0;
    waitForFuture();
    writer_.clear();
    numBlocksPushed_ = 0;
    isFirstIteration_ = true;
    transformAndPushWasCalled_ = false;
  }

 protected:
  // Clear the current block. If `reserve` is `true`, we subsequently also
  // reserve the `blocksize_`.
  void resetCurrentBlock(bool reserve) {
    currentBlock_.clear();
    if (reserve) {
      currentBlock_.reserve(blocksize_);
    }
  }

  // Asynchronously compress the `block` and write it to the underlying
  // `writer_`. Before compressing, apply the transformation that is specified
  // by the `Impl` via the `transformBlock` function.
  template <typename Transformation = ql::identity>
  void pushBlock(IdTableStatic<NumStaticCols> block) {
    waitForFuture();
    if (block.empty()) {
      if (numBlocksPushed_ > 0) {
        // NOTE: In `transformAndPushLastBlock` we assert that if at least one
        // block has been pushed, then `compressAndWriteFuture_` is valid.
        // Therefore, we have to set a valid future here, even if it does
        // nothing.
        setFuture(std::async(std::launch::deferred, []() {}));
      }
      return;
    }
    ++numBlocksPushed_;
    setFuture(std::async(
        std::launch::async, [block = std::move(block), this]() mutable {
          blockTransformation_(block);
          this->writer_.writeIdTable(std::move(block).toDynamic());
        }));
  }

  // If there is less than one complete block (meaning that the number of calls
  // to `push` was `< blocksize_`), apply the transformation to `currentBlock_`
  // and return `false`. Else, push the `currentBlock_` via `pushBlock_`, block
  // until the pushing is actually finished, and return `true`. Using this
  // function allows for an efficient usage of this class for very small inputs.
  bool transformAndPushLastBlock() {
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
      blockTransformation_(this->currentBlock_);
      return false;
    }
    pushBlock(std::move(this->currentBlock_));
    resetCurrentBlock(false);
    waitForFuture();
    return true;
  }
};

// This class allows the external and compressed storing of an `IdTable` that is
// too large to be stored in RAM. `NumStaticCols == 0` means that the `IdTable`
// is stored dynamically (see `IdTable.h` and `CallFixedSize.h` for details).
// The interface is as follows: First there is one call to `push` for each row
// of the `IdTable`, and then there is one single call to `getRows` which yields
// a generator that yields the rows that have previously been pushed.
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
      return joinBlocks(InputRangeTypeErased<Block>{lazySingleValueRange(
          [this]() { return std::move(this->currentBlock_); })});
    }
    this->pushBlock(std::move(this->currentBlock_));
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
// the IdTable, and then there is one single call to `getRows` which yields a
// generator that yields the sorted rows one by one.

// When using very small block sizes in unit tests, then sometimes there are
// false positives in the memory limit mechanism, so setting the following
// variable to `true` allows to disable the memory limit.
inline std::atomic<bool>
    EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = false;

// The implementation of sorting a single block
template <typename Comparator>
struct BlockSorter {
  [[no_unique_address]] Comparator comparator_{};
  template <typename T>
  void operator()(T& block) {
#ifdef _PARALLEL_SORT
    ad_utility::parallel_sort(std::begin(block), std::end(block), comparator_);
#else
    ql::ranges::sort(block, comparator_);
#endif
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
  // The number of merged blocks that are buffered during the
  //  output phase.
  int numBufferedOutputBlocks_ = 4;

  // See the `moveResultOnMerge()` getter function for documentation.
  bool moveResultOnMerge_ = true;

  // The executor on which the merge phase runs, together with the number of
  // threads that run it.
  boost::asio::any_io_executor mergeExecutor_ =
      parallelBlockMerge::defaultMergeExecutor();
  size_t mergeParallelism_ = parallelBlockMerge::defaultMergeParallelism();

  // Set as soon as the warning about a reduced parallelism (see
  // `makeMergeOptions`) was logged, such that it is logged at most once per
  // sorter.
  std::atomic<bool> reducedParallelismWasLogged_ = false;

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

    // Explanation for the second argument of `runStreamAsync`: One block is
    // buffered by this generator, one block is buffered inside the
    // `sortedBlocks` generator, so `numBufferedOutputBlocks_ - 2` blocks may be
    // buffered by the async stream.
    using namespace ad_utility;
    return InputRangeTypeErased{
        CallbackOnEndView{ad_utility::streams::runStreamAsync(
                              sortedBlocks<N>(blocksize),
                              std::max(1, numBufferedOutputBlocks_ - 2)),
                          [&, this]() noexcept {
                            this->isFirstIteration_ = false;
                            mergeIsActive_.store(false);
                          }}};
  }

  // The implementation of the type-erased interface. Push a complete block at
  // once.
  void pushBlock(const IdTableStatic<0>& block) override {
    pushBlockImpl(block);
  }

  // The implementation of the type-erased interface. Push a complete block
  // given as a non-owning view at once.
  void pushBlock(const IdTableView<0>& block) override { pushBlockImpl(block); }

  // The implementation of the type-erased interface. Get the sorted blocks as
  // dynamic IdTables.
  ad_utility::InputRangeTypeErased<IdTableStatic<0>> getSortedOutput(
      std::optional<size_t> blocksize) override {
    return sortedBlocks<0>(blocksize);
  }

 private:
  // Common implementation for the two `pushBlock` overloads above.
  template <typename IdTableLike>
  void pushBlockImpl(const IdTableLike& block) {
    AD_CONTRACT_CHECK(block.numColumns() == this->numColumns_);
    ql::ranges::for_each(block,
                         [ptr = this](const auto& row) { ptr->push(row); });
  }

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
        return block.empty()
                   ? InputRangeTypeErased{ql::views::empty<IdTableStatic<N>>}
                   : InputRangeTypeErased{
                         lazySingleValueRange([this]() -> IdTableStatic<N> {
                           if (this->moveResultOnMerge_) {
                             return std::move(this->currentBlock_)
                                 .template toStatic<N>();
                           } else {
                             return this->currentBlock_.clone()
                                 .template toStatic<N>();
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
            curBlock.insertAtEnd(block, chunkStart, chunkStart + chunkSize);
            return IdTableStatic<N>(std::move(curBlock).template toStatic<N>());
          });
      return ad_utility::InputRangeTypeErased(std::move(chunked));
    }

    // Merge the presorted runs (which live compressed in the `writer_`) in
    // parallel, see `util/parallelBlockMerge/ParallelBlockMerge.h`.
    const size_t numRuns = this->writer_.numIdTables();
    const size_t blockSizeOutput =
        blocksize.value_or(computeBlockSizeForMergePhase(numRuns));
    auto merged =
        parallelBlockMerge::parallelBlockMergeToRange</*moveElements=*/true>(
            mergeExecutor_, CompressedIdTableRunsInput<N>{this->writer_},
            this->comparator_, makeMergeOptions(numRuns, blockSizeOutput),
            mergeParallelism_);
    return ad_utility::InputRangeTypeErased{
        checkedMergeResult<N>(std::move(merged))};
  }

  // Compute the options of the parallel merge phase. The size of the output
  // blocks is the `blockSizeOutput` that was computed by the caller, and the
  // number of chunks that are merged concurrently is derived from the memory
  // limit: a single in-flight chunk needs one decompressed input block per run
  // plus one output block.
  //
  // TODO<joka921> The interplay of these two numbers is currently the
  // bottleneck of the merge phase, because `blockSizeOutput` comes from
  // `computeBlockSizeForMergePhase`, which was written for the *serial* merge
  // and therefore spends the whole memory limit on very few, very large output
  // blocks. Each in-flight chunk then costs one such block, so only a handful
  // of chunks fit into the limit and most of the threads of the merge executor
  // stay idle. Measured for 48 million rows in 16 runs of 4 columns each with a
  // memory limit of 192 MB (times are for the merge phase only, relative to the
  // serial merge, on a machine with 32 cores):
  //
  //   output block   chunks in flight   speedup with 16 threads
  //   1476562 rows   3                  2.9x   (the current default)
  //    750000 rows   7                  5.1x
  //    100000 rows   30                 1.5x
  //
  // So the default leaves roughly a factor of two on the table, but simply
  // dividing the output block size by the degree of parallelism overshoots:
  // with many small blocks the per-block overhead, the synchronization in the
  // sink, and the input blocks at the chunk boundaries (which are decompressed
  // by both of the adjacent chunks) dominate. The optimum was at *fewer*
  // concurrent chunks than there are threads, which suggests that the merge
  // saturates the memory bandwidth well before it runs out of threads. Sizing
  // the output blocks for the parallel case therefore needs its own formula
  // (and a floor on the block size), instead of inheriting the serial one.
  parallelBlockMerge::MergeOptions makeMergeOptions(size_t numRuns,
                                                    size_t blockSizeOutput) {
    parallelBlockMerge::MergeOptions options;
    // The number of rows is the only criterion for finishing an output block,
    // exactly as it was before the merge phase was parallelized.
    options.outputBlockSize =
        parallelBlockMerge::OutputBlockSize::numElements(blockSizeOutput);
    const MemorySize memoryPerOutputBlock =
        MemorySize::bytes(blockSizeOutput * this->numColumns_ * sizeof(Id));

    size_t maxInFlight = mergeParallelism_;
    // NOTE: When the memory limit is ignored (which is the case in many unit
    // tests that deliberately use tiny limits), the memory-derived cap is
    // skipped completely, because it would always collapse the merge to a
    // single chunk and the parallel code path would never be exercised.
    if (!EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING) {
      const MemorySize memoryPerChunk =
          numRuns * this->numColumns_ * this->writer_.blockSizeUncompressed() +
          memoryPerOutputBlock;
      const size_t bytesPerChunk = memoryPerChunk.getBytes();
      // NOTE: If not even a single chunk fits into the memory limit, then we
      // merge with a single chunk (which is exactly the serial merge that was
      // used before) instead of throwing. Note that this can only happen if the
      // caller explicitly specified a `blocksize`; if the `blockSizeOutput` was
      // derived from the memory limit, then `computeBlockSizeForMergePhase` has
      // already thrown "Insufficient memory for merging ..." in that case.
      const size_t numChunksThatFit =
          bytesPerChunk == 0 ? mergeParallelism_
                             : this->memory_.getBytes() / bytesPerChunk;
      maxInFlight =
          std::max<size_t>(1, std::min(numChunksThatFit, mergeParallelism_));
    }
    options.maxInFlightChunks = maxInFlight;
    // Warn (once per sorter) if the memory limit forces us to use less
    // parallelism than the merge executor offers.
    if (maxInFlight < mergeParallelism_ &&
        !reducedParallelismWasLogged_.exchange(true)) {
      AD_LOG_WARN << "The merge phase of the external sorter can only merge "
                  << maxInFlight << " chunks concurrently instead of the "
                  << mergeParallelism_
                  << " chunks that the available parallelism offers, because "
                     "of the memory limit of "
                  << this->memory_.asString()
                  << ". Increasing the memory limit will speed up the merge."
                  << std::endl;
    }
    return options;
  }

  // _____________________________________________________________
  void sortBlockInPlace(IdTableStatic<NumStaticCols>& block) const {
#ifdef _PARALLEL_SORT
    ad_utility::parallel_sort(block.begin(), block.end(), comparator_);
#else
    ql::ranges::sort(block, comparator_);
#endif
  }

  // A function with this name is needed by the mixin base class.
  void transformBlock(IdTableStatic<NumStaticCols>& block) const {
    sortBlockInPlace(block);
  }

  // Compute the size of the blocks that are yielded in the output phase. It is
  // computed from the total memory limit and the amount of memory required to
  // store one decompressed block from each presorted input.
  //
  // NOTE: This spends the whole memory limit on a few large blocks, which was
  // the right thing to do for the serial merge, but limits the number of
  // chunks that the parallel merge can keep in flight. See the `TODO` at
  // `makeMergeOptions` above for measurements.
  size_t computeBlockSizeForMergePhase(size_t numBlocksToMerge) {
    const size_t numColumns = this->numColumns_;
    MemorySize requiredMemoryForInputBlocks =
        numBlocksToMerge * numColumns * this->writer_.blockSizeUncompressed();
    if (EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING) {
      // For unit tests, always yield 5 outputs at once.
      return 5;
    } else {
      auto throwInsufficientMemory = [numBlocksToMerge]() {
        throw std::runtime_error{
            absl::StrCat("Insufficient memory for merging ", numBlocksToMerge,
                         " blocks. Please increase the memory settings")};
      };
      if (requiredMemoryForInputBlocks >= this->memory_) {
        throwInsufficientMemory();
      }
      using namespace ad_utility::memory_literals;
      // Don't use a too large output size.
      auto blockSizeOutputMemory =
          std::min((this->memory_ - requiredMemoryForInputBlocks) /
                       numBufferedOutputBlocks_,
                   maxOutputBlocksize_);

      size_t blockSizeForOutput =
          blockSizeOutputMemory.getBytes() / (sizeof(Id) * numColumns);
      // If blocks are smaller than this, the performance will probably be poor
      // because of the coroutine and vector resetting overhead.
      if (blockSizeForOutput <= 10'000) {
        throwInsufficientMemory();
      }
      return blockSizeForOutput;
    }
  }
};
}  // namespace ad_utility

#endif  // QLEVER_COMPRESSEDEXTERNALIDTABLE_H
