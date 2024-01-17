//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDEXTERNALIDTABLE_H
#define QLEVER_COMPRESSEDEXTERNALIDTABLE_H

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
#include "util/TransparentFunctors.h"
#include "util/Views.h"
#include "util/http/beast.h"

namespace ad_utility {

using namespace ad_utility::memory_literals;

// The default size for compressed blocks in the following classes.
static constexpr ad_utility::MemorySize DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE =
    500_kB;
// A class that stores a sequence of `IdTable`s in a file. Each `IdTable` is
// compressed blockwise. Typically, the blocksize is much smaller than the size
// of a single IdTable, such that there are multiple blocks per IdTable. This is
// an important building block for an external merge sort implementation where
// we want very large presorted `IdTables` over which we need to incrementally
// iterate (hence the smaller blocks for compression). These tables all have the
// same number of columns, so they can be thought of as large blocks of a very
// large `IdTable` which is formed by the concatenation of the single tables.
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
  ad_utility::MemorySize blockSizeUncompressed_ =
      DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE;

  // Keep track of the number of active output generators to detect whether we
  // are currently reading from the file and it is thus unsafe to add to the
  // contents.
  size_t numActiveGenerators_ = 0;

 public:
  // Constructor. The file at `filename` will be overwritten. Each of the
  // `IdTables` that will be passed in has to have exactly `numCols` columns.
  explicit CompressedExternalIdTableWriter(
      std::string filename, size_t numCols,
      ad_utility::AllocatorWithLimit<Id> allocator,
      ad_utility::MemorySize blockSizeUncompressed =
          DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE)
      : filename_{std::move(filename)},
        blocksPerColumn_(numCols),
        allocator_{std::move(allocator)},
        blockSizeUncompressed_(blockSizeUncompressed) {}

  // Destructor. Deletes the stored file.
  ~CompressedExternalIdTableWriter() {
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
    if (numActiveGenerators_ != 0) {
      AD_THROW(
          "Trying to call `writeIdTable` on an "
          "`CompressedExternalIdTableWriter` that is currently being iterated "
          "over");
    }
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
              auto compressed = ZstdWrapper::compress(
                  column.data() + lower, thisBlockSizeUncompressed);
              size_t offset = 0;
              file_.withWriteLock(
                  [&offset, &compressed](ad_utility::File& file) {
                    offset = file.tell();
                    file.write(compressed.data(), compressed.size());
                  });
              blockMetadata.emplace_back(compressed.size(),
                                         thisBlockSizeUncompressed, offset);
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
    // Note: As soon as we drop the support for GCC11 this can be
    // `return getAllRowGenerators<N>() | std::views::join;
    return std::views::join(ad_utility::OwningView{getAllRowGenerators<N>()});
  }

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
    std::ranges::for_each(blocksPerColumn_, [](auto& block) { block.clear(); });
    startOfSingleIdTables_.clear();
  }

 private:
  // Get the row generator for a single IdTable, specified by the `index`.
  template <size_t N = 0>
  auto makeGeneratorForRows(size_t index) {
    return std::views::join(
        ad_utility::OwningView{makeGeneratorForIdTable<N>(index)});
  }
  // Get the block generator for a single IdTable, specified by the `index`.
  template <size_t NumCols = 0>
  cppcoro::generator<const IdTableStatic<NumCols>> makeGeneratorForIdTable(
      size_t index) {
    // The following line has the effect that `writeIdTable` and `clear` also
    // throw an exception if a generator has been created but not yet
    // started.
    ++numActiveGenerators_;
    return makeGeneratorForIdTableImpl<NumCols>(
        index, absl::Cleanup{[this] { --numActiveGenerators_; }});
  }

  // The actual implementation of `makeGeneratorForIdTable` above.
  template <size_t NumCols = 0>
  cppcoro::generator<const IdTableStatic<NumCols>> makeGeneratorForIdTableImpl(
      size_t index, auto cleanup) {
    using Table = IdTableStatic<NumCols>;
    auto firstBlock = startOfSingleIdTables_.at(index);
    auto lastBlock = index + 1 < startOfSingleIdTables_.size()
                         ? startOfSingleIdTables_.at(index + 1)
                         : blocksPerColumn_.at(0).size();
    std::future<Table> fut;

    // Yield one block after the other. While one block is yielded the next
    // block is already read concurrently.
    for (size_t blockIdx = firstBlock; blockIdx < lastBlock; ++blockIdx) {
      std::optional<Table> table;
      if (fut.valid()) {
        table = fut.get();
      }
      fut = std::async(std::launch::async, [this, blockIdx]() {
        return readBlock<NumCols>(blockIdx);
      });
      if (table.has_value()) {
        co_yield table.value();
      }
    }
    // Last block.
    AD_CORRECTNESS_CHECK(fut.valid());
    auto table = fut.get();
    co_yield table;
    --numActiveGenerators_;
    std::move(cleanup).Cancel();
  }

  // Decompresses the block at the given `blockIdx`. The
  // individual columns are decompressed concurrently.
  template <size_t NumCols = 0>
  IdTableStatic<NumCols> readBlock(size_t blockIdx) {
    IdTableStatic<NumCols> block{numColumns(), allocator_};
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
            auto numBytesRead =
                file_.wlock()->read(compressed.data(), metaData.compressedSize_,
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
};

// The common base implementation of `CompressedExternalIdTable` and
// `CompressedExternalIdTableSorter` (see below). It is implemented as a mixin
// class.
template <size_t NumStaticCols, std::invocable<IdTableStatic<NumStaticCols>&>
                                    BlockTransformation = ad_utility::Noop>
class CompressedExternalIdTableBase {
 public:
  using value_type = IdTableStatic<NumStaticCols>::row_type;
  using reference = IdTableStatic<NumStaticCols>::row_reference;
  using const_reference = IdTableStatic<NumStaticCols>::const_row_reference;
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

  // Store whether this table has previously already been iterated over (in
  // which case this member becomes `false`).
  std::atomic<bool> isFirstIteration_ = true;

  [[no_unique_address]] BlockTransformation blockTransformation_{};

 public:
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
  void push(const auto& row) requires requires { currentBlock_.push_back(row); }
  {
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
    if (compressAndWriteFuture_.valid()) {
      compressAndWriteFuture_.get();
    }
    writer_.clear();
    numBlocksPushed_ = 0;
    isFirstIteration_ = true;
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
  template <typename Transformation = std::identity>
  void pushBlock(IdTableStatic<NumStaticCols> block) {
    if (compressAndWriteFuture_.valid()) {
      compressAndWriteFuture_.get();
    }
    if (block.empty()) {
      return;
    }
    ++numBlocksPushed_;
    compressAndWriteFuture_ = std::async(
        std::launch::async, [block = std::move(block), this]() mutable {
          blockTransformation_(block);
          this->writer_.writeIdTable(std::move(block).toDynamic());
        });
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
    // If we have pushed at least one (complete) block, then the last future
    // from pushing a block is still in flight. If we have never pushed a block,
    // then also the future cannot be valid.
    AD_CORRECTNESS_CHECK((numBlocksPushed_ == 0) !=
                         compressAndWriteFuture_.valid());
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
    if (compressAndWriteFuture_.valid()) {
      compressAndWriteFuture_.get();
    }
    return true;
  }
};

// This class allows the external and compressed storing of an `IdTable` that is
// too large to be stored in RAM. `NumStaticCols == 0` means that the IdTable is
// stored dynamically (see `IdTable.h` and `CallFixedSize.h` for details). The
// interface is as follows: First there is one call to `push` for each row of
// the IdTable, and then there is one single call to `getRows` which yields a
// generator that yields the rows that have previously been pushed.
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
  explicit CompressedExternalIdTable(
      std::string filename, ad_utility::MemorySize memory,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE)
      requires(NumStaticCols > 0)
      : CompressedExternalIdTable(std::move(filename), NumStaticCols, memory,
                                  std::move(allocator), blocksizeCompression) {}

  // Transition from the input phase, where `push()` may be called, to the
  // output phase and return a generator that yields the elements of the
  // `IdTable in the order that they were `push`ed. This function may be called
  // exactly once.
  auto getRows() {
    if (!this->transformAndPushLastBlock()) {
      // For the case of only a single block we have to mimick the exact same
      // return type as that of `writer_.getGeneratorForAllRows()`, that's why
      // there are the seemingly redundant multiple calls to
      // join(OwningView(vector(...))).
      auto generator = [](const auto& block)
          -> cppcoro::generator<const IdTableStatic<NumStaticCols>> {
        co_yield block;
      }(this->currentBlock_);
      auto rowView =
          std::views::join(ad_utility::OwningView{std::move(generator)});
      std::vector<decltype(rowView)> vec;
      vec.push_back(std::move(rowView));
      return std::views::join(ad_utility::OwningView(std::move(vec)));
    }
    this->pushBlock(std::move(this->currentBlock_));
    this->resetCurrentBlock(false);
    if (this->compressAndWriteFuture_.valid()) {
      this->compressAndWriteFuture_.get();
    }
    return this->writer_.template getGeneratorForAllRows<NumStaticCols>();
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
  // Get the sorted output after all blocks have been pushed. If `blocksize ==
  // nullopt`, the size of the returned blocks will be chosen automatically.
  virtual cppcoro::generator<IdTableStatic<0>> getSortedOutput(
      std::optional<size_t> blocksize = std::nullopt) = 0;
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
  void operator()(auto& block) {
#ifdef _PARALLEL_SORT
    ad_utility::parallel_sort(std::begin(block), std::end(block), comparator_);
#else
    std::ranges::sort(block, comparator_);
#endif
  }
};
// Deduction guide for the implicit aggregate initializtion (its  "constructor")
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
  CompressedExternalIdTableSorter(
      std::string filename, ad_utility::MemorySize memory,
      ad_utility::AllocatorWithLimit<Id> allocator,
      MemorySize blocksizeCompression = DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
      Comparator comp = {}) requires(NumStaticCols > 0)
      : CompressedExternalIdTableSorter(std::move(filename), NumStaticCols,
                                        memory, std::move(allocator),
                                        blocksizeCompression, comp) {}

  // Explicitly inherit the `push` function, such that we can use it unqualified
  // within this class.
  using Base::push;

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
  auto sortedView() {
    return std::views::join(ad_utility::OwningView{getSortedBlocks()});
  }

  // Similar to `sortedView` (see above), but the elements are yielded in
  // blocks. The size of the blocks is `blocksize` if specified, otherwise it
  // will be automatically determined from the given memory limit.
  template <size_t N = NumStaticCols>
  requires(N == NumStaticCols || N == 0)
  cppcoro::generator<IdTableStatic<N>> getSortedBlocks(
      std::optional<size_t> blocksize = std::nullopt) {
    // If we move the result out, there must only be a single merge phase.
    AD_CONTRACT_CHECK(this->isFirstIteration_ || !this->moveResultOnMerge_);
    mergeIsActive_.store(true);
    // Explanation for the second argument: One block is buffered by this
    // generator, one block is buffered inside the `sortedBlocks` generator, so
    // `numBufferedOutputBlocks_ - 2` blocks may be buffered by the async
    // stream.
    for (auto& block : ad_utility::streams::runStreamAsync(
             sortedBlocks<N>(blocksize),
             std::max(1, numBufferedOutputBlocks_ - 2))) {
      co_yield block;
    }
    this->isFirstIteration_ = false;
    mergeIsActive_.store(false);
  }

  // The implementation of the type-erased interface. Push a complete block at
  // once.
  void pushBlock(const IdTableStatic<0>& block) override {
    AD_CONTRACT_CHECK(block.numColumns() == this->numColumns_);
    std::ranges::for_each(block,
                          [ptr = this](const auto& row) { ptr->push(row); });
  }

  // The implementation of the type-erased interface. Get the sorted blocks as
  // dynamic IdTables.
  cppcoro::generator<IdTableStatic<0>> getSortedOutput(
      std::optional<size_t> blocksize) override {
    return getSortedBlocks<0>(blocksize);
  }

 private:
  // Transition from the input phase, where `push()` may be called, to the
  // output phase and return a generator that yields the sorted elements. This
  // function may be called exactly once.
  template <size_t N = NumStaticCols>
  requires(N == NumStaticCols || N == 0)
  cppcoro::generator<IdTableStatic<N>> sortedBlocks(
      std::optional<size_t> blocksize = std::nullopt) {
    if (!this->transformAndPushLastBlock()) {
      // There was only one block, return it. If a blocksize was explicitly
      // requested for the output, and the single block is larger than this
      // blocksize, we manually have to split it into chunks.
      auto& block = this->currentBlock_;
      const auto blocksizeOutput = blocksize.value_or(block.numRows());
      if (block.numRows() <= blocksizeOutput) {
        if (this->moveResultOnMerge_) {
          co_yield std::move(this->currentBlock_).template toStatic<N>();
        } else {
          auto blockAsStatic = IdTableStatic<N>(
              this->currentBlock_.clone().template toStatic<N>());
          co_yield blockAsStatic;
        }
      } else {
        // TODO<C++23> Use `std::views::chunk`.
        for (size_t i = 0; i < block.numRows(); i += blocksizeOutput) {
          size_t upper = std::min(i + blocksizeOutput, block.numRows());
          auto curBlock = IdTableStatic<NumStaticCols>(
              this->numColumns_, this->writer_.allocator());
          curBlock.reserve(upper - i);
          curBlock.insertAtEnd(block.begin() + i, block.begin() + upper);
          co_yield std::move(curBlock).template toStatic<N>();
        }
      }
      co_return;
    }
    auto rowGenerators =
        this->writer_.template getAllRowGenerators<NumStaticCols>();

    const size_t blockSizeOutput =
        blocksize.value_or(computeBlockSizeForMergePhase(rowGenerators.size()));

    using P = std::pair<decltype(rowGenerators[0].begin()),
                        decltype(rowGenerators[0].end())>;
    auto projection = [](const auto& el) -> decltype(auto) {
      return *el.first;
    };
    // NOTE: We have to switch the arguments, because the heap operations by
    // default order descending...
    auto comp = [&, this](const auto& a, const auto& b) {
      return comparator_(projection(b), projection(a));
    };
    std::vector<P> pq;

    for (auto& gen : rowGenerators) {
      pq.emplace_back(gen.begin(), gen.end());
    }
    std::ranges::make_heap(pq, comp);
    IdTableStatic<NumStaticCols> result(this->writer_.numColumns(),
                                        this->writer_.allocator());
    result.reserve(blockSizeOutput);
    size_t numPopped = 0;
    while (!pq.empty()) {
      std::ranges::pop_heap(pq, comp);
      auto& min = pq.back();
      result.push_back(*min.first);
      ++(min.first);
      if (min.first == min.second) {
        pq.pop_back();
      } else {
        std::ranges::push_heap(pq, comp);
      }
      if (result.size() >= blockSizeOutput) {
        numPopped += result.numRows();
        co_yield std::move(result).template toStatic<N>();
        // The `result` will be moved away, so we have to reset it again.
        result = IdTableStatic<NumStaticCols>(this->writer_.numColumns(),
                                              this->writer_.allocator());
        result.reserve(blockSizeOutput);
      }
    }
    numPopped += result.numRows();
    co_yield std::move(result).template toStatic<N>();
    AD_CORRECTNESS_CHECK(numPopped == this->numElementsPushed_);
  }

  // _____________________________________________________________
  void sortBlockInPlace(IdTableStatic<NumStaticCols>& block) const {
#ifdef _PARALLEL_SORT
    ad_utility::parallel_sort(block.begin(), block.end(), comparator_);
#else
    std::ranges::sort(block, comparator_);
#endif
  }

  // A function with this name is needed by the mixin base class.
  void transformBlock(IdTableStatic<NumStaticCols>& block) const {
    sortBlockInPlace(block);
  }

  // Compute the size of the blocks that are yielded in the output phase. It is
  // computed from the total memory limit and the amount of memory required to
  // store one decompressed block from each presorted input.
  size_t computeBlockSizeForMergePhase(size_t numBlocksToMerge) {
    MemorySize requiredMemoryForInputBlocks =
        numBlocksToMerge * this->numColumns_ *
        this->writer_.blockSizeUncompressed();
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
          blockSizeOutputMemory.getBytes() / (sizeof(Id) * this->numColumns_);
      // If blocks are smaller than this, the performance will probably be poor
      // because of the coroutine and vector resetting overhead.
      if (blockSizeForOutput <= 10'000) {
        throwInsufficientMemory();
      }
      return blockSizeForOutput;
    }
  };
};
}  // namespace ad_utility

#endif  // QLEVER_COMPRESSEDEXTERNALIDTABLE_H
