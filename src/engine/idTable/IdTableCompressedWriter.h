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
#include "util/http/beast.h"

// A class that stores a  compressed  sequence of `IdTable`s  in a file. These
// tables all have the same number of columns, so they can be thought of as
// large blocks of a very large `IdTable` which is formed by the concatenation
// of the single tables.
class IdTableCompressedWriter {
  // Metadata for a compressed block of bytes.
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
  // The metadata of a single column of the IdTable (each column is split up
  // into several blocks).
  using ColumnMetadata = std::vector<CompressedBlockMetadata>;
  // The metadata of each column.
  std::vector<ColumnMetadata> blocksPerColumn_;
  // For each contained `IdTable` contains the index in the `ColumnMetadata`
  // where the blocks of this table begin.
  std::vector<size_t> startOfSingleIdTables;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  // Each column of each `IdTable` will be split up into blocks of this size and
  // then separately compressed and stored. Has to be chosen s.t. it is much
  // smaller than the size of the single `IdTables` and  large enough to make
  // the used compression algorithm work well.
  ad_utility::MemorySize blockSizeCompression_ =
      ad_utility::MemorySize::megabytes(4ul);

 public:
  // Constructor. The file at `filename` will be overwritten. Each of the
  // `IdTables` that will be passed in has to have exactly `numCols` columns.
  explicit IdTableCompressedWriter(std::string filename, size_t numCols,
                                   ad_utility::AllocatorWithLimit<Id> allocator)
      : filename_{std::move(filename)},
        blocksPerColumn_(numCols),
        allocator_{std::move(allocator)} {}

  // Destructor. Deletes the stored file.
  ~IdTableCompressedWriter() {
    file_.wlock()->close();
    // TODO<joka921> Get it back in.
    // ad_utility::deleteFile(filename_);
  }

  // Simple getters for the stored allocator and the number of columns;
  const auto& allocator() const { return allocator_; }
  size_t numColumns() const { return blocksPerColumn_.size(); }

  // Store an `idTable`.
  void writeIdTable(const IdTable& table) {
    AD_CONTRACT_CHECK(table.numColumns() == numColumns());
    size_t blockSizeInBytes = blockSizeCompression_.getBytes() / sizeof(Id);
    AD_CONTRACT_CHECK(blockSizeInBytes > 0);
    startOfSingleIdTables.push_back(blocksPerColumn_.at(0).size());
    // The columns are compressed and stored in parallel.
    // TODO<joka921> Use parallelism per block instead of per column (more
    // fine-grained) but only once we have a reasonable abstraction for
    // parallelism.
    std::vector<std::future<void>> compressColumFutures;
    for (auto i : std::views::iota(0u, numColumns())) {
      compressColumFutures.push_back(
          std::async(std::launch::async, [this, i, blockSizeInBytes, &table]() {
            auto& blockMetadata = blocksPerColumn_.at(i);
            decltype(auto) column = table.getColumn(i);
            // TODO<C++23> Use `std::views::chunk`
            for (size_t lower = 0; lower < column.size();
                 lower += blockSizeInBytes) {
              size_t upper = std::min(lower + blockSizeInBytes, column.size());
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

 public:
  // Return a vector of generators where the `i-th` generator generates the
  // `i-th` IdTable that was stored. The IdTables are yielded in (smaller)
  // blocks which are `IdTables` themselves.
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

  // Return a vector of generators where the `i-th` generator generates the
  // `i-th` IdTable that was stored. The IdTables are yielded row by row.
  template <size_t N = 0>
  std::vector<
      cppcoro::generator<typename IdTableStatic<N>::const_row_reference>>
  getAllRowGenerators() {
    file_.wlock()->flush();
    std::vector<decltype(makeGeneratorForRows<N>(0))> result;
    result.reserve(startOfSingleIdTables.size());
    for (auto i : std::views::iota(0u, startOfSingleIdTables.size())) {
      result.push_back(makeGeneratorForRows<N>(i));
    }
    return result;
  }

 private:
  // Get the row generator for a single IdTable, specified by the `index`.
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

  // Get the block generator for a single IdTable, specified by the `index`.
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
      block.reserve(blockSizeCompression_.getBytes() / sizeof(Id));
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

// This class allows the external (on-disk) sorting of an `IdTable` that is too
// large to be stored in RAM. `NumStaticCols == 0` means that the IdTable is
// stored dynamically (see `IdTable.h` and `CallFixedSize.h` for details). The
// interface is as follows: First there is one call to `push` for each row of
// the IdTable, and then there is one single call to `sortedView` which yields a
// generator that yields the sorted rows one by one.
template <typename Comparator, size_t NumStaticCols>
class ExternalIdTableSorter {
 private:
  boost::asio::thread_pool threadPool_{4};
  using Strand = boost::asio::strand<boost::asio::thread_pool::executor_type>;
  Strand writeStrand_{threadPool_.get_executor()};
  // Used to aggregate rows for the next block.
  IdTableStatic<NumStaticCols> currentBlock_;
  // The number of rows per block.
  size_t blocksize_;
  IdTableCompressedWriter writer_;
  Comparator comp_{};
  // Used to compress and write a block on a background thread while we can
  // already collect the rows for the next block.
  std::future<void> sortAndWriteFuture_;

 public:
  //
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

 public:
  // Add a single row to the input. The type of `row` needs to be something that
  // can be `push_back`ed to a `IdTable`.
  void push(const auto& row) requires requires { currentBlock_.push_back(row); }
  {
    currentBlock_.push_back(row);
    if (currentBlock_.size() >= blocksize_) {
      // TODO<joka921> Also do this in parallel.
      pushBlock(std::move(currentBlock_));
      // TODO<joka921> The move operations of the `IdTable` currently have wrong
      // semantics as they don't restore the empty column vectors.
      currentBlock_ = IdTableStatic<NumStaticCols>(writer_.allocator());
      if constexpr (NumStaticCols == 0) {
        currentBlock_.setNumColumns(row.size());
      }
      currentBlock_.reserve(blocksize_);
    }
  }

  /// Return a lambda that takes a `ValueType` and calls `push` for that value.
  /// Note that `this` is captured by reference
  auto makePushCallback() {
    return [this](auto&& value) { push(AD_FWD(value)); };
  }

  /// Transition from the input phase, where `push()` may be called, to the
  /// output phase and return a generator that yields the sorted elements. This
  /// function may be called exactly once.
  cppcoro::generator<
      const typename IdTableStatic<NumStaticCols>::const_row_reference>
  sortedView() {
    for (const auto& block : sortedBlocks()) {
      for (const auto& row : block) {
        co_yield row;
      }
    }
  }

  /// Transition from the input phase, where `push()` may be called, to the
  /// output phase and return a generator that yields the sorted elements. This
  /// function may be called exactly once.
  cppcoro::generator<const IdTableStatic<NumStaticCols>> sortedBlocks() {
    pushBlock(std::move(currentBlock_));
    if (sortAndWriteFuture_.valid()) {
      sortAndWriteFuture_.get();
    }
    auto rowGenerators = writer_.getAllRowGenerators<NumStaticCols>();
    using P = std::pair<decltype(rowGenerators[0].begin()),
                        decltype(rowGenerators[0].end())>;
    auto projection = [](const auto& el) -> decltype(auto) {
      return *el.first;
    };
    // TODO<joka921> Debug why we can't use `std::ranges::min_element` here.
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

 private:
  void pushBlock(IdTableStatic<NumStaticCols> block) {
    if (sortAndWriteFuture_.valid()) {
      sortAndWriteFuture_.get();
    }
    if (block.empty()) {
      return;
    }
    /*
    auto sortAndWrite = [this](auto block) -> boost::asio::awaitable<void> {
// TODO<joka921> We probably need one thread for sorting and one for writing for
// the maximal performance.
#ifdef USE_PARALLEL
      ad_utility::parallel_sort(block.begin(), block.end(), comp_);
#else
      std::ranges::sort(block, comp_);
#endif
      //co_await boost::asio::dispatch(writeStrand_,
boost::asio::use_awaitable); writer_.writeIdTable(std::move(block).toDynamic());
      co_return;
    };
    sortAndWriteFuture_ =
        boost::asio::co_spawn(threadPool_.get_executor(),
sortAndWrite(std::move(block)), boost::asio::use_future);
*/
    sortAndWriteFuture_ = std::async(
        std::launch::async, [block = std::move(block), this]() mutable {
// TODO<joka921> We probably need one thread for sorting and one for writing for
// the maximal performance.
#ifdef USE_PARALLEL
          ad_utility::parallel_sort(block.begin(), block.end(), comp_);
#else
          std::ranges::sort(block, comp_);
#endif
          writer_.writeIdTable(std::move(block).toDynamic());
        });
  }
};

#endif  // QLEVER_IDTABLECOMPRESSEDWRITER_H
