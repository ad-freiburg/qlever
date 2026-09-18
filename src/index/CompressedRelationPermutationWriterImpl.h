// Copyright 2025 The QLever Authors, in particular:
//
// 2021 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_

#include <algorithm>
#include <boost/asio/strand.hpp>
#include <future>
#include <memory>
#include <optional>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/CompressedRelation.h"
#include "index/CompressedRelationHelpersImpl.h"
#include "util/AsyncTaskQueue.h"
#include "util/GlobalExecutor.h"
#include "util/ProgressBar.h"

// Set up the handling of small relations for the twin permutation.
// `AddBlockOfSmallRelationsToSwitched` receives a block of small relations from
// `writer1`, swaps columns 1 and 2, sorts the block by the resulting
// permutation and feeds the block to `writer2`.
struct CompressedRelationWriter::AddBlockOfSmallRelationsToSwitched {
  CompressedRelationWriter& writer_;

  void operator()(IdTable blockOfSmallRelations) const {
    using namespace compressedRelationHelpers;

    // We don't use the parallel twinRelationSorter to create the twin
    // relation as its overhead is far too high for small relations.
    blockOfSmallRelations.swapColumns(c1Idx, c2Idx);

    // We only need to sort by the columns of the triple + the graph
    // column, not the additional payload. The comparison is performed on the
    // bits of the `Id`s, which is much cheaper, see
    // `bitsOfIdWithoutLocalVocab`.
    auto pickBits = [](const auto& row) {
      return std::array{
          bitsOfIdWithoutLocalVocab(row[0]), bitsOfIdWithoutLocalVocab(row[1]),
          bitsOfIdWithoutLocalVocab(row[2]), bitsOfIdWithoutLocalVocab(row[3])};
    };
    auto compare = [&pickBits](const auto& a, const auto& b) {
      return pickBits(a) < pickBits(b);
    };
    ql::ranges::sort(blockOfSmallRelations, compare);
    AD_CORRECTNESS_CHECK(!blockOfSmallRelations.empty());
    // Note: it is important that we store these two IDs before moving the
    // `relation`, because the evaluation order of function arguments is
    // unspecified.
    auto firstCol0 = blockOfSmallRelations.at(0, 0);
    auto lastCol0 =
        blockOfSmallRelations.at(blockOfSmallRelations.numRows() - 1, 0);
    // NOTE: This function is called from within a task of the block write
    // queue of the other writer, which runs on the global thread pool. We
    // therefore must not `push` to the (bounded) block write queue of
    // `writer_`, because that push might block and thus occupy a thread of
    // that pool, which could deadlock the pool. Doing the work directly
    // instead is cheap, because we already are on a thread of the pool, and it
    // even saves the hop to another thread.
    writer_.compressAndWriteBlockInCallingThread(
        firstCol0, lastCol0, std::move(blockOfSmallRelations), false);
  }
};

// Helper that handles the queue of callbacks to be called for every block
// written.
struct BlockCallbackManager {
  const CompressedRelationWriter::PerBlockCallbacks perBlockCallbacks_;

  // A queue for the callbacks that have to be applied for each triple. It is
  // crucial that the callbacks are invoked one after the other and in the
  // order in which the blocks were pushed. The queue therefore runs on a
  // strand (and not directly on the global thread pool), which serializes the
  // tasks and runs them in the order in which they were posted. Note that the
  // latter guarantee requires that all the tasks are posted by a single
  // thread, which is the case here, because `passToBlockCallbacks` is only
  // called by the single thread that drives the `PermutationWriter`.
  ad_utility::AsyncTaskQueue blockCallbackQueue_{
      boost::asio::make_strand(ad_utility::globalExecutor()), 3,
      "Additional callbacks during permutation building"};
  ad_utility::Timer blockCallbackTimer_{ad_utility::Timer::Stopped};

  // Enqueue a call to each of the `perBlockCallbacks` for the current block.
  void passToBlockCallbacks(std::shared_ptr<const IdTableStatic<0>> block) {
    blockCallbackTimer_.cont();
    blockCallbackQueue_.push([block = std::move(block), this]() {
      for (auto& callback : perBlockCallbacks_) {
        callback(*block);
      }
    });
    blockCallbackTimer_.stop();
  }

  // Wait for the enqueued block callbacks to finish.
  void finishBlockCallbackQueue() {
    blockCallbackTimer_.cont();
    blockCallbackQueue_.finish();
    blockCallbackTimer_.stop();
  }
};

// `PermutationWriter` contains the actual logic for writing a single
// permutation or a pair of twin permutations (the twin is a permutation where
// column 1 and 2 have been switched).
template <bool WritePair>
struct CompressedRelationWriter::PermutationWriter {
  // The `IfPair` alias is used below to switch the attributes of the
  // `PermutationWriter` between writing a single permutation or a pair of
  // permutations. `std::monostate` is used for attributes which are not needed
  // in single permutation mode.
  template <typename TypeIfPair, typename TypeIfSingle = std::monostate>
  using IfPair = std::conditional_t<WritePair, TypeIfPair, TypeIfSingle>;

  qlever::KeyOrder permutation_;
  std::unique_ptr<CompressedRelationWriter> writer1_;
  IfPair<std::unique_ptr<CompressedRelationWriter>> writer2_;

  using MetadataWriter =
      IfPair<compressedRelationHelpers::PairMetadataWriter,
             compressedRelationHelpers::SingleMetadataWriter>;
  MetadataWriter writeMetadata_;

  const size_t blocksize_{writer1_->blocksize()};
  const size_t numColumns_{writer1_->numColumns()};
  size_t numDistinctCol0_ = 0;

  ad_utility::Timer inputWaitTimer_{ad_utility::Timer::Stopped};
  IfPair<ad_utility::Timer> largeTwinRelationTimer_;

  std::optional<Id> col0IdCurrentRelation_;
  ad_utility::AllocatorWithLimit<ValueId> alloc_{
      ad_utility::makeUnlimitedAllocator<Id>()};

  // TODO<joka921> Use call_fixed_size if there is benefit to it.
  IdTable relation_{numColumns_, alloc_};
  size_t numBlocksCurrentRel_ = 0;

  // The input block that is currently being processed (see
  // `writePermutation`). It is held via a `shared_ptr`, because a block of a
  // large relation may be written directly from it, without copying its rows
  // into the `relation_` buffer first. The writing happens in the background,
  // so it has to keep the input block alive, see
  // `addBlockOfLargeRelationWithoutCopying`.
  std::shared_ptr<const IdTableStatic<0>> inputBlock_;

  using TwinRelationSorter = ad_utility::CompressedExternalIdTableSorter<
      compressedRelationHelpers::ComparatorForConstCol0, 0>;
  IfPair<TwinRelationSorter> twinRelationSorter_;

  BlockCallbackManager blockCallbackManager_;

  // The number of distinct `col1` IDs of the large relation that is currently
  // written. It is accumulated from the counts that the background tasks
  // compute for the individual blocks (see `writeBlockOfLargeRelation`), so
  // that the thread which drives this `PermutationWriter` never has to look at
  // the `col1` IDs itself. For small relations it stays zero, because no
  // metadata (and thus no such count) is needed for them.
  size_t numDistinctCol1_ = 0;
  // The last `col1` ID of the block of the current relation that was counted
  // last. An ID that ends one block and starts the next one must not be counted
  // twice, see `addDistinctCol1CountOfBlock`.
  std::optional<Id> lastCol1OfPreviousBlock_;

  // Whether the writing of the blocks of large relations is offloaded to a
  // background thread, see `addBlockForLargeRelation` below. This requires the
  // global thread pool to have more than one thread, because such a background
  // task may block while it waits for a free slot in the (bounded) block write
  // queue of `writer1_`, so that at least one other thread of the pool has to
  // remain available to drain that queue.
  const bool offloadLargeRelationBlocks_ =
      ad_utility::globalExecutorNumThreads() > 1;

  // The result of `writeBlockOfLargeRelation` below.
  struct WrittenBlock {
    // An empty block buffer whose memory is already allocated, so that it can
    // be reused for one of the next blocks without a further allocation. It is
    // only set if the block that was written owned its rows, because only then
    // a buffer was consumed that has to be replaced.
    std::optional<IdTable> buffer_;
    // The number of distinct `col1` IDs in the block that was written.
    compressedRelationHelpers::DistinctIdCountOfBlock distinctCol1Count_;
  };

  // The result of the background task that currently writes a block of a large
  // relation, if there is one, see `addBlockForLargeRelation`.
  std::optional<std::future<WrittenBlock>> pendingBlockOfLargeRelation_;
  ad_utility::Timer largeRelationBlockTimer_{ad_utility::Timer::Stopped};

  // The queue via which the writing of the blocks of large relations is
  // offloaded to the global thread pool. At most one of its tasks is in flight
  // at any time, because the caller always waits for the previous task before
  // it submits the next one. The tasks are therefore automatically serialized,
  // which is required, as they use `writer1_` and the `twinRelationSorter_`.
  //
  // NOTE: This member is deliberately declared after all the members that its
  // tasks use, so that its destructor (which waits for a pending task) runs
  // before those members are destroyed.
  ad_utility::AsyncTaskQueue largeRelationBlockQueue_{
      ad_utility::globalExecutor(), 2, "Writing blocks of large relations"};

  size_t numTriplesProcessed_ = 0;
  ad_utility::ProgressBar progressBar_{numTriplesProcessed_,
                                       "Triples sorted: "};
  // Whether the progress bar above is displayed, see the constructor for a
  // single permutation below.
  bool showProgressBar_ = true;

  // Constructor for a `PermutationWriter` which writes pair of permutations.
  CPP_template(bool doWritePair = WritePair)(requires doWritePair)
      PermutationWriter(const std::string& basename,
                        WriterAndCallback writerAndCallback1,
                        WriterAndCallback writerAndCallback2,
                        qlever::KeyOrder permutation,
                        PerBlockCallbacks perBlockCallbacks)
      : permutation_{std::move(permutation)},
        writer1_{std::move(writerAndCallback1.writer_)},
        writer2_{std::move(writerAndCallback2.writer_)},
        writeMetadata_{std::move(writerAndCallback1.callback_),
                       std::move(writerAndCallback2.callback_),
                       writer1_->blocksize()},
        largeTwinRelationTimer_{ad_utility::Timer::Stopped},
        twinRelationSorter_{basename + ".twin-twinRelationSorter", numColumns_,
                            4_GB, alloc_},
        blockCallbackManager_{std::move(perBlockCallbacks)} {
    static_assert(WritePair);
    // This logic only works for permutations that have the graph as the fourth
    // column.
    AD_CORRECTNESS_CHECK(permutation_.keys().at(3) == 3);

    AD_CORRECTNESS_CHECK(blocksize_ == writer2_->blocksize());
    AD_CORRECTNESS_CHECK(numColumns_ == writer2_->numColumns());
    AD_CORRECTNESS_CHECK(blocksize_ > 0);

    writer1_->smallBlocksCallback_ =
        AddBlockOfSmallRelationsToSwitched{*writer2_};
    // The buffers of the blocks of large relations are reused, see
    // `addBlockForLargeRelation`.
    writer1_->enableBlockRecycling();
  }

  // Constructor for a `PermutationWriter` which writes a single permutation.
  // With `showProgressBar` set to `false`, the progress of this writer is not
  // displayed, which is for callers that display the progress themselves (see
  // `CompressedRelationWriter::createPermutation`).
  CPP_template(bool doWritePair = WritePair)(requires(!doWritePair))
      PermutationWriter(WriterAndCallback writerAndCallback1,
                        qlever::KeyOrder permutation,
                        PerBlockCallbacks perBlockCallbacks,
                        bool showProgressBar = true)
      : permutation_{std::move(permutation)},
        writer1_{std::move(writerAndCallback1.writer_)},
        writeMetadata_{std::move(writerAndCallback1.callback_),
                       writer1_->blocksize()},
        blockCallbackManager_{std::move(perBlockCallbacks)},
        showProgressBar_{showProgressBar} {
    static_assert(!WritePair);
    // This logic only works for permutations that have the graph as the fourth
    // column.
    AD_CORRECTNESS_CHECK(permutation_.keys().at(3) == 3);
    AD_CORRECTNESS_CHECK(blocksize_ > 0);
    // The buffers of the blocks of large relations are reused, see
    // `addBlockForLargeRelation`.
    writer1_->enableBlockRecycling();
  }

  // The actual work of `addBlockForLargeRelation` below: Count the distinct
  // `col1` IDs of the `relation`, push it into the twin sorter for `writer2`
  // (only if a pair of permutations is written), and add it as a block of the
  // large relation with the given `col0Id` to `writer1`.
  //
  // NOTE: This function is typically run on a background thread (see
  // `addBlockForLargeRelation`). It is the only user of `writer1_` and of the
  // `twinRelationSorter_` while it runs, because the caller waits for it
  // before it touches either of them again.
  WrittenBlock writeBlockOfLargeRelation(Id col0Id, BlockToWrite relation) {
    using namespace compressedRelationHelpers;
    const bool ownsRows = relation.ownsRows();
    auto view = relation.view();
    // Note: This scan of the `col1` column is the reason why counting the
    // distinct `col1` IDs is done here and not by the thread that fills the
    // buffers (which is the bottleneck of the permutation writing).
    auto distinctCol1Count = countDistinctIds(view.getColumn(c1Idx));
    if constexpr (WritePair) {
      auto twinRelation = view;
      twinRelation.swapColumns(c1Idx, c2Idx);
      // Note: `pushBlock` inserts the columns of the `twinRelation`
      // contiguously, which is much faster than pushing the rows one by one.
      twinRelationSorter_.pushBlock(twinRelation);
    }
    writer1_->addBlockForLargeRelation(col0Id, std::move(relation));
    std::optional<IdTable> buffer;
    if (ownsRows) {
      buffer = writer1_->takeRecycledBlock(numColumns_, alloc_);
    }
    return {std::move(buffer), distinctCol1Count};
  }

  // Add the distinct `col1` count of a single block of the current relation to
  // the `numDistinctCol1_`. An ID that ends the previous block and starts this
  // one is only counted once.
  void addDistinctCol1CountOfBlock(
      const compressedRelationHelpers::DistinctIdCountOfBlock& count) {
    using compressedRelationHelpers::bitsOfIdWithoutLocalVocab;
    numDistinctCol1_ += count.count_;
    if (lastCol1OfPreviousBlock_.has_value() &&
        bitsOfIdWithoutLocalVocab(lastCol1OfPreviousBlock_.value()) ==
            bitsOfIdWithoutLocalVocab(count.first_)) {
      --numDistinctCol1_;
    }
    lastCol1OfPreviousBlock_ = count.last_;
  }

  // Forget the distinct `col1` count that was accumulated so far, so that the
  // counting starts from scratch for the next relation.
  void resetNumDistinctCol1() {
    numDistinctCol1_ = 0;
    lastCol1OfPreviousBlock_.reset();
  }

  // Return the number of distinct `col1` IDs of the relation that was just
  // completed, and reset the counting for the next relation.
  size_t getAndResetNumDistinctCol1() {
    size_t result = numDistinctCol1_;
    resetNumDistinctCol1();
    return result;
  }

  // Wait for the background task that writes the previous block of a large
  // relation, if there is such a task. This has to be called before `writer1_`
  // or the `twinRelationSorter_` are used again, because the background task
  // uses both of them, see `writeBlockOfLargeRelation` above. If the task
  // yields a block buffer, then that buffer is handed back to `writer1_`, from
  // where it is taken again for one of the next blocks.
  void flushPendingBlockOfLargeRelation() {
    if (!pendingBlockOfLargeRelation_.has_value()) {
      return;
    }
    largeRelationBlockTimer_.cont();
    // Note: `get()` rethrows an exception that the background task has thrown.
    WrittenBlock written = pendingBlockOfLargeRelation_.value().get();
    pendingBlockOfLargeRelation_.reset();
    largeRelationBlockTimer_.stop();
    addDistinctCol1CountOfBlock(written.distinctCol1Count_);
    if (written.buffer_.has_value()) {
      writer1_->recycleBlock(std::move(written.buffer_).value());
    }
  }

  // Write a block of a large relation with `writer1` and also push the block
  // into the twin sorter for `writer2`. The actual work is performed on a
  // background thread (see `writeBlockOfLargeRelation` above), so that the
  // calling thread can already fill the buffer for the next block in the
  // meantime. The `relation_` buffer is passed to that background thread,
  // which in return yields the buffer of the previous block, so that the same
  // two buffers are alternately used and no further allocations are needed.
  // The background thread also counts the distinct `col1` IDs of the block,
  // see `writeBlockOfLargeRelation` above.
  void addBlockForLargeRelation() {
    if (relation_.empty()) {
      return;
    }
    Id col0Id = col0IdCurrentRelation_.value();
    if (offloadLargeRelationBlocks_) {
      // Wait for the previously offloaded block, whose buffer we fill next.
      flushPendingBlockOfLargeRelation();
      pendingBlockOfLargeRelation_ = largeRelationBlockQueue_.submit(
          [this, col0Id, relation = std::move(relation_)]() mutable {
            return writeBlockOfLargeRelation(col0Id, std::move(relation));
          });
      relation_ = writer1_->takeRecycledBlock(numColumns_, alloc_);
    } else {
      WrittenBlock written =
          writeBlockOfLargeRelation(col0Id, std::move(relation_));
      addDistinctCol1CountOfBlock(written.distinctCol1Count_);
      relation_ = std::move(written.buffer_).value();
    }
    relation_.clear();
    relation_.reserve(blocksize_);
    ++numBlocksCurrentRel_;
  }

  // Write the given rows of the current input block as the next block of the
  // current (large) relation, without copying them into the `relation_` buffer
  // first. The `block` has to be a view of the rows of `inputBlock_`, which is
  // kept alive until the block has been written. Apart from that, this behaves
  // exactly like `addBlockForLargeRelation` above; in particular the actual
  // work is also performed on a background thread. The `relation_` buffer has
  // to be empty, so that the blocks of the relation are still written in the
  // correct order.
  void addBlockOfLargeRelationWithoutCopying(IdTableView<0> block) {
    AD_CORRECTNESS_CHECK(relation_.empty() && !block.empty());
    Id col0Id = col0IdCurrentRelation_.value();
    BlockToWrite blockToWrite{std::move(block), inputBlock_};
    if (offloadLargeRelationBlocks_) {
      // Wait for the previously offloaded block, because the blocks of a
      // relation have to be written in order.
      flushPendingBlockOfLargeRelation();
      pendingBlockOfLargeRelation_ = largeRelationBlockQueue_.submit(
          [this, col0Id, blockToWrite = std::move(blockToWrite)]() mutable {
            return writeBlockOfLargeRelation(col0Id, std::move(blockToWrite));
          });
    } else {
      WrittenBlock written =
          writeBlockOfLargeRelation(col0Id, std::move(blockToWrite));
      addDistinctCol1CountOfBlock(written.distinctCol1Count_);
    }
    ++numBlocksCurrentRel_;
  }

  // We have encountered the last occurrence of the current relation (value for
  // column 0). Thus we need to write the remaining buffered rows and metadata.
  // This also resets counters and buffers for writing the next relation.
  void finishRelation() {
    // The relation was already written completely by
    // `writeCompleteSmallRelations` below, which has also already done all the
    // bookkeeping, so there is nothing left to do.
    if (!col0IdCurrentRelation_.has_value()) {
      AD_CORRECTNESS_CHECK(relation_.empty() && numBlocksCurrentRel_ == 0);
      return;
    }
    ++numDistinctCol0_;
    if (numBlocksCurrentRel_ > 0 || static_cast<double>(relation_.numRows()) >
                                        0.8 * static_cast<double>(blocksize_)) {
      // The relation is large;
      addBlockForLargeRelation();
      // All the remaining work of this function uses `writer1_`, `writer2_`,
      // and the `twinRelationSorter_`, so the block that was just offloaded
      // has to be completely written first.
      flushPendingBlockOfLargeRelation();
      auto md1 = writer1_->finishLargeRelation(getAndResetNumDistinctCol1());
      if constexpr (WritePair) {
        largeTwinRelationTimer_.cont();
        auto md2 = writer2_->addCompleteLargeRelation(
            col0IdCurrentRelation_.value(),
            twinRelationSorter_.getSortedBlocks(blocksize_));
        largeTwinRelationTimer_.stop();
        twinRelationSorter_.clear();
        writeMetadata_(md1, md2);
      } else {
        writeMetadata_(md1);
      }
    } else {
      // Small relations are written in one go. Note: No metadata is computed
      // or stored for them, so no distinct `col1` count is needed here. It is
      // zero anyway, because it is only computed for the blocks of large
      // relations, but reset it to make that explicit.
      resetNumDistinctCol1();
      writer1_->addSmallRelation(col0IdCurrentRelation_.value(), relation_);
      // We don't need to do anything for the twin permutation and writer2,
      // because we have set up `writer1.smallBlocksCallback_` to do that work
      // for us (see above).
    }
    relation_.clear();
    numBlocksCurrentRel_ = 0;
  }

  // ___________________________________________________________________________
  void logTimers() const {
    AD_LOG_TIMING << "Time spent waiting for the input "
                  << ad_utility::Timer::toSeconds(inputWaitTimer_.msecs())
                  << "s" << std::endl;
    AD_LOG_TIMING << "Time spent waiting for writer1's queue "
                  << ad_utility::Timer::toSeconds(
                         writer1_->blockWriteQueueTimer_.msecs())
                  << "s" << std::endl;
    if constexpr (WritePair) {
      AD_LOG_TIMING << "Time spent waiting for writer2's queue "
                    << ad_utility::Timer::toSeconds(
                           writer2_->blockWriteQueueTimer_.msecs())
                    << "s" << std::endl;
      AD_LOG_TIMING << "Time spent waiting for large twin relations "
                    << ad_utility::Timer::toSeconds(
                           largeTwinRelationTimer_.msecs())
                    << "s" << std::endl;
    }
    AD_LOG_TIMING << "Time spent waiting for the blocks of large relations "
                  << ad_utility::Timer::toSeconds(
                         largeRelationBlockTimer_.msecs())
                  << "s" << std::endl;
    AD_LOG_TIMING
        << "Time spent waiting for triple callbacks (e.g. the next sorter) "
        << ad_utility::Timer::toSeconds(
               blockCallbackManager_.blockCallbackTimer_.msecs())
        << "s" << std::endl;
  }

  // Return the index of the first row in `rows[begin, end)` whose first three
  // columns differ from the `lastTriple`, or `end` if there is no such row.
  // This is the first position at which a new block for a large relation may
  // be started, because equal triples (when disregarding the graph and the
  // payload columns) have to stay in the same block.
  //
  // Note: The rows are compared row-wise (and on the bits of the `Id`s, which
  // is much cheaper, see `pickFirstThreeColumnsOfIdsWithoutLocalVocab`).
  // Scanning each of the three columns separately and taking the minimum of
  // the resulting offsets would use the cache more efficiently, but it would
  // also be slower if one of the columns is (almost) constant. This function
  // is only called once a complete block of a large relation has been found,
  // and it then typically finds a differing row immediately, so it is not a
  // bottleneck (unless in a very degenerate case) and we keep the simpler
  // implementation.
  template <typename Rows, typename Triple>
  static size_t findFirstTripleChange(const Rows& rows, size_t begin,
                                      size_t end, const Triple& lastTriple) {
    using compressedRelationHelpers::
        pickFirstThreeColumnsOfIdsWithoutLocalVocab;
    auto it = ql::ranges::find_if(
        rows.begin() + begin, rows.begin() + end,
        [&lastTriple](const auto& triple) { return triple != lastTriple; },
        pickFirstThreeColumnsOfIdsWithoutLocalVocab);
    return static_cast<size_t>(it - rows.begin());
  }

  // The special case of `findFirstTripleChange` above where the rows are
  // compared to the last triple that is currently buffered in `relation_`,
  // which therefore must not be empty.
  template <typename Rows>
  size_t findFirstTripleChangeAfterBuffer(const Rows& rows, size_t begin,
                                          size_t end) const {
    using compressedRelationHelpers::
        pickFirstThreeColumnsOfIdsWithoutLocalVocab;
    AD_CORRECTNESS_CHECK(!relation_.empty());
    return findFirstTripleChange(
        rows, begin, end,
        pickFirstThreeColumnsOfIdsWithoutLocalVocab(relation_.back()));
  }

  // Append the rows `[begin, end)` of `permutedCols`, which all belong to the
  // current relation (that is, they all have the same value for column 0), to
  // the `relation_` buffer. The rows are appended in chunks that are as large
  // as possible, which is much faster than appending them one by one, because
  // the `IdTable`s are stored column-based. A new block for a large relation
  // is started whenever the buffer has reached the `blocksize_` and the first
  // three columns change (see `findFirstTripleChange` above).
  //
  // Note: This function is always called for the rows of a large relation, but
  // also for the rows of a small relation that spans several input blocks,
  // because in that case we don't know yet that the relation will be small
  // (see `isCompleteSmallRelation` below). It therefore has to work correctly
  // for both cases.
  template <typename PermutedCols>
  void addRowsOfCurrentRelation(const PermutedCols& permutedCols, size_t begin,
                                size_t end) {
    using compressedRelationHelpers::
        pickFirstThreeColumnsOfIdsWithoutLocalVocab;
    while (begin < end) {
      // Determine the largest chunk of rows that may be appended in one go.
      size_t chunkEnd;
      if (relation_.numRows() < blocksize_) {
        // If the buffer is empty and the remaining rows suffice for a complete
        // block, then that block can be written directly from the input block,
        // without copying its rows into the buffer first (which for a large
        // relation is a substantial part of the work of this thread). This
        // requires that the block can be completed within the current input
        // block, which is exactly the case if a triple change is found before
        // the end of the rows; otherwise further equal triples may follow in
        // the next input block, which then have to end up in the same block.
        if (relation_.empty() && end - begin >= blocksize_) {
          size_t blockEnd =
              findFirstTripleChange(permutedCols, begin + blocksize_, end,
                                    pickFirstThreeColumnsOfIdsWithoutLocalVocab(
                                        permutedCols[begin + blocksize_ - 1]));
          if (blockEnd < end) {
            addBlockOfLargeRelationWithoutCopying(
                permutedCols.subView(begin, blockEnd - begin));
            increaseTripleCounter(blockEnd - begin);
            begin = blockEnd;
            continue;
          }
        }
        // The buffer is not yet full, so we can simply append the rows that
        // are missing to reach the `blocksize_`.
        chunkEnd = std::min(end, begin + (blocksize_ - relation_.numRows()));
      } else {
        // The buffer is full, so we may only append rows that are equal to the
        // last buffered row with respect to the first three columns, because
        // equal triples have to stay in the same block.
        chunkEnd = findFirstTripleChangeAfterBuffer(permutedCols, begin, end);
        if (chunkEnd == begin) {
          // The very next row is already different, so the block is complete.
          // The next iteration then starts filling a fresh buffer.
          addBlockForLargeRelation();
          continue;
        }
        // Otherwise the buffer deliberately grows beyond the `blocksize_`. If
        // `chunkEnd < end`, then the block is completed by the branch above in
        // the very next iteration. If `chunkEnd == end`, then we have to leave
        // the block open, because the rows of this relation may continue in
        // the next input block with further equal triples, which then have to
        // end up in the same block. Such a block is eventually written either
        // by the next call to this function or by `finishRelation`.
      }
      relation_.insertAtEnd(permutedCols, begin, chunkEnd);
      increaseTripleCounter(chunkEnd - begin);
      begin = chunkEnd;
    }
  }

  // Return true if the rows `[begin, end)` of the current input block form a
  // complete relation that has to be written as a small relation. That is the
  // case if the relation neither has started in a previous input block (then
  // `relation_` would be non-empty, or blocks for it would already have been
  // written), nor may continue in the next one (then the run would extend to
  // the end of the block), and if it is small enough. The criterion for being
  // small is exactly the one that `finishRelation` above uses.
  bool isCompleteSmallRelation(size_t begin, size_t end,
                               size_t numRowsOfBlock) const {
    return relation_.empty() && numBlocksCurrentRel_ == 0 &&
           end < numRowsOfBlock &&
           static_cast<double>(end - begin) <=
               0.8 * static_cast<double>(blocksize_);
  }

  // Return the end of the run of rows of the input block that starts at row
  // `begin` and consists of all rows that have the same `col0` ID as that row.
  // Such a run is exactly the part of one relation that is contained in the
  // current input block.
  //
  // Note: The `col0` column is sorted, so the end of the run could be found by
  // a binary search. That would be much slower than a linear scan for the
  // short runs of the many small relations though, so we use an exponential
  // ("galloping") search: The step size is doubled until the end of the run
  // has been passed, and only the remaining range is then searched
  // binarily. That way a short run is found with a handful of comparisons,
  // while a long run (a large relation, whose run may span the complete input
  // block) doesn't require a linear scan of its `col0` column. The comparison
  // is performed on the bits of the `Id`s, which is much cheaper, see
  // `bitsOfIdWithoutLocalVocab`.
  template <typename Col0>
  static size_t findEndOfRun(const Col0& col0, size_t begin) {
    using compressedRelationHelpers::bitsOfIdWithoutLocalVocab;
    const auto col0Bits = bitsOfIdWithoutLocalVocab(col0[begin]);
    auto belongsToRun = [col0Bits](Id id) {
      return bitsOfIdWithoutLocalVocab(id) == col0Bits;
    };
    const size_t size = static_cast<size_t>(col0.size());
    // Invariant of the following loop: The row `lastInRun` still belongs to
    // the run, and `firstAfterRun` is an upper bound for the end of the run
    // (it either is the end of the column, or a row that no longer belongs to
    // the run).
    size_t lastInRun = begin;
    size_t firstAfterRun = size;
    for (size_t step = 1; size - lastInRun > step; step *= 2) {
      size_t next = lastInRun + step;
      if (!belongsToRun(col0[next])) {
        firstAfterRun = next;
        break;
      }
      lastInRun = next;
    }
    auto it = std::partition_point(col0.begin() + lastInRun + 1,
                                   col0.begin() + firstAfterRun, belongsToRun);
    return static_cast<size_t>(it - col0.begin());
  }

  // Write the maximal batch of consecutive complete small relations that
  // starts with the rows `[begin, firstRunEnd)` of `permutedCols` directly to
  // `writer1_`, and return the first row of the input block that is not part
  // of that batch. The batch consists of the relation `[begin, firstRunEnd)`,
  // which the caller has to have checked to be a complete small relation (see
  // `isCompleteSmallRelation` above), plus all directly following complete
  // small relations that still fit into the current block of small relations
  // of `writer1_`.
  //
  // Writing the batch has two advantages over writing its relations one by
  // one: All its rows are copied into `writer1_`'s buffer with a single
  // `insertAtEnd`, which is much faster for relations with only a handful of
  // rows, and the `relation_` buffer is bypassed completely, so that the rows
  // are copied only once in total. All the bookkeeping that `finishRelation`
  // does for a small relation is performed here as well.
  template <typename PermutedCols, typename Col0>
  size_t writeCompleteSmallRelations(const PermutedCols& permutedCols,
                                     const Col0& col0, size_t begin,
                                     size_t firstRunEnd,
                                     size_t numRowsOfBlock) {
    AD_CORRECTNESS_CHECK(begin < firstRunEnd);
    // The number of rows that the batch may hold at most. Note that
    // `addSmallRelations` starts a new block if the batch doesn't fit into the
    // current one. So if not even the first relation fits, then that new block
    // is started in any case, and the capacity of a complete fresh block is
    // available for the batch. That way a batch is never cut short just
    // because the current block happens to be almost full, while the resulting
    // blocks are still exactly the same as if the relations were written one
    // by one.
    size_t capacity = writer1_->numRowsUntilSmallRelationBlockIsFull();
    if (firstRunEnd - begin > capacity) {
      capacity = writer1_->smallRelationBlockCapacity();
    }

    // Greedily extend the batch by the following relations, as long as they
    // are complete small relations that still fit. Note that the first
    // relation is always part of the batch, even in the (currently impossible,
    // see `isCompleteSmallRelation`) case that it exceeds the capacity all by
    // itself. The loop is always left via one of the `break`s, because a run
    // that reaches the end of the input block is never a complete small
    // relation, which is exactly what the check at the beginning of the loop
    // body asserts. That check also guarantees that the indexing of `col0`
    // inside `findEndOfRun` is safe.
    size_t end = firstRunEnd;
    size_t numRelations = 1;
    Id lastCol0Id = col0[begin];
    for (;;) {
      AD_CORRECTNESS_CHECK(end < numRowsOfBlock);
      size_t nextEnd = findEndOfRun(col0, end);
      if (!isCompleteSmallRelation(end, nextEnd, numRowsOfBlock) ||
          nextEnd - begin > capacity) {
        break;
      }
      lastCol0Id = col0[end];
      end = nextEnd;
      ++numRelations;
    }

    numDistinctCol0_ += numRelations;
    // The batch is written with `writer1_`, so a block of a large relation that
    // is still being written in the background has to be finished first. Note
    // that there never is such a block here, because `finishRelation` waits for
    // it, but the check is cheap and makes the invariant explicit.
    flushPendingBlockOfLargeRelation();
    // Note: The distinct `col1` IDs are deliberately not counted here, because
    // no metadata is stored for small relations (see `addSmallRelations`). The
    // count is zero for these relations anyway, but reset it, so that a future
    // relaxation of `isCompleteSmallRelation` cannot silently corrupt the count
    // of the next large relation.
    resetNumDistinctCol1();
    writer1_->addSmallRelations(col0IdCurrentRelation_.value(), lastCol0Id,
                                permutedCols, begin, end);
    // We don't need to do anything for the twin permutation and writer2,
    // because we have set up `writer1.smallBlocksCallback_` to do that work
    // for us (see above).
    increaseTripleCounter(end - begin);
    // All relations of the batch are complete, so the next run of the input
    // starts a new relation and `finishRelation` has nothing left to do for
    // the last relation of the batch.
    col0IdCurrentRelation_.reset();
    return end;
  }

  // ___________________________________________________________________________
  void increaseTripleCounter(size_t numTriples) {
    numTriplesProcessed_ += numTriples;
    if (showProgressBar_ && progressBar_.update()) {
      AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
    }
  }

  // Get the indices of all columns in the order in which they have to be added
  // to the relation.
  std::vector<ColumnIndex> getPermutedColIndices() const {
    auto [c0, c1, c2, c3] = permutation_.keys();
    std::vector<ColumnIndex> permutedColIndices{c0, c1, c2};
    for (size_t colIdx = 3; colIdx < numColumns_; ++colIdx) {
      permutedColIndices.push_back(colIdx);
    }
    return permutedColIndices;
  }

  // This function actually writes the permutation using the blocks of rows from
  // the input range `sortedTriples`. This should only be called once on a
  // `PermutationWriter` object.
  IfPair<PermutationPairResult, PermutationSingleResult> writePermutation(
      ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples) {
    using compressedRelationHelpers::bitsOfIdWithoutLocalVocab;
    inputWaitTimer_.cont();

    auto col0 = permutation_.keys().at(0);

    for (auto& blockFromInput : AD_FWD(sortedTriples)) {
      AD_CORRECTNESS_CHECK(blockFromInput.numColumns() == numColumns_);
      inputWaitTimer_.stop();
      // This only happens when the index is completely empty.
      if (blockFromInput.empty()) {
        continue;
      }
      // The input block is shared, because blocks of large relations may be
      // written directly from it, see `addBlockOfLargeRelationWithoutCopying`.
      inputBlock_ =
          std::make_shared<const IdTableStatic<0>>(std::move(blockFromInput));
      const auto& block = *inputBlock_;
      auto firstCol = block.getColumn(col0);
      auto permutedCols = block.asColumnSubsetView(getPermutedColIndices());
      if (!col0IdCurrentRelation_.has_value()) {
        col0IdCurrentRelation_ = firstCol[0];
      }

      // The input is sorted by `col0`, so the block consists of consecutive
      // runs of rows that all belong to the same relation. We first determine
      // the extent of such a run by looking at `col0` only, and then handle
      // the run as a whole instead of row by row. That way the columns can be
      // copied contiguously, and we often know the fate of the complete
      // relation before touching any of its data (see
      // `isCompleteSmallRelation` and `addRowsOfCurrentRelation` above).
      size_t runBegin = 0;
      while (runBegin < block.numRows()) {
        Id col0Id = firstCol[runBegin];
        if (!col0IdCurrentRelation_.has_value() ||
            bitsOfIdWithoutLocalVocab(col0IdCurrentRelation_.value()) !=
                bitsOfIdWithoutLocalVocab(col0Id)) {
          finishRelation();
          col0IdCurrentRelation_ = col0Id;
        }
        size_t runEnd = findEndOfRun(firstCol, runBegin);
        // If the complete relation is already known here, and it is small,
        // then we can write it without buffering it in `relation_` first,
        // together with as many of the directly following relations as
        // possible.
        if (isCompleteSmallRelation(runBegin, runEnd, block.numRows())) {
          runBegin = writeCompleteSmallRelations(
              permutedCols, firstCol, runBegin, runEnd, block.numRows());
        } else {
          addRowsOfCurrentRelation(permutedCols, runBegin, runEnd);
          runBegin = runEnd;
        }
      }
      blockCallbackManager_.passToBlockCallbacks(std::move(inputBlock_));
      inputWaitTimer_.cont();
    }
    if (showProgressBar_) {
      AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;
    }
    inputWaitTimer_.stop();
    if (!relation_.empty() || numBlocksCurrentRel_ > 0) {
      finishRelation();
    }
    flushPendingBlockOfLargeRelation();

    writer1_->finish();
    if constexpr (WritePair) {
      writer2_->finish();
    }
    blockCallbackManager_.finishBlockCallbackQueue();
    logTimers();
    if constexpr (WritePair) {
      return PermutationPairResult{numDistinctCol0_,
                                   std::move(*writer1_).getFinishedBlocks(),
                                   std::move(*writer2_).getFinishedBlocks()};
    } else {
      return PermutationSingleResult{numDistinctCol0_,
                                     std::move(*writer1_).getFinishedBlocks()};
    }
  }
};

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_
