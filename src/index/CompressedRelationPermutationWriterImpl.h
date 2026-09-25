// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2021 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_

#include <algorithm>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/CompressedRelationHelpersImpl.h"
#include "index/CompressedRelationWriter.h"
#include "util/GlobalExecutor.h"
#include "util/ProgressBar.h"
#include "util/TaskQueueOnExecutor.h"

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
  ad_utility::TaskQueueOnExecutor blockCallbackQueue_{
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

  // The number of threads that write the blocks of large relations, see
  // `largeRelationBlockPool_` below.
  static constexpr size_t numThreadsForBlocksOfLargeRelations = 10;

  // The number of distinct `col1` IDs of each block of the large relation that
  // is currently written, in the order in which the blocks were scheduled (see
  // `scheduleBlockOfLargeRelation`). The counting is done by the background
  // tasks, so that the thread which drives this `PermutationWriter` never has
  // to look at the `col1` IDs itself. The counts are only folded into the
  // single number that the metadata needs once the relation is complete, see
  // `getAndResetNumDistinctCol1`. For small relations this stays empty,
  // because no metadata (and thus no such count) is needed for them.
  //
  // NOTE: This is a `std::deque` and not a `std::vector`, because the
  // background tasks write into its elements while the driving thread appends
  // further elements, and appending to a `deque` (unlike to a `vector`) leaves
  // the elements that are already there exactly where they are.
  std::deque<compressedRelationHelpers::DistinctIdCountOfBlock>
      distinctCol1Counts_;

  // The total time that this writer has spent waiting for the background tasks
  // that write the blocks of large relations, see
  // `waitForBlocksOfLargeRelation`. It is reported at the end of
  // `writePermutation`.
  ad_utility::Timer largeRelationBlockTimer_{ad_utility::Timer::Stopped};

  // The pool of threads on which the blocks of large relations are written,
  // see `scheduleBlockOfLargeRelation` below.
  //
  // This is deliberately a pool of its own and not the global thread pool.
  // Writing those blocks is one of the more expensive parts of the permutation
  // phase, and threads that do nothing else make that cost directly visible in
  // a profile. It also removes the reason to ever write the blocks in the
  // calling thread instead: a task of this pool may block while it waits for a
  // free slot in the (bounded) block write queue of `writer1_`, and blocking
  // these threads never keeps the global thread pool from draining that queue.
  boost::asio::thread_pool largeRelationBlockPool_{
      numThreadsForBlocksOfLargeRelations};

  // The tasks of the `largeRelationBlockQueue_` run concurrently, but
  // `writer1_` is not thread-safe, so the calls to its
  // `addBlockForLargeRelation` are serialized by this mutex. That is cheap,
  // because that function only does some bookkeeping and then hands the block
  // to the (asynchronous) block write queue of `writer1_`; all the expensive
  // work of a task (counting the distinct `col1` IDs and copying the block
  // into the `twinRelationSorter_`) happens outside of this mutex, see the
  // `twinSorterMutex_` below.
  std::mutex writer1Mutex_;

  // The `twinRelationSorter_` is not thread-safe either, so the pushes of the
  // blocks into it are serialized by a mutex of their own (and not by the
  // `writer1Mutex_` above, so that the two serialized sections don't block
  // each other).
  //
  // NOTE: In contrast to the section above, the work inside this mutex is
  // *not* cheap: it copies the complete block into the buffer of the sorter,
  // which is why this currently is the part of a task that limits how much
  // the tasks can actually run in parallel. A follow-up PR removes the mutex
  // again by letting the sorter accept concurrent pushes of blocks.
  std::mutex twinSorterMutex_;

  // The queue via which the writing of the blocks of large relations is
  // offloaded to the `largeRelationBlockPool_`. Its tasks may run concurrently
  // and in an arbitrary order, which is fine: the blocks of a relation may be
  // written in any order (the block metadata is sorted at the end, see
  // `getFinishedBlocks`), and the twin relation is re-sorted anyway.
  //
  // NOTE: This member is deliberately declared after all the members that its
  // tasks use (the `largeRelationBlockPool_` included), so that its destructor
  // (which waits for the pending tasks) runs before those members are
  // destroyed.
  ad_utility::TaskQueueOnExecutor largeRelationBlockQueue_{
      largeRelationBlockPool_.get_executor(),
      2 * numThreadsForBlocksOfLargeRelations,
      "Writing blocks of large relations"};

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
    // `makeRecyclingOwner`. One buffer is needed per block that is in flight,
    // plus the one that is currently filled.
    writer1_->enableBlockRecycling(
        largeRelationBlockQueue_.maxNumTasksInFlight() + 1);
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
    // `makeRecyclingOwner`. One buffer is needed per block that is in flight,
    // plus the one that is currently filled.
    writer1_->enableBlockRecycling(
        largeRelationBlockQueue_.maxNumTasksInFlight() + 1);
  }

  // Schedule the writing of a single block of the large relation with the given
  // `col0Id`. The `block` is a view of the rows of the block, and the `owner`
  // keeps those rows alive for as long as any of the scheduled tasks (or
  // `writer1_`) still looks at them.
  //
  // Two tasks are scheduled for each block, which run concurrently to each
  // other and to the tasks of the other blocks (that is the whole point of
  // them): One counts the distinct `col1` IDs of the block, the other copies
  // the block into the `twinRelationSorter_` (only if a pair of permutations
  // is written) and hands it to `writer1_`. Both of them scan the complete
  // block, which is why they are deliberately not merged into a single task.
  void scheduleBlockOfLargeRelation(IdTableView<0> block,
                                    BlockToWrite::Owner owner) {
    AD_CORRECTNESS_CHECK(!block.empty());
    Id col0Id = col0IdCurrentRelation_.value();
    ++numBlocksCurrentRel_;
    // Note: The counting task writes into this element, which is why it is
    // appended (and not assigned) here, before that task is scheduled.
    distinctCol1Counts_.emplace_back();
    auto* distinctCol1Count = &distinctCol1Counts_.back();
    // Note: This scan of the `col1` column is the reason why counting the
    // distinct `col1` IDs is done here and not by the thread that fills the
    // buffers (which is the bottleneck of the permutation writing).
    largeRelationBlockQueue_.push([block, owner, distinctCol1Count]() {
      *distinctCol1Count = compressedRelationHelpers::countDistinctIds(
          block.getColumn(compressedRelationHelpers::c1Idx));
    });
    largeRelationBlockQueue_.push(
        [this, block, owner = std::move(owner), col0Id]() mutable {
          using namespace compressedRelationHelpers;
          if constexpr (WritePair) {
            auto twinRelation = block;
            twinRelation.swapColumns(c1Idx, c2Idx);
            // Note: `pushBlock` inserts the columns of the `twinRelation`
            // contiguously, which is much faster than pushing the rows one by
            // one. The order in which the blocks end up in the sorter doesn't
            // matter, because they are sorted anyway.
            std::lock_guard lock{twinSorterMutex_};
            twinRelationSorter_.pushBlock(twinRelation);
          }
          std::lock_guard lock{writer1Mutex_};
          writer1_->addBlockForLargeRelation(
              col0Id, BlockToWrite{block, std::move(owner)});
        });
  }

  // Wrap a filled block buffer in a `shared_ptr`, which keeps the rows of the
  // block alive for as long as any of the background tasks (or `writer1_`)
  // still looks at them. Once the last of them is done with it, the buffer is
  // given back to the pool of `writer1_`, from where it is taken again for one
  // of the next blocks (see `takeRecycledBlock`), so that the same few buffers
  // are used over and over again and almost no allocations are needed.
  std::shared_ptr<IdTable> makeRecyclingOwner(IdTable buffer) {
    auto recycle = [writer = writer1_.get()](IdTable* table) {
      writer->recycleBlock(std::move(*table));
      delete table;
    };
    return std::shared_ptr<IdTable>{new IdTable{std::move(buffer)},
                                    std::move(recycle)};
  }

  // Wait for all the background tasks that write blocks of the current large
  // relation. This has to be called before `writer1_` or the
  // `twinRelationSorter_` are used by this thread, because those tasks use
  // both of them, see `scheduleBlockOfLargeRelation` above. It also has to be
  // called before the counts in the `distinctCol1Counts_` are read.
  void waitForBlocksOfLargeRelation() {
    largeRelationBlockTimer_.cont();
    largeRelationBlockQueue_.waitUntilAllTasksAreDone();
    largeRelationBlockTimer_.stop();
  }

  // Forget the distinct `col1` counts that were collected so far, so that the
  // counting starts from scratch for the next relation. May only be called
  // when no background task writes into them anymore, see
  // `waitForBlocksOfLargeRelation` above.
  void resetNumDistinctCol1() { distinctCol1Counts_.clear(); }

  // Return the number of distinct `col1` IDs of the relation that was just
  // completed, and reset the counting for the next relation. An ID that ends
  // one block and starts the next one is only counted once, which is why the
  // counts of the single blocks have to be folded in the order of the blocks.
  // May only be called when no background task writes into the counts anymore,
  // see `waitForBlocksOfLargeRelation` above.
  size_t getAndResetNumDistinctCol1() {
    using compressedRelationHelpers::bitsOfIdWithoutLocalVocab;
    size_t result = 0;
    std::optional<Id> lastCol1OfPreviousBlock;
    for (const auto& count : distinctCol1Counts_) {
      result += count.count_;
      if (lastCol1OfPreviousBlock.has_value() &&
          bitsOfIdWithoutLocalVocab(lastCol1OfPreviousBlock.value()) ==
              bitsOfIdWithoutLocalVocab(count.first_)) {
        --result;
      }
      lastCol1OfPreviousBlock = count.last_;
    }
    resetNumDistinctCol1();
    return result;
  }

  // Write the buffered rows of the current (large) relation as its next block.
  // The actual work is performed in the background (see
  // `scheduleBlockOfLargeRelation` above), so that the calling thread can
  // immediately go on filling the buffer for the next block. That buffer is
  // taken from the pool of recycled buffers of `writer1_`, to which the buffer
  // of this block is given back once it is no longer needed, so that the same
  // few buffers are used over and over again.
  void addBlockForLargeRelation() {
    if (relation_.empty()) {
      return;
    }
    auto owner = makeRecyclingOwner(std::move(relation_));
    auto block = owner->template asStaticView<0>();
    relation_ = writer1_->takeRecycledBlock(numColumns_, alloc_);
    relation_.clear();
    relation_.reserve(blocksize_);
    scheduleBlockOfLargeRelation(block, std::move(owner));
  }

  // Write the given rows of the current input block as the next block of the
  // current (large) relation, without copying them into the `relation_` buffer
  // first. The `block` has to be a view of the rows of `inputBlock_`, which is
  // kept alive until the block has been written. Apart from that, this behaves
  // exactly like `addBlockForLargeRelation` above; in particular the actual
  // work is also performed in the background. The `relation_` buffer has to be
  // empty, because its rows precede the rows of this block.
  void addBlockOfLargeRelationWithoutCopying(IdTableView<0> block) {
    AD_CORRECTNESS_CHECK(relation_.empty() && !block.empty());
    scheduleBlockOfLargeRelation(block, inputBlock_);
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
      // and the `twinRelationSorter_`, so the blocks that are still being
      // written in the background have to be completely written first.
      waitForBlocksOfLargeRelation();
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
    // The batch is written with `writer1_`, so the blocks of a large relation
    // that are still being written in the background have to be finished
    // first. Note that there never are such blocks here, because
    // `finishRelation` waits for them, but the check is cheap and makes the
    // invariant explicit.
    waitForBlocksOfLargeRelation();
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
    waitForBlocksOfLargeRelation();
    // Note: This is logged only now (and not directly after the final progress
    // string above), because the last blocks of the last relation are written
    // by the two calls above, so only now is the measurement complete.
    // Note: The `value()` (in microseconds) is used instead of the `msecs()`
    // of the other timers, because this wait is expected to be short and would
    // otherwise be truncated to zero.
    AD_LOG_INFO << "Time spent waiting for the background tasks that write the "
                   "blocks of large relations: "
                << ad_utility::Timer::toSeconds(
                       largeRelationBlockTimer_.value())
                << "s" << std::endl;

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
