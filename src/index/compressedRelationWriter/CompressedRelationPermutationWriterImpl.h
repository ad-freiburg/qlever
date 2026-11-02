// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_

#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <deque>
#include <future>
#include <memory>
#include <optional>
#include <utility>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/compressedRelationWriter/CompressedRelationHelpersImpl.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "index/compressedRelationWriter/RelationBlockCutter.h"
#include "util/Algorithm.h"
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
    // bits of the `Id`s, which is much cheaper, see `pickBitsOfColumns`.
    auto compare = [](const auto& a, const auto& b) {
      auto pickBits =
          pickBitsOfColumns<c0Idx, c1Idx, c2Idx, ADDITIONAL_COLUMN_GRAPH_ID>;
      return pickBits(a) < pickBits(b);
    };
    ql::ranges::sort(blockOfSmallRelations, compare);
    AD_CORRECTNESS_CHECK(!blockOfSmallRelations.empty());
    // Note: it is important that we store these two IDs before moving the
    // `relation`, because the evaluation order of function arguments is
    // unspecified.
    Id firstCol0 = blockOfSmallRelations.at(0, 0);
    Id lastCol0 =
        blockOfSmallRelations.at(blockOfSmallRelations.numRows() - 1, 0);
    // NOTE: This function is called from within a task of the block write
    // queue of the other writer, which runs on the global thread pool. We
    // therefore must not `push` to the (bounded) block write queue of
    // `writer_`, because that push might block and thus occupy a thread of
    // that pool, which could deadlock the pool. Doing the work directly
    // instead is cheap, because we already are on a thread of the pool, and it
    // even saves the hop to another thread.
    writer_.compressAndWriteBlockInCallingThread(
        firstCol0, lastCol0, BlockToWrite{std::move(blockOfSmallRelations)},
        false);
  }
};

// Helper that handles the queue of callbacks to be called for every block
// written.
struct BlockCallbackManager {
  const CompressedRelationWriter::PerBlockCallbacks perBlockCallbacks_;

  // A queue for the callbacks that have to be applied for each triple. It runs
  // on a strand of the `executor` (and not directly on the `executor`), so
  // that the callbacks are invoked one after the other and in the order in
  // which the blocks were pushed (both is crucial).
  //
  // NOTE: A strand only preserves the order of tasks that are posted by a
  // single thread. This is the case here, because `passToBlockCallbacks` is
  // only called by the thread that drives the `PermutationWriter`.
  ad_utility::TaskQueueOnExecutor blockCallbackQueue_;

  // Run the `perBlockCallbacks` on a strand of the `executor`.
  BlockCallbackManager(
      CompressedRelationWriter::PerBlockCallbacks perBlockCallbacks,
      const qlever::Executor& executor)
      : perBlockCallbacks_{std::move(perBlockCallbacks)},
        blockCallbackQueue_{boost::asio::make_strand(executor), 3,
                            "Additional callbacks during permutation "
                            "building"} {}
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
// column 1 and 2 have been switched). The cutting of the input into relations
// and blocks is inherited from the `RelationBlockCutter`, see there for the
// hooks that are implemented below.
template <bool WritePair>
struct CompressedRelationWriter::PermutationWriter
    : RelationBlockCutter<
          CompressedRelationWriter::PermutationWriter<WritePair>> {
  // The `IfPair` alias is used below to switch the attributes of the
  // `PermutationWriter` between writing a single permutation or a pair of
  // permutations. `std::monostate` is used for attributes which are not needed
  // in single permutation mode.
  template <typename TypeIfPair, typename TypeIfSingle = std::monostate>
  using IfPair = std::conditional_t<WritePair, TypeIfPair, TypeIfSingle>;

  using Base = RelationBlockCutter<PermutationWriter>;
  using Base::blocksize_;
  using Base::col0IdCurrentRelation_;
  using Base::inputBlock_;
  using Base::numBlocksCurrentRel_;
  using Base::relation_;

  qlever::KeyOrder permutation_;
  std::unique_ptr<CompressedRelationWriter> writer1_;
  IfPair<std::unique_ptr<CompressedRelationWriter>> writer2_;

  using MetadataWriter =
      IfPair<compressedRelationHelpers::PairMetadataWriter,
             compressedRelationHelpers::SingleMetadataWriter>;
  MetadataWriter writeMetadata_;

  // The blocks that the `twinRelationSorter_` yields for a large relation are
  // this many times larger than the `blocksize_`, and
  // `CompressedRelationWriter::writeLargeRelationBlockInSlices` cuts them into
  // blocks of about `blocksize_` rows again.
  //
  // NOTE: Larger blocks reduce the per-block cost of the merge phase of that
  // sorter. The factor must not be too large, because each of those blocks
  // stays in memory until all of its slices have been written.
  static constexpr size_t twinSorterBlocksizeFactor_ = 16;
  const size_t numColumns_{writer1_->numColumns()};
  size_t numDistinctCol0_ = 0;

  ad_utility::Timer inputWaitTimer_{ad_utility::Timer::Stopped};
  IfPair<ad_utility::Timer> largeTwinRelationTimer_;

  ad_utility::AllocatorWithLimit<ValueId> alloc_{
      ad_utility::makeUnlimitedAllocator<Id>()};

  using TwinRelationSorter = ad_utility::CompressedExternalIdTableSorter<
      compressedRelationHelpers::ComparatorForConstCol0, 0>;
  IfPair<TwinRelationSorter> twinRelationSorter_;

  BlockCallbackManager blockCallbackManager_;

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
  // that write the blocks of large relations (for a free slot when scheduling
  // a block, see `scheduleBlockOfLargeRelation`, and for the completion of all
  // the tasks, see `waitForBlocksOfLargeRelation`). It is reported at the end
  // of `writePermutation`.
  ad_utility::Timer largeRelationBlockTimer_{ad_utility::Timer::Stopped};

  // The pending pushes of the blocks of the current large relation into the
  // `twinRelationSorter_` (see `scheduleBlockOfLargeRelation`), in the order in
  // which they were started. Only used if a pair of permutations is written.
  //
  // NOTE: This member is deliberately declared after the
  // `twinRelationSorter_`, and the destructor waits for these pushes, because
  // the sorter must not be destroyed while a push into it is in flight.
  std::deque<std::future<void>> twinRelationPushes_;

  // The queue via which the counting of the distinct `col1` IDs and the
  // writing of the blocks of large relations is offloaded to the global thread
  // pool. Its tasks may run concurrently and in an arbitrary order, which is
  // fine: the blocks of a relation may be written in any order (the block
  // metadata is sorted at the end, see `getFinishedBlocks`).
  //
  // NOTE 1: The queue is configured exactly like the `blockWriteQueue_` of
  // `writer1_`, so that the blocks of large relations run on the same executor
  // and with the same bound on the blocks in flight as all the other blocks (in
  // particular, the runtime index rebuild limits the concurrency of that
  // executor, see `rebuild-permutation-writer-num-threads`). Each block in
  // flight is held in memory until it has been written, so the bound of the
  // queue also bounds the memory that the background writing consumes.
  //
  // NOTE 2: The tasks never block (in particular they never wait for other
  // tasks of the global thread pool), which is what makes it safe to run them
  // on that pool. All the operations that may block (the bookkeeping of
  // `writer1_`, which may push to its bounded block write queue, and the
  // waiting for free slots in this queue) are performed by the thread that
  // drives this `PermutationWriter`, see `scheduleBlockOfLargeRelation`.
  //
  // NOTE 3: This member is deliberately declared after all the members that
  // its tasks use, so that its destructor (which waits for the pending tasks)
  // runs before those members are destroyed.
  ad_utility::TaskQueueOnExecutor largeRelationBlockQueue_{
      CompressedRelationWriter::makeBlockWriteQueue(
          writer1_->executor_, writer1_->numBlocksInFlight_)};

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
      : Base{makeBase(*writerAndCallback1.writer_)},
        permutation_{std::move(permutation)},
        writer1_{std::move(writerAndCallback1.writer_)},
        writer2_{std::move(writerAndCallback2.writer_)},
        writeMetadata_{std::move(writerAndCallback1.callback_),
                       std::move(writerAndCallback2.callback_),
                       writer1_->blocksize()},
        largeTwinRelationTimer_{ad_utility::Timer::Stopped},
        twinRelationSorter_{basename + ".twin-twinRelationSorter", numColumns_,
                            4_GB, alloc_},
        blockCallbackManager_{std::move(perBlockCallbacks),
                              writer1_->executor_} {
    static_assert(WritePair);
    // This logic only works for permutations that have the graph as the fourth
    // column.
    AD_CORRECTNESS_CHECK(permutation_.keys().at(3) == 3);

    AD_CORRECTNESS_CHECK(blocksize_ == writer2_->blocksize());
    AD_CORRECTNESS_CHECK(numColumns_ == writer2_->numColumns());

    writer1_->smallBlocksCallback_ =
        AddBlockOfSmallRelationsToSwitched{*writer2_};
    // The blocks of small relations of `writer1_` end up in `writer2_` (see
    // `AddBlockOfSmallRelationsToSwitched`), so their buffers are also given
    // back by `writer2_`, which therefore has to use the same pool.
    writer2_->shareBlockBufferPoolWith(*writer1_);
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
      : Base{makeBase(*writerAndCallback1.writer_)},
        permutation_{std::move(permutation)},
        writer1_{std::move(writerAndCallback1.writer_)},
        writeMetadata_{std::move(writerAndCallback1.callback_),
                       writer1_->blocksize()},
        blockCallbackManager_{std::move(perBlockCallbacks),
                              writer1_->executor_},
        showProgressBar_{showProgressBar} {
    static_assert(!WritePair);
    // This logic only works for permutations that have the graph as the fourth
    // column.
    AD_CORRECTNESS_CHECK(permutation_.keys().at(3) == 3);
  }

  // Return the `Base` for a `PermutationWriter` that writes with the `writer`
  // (the first one, if a pair is written).
  //
  // TODO<joka921> Use call_fixed_size for the `relation_` buffer if there is
  // benefit to it.
  static Base makeBase(const CompressedRelationWriter& writer) {
    return Base{
        writer.blocksize(),
        IdTable{writer.numColumns(), ad_utility::makeUnlimitedAllocator<Id>()}};
  }

  // Wait for the pending pushes into the `twinRelationSorter_` (see
  // `twinRelationPushes_`), because the sorter must not be destroyed while
  // they are in flight. This only does something if the writing was aborted by
  // an exception, otherwise `writePermutation` has already waited for them.
  ~PermutationWriter() {
    for (auto& push : twinRelationPushes_) {
      // Note: A push whose `get()` has already thrown is no longer `valid()`.
      if (!push.valid()) {
        continue;
      }
      try {
        push.get();
      } catch (...) {
        // The first exception has already been propagated (or is currently
        // being propagated), so further ones are ignored.
      }
    }
  }

  // Schedule the writing of a single block of the large relation with the given
  // `col0Id`. The `block` is a view of the rows of the block, and the `owner`
  // keeps those rows alive for as long as any of the scheduled tasks still
  // looks at them.
  //
  // Up to three operations are started for each block, which run concurrently
  // to each other and to the operations of the other blocks (that is the whole
  // point of them): One task counts the distinct `col1` IDs of the block,
  // another one compresses the block and writes it with `writer1_`, and (only
  // if a pair of permutations is written) the block is pushed into the
  // `twinRelationSorter_` via its `asyncPushBlock`.
  void scheduleBlockOfLargeRelation(IdTableView<0> block,
                                    BlockToWrite::Owner owner) {
    using namespace compressedRelationHelpers;
    AD_CORRECTNESS_CHECK(!block.empty());
    Id col0Id = col0IdCurrentRelation_.value();
    ++numBlocksCurrentRel_;
    // The bookkeeping of `writer1_` is not thread-safe and may block, so it is
    // done here, and the task below only compresses and writes the block.
    writer1_->prepareBlockForLargeRelation(col0Id, block.numRows());
    if constexpr (WritePair) {
      // Bound the number of pending pushes (and thus the number of blocks that
      // they keep alive) by the same number as the tasks of the
      // `largeRelationBlockQueue_`.
      if (twinRelationPushes_.size() >=
          largeRelationBlockQueue_.maxNumTasksInFlight()) {
        largeRelationBlockTimer_.cont();
        twinRelationPushes_.front().get();
        largeRelationBlockTimer_.stop();
        twinRelationPushes_.pop_front();
      }
      auto twinRelation = block;
      twinRelation.swapColumns(c1Idx, c2Idx);
      // The aliasing `shared_ptr` keeps the rows (via the `owner`) alive until
      // the push has completed. The order in which the blocks end up in the
      // sorter doesn't matter, because they are sorted anyway.
      using OwnerAndView = std::pair<BlockToWrite::Owner, IdTableView<0>>;
      auto ownerAndView =
          std::make_shared<OwnerAndView>(owner, std::move(twinRelation));
      std::shared_ptr<const IdTableView<0>> twinRelationPtr{
          ownerAndView, &ownerAndView->second};
      twinRelationPushes_.push_back(twinRelationSorter_.asyncPushBlock(
          std::move(twinRelationPtr), boost::asio::use_future));
    }
    // Note: The counting task writes into this element, which is why it is
    // appended (and not assigned) here, before that task is scheduled. The
    // pointer stays valid while further elements are appended, because
    // `distinctCol1Counts_` is a `std::deque`, see its declaration.
    distinctCol1Counts_.emplace_back();
    auto* distinctCol1Count = &distinctCol1Counts_.back();
    // Note: This task could be merged with the task below, but two smaller
    // tasks can be spread more evenly across the threads of the pool. The
    // pushes block while the queue is full, which is time spent waiting for
    // the background tasks, hence the timer.
    largeRelationBlockTimer_.cont();
    largeRelationBlockQueue_.push([block, owner, distinctCol1Count]() {
      *distinctCol1Count = countDistinctIds(block.getColumn(c1Idx));
    });
    largeRelationBlockQueue_.push(
        [this, block, owner = std::move(owner), col0Id]() mutable {
          // This is a block of a large relation, so we don't invoke the
          // `smallBlocksCallback_`. Hence the last argument is `false`.
          writer1_->compressAndWriteBlockInCallingThread(
              col0Id, col0Id, BlockToWrite{block, std::move(owner)}, false);
        });
    largeRelationBlockTimer_.stop();
  }

  // Wait for all the background operations that write blocks of the current
  // large relation (see `scheduleBlockOfLargeRelation` above), and rethrow the
  // first exception of a push into the `twinRelationSorter_`, if any. This has
  // to be called before `writer1_` is finished or the `twinRelationSorter_` is
  // reused by this thread. It also has to be called before the counts in the
  // `distinctCol1Counts_` are read.
  void waitForBlocksOfLargeRelation() {
    largeRelationBlockTimer_.cont();
    largeRelationBlockQueue_.waitUntilAllTasksAreDone();
    while (!twinRelationPushes_.empty()) {
      // Note: The element is only removed after its `get()`, so that the
      // destructor still waits for the remaining pushes if `get()` throws.
      twinRelationPushes_.front().get();
      twinRelationPushes_.pop_front();
    }
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
    compressedRelationHelpers::DistinctIdCounter counter;
    for (const auto& countOfBlock : distinctCol1Counts_) {
      counter.addCountOfBlock(countOfBlock);
    }
    resetNumDistinctCol1();
    return counter.getAndReset();
  }

  // Write the buffered rows of the current (large) relation as its next block.
  // The actual work is performed in the background (see
  // `scheduleBlockOfLargeRelation` above), so that the calling thread can
  // immediately go on filling the buffer for the next block. The buffer is
  // given back to the block buffer pool of `writer1_` once all the background
  // tasks and `writer1_` are done with it, and the buffer for the next block is
  // taken from that pool, so that the same few buffers are used over and over
  // again and almost no allocations are needed.
  void addBlockForLargeRelation() {
    if (relation_.empty()) {
      return;
    }
    auto owner = BlockBufferPool::makeRecyclingOwner(
        writer1_->blockBufferPool(), std::move(relation_));
    auto block = owner->template asStaticView<0>();
    relation_ = writer1_->takeBlockBuffer();
    scheduleBlockOfLargeRelation(std::move(block), std::move(owner));
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
    // `RelationBlockCutter::writeCompleteSmallRelations` (via
    // `addCompleteSmallRelations` below), which has also already done all the
    // bookkeeping, so there is nothing left to do.
    if (!col0IdCurrentRelation_.has_value()) {
      AD_CORRECTNESS_CHECK(relation_.empty() && numBlocksCurrentRel_ == 0);
      return;
    }
    ++numDistinctCol0_;
    if (this->currentRelationIsLarge()) {
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
            twinRelationSorter_.getSortedBlocks(twinSorterBlocksizeFactor_ *
                                                blocksize_));
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

  // Write the batch of `numRelations` complete small relations in the rows
  // `[begin, end)` of `permutedCols` to `writer1_`, and do all the bookkeeping
  // that `finishRelation` does for a small relation. This is the hook for
  // `RelationBlockCutter::writeCompleteSmallRelations`, see there.
  template <typename PermutedCols>
  void addCompleteSmallRelations(Id firstCol0Id, Id lastCol0Id,
                                 const PermutedCols& permutedCols, size_t begin,
                                 size_t end, size_t numRelations) {
    numDistinctCol0_ += numRelations;
    // The batch is written with `writer1_`, so the blocks of a large relation
    // that are still being written in the background have to be finished
    // first. Note that there never are such blocks here, because
    // `finishRelation` waits for them, but the check is cheap and makes the
    // invariant explicit.
    waitForBlocksOfLargeRelation();
    // Note: The distinct `col1` IDs are deliberately not counted here, because
    // no metadata is stored for small relations (see
    // `CompressedRelationWriter::addSmallRelations`). The count is zero for
    // these relations anyway, but reset it, so that a future relaxation of
    // `RelationBlockCutter::isCompleteSmallRelation` cannot silently corrupt
    // the count of the next large relation.
    resetNumDistinctCol1();
    writer1_->addSmallRelations(firstCol0Id, lastCol0Id, permutedCols, begin,
                                end);
    // We don't need to do anything for the twin permutation and writer2,
    // because we have set up `writer1.smallBlocksCallback_` to do that work
    // for us (see above).
  }

  // The builder of the blocks of small relations of `writer1_`, to which
  // `addCompleteSmallRelations` adds the batches.
  const SmallRelationsBlockBuilder& smallRelationsBlockBuilder() const {
    return writer1_->smallRelations_;
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
      this->addInputBlock(
          std::make_shared<const IdTableStatic<0>>(std::move(blockFromInput)),
          col0, getPermutedColIndices());
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
    // Log the total waiting time only now (and not directly after the final
    // progress string above), because the last blocks of the last relation are
    // written by the two calls above.
    //
    // NOTE: The `value()` (in microseconds) is used instead of the `msecs()`
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

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONWRITER_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_
