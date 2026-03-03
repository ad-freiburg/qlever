// Copyright 2025 The QLever Authors, in particular:
//
// 2021 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/CompressedRelation.h"
#include "index/CompressedRelationHelpersImpl.h"
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
    // column, not the additional payload. Note: We could also use
    // `compareWithoutLocalVocab` to compare the IDs cheaper, but this
    // sort is far from being a performance bottleneck.
    auto compare = [](const auto& a, const auto& b) {
      return std::tie(a[0], a[1], a[2], a[3]) <
             std::tie(b[0], b[1], b[2], b[3]);
    };
    ql::ranges::sort(blockOfSmallRelations, compare);
    AD_CORRECTNESS_CHECK(!blockOfSmallRelations.empty());
    // Note: it is important that we store these two IDs before moving the
    // `relation`, because the evaluation order of function arguments is
    // unspecified.
    auto firstCol0 = blockOfSmallRelations.at(0, 0);
    auto lastCol0 =
        blockOfSmallRelations.at(blockOfSmallRelations.numRows() - 1, 0);
    writer_.compressAndWriteBlock(firstCol0, lastCol0,
                                  std::move(blockOfSmallRelations), false);
  };
};

// Helper that handles the queue of callbacks to be called for every block
// written.
struct BlockCallbackManager {
  const CompressedRelationWriter::PerBlockCallbacks perBlockCallbacks_;

  // A queue for the callbacks that have to be applied for each triple.
  // The second argument is the number of threads. It is crucial that this
  // queue is single threaded.
  ad_utility::TaskQueue<false> blockCallbackQueue_{
      3, 1, "Additional callbacks during permutation building"};
  ad_utility::Timer blockCallbackTimer_{ad_utility::Timer::Stopped};

  // Enqueue a call to each of the `perBlockCallbacks` for the current block.
  void passToBlockCallbacks(IdTable block) {
    blockCallbackTimer_.cont();
    blockCallbackQueue_.push(
        [block =
             std::make_shared<std::decay_t<decltype(block)>>(std::move(block)),
         this]() {
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
  IdTableStatic<0> relation_{numColumns_, alloc_};
  size_t numBlocksCurrentRel_ = 0;

  using TwinRelationSorter = ad_utility::CompressedExternalIdTableSorter<
      compressedRelationHelpers::ComparatorForConstCol0, 0>;
  IfPair<TwinRelationSorter> twinRelationSorter_;

  compressedRelationHelpers::DistinctIdCounter distinctCol1Counter_;
  BlockCallbackManager blockCallbackManager_;

  size_t numTriplesProcessed_ = 0;
  ad_utility::ProgressBar progressBar_{numTriplesProcessed_,
                                       "Triples sorted: "};

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

    writer1_->smallBlocksCallback_ =
        AddBlockOfSmallRelationsToSwitched{*writer2_};
  };

  // Constructor for a `PermutationWriter` which writes a single permutation.
  CPP_template(bool doWritePair = WritePair)(requires(!doWritePair))
      PermutationWriter(WriterAndCallback writerAndCallback1,
                        qlever::KeyOrder permutation,
                        PerBlockCallbacks perBlockCallbacks)
      : permutation_{std::move(permutation)},
        writer1_{std::move(writerAndCallback1.writer_)},
        writeMetadata_{std::move(writerAndCallback1.callback_),
                       writer1_->blocksize()},
        blockCallbackManager_{std::move(perBlockCallbacks)} {
    static_assert(!WritePair);
    // This logic only works for permutations that have the graph as the fourth
    // column.
    AD_CORRECTNESS_CHECK(permutation_.keys().at(3) == 3);
  };

  // Write a block of a large relation with `writer1` and also push the block
  // into the twin sorter for `writer2`.
  void addBlockForLargeRelation() {
    using namespace compressedRelationHelpers;
    if (relation_.empty()) {
      return;
    }
    if constexpr (WritePair) {
      auto twinRelation = relation_.asStaticView<0>();
      twinRelation.swapColumns(c1Idx, c2Idx);
      for (const auto& row : twinRelation) {
        twinRelationSorter_.push(row);
      }
    }
    writer1_->addBlockForLargeRelation(col0IdCurrentRelation_.value(),
                                       std::move(relation_).toDynamic());
    relation_.clear();
    relation_.reserve(blocksize_);
    ++numBlocksCurrentRel_;
  };

  // We have encountered the last occurrence of the current relation (value for
  // column 0). Thus we need to write the remaining buffered rows and metadata.
  // This also resets counters and buffers for writing the next relation.
  void finishRelation() {
    ++numDistinctCol0_;
    if (numBlocksCurrentRel_ > 0 || static_cast<double>(relation_.numRows()) >
                                        0.8 * static_cast<double>(blocksize_)) {
      // The relation is large;
      addBlockForLargeRelation();
      auto md1 =
          writer1_->finishLargeRelation(distinctCol1Counter_.getAndReset());
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
      // Small relations are written in one go.
      [[maybe_unused]] auto md1 = writer1_->addSmallRelation(
          col0IdCurrentRelation_.value(), distinctCol1Counter_.getAndReset(),
          relation_.asStaticView<0>());
      // We don't need to do anything for the twin permutation and writer2,
      // because we have set up `writer1.smallBlocksCallback_` to do that work
      // for us (see above).
    }
    relation_.clear();
    numBlocksCurrentRel_ = 0;
  };

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

  // Check if we need to create a new block before adding the current
  // triple. We create a new block if:
  // 1. The relation buffer is at the block size limit, AND
  // 2. The current triple has different first three columns than the last
  //    triple in the buffer (to ensure equal triples stay in same block)
  bool isEndOfBlockForLargeRelation(const auto& curRemainingCols) {
    AD_CORRECTNESS_CHECK(blocksize_ > 0);
    if (relation_.size() < blocksize_) {
      return false;
    }

    // Compare first three columns of current triple with last buffered
    // triple
    const auto& lastBufferedRow = relation_.back();
    return compressedRelationHelpers::tieFirstThreeColumns(curRemainingCols) !=
           compressedRelationHelpers::tieFirstThreeColumns(lastBufferedRow);
  }

  // ___________________________________________________________________________
  void increaseTripleCounter() {
    ++numTriplesProcessed_;
    if (progressBar_.update()) {
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
    using namespace compressedRelationHelpers;

    inputWaitTimer_.cont();

    auto col0 = permutation_.keys().at(0);

    for (auto& block : AD_FWD(sortedTriples)) {
      AD_CORRECTNESS_CHECK(block.numColumns() == numColumns_);
      inputWaitTimer_.stop();
      // This only happens when the index is completely empty.
      if (block.empty()) {
        continue;
      }
      auto firstCol = block.getColumn(col0);
      auto permutedCols = block.asColumnSubsetView(getPermutedColIndices());
      if (!col0IdCurrentRelation_.has_value()) {
        col0IdCurrentRelation_ = firstCol[0];
      }

      // TODO<C++23> Use `views::zip` (some compilers currently have trouble
      // with `::ranges::views::zip`).
      for (size_t idx : ad_utility::integerRange(block.numRows())) {
        Id col0Id = firstCol[idx];
        decltype(auto) curRemainingCols = permutedCols[idx];

        if (col0Id != col0IdCurrentRelation_) {
          finishRelation();
          col0IdCurrentRelation_ = col0Id;
        }

        if (isEndOfBlockForLargeRelation(curRemainingCols)) {
          addBlockForLargeRelation();
        }

        distinctCol1Counter_(curRemainingCols[c1Idx]);
        relation_.push_back(curRemainingCols);

        increaseTripleCounter();
      }
      blockCallbackManager_.passToBlockCallbacks(std::move(block));
      inputWaitTimer_.cont();
    }
    AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;
    inputWaitTimer_.stop();
    if (!relation_.empty() || numBlocksCurrentRel_ > 0) {
      finishRelation();
    }

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
