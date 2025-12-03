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

// __________________________________________________________________________
struct CompressedRelationWriter::PermutationWriter {
  qlever::KeyOrder permutation_;
  CompressedRelationWriter& writer1_;
  CompressedRelationWriter& writer2_;
  MetadataWriter writeMetadata_;

  const size_t blocksize_;
  const size_t numColumns_;
  static constexpr size_t c1Idx_ = 1;
  static constexpr size_t c2Idx_ = 2;
  size_t numDistinctCol0_ = 0;

  // A queue for the callbacks that have to be applied for each triple.
  // The second argument is the number of threads. It is crucial that this
  // queue is single threaded.
  ad_utility::TaskQueue<false> blockCallbackQueue_{
      3, 1, "Additional callbacks during permutation building"};

  ad_utility::Timer inputWaitTimer_{ad_utility::Timer::Stopped};
  ad_utility::Timer largeTwinRelationTimer_{ad_utility::Timer::Stopped};
  ad_utility::Timer blockCallbackTimer_{ad_utility::Timer::Stopped};

  std::optional<Id> col0IdCurrentRelation_;
  ad_utility::AllocatorWithLimit<ValueId> alloc_{
      ad_utility::makeUnlimitedAllocator<Id>()};

  // TODO<joka921> Use call_fixed_size if there is benefit to it.
  IdTableStatic<0> relation_;
  size_t numBlocksCurrentRel_ = 0;

  // ___________________________________________________________________________
  struct TwinComparator {
    bool operator()(const auto& a, const auto& b) const {
      return std::tie(a[c1Idx_], a[c2Idx_], a[ADDITIONAL_COLUMN_GRAPH_ID]) <
             std::tie(b[c1Idx_], b[c2Idx_], b[ADDITIONAL_COLUMN_GRAPH_ID]);
    }
  };

  ad_utility::CompressedExternalIdTableSorter<TwinComparator, 0>
      twinRelationSorter_;

  DistinctIdCounter distinctCol1Counter_;

  using PerBlockCallbacks =
      std::vector<std::function<void(const IdTableStatic<0>&)>>;
  const PerBlockCallbacks perBlockCallbacks_;

  // Set up the handling of small relations for the twin permutation.
  // A complete block of them is handed from `writer1` to the following lambda
  // (via the `smallBlocksCallback_` mechanism. The lambda the resorts the
  // block and feeds it to `writer2`.)
  struct AddBlockOfSmallRelationsToSwitched {
    CompressedRelationWriter& writer_;
    void operator()(IdTable relation) const {
      // We don't use the parallel twinRelationSorter to create the twin
      // relation as its overhead is far too high for small relations.
      relation.swapColumns(c1Idx_, c2Idx_);

      // We only need to sort by the columns of the triple + the graph
      // column, not the additional payload. Note: We could also use
      // `compareWithoutLocalVocab` to compare the IDs cheaper, but this
      // sort is far from being a performance bottleneck.
      auto compare = [](const auto& a, const auto& b) {
        return std::tie(a[0], a[1], a[2], a[3]) <
               std::tie(b[0], b[1], b[2], b[3]);
      };
      ql::ranges::sort(relation, compare);
      AD_CORRECTNESS_CHECK(!relation.empty());
      // Note: it is important that we store these two IDs before moving the
      // `relation`, because the evaluation order of function arguments is
      // unspecified.
      auto firstCol0 = relation.at(0, 0);
      auto lastCol0 = relation.at(relation.numRows() - 1, 0);
      writer_.compressAndWriteBlock(firstCol0, lastCol0, std::move(relation),
                                    false);
    };
  };

  // ___________________________________________________________________________
  PermutationWriter(const std::string& basename,
                    WriterAndCallback writerAndCallback1,
                    WriterAndCallback writerAndCallback2,
                    qlever::KeyOrder permutation,
                    PerBlockCallbacks perBlockCallbacks)
      : permutation_{std::move(permutation)},
        writer1_{writerAndCallback1.writer_},
        writer2_{writerAndCallback2.writer_},
        writeMetadata_{std::move(writerAndCallback1.callback_),
                       std::move(writerAndCallback2.callback_),
                       writerAndCallback1.writer_.blocksize()},
        blocksize_{writerAndCallback1.writer_.blocksize()},
        numColumns_{writerAndCallback1.writer_.numColumns()},
        relation_{numColumns_, alloc_},
        twinRelationSorter_{basename + ".twin-twinRelationSorter",
                            numColumns_,
                            4_GB,
                            alloc_,
                            ad_utility::DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
                            TwinComparator{}},
        perBlockCallbacks_{std::move(perBlockCallbacks)} {
    auto [c0, c1, c2, c3] = permutation.keys();
    // This logic only works for permutations that have the graph as the fourth
    // column.
    AD_CORRECTNESS_CHECK(c3 == 3);

    AD_CORRECTNESS_CHECK(blocksize_ == writer2_.blocksize());
    AD_CORRECTNESS_CHECK(numColumns_ == writer2_.numColumns());

    writer1_.smallBlocksCallback_ =
        AddBlockOfSmallRelationsToSwitched{writer2_};
  };

  // ___________________________________________________________________________
  void addBlockForLargeRelation() {
    if (relation_.empty()) {
      return;
    }
    auto twinRelation = relation_.asStaticView<0>();
    twinRelation.swapColumns(c1Idx_, c2Idx_);
    for (const auto& row : twinRelation) {
      twinRelationSorter_.push(row);
    }
    writer1_.addBlockForLargeRelation(col0IdCurrentRelation_.value(),
                                      std::move(relation_).toDynamic());
    relation_.clear();
    relation_.reserve(blocksize_);
    ++numBlocksCurrentRel_;
  };

  // ___________________________________________________________________________
  void finishRelation() {
    ++numDistinctCol0_;
    if (numBlocksCurrentRel_ > 0 || static_cast<double>(relation_.numRows()) >
                                        0.8 * static_cast<double>(blocksize_)) {
      // The relation is large;
      addBlockForLargeRelation();
      auto md1 =
          writer1_.finishLargeRelation(distinctCol1Counter_.getAndReset());
      largeTwinRelationTimer_.cont();
      auto md2 = writer2_.addCompleteLargeRelation(
          col0IdCurrentRelation_.value(),
          twinRelationSorter_.getSortedBlocks(blocksize_));
      largeTwinRelationTimer_.stop();
      twinRelationSorter_.clear();
      writeMetadata_(md1, md2);
    } else {
      // Small relations are written in one go.
      [[maybe_unused]] auto md1 = writer1_.addSmallRelation(
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
                         writer1_.blockWriteQueueTimer_.msecs())
                  << "s" << std::endl;
    AD_LOG_TIMING << "Time spent waiting for writer2's queue "
                  << ad_utility::Timer::toSeconds(
                         writer2_.blockWriteQueueTimer_.msecs())
                  << "s" << std::endl;
    AD_LOG_TIMING << "Time spent waiting for large twin relations "
                  << ad_utility::Timer::toSeconds(
                         largeTwinRelationTimer_.msecs())
                  << "s" << std::endl;
    AD_LOG_TIMING
        << "Time spent waiting for triple callbacks (e.g. the next sorter) "
        << ad_utility::Timer::toSeconds(blockCallbackTimer_.msecs()) << "s"
        << std::endl;
  }

  // ___________________________________________________________________________
  PermutationPairResult writePermutation(
      ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples) {
    // All columns n the order in which they have to be added to
    // the relation.
    auto [c0, c1, c2, c3] = permutation_.keys();
    std::vector<ColumnIndex> permutedColIndices{c0, c1, c2};
    for (size_t colIdx = 3; colIdx < numColumns_; ++colIdx) {
      permutedColIndices.push_back(colIdx);
    }
    inputWaitTimer_.cont();
    size_t numTriplesProcessed = 0;
    ad_utility::ProgressBar progressBar{numTriplesProcessed,
                                        "Triples sorted: "};
    for (auto& block : AD_FWD(sortedTriples)) {
      AD_CORRECTNESS_CHECK(block.numColumns() == numColumns_);
      inputWaitTimer_.stop();
      // This only happens when the index is completely empty.
      if (block.empty()) {
        continue;
      }
      auto firstCol = block.getColumn(c0);
      auto permutedCols = block.asColumnSubsetView(permutedColIndices);
      if (!col0IdCurrentRelation_.has_value()) {
        col0IdCurrentRelation_ = firstCol[0];
      }
      // TODO<C++23> Use `views::zip`
      for (size_t idx : ad_utility::integerRange(block.numRows())) {
        Id col0Id = firstCol[idx];
        decltype(auto) curRemainingCols = permutedCols[idx];

        if (col0Id != col0IdCurrentRelation_) {
          finishRelation();
          col0IdCurrentRelation_ = col0Id;
        }

        // Check if we need to create a new block before adding the current
        // triple. We create a new block if:
        // 1. The relation buffer is at the block size limit, AND
        // 2. The current triple has different first three columns than the last
        //    triple in the buffer (to ensure equal triples stay in same block)
        AD_CORRECTNESS_CHECK(blocksize_ > 0);
        if (relation_.size() >= blocksize_) {
          // Compare first three columns of current triple with last buffered
          // triple
          const auto& lastBufferedRow = relation_.back();
          if (tieFirstThreeColumns(curRemainingCols) !=
              tieFirstThreeColumns(lastBufferedRow)) {
            addBlockForLargeRelation();
          }
        }

        distinctCol1Counter_(curRemainingCols[c1Idx_]);
        relation_.push_back(curRemainingCols);

        ++numTriplesProcessed;
        if (progressBar.update()) {
          AD_LOG_INFO << progressBar.getProgressString() << std::flush;
        }
      }
      // Call each of the `perBlockCallbacks` for the current block.
      blockCallbackTimer_.cont();
      blockCallbackQueue_.push(
          [block = std::make_shared<std::decay_t<decltype(block)>>(
               std::move(block)),
           this]() {
            for (auto& callback : perBlockCallbacks_) {
              callback(*block);
            }
          });
      blockCallbackTimer_.stop();
      inputWaitTimer_.cont();
    }
    AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
    inputWaitTimer_.stop();
    if (!relation_.empty() || numBlocksCurrentRel_ > 0) {
      finishRelation();
    }

    writer1_.finish();
    writer2_.finish();
    blockCallbackTimer_.cont();
    blockCallbackQueue_.finish();
    blockCallbackTimer_.stop();
    logTimers();
    return {numDistinctCol0_, std::move(writer1_).getFinishedBlocks(),
            std::move(writer2_).getFinishedBlocks()};
  }
};

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONPERMUTATIONWRITERIMPL_H_
