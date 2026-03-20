// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_PARTITIONBUCKET_H
#define QLEVER_SRC_ENGINE_PARTITIONBUCKET_H

#include <vector>

#include "engine/Engine.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"

namespace qlever::partitionedJoin {

// A bucket that accumulates rows for a single partition during the
// partitioning pass of a partitioned join. Rows are stored in fixed-size
// IdTable pages. When a page fills up it is moved to a completed-pages list
// and a new page is allocated.
//
// TODO: Once the QLever wrapper around DuckDB's StandardBufferManager is
// available, the completed pages should be stored as unpinned buffer blocks
// so they can auto-evict to disk under memory pressure.
class PartitionBucket {
 public:
  // `numColumns` is the width of each row. `pageCapacity` is the number of
  // rows per page (default chosen to give ~256KB pages).
  explicit PartitionBucket(size_t numColumns,
                           ad_utility::AllocatorWithLimit<Id> allocator,
                           size_t pageCapacity = 0)
      : numColumns_(numColumns),
        allocator_(std::move(allocator)),
        pageCapacity_(pageCapacity > 0 ? pageCapacity
                                       : defaultPageCapacity(numColumns)),
        currentPage_(numColumns, allocator_) {
    currentPage_.reserve(pageCapacity_);
  }

  // Append a single row to this bucket.
  template <typename Row>
  void append(const Row& row) {
    if (currentPage_.size() >= pageCapacity_) {
      flushCurrentPage();
    }
    currentPage_.push_back(row);
    ++numRows_;
  }

  // Materialize all pages into a single IdTable sorted by `joinCol`.
  IdTable materializeAndSort(ColumnIndex joinCol) {
    IdTable result(numColumns_, allocator_);
    result.reserve(numRows_);

    for (auto& page : completedPages_) {
      for (size_t i = 0; i < page.size(); ++i) {
        result.push_back(page[i]);
      }
    }
    for (size_t i = 0; i < currentPage_.size(); ++i) {
      result.push_back(currentPage_[i]);
    }

    // Sort by join column using the engine's parallel sort.
    Engine::sort(result, std::vector<ColumnIndex>{joinCol});

    // Free page memory.
    completedPages_.clear();
    currentPage_ = IdTable(numColumns_, allocator_);

    return result;
  }

  bool empty() const { return numRows_ == 0; }
  size_t numRows() const { return numRows_; }

 private:
  static size_t defaultPageCapacity(size_t numColumns) {
    // Target ~256KB per page.
    constexpr size_t TARGET_PAGE_BYTES = 256 * 1024;
    size_t rowSize = numColumns * sizeof(Id);
    return rowSize > 0 ? std::max(size_t{1}, TARGET_PAGE_BYTES / rowSize)
                       : 1024;
  }

  void flushCurrentPage() {
    completedPages_.push_back(std::move(currentPage_));
    currentPage_ = IdTable(numColumns_, allocator_);
    currentPage_.reserve(pageCapacity_);
  }

  size_t numColumns_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  size_t pageCapacity_;
  std::vector<IdTable> completedPages_;
  IdTable currentPage_;
  size_t numRows_ = 0;
};

}  // namespace qlever::partitionedJoin

#endif  // QLEVER_SRC_ENGINE_PARTITIONBUCKET_H
