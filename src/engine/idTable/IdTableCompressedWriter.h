//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_IDTABLECOMPRESSEDWRITER_H
#define QLEVER_IDTABLECOMPRESSEDWRITER_H

#include <ranges>

#include "engine/idTable/IdTable.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"

class IdTableCompressedWriter {
  struct BlockMetadata {
    size_t compressedSize_;
    size_t uncompressedSize_;
    size_t offsetInFile_;
  };
 private:
  ad_utility::File file_;
  // For each IdTable for each column for each block store the metadata.
  using ColumnMetadata = std::vector<BlockMetadata>;
  std::vector<ColumnMetadata> blocksPerColumn_;
  size_t numColumns() const {return blocksPerColumn_.size();}


  ad_utility::MemorySize blockSize_ = ad_utility::MemorySize::megabytes(4ul);

 public:
  explicit IdTableCompressedWriter(std::string filename, size_t numCols) : file_{std::move(filename), "w"}, blocksPerColumn_(numCols) {}

  void writeIdTable(const IdTable& table) {
    AD_CONTRACT_CHECK(table.numColumns() == numColumns());
    size_t bs = blockSize_.getBytes() / sizeof(Id);
    AD_CONTRACT_CHECK(bs > 0);
    for (auto i :std::views::iota(0u, numColumns())) {
      auto& blockMetadata = blocksPerColumn_.at(i);
      decltype(auto) column = table.getColumn(i);
      for (size_t lower = 0; lower < column.size(); lower += bs) {
        size_t upper = std::min(lower + bs, column.size());
        auto offset = file_.tell();
        file_.write(column.data() + lower, upper * sizeof(Id));
        // TODO<joka921> continue here.
      }



    }
  }


};

#endif  // QLEVER_IDTABLECOMPRESSEDWRITER_H
