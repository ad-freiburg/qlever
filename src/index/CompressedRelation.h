// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_COMPRESSEDRELATION_H
#define QLEVER_COMPRESSEDRELATION_H

#include <algorithm>
#include <vector>

#include "../global/Id.h"
#include "../util/BufferedVector.h"
#include "../util/CoroToStateMachine.h"
#include "../util/File.h"
#include "../util/Serializer/SerializeVector.h"
#include "../util/Serializer/Serializer.h"
#include "../util/Timer.h"
#include "./CompressedRelationWriterHelpers.h"


/// Manage the compression and serialization of relations during the index
/// build.
class CompressedRelationWriter {
 private:
  ad_utility::File _outfile;
  std::vector<CompressedRelationMetaData> _metaDataBuffer;
  std::vector<CompressedBlockMetaData> _blockBuffer;

 public:
  /// Create using a filename, to which the relation data will be written.
  CompressedRelationWriter(ad_utility::File f) : _outfile{std::move(f)} {}

  /// Get the complete CompressedRelationMetaData created by the calls to
  /// addRelation. This meta data is then deleted from the
  /// CompressedRelationWriter. The typical workflow is: add all relations,
  /// then call `finish()` and then call this method.
  auto getFinishedMetaData() {
    return std::move(_metaDataBuffer);
    _metaDataBuffer.clear();
  }

  /// Get all the CompressedBlockMetaData that were created by the calls to
  /// addRelation. This meta data is then deleted from the
  /// CompressedRelationWriter. The typical workflow is: add all relations,
  /// then call `finish()` and then call this method.
  auto getFinishedBlocks() {
    return std::move(_blockBuffer);
    _blockBuffer.clear();
  }

 public:
  auto makeTriplePusher() {
    return TriplePusher{std::bind_front(&CompressedRelationWriter::writeBlock, this), _metaDataBuffer};
  }
  auto permutingTriplePusher() {
    return PermutingTriplePusher{makeTriplePusher()};
  }

 private:
  // A block of triples with the same first column.
 public:
 private:
  using BlockPusher = ad_utility::CoroToStateMachine<Block>;

  // Compress and write a block of (col1Id, col2Id)` pairs to disk, and append
  // the corresponding block meta data to `_blockBuffer`
  void writeBlock(Id firstCol0Id, Id lastCol0Id,
                  const std::vector<std::array<Id, 2>>& data);
};

#endif  // QLEVER_COMPRESSEDRELATION_H
