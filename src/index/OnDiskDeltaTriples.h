// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Claude (Anthropic AI)

#ifndef QLEVER_SRC_INDEX_ONDISKDELTATRIPLES_H
#define QLEVER_SRC_INDEX_ONDISKDELTATRIPLES_H

#include <array>
#include <optional>
#include <string>
#include <vector>

#include "index/CompressedRelation.h"
#include "index/Permutation.h"
#include "util/File.h"

// Forward declarations
class IdTable;

// Manages on-disk storage of delta triples (inserted and deleted triples from
// SPARQL UPDATE operations). When in-memory delta triples exceed a threshold,
// they are written to compressed files on disk using the same format as the
// main index permutations. This allows efficient merging during scans while
// keeping memory usage bounded.
//
// Each permutation has two files:
// - inserts file: triples that were inserted
// - deletes file: triples that were deleted
//
// The files use the CompressedRelation format with block metadata, enabling:
// - Efficient block-level merging during scans
// - Parallelism using block metadata
// - Compatibility with existing infrastructure
class OnDiskDeltaTriples {
 public:
  // Construct for the given base directory (where the main index is stored).
  explicit OnDiskDeltaTriples(std::string baseDir);

  // Check if on-disk delta files exist for any permutation.
  bool hasOnDiskDeltas() const;

  // Check if on-disk delta files exist for the specific permutation.
  bool hasOnDiskDeltasForPermutation(PermutationEnum permutation) const;

  // Get block metadata for inserted triples in the given permutation.
  // Returns empty vector if no on-disk inserts exist.
  const std::vector<CompressedBlockMetadata>& getInsertBlocksMetadata(
      PermutationEnum permutation) const;

  // Get block metadata for deleted triples in the given permutation.
  // Returns empty vector if no on-disk deletes exist.
  const std::vector<CompressedBlockMetadata>& getDeleteBlocksMetadata(
      PermutationEnum permutation) const;

  // Check if a specific block index has on-disk deltas (inserts or deletes).
  bool hasOnDiskDeltasForBlock(PermutationEnum permutation,
                               size_t blockIndex) const;

  // Read and decompress the inserted delta triples for the given block index
  // in the specified permutation. Returns std::nullopt if no inserts exist for
  // this block.
  std::optional<IdTable> readInsertBlock(
      PermutationEnum permutation, size_t blockIndex,
      CompressedRelationReader::ColumnIndicesRef columns,
      const CompressedRelationReader::Allocator& allocator) const;

  // Read and decompress the deleted delta triples for the given block index
  // in the specified permutation. Returns std::nullopt if no deletes exist for
  // this block.
  std::optional<IdTable> readDeleteBlock(
      PermutationEnum permutation, size_t blockIndex,
      CompressedRelationReader::ColumnIndicesRef columns,
      const CompressedRelationReader::Allocator& allocator) const;

  // Load on-disk delta files from disk (reads metadata).
  void loadFromDisk();

  // Delete all on-disk delta files (cleanup).
  void deleteFiles();

  // Read all triples from a delta file (inserts or deletes) for a given
  // permutation. Returns an IdTable with all triples in sorted order.
  // This is used during rebuild to merge old on-disk deltas with new in-memory
  // ones. Returns empty table if no file exists.
  IdTable readAllTriples(
      PermutationEnum permutation, bool isInsert,
      const CompressedRelationReader::Allocator& allocator) const;

  // Get the base directory where delta files are stored.
  const std::string& baseDir() const { return baseDir_; }

 private:
  // Per-permutation storage for delta triple files and metadata.
  struct DeltaPermutationFiles {
    // File handles for reading (opened lazily).
    mutable std::optional<ad_utility::File> insertsFile_;
    mutable std::optional<ad_utility::File> deletesFile_;

    // Block metadata loaded from disk.
    std::vector<CompressedBlockMetadata> insertsMetadata_;
    std::vector<CompressedBlockMetadata> deletesMetadata_;

    // Helper: check if this permutation has any on-disk deltas.
    bool hasDeltas() const {
      return !insertsMetadata_.empty() || !deletesMetadata_.empty();
    }
  };

  std::string baseDir_;
  std::array<DeltaPermutationFiles, 6>
      permutations_;  // One for each permutation.

  // Helper: get the file for the given permutation and type (insert/delete).
  ad_utility::File& getFile(PermutationEnum permutation, bool isInsert,
                            const std::string& path) const;

  // Helper: read a specific block from a delta file.
  std::optional<IdTable> readBlockFromFile(
      const std::vector<CompressedBlockMetadata>& metadata,
      ad_utility::File& file, size_t blockIndex,
      CompressedRelationReader::ColumnIndicesRef columns,
      const CompressedRelationReader::Allocator& allocator) const;
};

#endif  // QLEVER_SRC_INDEX_ONDISKDELTATRIPLES_H
