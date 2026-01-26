// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Claude (Anthropic AI)

#ifndef QLEVER_SRC_INDEX_DELTATRIPLESWRITER_H
#define QLEVER_SRC_INDEX_DELTATRIPLESWRITER_H

#include <string>
#include <vector>

#include "index/CompressedRelation.h"
#include "index/DeltaTriples.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"

// Forward declarations
class IndexImpl;

// Writes in-memory delta triples (from LocatedTriplesPerBlock) to disk in
// compressed format compatible with the main index permutations. This enables
// efficient block-level merging during scans while keeping memory usage
// bounded.
//
// The writer:
// 1. Extracts triples from LocatedTriplesPerBlock (organized by block index)
// 2. Sorts triples by permutation order
// 3. Groups into compressed blocks
// 4. Writes using CompressedRelationWriter infrastructure
// 5. Generates block metadata compatible with existing format
// 6. Uses atomic writes (temp files + rename)
class DeltaTriplesWriter {
 public:
  // Construct writer for the given index and base directory.
  DeltaTriplesWriter(const IndexImpl& index, std::string baseDir);

  // Write delta triples for a single permutation to disk. Extracts triples
  // from the LocatedTriplesPerBlock, sorts them, compresses them into blocks,
  // and writes to the specified file with associated metadata.
  //
  // `permutation`: which permutation to write
  // `locatedTriples`: the in-memory located triples to write
  // `isInsert`: true for inserts file, false for deletes file
  // `useTemporary`: if true, write to temporary file (for atomic rebuild)
  //
  // Returns the block metadata for the written file.
  std::vector<CompressedBlockMetadata> writePermutation(
      Permutation::Enum permutation,
      const LocatedTriplesPerBlock& locatedTriples, bool isInsert,
      bool useTemporary = false);

  // Write all delta triples (both regular and internal permutations) to disk.
  // This is called during spillToDisk() and rebuildOnDiskDeltas().
  //
  // `locatedTriplesNormal`: regular permutations (PSO, POS, SPO, SOP, OPS, OSP)
  // `locatedTriplesInternal`: internal permutations (PSO, POS)
  // `useTemporary`: if true, write to temporary files
  void writeAllPermutations(
      const LocatedTriplesPerBlockAllPermutations<false>& locatedTriplesNormal,
      const LocatedTriplesPerBlockAllPermutations<true>& locatedTriplesInternal,
      bool useTemporary = false);

  // Atomically rename temporary files to permanent files. Called after
  // writeAllPermutations with useTemporary=true during rebuild.
  void commitTemporaryFiles();

 private:
  const IndexImpl& index_;
  std::string baseDir_;

  // Extract all triples from LocatedTriplesPerBlock and sort them by the given
  // permutation order. Returns a sorted IdTable.
  // `filterInserts`: if true, only extract inserts; if false, only extract
  // deletes.
  IdTable extractAndSortTriples(const LocatedTriplesPerBlock& locatedTriples,
                                const qlever::KeyOrder& keyOrder,
                                bool filterInserts);

  // Write the given sorted triples to disk in compressed format. Returns block
  // metadata.
  std::vector<CompressedBlockMetadata> writeSortedTriplesToFile(
      const IdTable& sortedTriples, const std::string& filename);
};

#endif  // QLEVER_SRC_INDEX_DELTATRIPLESWRITER_H
