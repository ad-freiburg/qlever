// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_BLANKNODEIRIVOCABULARY_H
#define QLEVER_SRC_INDEX_BLANKNODEIRIVOCABULARY_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "index/Vocabulary.h"
#include "index/vocabulary/VocabularyType.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Serializer/Serializer.h"

// Remembers which blank node index was assigned to each IRI that was treated as
// a blank node during index building (see `IndexImpl::setBlankNodePrefixes`).
// These IRIs are not stored in the main vocabulary, so this class provides the
// only persistent record of the mapping. It is used to consistently remap such
// IRIs to the same blank node when they are referenced again later (in
// particular via SPARQL UPDATE).
//
// The matching IRIs are stored in a dedicated, sorted, on-disk vocabulary (so
// the structure scales to very large numbers of IRIs). The mapping from IRI to
// blank node index is stored compactly: the vocabulary merger processes words
// in sorted order and hands out blank node indices sequentially, and because
// all IRIs sort into a single contiguous block (separate from `_:` blank nodes
// and from literals; see `TripleComponentComparator`), the matching IRIs
// occupy contiguous runs of blank node indices. Each such run is stored as a
// single `Chunk`, so in the common case a single `Chunk` describes the whole
// mapping.
class BlankNodeIriVocabulary {
 public:
  // A contiguous run: the vocabulary positions
  // `[vocabStartPos_, vocabStartPos_ + count_)` map to the blank node indices
  // `[blankNodeStartIndex_, blankNodeStartIndex_ + count_)`.
  struct Chunk {
    uint64_t vocabStartPos_ = 0;
    uint64_t blankNodeStartIndex_ = 0;
    uint64_t count_ = 0;

    AD_SERIALIZE_FRIEND_FUNCTION(Chunk) {
      serializer | arg.vocabStartPos_;
      serializer | arg.blankNodeStartIndex_;
      serializer | arg.count_;
    }
  };

  // Incrementally write the vocabulary and its chunk metadata to disk. The
  // matching IRIs together with their assigned blank node index have to be
  // pushed via `operator()` in ascending (sorted) order, exactly as they are
  // encountered by the vocabulary merger.
  class Writer {
   public:
    // Create a writer that writes the IRIs via `wordWriter` and the chunk
    // metadata to `chunksFilename` (only) once `finish()` is called.
    Writer(std::unique_ptr<WordWriterBase> wordWriter,
           std::string chunksFilename);

    // Register that `iri` was assigned the blank node `blankNodeIndex`. Must be
    // called in ascending vocabulary order.
    void operator()(std::string_view iri, uint64_t blankNodeIndex);

    // Finish the vocabulary and persist the chunk metadata. Idempotent.
    void finish();

   private:
    std::unique_ptr<WordWriterBase> wordWriter_;
    std::string chunksFilename_;
    std::vector<Chunk> chunks_;
    uint64_t nextVocabPos_ = 0;
    bool finished_ = false;
  };

  // Default-constructed vocabulary is empty; the feature is then inactive.
  BlankNodeIriVocabulary() = default;

  // Load the vocabulary and the chunk metadata from disk. `vocabBaseName` is
  // the base name that was passed to the `Writer` (the chunk file is derived
  // from it via `chunksFilename`). The `type` and locale must match those of
  // the main vocabulary so that lookups use the correct comparator.
  void readFromFile(const std::string& vocabBaseName,
                    ad_utility::VocabularyType type,
                    const std::string& language, const std::string& country,
                    bool ignorePunctuation);

  // Return the blank node index that was assigned to `iri` during index
  // building, or `std::nullopt` if `iri` was not treated as a blank node.
  std::optional<uint64_t> getBlankNodeIndex(std::string_view iri) const;

  // Return the filename for the chunk metadata that belongs to a vocabulary
  // with the given base name.
  static std::string chunksFilename(const std::string& vocabBaseName) {
    return vocabBaseName + ".chunks";
  }

  // Whether this vocabulary contains any entries (i.e. the feature is active).
  bool empty() const { return chunks_.empty(); }

 private:
  RdfsVocabulary vocab_;
  std::vector<Chunk> chunks_;
};

#endif  // QLEVER_SRC_INDEX_BLANKNODEIRIVOCABULARY_H
