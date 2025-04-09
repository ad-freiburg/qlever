// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H

#include <cstdint>
#include <string>

#include "index/vocabulary/VocabularyTypes.h"

// A GeoVocabulary holds Well-Known Text literals. In contrast to the regular
// vocabulary classes it does not only store the strings. Instead it stores both
// preprocessed and original forms of its input words. Preprocessing includes
// for example the computation of bounding boxes for accelerated spatial
// queries.
template <typename UnderlyingVocabulary>
class GeoVocabulary {
 private:
  UnderlyingVocabulary literals_;

 public:
  // Constructor
  template <typename... Args>
  explicit GeoVocabulary(Args&&... args)
      : literals_{std::forward<Args>(args)...} {};

  // Forward all the standard operations to the underlying literal vocabulary.

  auto operator[](uint64_t id) const { return literals_[id]; }

  [[nodiscard]] uint64_t size() const { return literals_.size(); }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return literals_.lower_bound(word, comparator);
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return literals_.upper_bound(word, comparator);
  }

  UnderlyingVocabulary& getUnderlyingVocabulary() { return literals_; }

  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return literals_;
  }

  void open(const std::string& filename) {
    literals_.open(filename);
    // TODO: and custom files
  };

  class WordWriter {
   private:
    using UV = UnderlyingVocabulary;
    using WW = UnderlyingVocabulary::WordWriter;
    WW underlyingWordWriter_;

   public:
    WordWriter(const UV& vocabulary, const std::string& filename);

    // Add the next literal to the vocabulary, precompute additional information
    // and return the literal's new index.
    uint64_t operator()(std::string_view word, bool isExternal);

    // Finish the writing on the underlying writers. After this no more
    // calls to `operator()` are allowed.
    void finish();

    std::string& readableName();
  };

  WordWriter makeWordWriter(const std::string& filename) const {
    return {literals_, filename};
  }

  void build(const std::vector<std::string>& v, const std::string& filename);

  void close() { literals_.close(); }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
