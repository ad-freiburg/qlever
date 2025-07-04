// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H

#include <cstdint>
#include <memory>
#include <string>

#include "index/vocabulary/VocabularyTypes.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/GeometryInfo.h"

using ad_utility::GeometryInfo;

// A `GeoVocabulary` holds Well-Known Text (WKT) literals. In contrast to the
// regular vocabulary classes it does not only store the strings. Instead it
// stores both preprocessed and original forms of its input words. Preprocessing
// includes for example the computation of bounding boxes for accelerated
// spatial queries. Note: A `GeoVocabulary` may only store WKT literals,
// therefore it should be used as part of a `SplitVocabulary`.
template <typename UnderlyingVocabulary>
class GeoVocabulary {
 private:
  UnderlyingVocabulary literals_;

  // The file in which the additional information on the geometries (like
  // bounding box) is stored.
  mutable ad_utility::File geoInfoFile_;

  // TODO<ullingerc> Possibly add in-memory cache of bounding boxes here

  // Filename suffix for geometry information file
  static constexpr std::string_view geoInfoSuffix = ".geoinfo";

  // Offset per index inside the geometry information file
  static constexpr size_t geoInfoOffset = sizeof(GeometryInfo);

  // Offset for the header of the geometry information file
  static constexpr size_t geoInfoHeader =
      sizeof(ad_utility::GEOMETRY_INFO_VERSION);

 public:
  // Constructor
  template <typename... Args>
  explicit GeoVocabulary(Args&&... args) : literals_{AD_FWD(args)...} {};

  // Retrieve the geometry info object stored for the literal with a given
  // index.
  GeometryInfo getGeoInfo(uint64_t index) const;

  std::string getGeoInfoFilename(const std::string& filename) {
    return filename + std::string(geoInfoSuffix);
  }

  // Forward all the standard operations to the underlying literal vocabulary.

  decltype(auto) operator[](uint64_t id) const { return literals_[id]; }

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

  void open(const std::string& filename);

  class WordWriter : public WordWriterBase {
   private:
    std::unique_ptr<typename UnderlyingVocabulary::WordWriter>
        underlyingWordWriter_;
    ad_utility::File geoInfoFile_;

   public:
    WordWriter(const UnderlyingVocabulary& vocabulary,
               const std::string& filename);

    // Add the next literal to the vocabulary, precompute additional information
    // and return the literal's new index.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    // Finish the writing on the underlying writers. After this no more
    // calls to `operator()` are allowed.
    void finishImpl() override;
  };

  std::unique_ptr<WordWriter> makeDiskWriterPtr(
      const std::string& filename) const {
    return std::make_unique<WordWriter>(literals_, filename);
  }

  void close();
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
