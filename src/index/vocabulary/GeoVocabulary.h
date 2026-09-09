// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "rdfTypes/GeoCellGrid.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"
#include "util/Views.h"

// A `GeoVocabulary` holds Well-Known Text (WKT) literals. In contrast to the
// regular vocabulary classes it does not only store the strings. Instead it
// stores both preprocessed and original forms of its input words. Preprocessing
// includes for example the computation of bounding boxes for accelerated
// spatial queries. See the `GeometryInfo` class for details. Note: A
// `GeoVocabulary` is only suitable for WKT literals, therefore it should be
// used as part of a `SplitVocabulary`.
//
// With a `GeoCellGrid` (see `setGeoCellGrid`), the index of a word is no
// longer its position in the vocabulary: the upper bits of the index hold the
// grid cell of the word, the lower `GeoCellGrid::numPositionBits()` bits hold
// the position. The words must then arrive at the `WordWriter` ordered by
// their grid cell (a follow-up change lets the `TripleComponentComparator`
// produce this order), so that all words of one cell form a contiguous index
// range that can be computed from the cell index alone. This is the basis for
// the geo cell prefilter of spatial joins. The grid itself is not stored by
// this class; the index configuration provides it before the vocabulary is
// opened.
template <typename UnderlyingVocabulary>
class GeoVocabulary {
 private:
  using GeometryInfo = ad_utility::GeometryInfo;
  using GeoCellGrid = ad_utility::GeoCellGrid;

  // The index of a word is computed from its position in the underlying
  // vocabulary (see `indexFromPosition`), so the positions must be contiguous,
  // which they are not for the vocabularies with "holes".
  static_assert(!ad_utility::SameAsAny<
                UnderlyingVocabulary, VocabularyInMemoryBinSearch,
                CompressedVocabulary<VocabularyInMemoryBinSearch>>);

  UnderlyingVocabulary literals_;

  // The file in which the additional information on the geometries (like
  // bounding box) is stored.
  ad_utility::File geoInfoFile_;

  // The grid, or `std::nullopt` if the index of a word is its position.
  std::optional<GeoCellGrid> grid_;

  // See `endIndex`, computed once when the vocabulary is opened.
  uint64_t endIndex_ = 0;

  // TODO<ullingerc> Possibly add in-memory cache of bounding boxes here

  // Filename suffix for geometry information file
  static constexpr std::string_view geoInfoSuffix = ".geoinfo";

  // Offset per index inside the geometry information file
  static constexpr size_t geoInfoOffset = sizeof(GeometryInfo);

  // Serialized version of `GeometryInfo`
  using GeometryInfoBuffer = std::array<uint8_t, geoInfoOffset>;

  // For an invalid WKT literal, the serialized geometry info is all-zero
  static constexpr GeometryInfoBuffer invalidGeoInfoBuffer = {};

  // Offset for the header of the geometry information file
  static constexpr size_t geoInfoHeader =
      sizeof(ad_utility::GEOMETRY_INFO_VERSION);

 public:
  GeoVocabulary() = default;

  // Load the precomputed `GeometryInfo` object for the literal with
  // the given index from disk. Return `std::nullopt` for invalid geometries.
  std::optional<GeometryInfo> getGeoInfo(uint64_t index) const {
    return geoInfoAtPosition(positionFromIndex(index));
  }

  // Construct a filename for the geo info file by appending a suffix to the
  // given filename.
  static std::string getGeoInfoFilename(std::string_view filename) {
    return absl::StrCat(filename, geoInfoSuffix);
  }

  // Set the grid. Must be called before `open` (when loading a vocabulary
  // that was built with this grid) or before `makeDiskWriterPtr` (when
  // building one); it has no effect on an already opened vocabulary.
  void setGeoCellGrid(std::optional<GeoCellGrid> grid) {
    if (!literals_.size()) {
      grid_ = grid;
    }
  }

  // The grid, or `std::nullopt` if the index of a word is its position.
  const std::optional<GeoCellGrid>& getGeoCellGrid() const { return grid_; }

  // The position in the vocabulary of the word with the given index (the
  // identity without a grid).
  uint64_t positionFromIndex(uint64_t index) const {
    return grid_.has_value() ? grid_->positionOfIndex(index) : index;
  }

  // The index of the word at the given position (the identity without a
  // grid). The grid cell of the word is recomputed from its precomputed
  // bounding box, or from the word itself if the geometry is invalid, by the
  // same rule that the `WordWriter` used to assign it.
  uint64_t indexFromPosition(uint64_t position, std::string_view word) const;

  // The index that is larger than the index of every word in this vocabulary
  // (used to represent "past the end" positions of binary searches, and as
  // the upper bound for valid indices).
  uint64_t endIndex() const { return endIndex_; }

  // Forward all the standard operations to the underlying literal vocabulary.
  // See there for more details.

  // ___________________________________________________________________________
  decltype(auto) operator[](uint64_t id) const {
    auto position = positionFromIndex(id);
    AD_CORRECTNESS_CHECK(position < size());
    return literals_[position];
  }

  //____________________________________________________________________________
  VocabBatchLookupResult lookupBatch(ql::span<const size_t> indices) const;

  //____________________________________________________________________________
  VocabLookupOutput lookupBatchesStreamed(VocabLookupInput input) const {
    return ad_utility::vocabulary::lookupBatchesStreamed(*this,
                                                         std::move(input));
  }

  // Iterate over all words together with their index.
  auto scanAll() const {
    return ad_utility::OwningView{literals_.scanAll()} |
           ql::views::transform([this](IndexAndWord indexAndWord) {
             indexAndWord.index_ =
                 indexFromPosition(indexAndWord.index_, indexAndWord.word_);
             return indexAndWord;
           });
  }

  // ___________________________________________________________________________
  [[nodiscard]] uint64_t size() const { return literals_.size(); }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return withIndexFromPosition(literals_.lower_bound(word, comparator));
  }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return withIndexFromPosition(literals_.upper_bound(word, comparator));
  }

  // ___________________________________________________________________________
  UnderlyingVocabulary& getUnderlyingVocabulary() { return literals_; }

  // ___________________________________________________________________________
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return literals_;
  }

  // ___________________________________________________________________________
  void open(const std::string& filename);

  // Custom word writer, which precomputes and writes geometry info along with
  // the words. With a grid, it also checks that the words arrive ordered by
  // cell and puts the cell index into the upper bits of the returned indices.
  class WordWriter : public WordWriterBase {
   private:
    std::unique_ptr<typename UnderlyingVocabulary::WordWriter>
        underlyingWordWriter_;
    ad_utility::File geoInfoFile_;
    // The grid, or `std::nullopt` if the index of a word is its position.
    std::optional<GeoCellGrid> grid_;
    // The cell of the previous word (only with a grid), to check the order.
    std::optional<GeoCellGrid::CellIndex> lastCellIndex_;
    // The number of words written so far (the position of the next word).
    uint64_t numWords_ = 0;
    size_t numInvalidGeometries_ = 0;
    size_t numInvalidPolygonArea_ = 0;

   public:
    // Initialize the `geoInfoFile_` by writing its header and open a word
    // writer on the underlying vocabulary.
    WordWriter(const UnderlyingVocabulary& vocabulary,
               const std::string& filename, std::optional<GeoCellGrid> grid);

    // Add the next literal to the vocabulary, precompute additional information
    // using `GeometryInfo` and return the literal's new index.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    // Finish the writing on the underlying writer and close the `geoInfoFile_`
    // file handle. After this no more calls to `operator()` are allowed.
    void finishImpl() override;

    ~WordWriter() override;
  };

  // The files of the underlying vocabulary, which is stored under the base
  // filename itself, plus the file with the geometry information.
  static FileSuffixes fileSuffixes() {
    FileSuffixes suffixes = UnderlyingVocabulary::fileSuffixes();
    suffixes.emplace_back(geoInfoSuffix);
    return suffixes;
  }

  // ___________________________________________________________________________
  std::unique_ptr<WordWriter> makeDiskWriterPtr(
      const std::string& filename) const {
    return std::make_unique<WordWriter>(literals_, filename, grid_);
  }

  // ___________________________________________________________________________
  void close();

  // Generic serialization support.
  AD_SERIALIZE_FRIEND_FUNCTION(GeoVocabulary) {
    (void)serializer;
    (void)arg;
    throw std::runtime_error(
        "Generic serialization is not implemented for GeoVocabulary.");
  }

 private:
  // The grid cell of a word, given its precomputed geometry info (if the
  // geometry is valid) and the word itself. This single rule is used by the
  // `WordWriter` to assign the cell and by `indexFromPosition` to recompute
  // it, so the two always agree.
  static GeoCellGrid::CellIndex cellIndexOfWord(
      const GeoCellGrid& grid, const std::optional<GeometryInfo>& info,
      std::string_view word) {
    // When the `GeometryInfo` is valid, its bounding box is the one that
    // `cellIndexFromWktLiteral` would compute, so it can be reused; otherwise
    // `cellIndexFromWktLiteral` can still assign a regular cell in corner
    // cases where only parts of the `GeometryInfo` computation failed.
    return info.has_value()
               ? grid.cellIndexFromBoundingBox(info->getBoundingBox())
               : grid.cellIndexFromWktLiteral(word);
  }

  // The precomputed `GeometryInfo` of the word at the given position.
  std::optional<GeometryInfo> geoInfoAtPosition(uint64_t position) const;

  // Compute `endIndex_` for the opened vocabulary.
  uint64_t computeEndIndex() const;

  // Replace the position in a non-end `WordAndIndex` by the index.
  WordAndIndex withIndexFromPosition(WordAndIndex wordAndIndex) const {
    if (wordAndIndex.isEnd() || !grid_.has_value()) {
      return wordAndIndex;
    }
    return {wordAndIndex.word(),
            indexFromPosition(wordAndIndex.index(), wordAndIndex.word())};
  }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
