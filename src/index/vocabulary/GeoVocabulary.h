// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "rdfTypes/GeoCellGrid.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/Serializer/Serializer.h"
#include "util/Views.h"

// A `GeoVocabulary` holds Well-Known Text (WKT) literals. In contrast to the
// regular vocabulary classes it does not only store the strings. Instead it
// stores both preprocessed and original forms of its input words. Preprocessing
// includes for example the computation of bounding boxes for accelerated
// spatial queries. See the `GeometryInfo` class for details. Note: A
// `GeoVocabulary` is only suitable for WKT literals, therefore it should be
// used as part of a `SplitVocabulary`.
//
// When a `GeoCellGrid` is configured (see `setGeoCellGrid`), the words must
// arrive at the `WordWriter` ordered by their grid cell first (the
// `TripleComponentComparator` produces exactly this order), and the indices
// handed out and accepted by this class are "cell-annotated": the grid cell of
// the word occupies the upper bits and the word's dense position in the
// vocabulary occupies the lower `GeoCellGrid::numPositionBits()` bits. All
// words of one cell thereby form one contiguous index range that can be
// computed from the cell number alone, which is the basis for the geo cell
// prefilter of spatial joins. Whether a loaded vocabulary uses annotated
// indices is stored in (and read from) an extra `.geocells` file.
template <typename UnderlyingVocabulary>
class GeoVocabulary {
 private:
  using GeometryInfo = ad_utility::GeometryInfo;
  using GeoCellGrid = ad_utility::GeoCellGrid;

  UnderlyingVocabulary literals_;

  // The file in which the additional information on the geometries (like
  // bounding box) is stored.
  ad_utility::File geoInfoFile_;

  // The grid used for cell-annotated indices, or `std::nullopt` if indices
  // are plain positions. At build time this is configured via
  // `setGeoCellGrid` before the `WordWriter` is created; at load time it is
  // restored from the `.geocells` file.
  std::optional<GeoCellGrid> grid_;

  // For each maximal run of words with the same grid cell, the pair
  // (position of the first word of the run, cell). Sorted by position. Only
  // non-empty if `grid_` is set. This is only needed to translate plain
  // positions back into annotated indices (`toAnnotatedIndex`); the opposite
  // direction is a bit operation.
  std::vector<std::pair<uint64_t, uint64_t>> cellRuns_;

  // TODO<ullingerc> Possibly add in-memory cache of bounding boxes here

  // Filename suffix for geometry information file
  static constexpr std::string_view geoInfoSuffix = ".geoinfo";

  // Filename suffix for the geo cell grid file
  static constexpr std::string_view geoCellsSuffix = ".geocells";

  // Version of the `.geocells` file format.
  static constexpr uint64_t geoCellsVersion = 1;

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
  std::optional<GeometryInfo> getGeoInfo(uint64_t index) const;

  // Construct a filename for the geo info file by appending a suffix to the
  // given filename.
  static std::string getGeoInfoFilename(std::string_view filename) {
    return absl::StrCat(filename, geoInfoSuffix);
  }

  // Construct a filename for the geo cell grid file by appending a suffix to
  // the given filename.
  static std::string getGeoCellsFilename(std::string_view filename) {
    return absl::StrCat(filename, geoCellsSuffix);
  }

  // Set the geo cell grid to be used by the next `WordWriter`. Must be called
  // before `makeDiskWriterPtr` when building a vocabulary with cell-annotated
  // indices. Has no effect on an already opened vocabulary (there the grid
  // from the `.geocells` file is authoritative).
  void setGeoCellGrid(std::optional<GeoCellGrid> grid) {
    if (!literals_.size()) {
      grid_ = grid;
    }
  }

  // The grid of this vocabulary, or `std::nullopt` if it uses plain indices.
  const std::optional<GeoCellGrid>& getGeoCellGrid() const { return grid_; }

  // Translate a (possibly cell-annotated) index to the plain position of the
  // word in the underlying vocabulary and back. Without a configured grid
  // both are the identity.
  uint64_t toPosition(uint64_t index) const {
    return grid_.has_value() ? grid_->positionOfIndex(index) : index;
  }
  uint64_t toAnnotatedIndex(uint64_t position) const;

  // The index that is larger than the index of every word in this vocabulary
  // (used to represent "past the end" positions of binary searches).
  uint64_t endIndex() const;

  // Forward all the standard operations to the underlying literal vocabulary.
  // See there for more details.

  // ___________________________________________________________________________
  decltype(auto) operator[](uint64_t id) const {
    auto position = toPosition(id);
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

  // Iterate over all words together with their (possibly annotated) index.
  auto scanAll() const {
    return ad_utility::OwningView{literals_.scanAll()} |
           ql::views::transform([this](IndexAndWord indexAndWord) {
             indexAndWord.index_ = toAnnotatedIndex(indexAndWord.index_);
             return indexAndWord;
           });
  }

  // ___________________________________________________________________________
  [[nodiscard]] uint64_t size() const { return literals_.size(); }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return annotateWordAndIndex(literals_.lower_bound(word, comparator));
  }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return annotateWordAndIndex(literals_.upper_bound(word, comparator));
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
  // the words. If a geo cell grid is configured, it also checks that the
  // words arrive ordered by cell, assigns cell-annotated indices, and writes
  // the cell runs to the `.geocells` file.
  class WordWriter : public WordWriterBase {
   private:
    std::unique_ptr<typename UnderlyingVocabulary::WordWriter>
        underlyingWordWriter_;
    ad_utility::File geoInfoFile_;
    std::string geoCellsFilename_;
    std::optional<GeoCellGrid> grid_;
    std::vector<std::pair<uint64_t, uint64_t>> cellRuns_;
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

    // Finish the writing on the underlying writer, write the `.geocells` file
    // if a grid is configured, and close the `geoInfoFile_` file handle. After
    // this no more calls to `operator()` are allowed.
    void finishImpl() override;

    ~WordWriter() override;
  };

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
  // Translate the index of a non-end `WordAndIndex` from a plain position to
  // an annotated index.
  WordAndIndex annotateWordAndIndex(WordAndIndex wordAndIndex) const {
    if (wordAndIndex.isEnd() || !grid_.has_value()) {
      return wordAndIndex;
    }
    return {wordAndIndex.word(), toAnnotatedIndex(wordAndIndex.index())};
  }

  // The grid cell of the word at the given plain position, from `cellRuns_`.
  uint64_t cellOfPosition(uint64_t position) const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
