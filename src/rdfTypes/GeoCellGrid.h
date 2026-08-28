// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_RDFTYPES_GEOCELLGRID_H
#define QLEVER_SRC_RDFTYPES_GEOCELLGRID_H

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "global/ValueId.h"
#include "rdfTypes/GeometryInfo.h"

namespace ad_utility {

// A flat grid over the earth's surface in geographic coordinates, used to
// encode a coarse spatial key ("grid cell") into the vocabulary indices of WKT
// literals. A grid of `level` L subdivides the longitude range [-180, 180] and
// the latitude range [-90, 90] into 2^L intervals each, yielding 2^(2L)
// cells. The number of a cell is `(cellY << L) | cellX`, so cells are numbered
// row-major and a band of equal latitude is contiguous.
//
// A WKT literal is assigned the cell that contains its bounding box entirely.
// Literals whose bounding box crosses a cell border, and literals that cannot
// be parsed, are assigned the reserved `sentinelCell()` instead, which is
// larger than every regular cell number. Consumers that prune by cells must
// always keep the sentinel cell (it means "no information").
//
// The cell number occupies `numCellBits() = 2L + 1` bits: one bit more than
// the 2L bits of the regular cell numbers, so that the all-ones sentinel is
// distinct from the largest regular cell.
class GeoCellGrid {
 public:
  // Cell numbers and cell-annotated vocabulary indices.
  using Cell = uint64_t;

  // Return true iff `word` is a WKT literal (with quotes and datatype
  // suffix). This is the same criterion by which the `SplitGeoVocabulary`
  // routes words into the geo vocabulary.
  static bool isWktLiteral(std::string_view word) {
    return ql::starts_with(word, "\"") &&
           ql::ends_with(word, GEO_LITERAL_SUFFIX);
  }

 private:
  uint8_t level_;

 public:
  // The level must be at least 1 and small enough that the cell bits plus at
  // least one position bit fit into a vocabulary index (see
  // `numPositionBits()`).
  explicit GeoCellGrid(uint8_t level);

  uint8_t level() const { return level_; }

  // Number of grid columns (= rows).
  uint64_t numCellsPerDimension() const { return uint64_t{1} << level_; }

  // Number of bits occupied by a cell number, including the sentinel.
  uint64_t numCellBits() const { return 2 * uint64_t{level_} + 1; }

  // The reserved cell number for "no cell" (bounding box crosses a cell
  // border, or the literal is not a valid geometry). All ones, in particular
  // larger than every regular cell number.
  Cell sentinelCell() const { return (uint64_t{1} << numCellBits()) - 1; }

  // Number of bits remaining for the position of a word inside the geo
  // vocabulary: the data bits of a `ValueId` minus one marker bit of the
  // `SplitVocabulary` minus the cell bits.
  uint64_t numPositionBits() const {
    return ValueId::numDataBits - 1 - numCellBits();
  }

  // Maximum number of words the geo vocabulary can hold with this grid.
  uint64_t maxNumWords() const { return uint64_t{1} << numPositionBits(); }

  // The cell containing the point (`lng`, `lat`). Coordinates are clamped to
  // the valid ranges.
  Cell cellFromPoint(double lng, double lat) const;

  // The cell that contains the bounding box entirely, or `sentinelCell()` if
  // the bounding box crosses a cell border.
  Cell cellFromBoundingBox(const BoundingBox& box) const;

  // The cell for a full WKT literal (with quotes and datatype suffix):
  // `cellFromBoundingBox` of its parsed bounding box, or `sentinelCell()` if
  // the literal cannot be parsed. This is the canonical assignment used both
  // when sorting the vocabulary and when comparing words at query time, so it
  // must be a pure function of the literal string.
  Cell cellFromWktLiteral(std::string_view wktLiteral) const;

  // Combine a cell and a position into a cell-annotated vocabulary index and
  // take it apart again. The position must be smaller than `maxNumWords()`.
  uint64_t annotateIndex(Cell cell, uint64_t position) const {
    return (cell << numPositionBits()) | position;
  }
  Cell cellOfIndex(uint64_t annotatedIndex) const {
    return annotatedIndex >> numPositionBits();
  }
  uint64_t positionOfIndex(uint64_t annotatedIndex) const {
    return annotatedIndex & (maxNumWords() - 1);
  }

  // All cells that intersect the geographic rectangle given by the two corner
  // points, as inclusive ranges [first, last] of cell numbers (one range per
  // grid row), followed by the sentinel cell as final range. Pruning by the
  // result is conservative: every geometry that intersects the rectangle has
  // its cell (or the sentinel) in one of the ranges.
  std::vector<std::pair<Cell, Cell>> coveringCellRanges(double minLng,
                                                        double minLat,
                                                        double maxLng,
                                                        double maxLat) const;

  // Helpers for the vocabulary indices of WKT literals in a
  // `SplitGeoVocabulary` (see `SplitVocabulary`): there the WKT region is
  // marked by the top bit of the index payload, below which the
  // cell-annotated index of this grid is stored. NOTE: This encodes the
  // marker layout of a `SplitVocabulary` with exactly two underlying
  // vocabularies; unit tests check that the two stay consistent.
  static constexpr uint64_t geoVocabMarkerBit = uint64_t{1}
                                                << (ValueId::numDataBits - 1);
  static constexpr bool isGeoVocabIndex(uint64_t vocabIndexBits) {
    return (vocabIndexBits & geoVocabMarkerBit) != 0;
  }

  // The half-open range of vocabulary index payloads (marker bit included)
  // that contains exactly the WKT literals with a cell in [first, last].
  // NOTE: The bounds are computed by addition, not bitwise or: for the
  // sentinel cell the exclusive upper bound `(last + 1) << numPositionBits()`
  // carries into (or past) the marker bit.
  std::pair<uint64_t, uint64_t> vocabIndexRangeForCells(Cell first,
                                                        Cell last) const {
    return {geoVocabMarkerBit + (first << numPositionBits()),
            geoVocabMarkerBit + ((last + 1) << numPositionBits())};
  }

  bool operator==(const GeoCellGrid&) const = default;

 private:
  // Grid coordinates of a point, clamped to [0, 2^level - 1].
  uint64_t gridX(double lng) const;
  uint64_t gridY(double lat) const;
};

// A prefilter for the canonical spatial join situation: given the (padded)
// query rectangle, it decides from a WKT literal's vocabulary index alone -
// two bit operations and a binary search over a handful of ranges, no disk
// access - whether the literal can be skipped because its grid cell does not
// intersect the rectangle. Conservative: literals without a cell (sentinel)
// and indices outside the WKT region are never skipped.
class GeoCellIdPrefilter {
  // Half-open, ascending ranges of vocabulary index payloads that must be
  // kept (covering cells plus the sentinel cell).
  std::vector<std::pair<uint64_t, uint64_t>> keepRanges_;

 public:
  GeoCellIdPrefilter(const GeoCellGrid& grid, double minLng, double minLat,
                     double maxLng, double maxLat);

  // Return true iff the word with the given vocabulary index payload is
  // certainly outside the query rectangle.
  bool canBeSkipped(uint64_t vocabIndexBits) const;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_GEOCELLGRID_H
