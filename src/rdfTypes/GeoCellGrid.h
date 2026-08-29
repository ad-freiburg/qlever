// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_RDFTYPES_GEOCELLGRID_H
#define QLEVER_SRC_RDFTYPES_GEOCELLGRID_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "global/ValueId.h"
#include "rdfTypes/GeometryInfo.h"

namespace ad_utility {

// A geographic rectangle in plain degrees. In contrast to `BoundingBox` it
// is a simple aggregate without invariants, suitable for query rectangles
// that may cover the whole world.
struct GeoRectangle {
  double minLng_;
  double minLat_;
  double maxLng_;
  double maxLat_;
  bool operator==(const GeoRectangle&) const = default;
};

// Grow `rectangle` on all sides by at least `distanceMeters` (measured on the
// earth's surface) and clamp it to the valid coordinate ranges. The result is
// conservative: every point within `distanceMeters` of the input rectangle is
// contained in the result. Near the poles and across the antimeridian the
// longitude range degrades to [-180, 180].
GeoRectangle padGeoRectangle(GeoRectangle rectangle, double distanceMeters);

// The four cell assignment schemes of the `GeoCellGrid` below. All schemes
// use square-ish base grids of 2^level x 2^level cells:
// - `Flat`: a single flat grid. Geometries whose bounding box crosses a cell
//   border get the sentinel cell.
// - `Flat4Shifts`: four flat grids, shifted against each other by half a
//   cell in longitude and/or latitude. Every geometry with a bounding box of
//   at most half a cell in both dimensions fits a cell of at least one of
//   the grids; only larger geometries get the sentinel cell.
// - `Hierarchical`: a single quadtree of depth `level` with S2-style cell
//   IDs. Every geometry is stored at the smallest enclosing cell; the root
//   takes the role of the sentinel.
// - `Hierarchical3Shifts`: three quadtrees, shifted against each other by a
//   third of the domain in both dimensions (Chan's shifted quadtrees).
//   Every geometry is contained in a cell of side length at most six times
//   its own size in at least one of the trees, so nothing escalates more
//   than a constant number of levels.
enum class GeoCellGridScheme : uint8_t {
  Flat = 0,
  Flat4Shifts = 1,
  Hierarchical = 2,
  Hierarchical3Shifts = 3,
};

// Conversion of a `GeoCellGridScheme` to and from its parameter value
// ("flat", "flat-4-shifts", "hierarchical", "hierarchical-3-shifts").
std::string_view toString(GeoCellGridScheme scheme);
std::optional<GeoCellGridScheme> geoCellGridSchemeFromString(
    std::string_view name);

// A grid over the earth's surface in geographic coordinates, used to encode
// a coarse spatial key ("grid cell") into the vocabulary indices of WKT
// literals. The base grid of `level` L subdivides the longitude range
// [-180, 180] and the latitude range [-90, 90] into 2^L intervals each; the
// cell assignment on top of it is determined by the `GeoCellGridScheme`.
//
// A WKT literal is assigned one cell number (a pure function of the literal
// string). Consumers that prune by cells must always keep the ranges that
// `coveringCellRanges` reports for their query rectangle; that includes the
// scheme's "no information" cell(s) (the sentinel cell of the flat schemes,
// the root cell(s) of the hierarchical schemes), so pruning stays
// conservative for geometries that fit no regular cell and for unparsable
// literals.
//
// The cell number occupies `numCellBits()` bits: 2L+1 for the single-grid
// schemes, 2L+3 for the shifted schemes (two extra bits select the shifted
// copy).
class GeoCellGrid {
 public:
  // Cell numbers and cell-annotated vocabulary indices.
  using Cell = uint64_t;

  // A list of closed, ascending, non-overlapping cell ranges.
  using CellRanges = std::vector<std::pair<Cell, Cell>>;

  // Return true iff `word` is a WKT literal (with quotes and datatype
  // suffix). This is the same criterion by which the `SplitGeoVocabulary`
  // routes words into the geo vocabulary.
  static bool isWktLiteral(std::string_view word) {
    return ql::starts_with(word, "\"") &&
           ql::ends_with(word, GEO_LITERAL_SUFFIX);
  }

 private:
  uint8_t level_;
  GeoCellGridScheme scheme_;

 public:
  // The level must be at least 1 and small enough that the cell bits plus at
  // least one position bit fit into a vocabulary index (see
  // `numPositionBits()`).
  explicit GeoCellGrid(uint8_t level,
                       GeoCellGridScheme scheme = GeoCellGridScheme::Flat);

  uint8_t level() const { return level_; }
  GeoCellGridScheme scheme() const { return scheme_; }

  // Number of grid columns (= rows) of each grid copy.
  uint64_t numCellsPerDimension() const { return uint64_t{1} << level_; }

  // Whether the scheme uses multiple shifted grid copies, and how many.
  bool isShifted() const {
    return scheme_ == GeoCellGridScheme::Flat4Shifts ||
           scheme_ == GeoCellGridScheme::Hierarchical3Shifts;
  }
  bool isHierarchical() const {
    return scheme_ == GeoCellGridScheme::Hierarchical ||
           scheme_ == GeoCellGridScheme::Hierarchical3Shifts;
  }
  uint64_t numShifts() const;

  // Number of bits occupied by a cell number.
  uint64_t numCellBits() const {
    return 2 * uint64_t{level_} + 1 + (isShifted() ? 2 : 0);
  }

  // The cell number for "no information": for the flat schemes the reserved
  // all-ones sentinel (larger than every regular cell number), for the
  // hierarchical schemes the root cell of the first grid copy. It is always
  // part of `coveringCellRanges`, so it is never pruned.
  Cell sentinelCell() const;

  // Number of bits remaining for the position of a word inside the geo
  // vocabulary: the data bits of a `ValueId` minus one marker bit of the
  // `SplitVocabulary` minus the cell bits.
  uint64_t numPositionBits() const {
    return ValueId::numDataBits - 1 - numCellBits();
  }

  // Maximum number of words the geo vocabulary can hold with this grid.
  uint64_t maxNumWords() const { return uint64_t{1} << numPositionBits(); }

  // The flat-grid cell containing the point (`lng`, `lat`), with clamping.
  // Only valid for the `Flat` scheme (used by tests and diagnostics).
  Cell cellFromPoint(double lng, double lat) const;

  // The cell for the given bounding box: the smallest cell that contains it
  // entirely (for shifted schemes over all grid copies), or `sentinelCell()`
  // if no regular cell contains it. NOTE: The corners are normalized through
  // the `GeoPoint` bit encoding, so that the result is identical for a
  // freshly parsed bounding box and for one that took a round trip through
  // the precomputed `GeometryInfo`.
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

  // All cells that can be assigned to a geometry that intersects the
  // geographic rectangle given by the two corner points, as closed ranges
  // [first, last] of cell numbers, ascending and non-overlapping. Pruning by
  // the result is conservative: every geometry whose bounding box intersects
  // the rectangle has its cell in one of the ranges, and so do the
  // "no information" cells.
  CellRanges coveringCellRanges(double minLng, double minLat, double maxLng,
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
  // largest cell number the exclusive upper bound `(last + 1) <<
  // numPositionBits()` carries into (or past) the marker bit.
  std::pair<uint64_t, uint64_t> vocabIndexRangeForCells(Cell first,
                                                        Cell last) const {
    return {geoVocabMarkerBit + (first << numPositionBits()),
            geoVocabMarkerBit + ((last + 1) << numPositionBits())};
  }

  bool operator==(const GeoCellGrid&) const = default;

 private:
  // Integer grid coordinates (leaf coordinates for the hierarchical schemes)
  // of a normalized coordinate in [0, 1], clamped to [0, 2^level - 1].
  uint64_t gridCoordinate(double normalized) const;

  // Cell assignment per scheme, on normalized coordinates in [0, 1].
  Cell flatCell(double u1, double v1, double u2, double v2) const;
  Cell flat4ShiftsCell(double u1, double v1, double u2, double v2,
                       uint64_t hash) const;
  Cell hierarchicalCell(double u1, double v1, double u2, double v2) const;
  Cell hierarchical3ShiftsCell(double u1, double v1, double u2,
                               double v2) const;

  // The S2-style cell ID (without shift bits) of the smallest cell of one
  // (unshifted) quadtree that contains the leaf box [x1, x2] x [y1, y2].
  Cell hierarchicalCellOfLeafBox(uint64_t x1, uint64_t y1, uint64_t x2,
                                 uint64_t y2) const;

  // Cover computation per scheme, appending to `ranges`.
  void flatCover(double u1, double v1, double u2, double v2,
                 CellRanges& ranges) const;
  void flat4ShiftsCover(double u1, double v1, double u2, double v2,
                        CellRanges& ranges) const;
  void hierarchicalCover(double u1, double v1, double u2, double v2,
                         Cell shiftPrefix, CellRanges& ranges) const;
  void hierarchical3ShiftsCover(double u1, double v1, double u2, double v2,
                                CellRanges& ranges) const;

  // Recursive quadtree walk for the hierarchical cover, in leaf coordinates.
  void hierarchicalCoverRecurse(uint64_t queryX1, uint64_t queryY1,
                                uint64_t queryX2, uint64_t queryY2,
                                uint64_t levelK, uint64_t cellX, uint64_t cellY,
                                Cell shiftPrefix, CellRanges& ranges) const;
};

// A prefilter for the canonical spatial join situation: given the (padded)
// query rectangle, it decides from a WKT literal's vocabulary index alone -
// two bit operations and a binary search over a handful of ranges, no disk
// access - whether the literal can be skipped because its grid cell does not
// intersect the rectangle. Conservative: literals without cell information
// and indices outside the WKT region are never skipped.
class GeoCellIdPrefilter {
  // Half-open, ascending ranges of vocabulary index payloads that must be
  // kept (covering cells plus the "no information" cells).
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
