// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_RDFTYPES_GEOCELLGRID_H
#define QLEVER_SRC_RDFTYPES_GEOCELLGRID_H

#include <array>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/three_way_comparison.h"
#include "global/ValueId.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/EnumWithStrings.h"
#include "util/Exception.h"

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
GeoRectangle padGeoRectangle(const GeoRectangle& rectangle,
                             double distanceMeters);

namespace detail {
// The available schemes for the `GeoCellGrid` class below. All schemes use
// square-ish base grids of `2^level x 2^level` cells:
//
// - `Flat`: a single flat grid. Geometries whose bounding box crosses a cell
//   border get the sentinel cell.
// - `Flat4Shifts`: four flat grids, shifted against each other by half a
//   cell in longitude and/or latitude. Every geometry with a bounding box of
//   at most half a cell in both dimensions fits a cell of at least one of
//   the grids; only larger geometries get the sentinel cell.
// - `Hierarchical`: a single quadtree of depth `level` with S2-style cell
//   indices. Every geometry is stored at the smallest enclosing cell; the
//   root takes the role of the sentinel.
// - `Hierarchical3Shifts`: three quadtrees, shifted against each other by a
//   third of the domain in both dimensions (Chan's shifted quadtrees).
//   Every geometry is contained in a cell of side length at most six times
//   its own size in at least one of the trees, so nothing escalates more
//   than a constant number of levels.
//
// NOTE: The numeric values are part of the index format (they are stored in
// the `.geocells` file of a `GeoVocabulary`), so they must never change.
enum class GeoCellGridSchemeEnum : uint8_t {
  Flat = 0,
  Flat4Shifts = 1,
  Hierarchical = 2,
  Hierarchical3Shifts = 3,
};
}  // namespace detail

// Wrapper around `detail::GeoCellGridSchemeEnum` that provides conversion to
// and from the parameter values ("flat", "flat-4-shifts", "hierarchical",
// "hierarchical-3-shifts").
class GeoCellGridScheme
    : public EnumWithStrings<GeoCellGridScheme, detail::GeoCellGridSchemeEnum> {
 public:
  using Enum = detail::GeoCellGridSchemeEnum;

  static constexpr std::array<std::pair<Enum, std::string_view>, 4>
      descriptions_{{{Enum::Flat, "flat"},
                     {Enum::Flat4Shifts, "flat-4-shifts"},
                     {Enum::Hierarchical, "hierarchical"},
                     {Enum::Hierarchical3Shifts, "hierarchical-3-shifts"}}};
  static const GeoCellGridScheme Flat;
  static const GeoCellGridScheme Flat4Shifts;
  static const GeoCellGridScheme Hierarchical;
  static const GeoCellGridScheme Hierarchical3Shifts;

  static constexpr std::string_view typeName() {
    return "geo cell grid scheme";
  }

  using EnumWithStrings::EnumWithStrings;
};

// A grid over the earth's surface in geographic coordinates. The base grid of
// `level` L subdivides the longitude range [-180, 180] and the latitude range
// [-90, 90] into 2^L intervals each. How exactly the grid is placed and how
// the grid cells are indexed is determined by the `GeoCellGridScheme`. The
// relation between the grid and a WKT literal is defined by the following two
// methods (see the methods themselves for details):
//
// 1. For a given WKT literal, `cellIndexFromWktLiteral` returns the index of a
// cell that covers the geometry, or, if there is no such cell, the index of the
// special "sentinel" cell.
//
// 2. For any given rectangle, `coveringCellRanges` computes ranges of cell
// indices that are guaranteed to contain the cell index of every geometry
// whose bounding box intersects the rectangle.
class GeoCellGrid {
 public:
  // The type for the index of a cell in the grid. In particular, this index is
  // encoded into the vocabulary index of a WKT literal.
  using CellIndex = uint64_t;

  // A list of closed, ascending, non-overlapping ranges of cell indices.
  using CellRanges = std::vector<std::pair<CellIndex, CellIndex>>;

 private:
  // The level of the base grid, which has `2^level x 2^level` cells.
  uint8_t level_;

  // The scheme by which cell indices are assigned to geometries.
  GeoCellGridScheme scheme_;

 public:
  // Create a grid of the given `level` with the given `scheme`. The grid has
  // `2^level x 2^level` cells.
  //
  // NOTE: The level must be at least 1 and small enough that a vocabulary
  // index can hold the cell index plus at least one bit for the position of
  // the word inside the geo vocabulary; see `numPositionBits()`.
  explicit GeoCellGrid(
      uint8_t level, GeoCellGridScheme scheme = GeoCellGridScheme::Enum::Flat);

  uint8_t level() const { return level_; }
  GeoCellGridScheme scheme() const { return scheme_; }

  // The number of grid columns or rows (the same in all schemes).
  uint64_t numCellsPerDimension() const { return uint64_t{1} << level_; }

  // The number of bits occupied by a cell index: `2 * level + 1` for the
  // single-grid schemes, two more for the shifted schemes (the extra bits
  // select the grid copy).
  //
  // NOTE: This depends on the scheme, so the bit count must never be
  // hard-coded elsewhere.
  uint64_t numCellBits() const {
    return 2 * uint64_t{level_} + 1 + (isShifted() ? 2 : 0);
  }

  // The largest cell index that fits into `numCellBits()` bits. Every cell
  // index of every scheme is at most this value.
  CellIndex maxCellIndex() const { return (uint64_t{1} << numCellBits()) - 1; }

  // The index of the special "sentinel" cell, which is assigned to every WKT
  // literal that does not fit into any regular cell (and to literals that
  // cannot be parsed): for the flat schemes the reserved all-ones index
  // (`maxCellIndex()`), for the hierarchical schemes the root cell of the
  // first quadtree copy. It is always part of `coveringCellRanges`.
  CellIndex sentinelCell() const;

  // The number of bits remaining for the position of a word inside the geo
  // vocabulary: the data bits of a `ValueId` minus one marker bit of the
  // `SplitVocabulary` minus the cell bits.
  uint64_t numPositionBits() const {
    return ValueId::numDataBits - 1 - numCellBits();
  }

  // The maximum number of words the geo vocabulary can hold with this grid.
  uint64_t maxNumWords() const { return uint64_t{1} << numPositionBits(); }

  // The cell index containing the point (`lng`, `lat`), with clamping. This
  // is the cell index that `cellIndexFromBoundingBox` assigns to the
  // degenerate bounding box consisting of just that point.
  CellIndex cellIndexFromPoint(double lng, double lat) const;

  // The cell index for the given bounding box: the smallest regular cell that
  // contains it entirely (for the shifted schemes over all grid copies), or
  // `sentinelCell()` if no such cell exists.
  //
  // NOTE: The corners are normalized through the `GeoPoint` bit encoding, so
  // that the result is identical for a freshly parsed bounding box and for
  // one that took a round trip through the precomputed `GeometryInfo`.
  CellIndex cellIndexFromBoundingBox(const BoundingBox& box) const;

  // The cell index for a full WKT literal (with quotes and datatype suffix):
  // `cellIndexFromBoundingBox` of its parsed bounding box, or `sentinelCell()`
  // if the literal cannot be parsed. This is the canonical assignment used both
  // when sorting the vocabulary and when comparing words at query time, so it
  // must be a pure function of the literal string.
  CellIndex cellIndexFromWktLiteral(std::string_view wktLiteral) const;

  // Combine a cell index and a position into an annotated vocabulary index
  // and take it apart again. The cell index must be at most `maxCellIndex()`
  // and the position must be smaller than `maxNumWords()`.
  uint64_t annotateIndex(CellIndex cellIndex, uint64_t position) const {
    AD_EXPENSIVE_CHECK(cellIndex <= maxCellIndex());
    AD_EXPENSIVE_CHECK(position < maxNumWords());
    return (cellIndex << numPositionBits()) | position;
  }
  CellIndex cellOfIndex(uint64_t annotatedIndex) const {
    return annotatedIndex >> numPositionBits();
  }
  uint64_t positionOfIndex(uint64_t annotatedIndex) const {
    return annotatedIndex & (maxNumWords() - 1);
  }

  // For the rectangle given by the two corner points, compute closed,
  // ascending, and non-overlapping ranges of cell indices with the following
  // property:
  //
  // If the bounding box of a geometry intersects the rectangle, then the cell
  // index of that geometry (each geometry has exactly one, see
  // `cellIndexFromBoundingBox`) is contained in one of the ranges. The index
  // of the sentinel cell is always part of the ranges.
  //
  // NOTE: When using these ranges for pre-filtering, this will keep blocks
  // that contain potential results; all other blocks are guaranteed to contain
  // no results.
  CellRanges coveringCellRanges(double minLng, double minLat, double maxLng,
                                double maxLat) const;

  // Helper that checks whether a vocabulary index is in the geo vocabulary.
  //
  // NOTE: This encodes the marker layout of a `SplitVocabulary` with exactly
  // two underlying vocabularies. It cannot use the constants of the
  // `SplitGeoVocabulary` directly, because that class lives in the vocabulary
  // layer above this one. In the unit tests, there are checks that the two
  // are consistent.
  static constexpr uint64_t geoVocabMarkerBit = uint64_t{1}
                                                << (ValueId::numDataBits - 1);
  static constexpr bool isGeoVocabIndex(uint64_t vocabIndexBits) {
    return (vocabIndexBits & geoVocabMarkerBit) != 0;
  }

  // For the given closed range of cell indices, compute the half-open range of
  // vocabulary indices (with the marker bit set) that contains exactly the WKT
  // literals whose cell index is in `[first, last]`.
  //
  // NOTE: The bounds are computed by addition, not bitwise or: for the
  // largest cell index, `(last + 1) << numPositionBits()` carries into (or
  // past) the marker bit. That is fine because the upper bound is exclusive
  // and only used in comparisons.
  std::pair<uint64_t, uint64_t> vocabIndexRangeForCells(CellIndex first,
                                                        CellIndex last) const {
    AD_CONTRACT_CHECK(first <= last && last <= maxCellIndex());
    return {geoVocabMarkerBit + (first << numPositionBits()),
            geoVocabMarkerBit + ((last + 1) << numPositionBits())};
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(GeoCellGrid, level_, scheme_)

 private:
  // Whether the scheme uses several shifted grid copies, and whether it is
  // hierarchical (a quadtree instead of a flat grid).
  bool isShifted() const {
    return scheme_ == GeoCellGridScheme::Enum::Flat4Shifts ||
           scheme_ == GeoCellGridScheme::Enum::Hierarchical3Shifts;
  }
  bool isHierarchical() const {
    return scheme_ == GeoCellGridScheme::Enum::Hierarchical ||
           scheme_ == GeoCellGridScheme::Enum::Hierarchical3Shifts;
  }

  // Coordinate convention of the private helpers: `u` and `v` are normalized
  // longitude and latitude in `[0, 1]` (as computed by `normalize`), `x` and
  // `y` are integer grid coordinates in `[0, 2^level - 1]` (leaf coordinates
  // for the hierarchical schemes).

  // The integer grid coordinates of the two corners of a normalized
  // rectangle.
  struct GridBox {
    uint64_t x1_;
    uint64_t y1_;
    uint64_t x2_;
    uint64_t y2_;
  };
  GridBox gridBox(double u1, double v1, double u2, double v2) const;

  // Integer grid coordinates of a normalized coordinate in `[0, 1]`, clamped
  // to `[0, 2^level - 1]`. Must not be called with `NaN`, which would trigger
  // an `AD_CONTRACT_CHECK` failure.
  uint64_t gridCoordinate(double normalized) const;

  // Map a longitude and a latitude to normalized coordinates in `[0, 1]`.
  static std::pair<double, double> normalize(double lng, double lat) {
    return {(lng + 180.0) / 360.0, (lat + 90.0) / 180.0};
  }

  // Merge those of the given ranges that overlap or are adjacent, so that the
  // result is non-overlapping. The ranges must be ascending; a scheme that
  // does not produce them in that order has to sort them first.
  static CellRanges mergeRanges(const CellRanges& ranges);

  // Cell assignment of the four schemes, on normalized coordinates in
  // `[0, 1]`. The `hash` of the `Flat4Shifts` scheme determines the order in
  // which the grid copies are tried (see the implementation).
  CellIndex flatCell(double u1, double v1, double u2, double v2) const;
  CellIndex flat4ShiftsCell(double u1, double v1, double u2, double v2,
                            uint64_t hash) const;
  CellIndex hierarchicalCell(double u1, double v1, double u2, double v2) const;
  CellIndex hierarchical3ShiftsCell(double u1, double v1, double u2,
                                    double v2) const;

  // The S2-style cell index (without shift bits) of the smallest cell of one
  // (unshifted) quadtree that contains the leaf box `[x1, x2] x [y1, y2]`.
  CellIndex hierarchicalCellOfLeafBox(uint64_t x1, uint64_t y1, uint64_t x2,
                                      uint64_t y2) const;

  // Cover computation of the four schemes, appending to `ranges` (not
  // necessarily in ascending order).
  void flatCover(double u1, double v1, double u2, double v2,
                 CellRanges& ranges) const;
  void flat4ShiftsCover(double u1, double v1, double u2, double v2,
                        CellRanges& ranges) const;
  void hierarchicalCover(double u1, double v1, double u2, double v2,
                         CellIndex shiftPrefix, CellRanges& ranges) const;
  void hierarchical3ShiftsCover(double u1, double v1, double u2, double v2,
                                CellRanges& ranges) const;

  // Recursive quadtree walk for the hierarchical cover, in leaf coordinates.
  void hierarchicalCoverRecurse(uint64_t queryX1, uint64_t queryY1,
                                uint64_t queryX2, uint64_t queryY2,
                                uint64_t levelK, uint64_t cellX, uint64_t cellY,
                                CellIndex shiftPrefix,
                                CellRanges& ranges) const;
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
