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

// The cell assignment scheme of the `GeoCellGrid` below. There is currently
// only one scheme: `Flat`, a single grid of 2^level x 2^level cells, where
// each geometry that fits inside a single cell gets that cell and every
// other geometry gets the sentinel cell. A further scheme (for example,
// several grid copies shifted against each other, or a hierarchical grid)
// only adds an enum value and private methods to the `GeoCellGrid`, because
// all its public methods work with plain cell numbers of `numCellBits()`
// bits.
enum class GeoCellGridScheme : uint8_t {
  Flat = 0,
};

// All schemes, in the order of their numeric values. Used for parsing and
// for iterating over the schemes in tests.
inline constexpr std::array allGeoCellGridSchemes{GeoCellGridScheme::Flat};

// Conversion of a `GeoCellGridScheme` to and from its parameter value
// ("flat").
std::string_view toString(GeoCellGridScheme scheme);
std::optional<GeoCellGridScheme> geoCellGridSchemeFromString(
    std::string_view name);

// A grid over the earth's surface in geographic coordinates, used to encode
// a coarse spatial key ("grid cell") into the vocabulary indices of WKT
// literals. The base grid of `level` L subdivides the longitude range
// [-180, 180] and the latitude range [-90, 90] into 2^L intervals each; the
// cell assignment on top of it is determined by the `GeoCellGridScheme`.
//
// Each WKT literal has exactly one cell, computed by `cellFromWktLiteral`
// from the WKT string alone. The method `coveringCellRanges` guarantees: if
// the bounding box of a geometry intersects a given rectangle, then the cell
// of that geometry is contained in one of the ranges computed for that
// rectangle. Code that only wants the geometries in the rectangle can
// therefore safely skip every WKT literal whose cell lies outside all of the
// ranges. In particular, the ranges always contain the sentinel cell, so
// that geometries that span several cells and literals that cannot be
// parsed are never skipped.
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

  // Number of grid columns (= rows).
  uint64_t numCellsPerDimension() const { return uint64_t{1} << level_; }

  // Number of bits occupied by a cell number.
  //
  // NOTE: This can depend on the scheme (a scheme with several grid copies
  // needs extra bits to select the copy), so the bit count must never be
  // hard-coded elsewhere.
  uint64_t numCellBits() const { return 2 * uint64_t{level_} + 1; }

  // The cell number for "no information": for the flat scheme the reserved
  // all-ones sentinel (larger than every regular cell number). It is always
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
  // entirely, or `sentinelCell()` if no regular cell contains it.
  //
  // NOTE: The corners are normalized through the `GeoPoint` bit encoding, so
  // that the result is identical for a freshly parsed bounding box and for
  // one that took a round trip through the precomputed `GeometryInfo`.
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

  // Compute the cell ranges for the geographic rectangle given by the two
  // corner points, as closed ranges [first, last] of cell numbers, ascending
  // and non-overlapping. The guarantee is: if the bounding box of a geometry
  // intersects the rectangle, then the cell of that geometry (each geometry
  // has exactly one, see `cellFromBoundingBox`) is contained in one of the
  // ranges. The sentinel cell is always part of the ranges.
  CellRanges coveringCellRanges(double minLng, double minLat, double maxLng,
                                double maxLat) const;

  // Helpers for the vocabulary indices of WKT literals in a
  // `SplitGeoVocabulary` (see `SplitVocabulary`): there the topmost data bit
  // of a vocabulary index is 1 exactly for the WKT literals, and the bits
  // below it hold the cell-annotated index of this grid.
  //
  // NOTE: This encodes the marker layout of a `SplitVocabulary` with exactly
  // two underlying vocabularies; unit tests check that the two stay
  // consistent.
  static constexpr uint64_t geoVocabMarkerBit = uint64_t{1}
                                                << (ValueId::numDataBits - 1);
  static constexpr bool isGeoVocabIndex(uint64_t vocabIndexBits) {
    return (vocabIndexBits & geoVocabMarkerBit) != 0;
  }

  // The half-open range of vocabulary indices (with the marker bit set) that
  // contains exactly the WKT literals whose cell is in [first, last].
  //
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
  // Integer grid coordinates of a normalized coordinate in [0, 1], clamped
  // to [0, 2^level - 1].
  uint64_t gridCoordinate(double normalized) const;

  // Cell assignment and cover computation of the `Flat` scheme, on
  // normalized coordinates in [0, 1].
  Cell flatCell(double u1, double v1, double u2, double v2) const;
  void flatCover(double u1, double v1, double u2, double v2,
                 CellRanges& ranges) const;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_GEOCELLGRID_H
