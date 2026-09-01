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

// The available schemes for the `GeoCellGrid` class below.
//
// NOTE: There is currently only one scheme, `Flat`, implemented below. It's a
// simple flat grid. Future schemes may be hierarchical or have several copies
// of the grid shifted against each other. The abstract interface of the
// `GeoCellGrid` class below is general enough to support these future schemes.
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

  // Return true iff `word` is a WKT literal (with quotes and datatype
  // suffix). This is the criterion by which the `SplitGeoVocabulary` routes
  // words into the geo vocabulary (its `GeoSplitFunc` calls this function).
  static bool isWktLiteral(std::string_view word) {
    return ql::starts_with(word, "\"") &&
           ql::ends_with(word, GEO_LITERAL_SUFFIX);
  }

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
  explicit GeoCellGrid(uint8_t level,
                       GeoCellGridScheme scheme = GeoCellGridScheme::Flat);

  uint8_t level() const { return level_; }
  GeoCellGridScheme scheme() const { return scheme_; }

  // The number of grid columns or rows (the same in all schemes).
  uint64_t numCellsPerDimension() const { return uint64_t{1} << level_; }

  // The number of bits occupied by a cell index.
  //
  // NOTE: This can depend on the scheme (a scheme with several grid copies
  // needs extra bits to select the copy), so the bit count must never be
  // hard-coded elsewhere.
  uint64_t numCellBits() const { return 2 * uint64_t{level_} + 1; }

  // The index of the special "sentinel" cell, which is assigned to every WKT
  // literal that does not fit into any regular cell.
  CellIndex sentinelCell() const;

  // The number of bits remaining for the position of a word inside the geo
  // vocabulary: the data bits of a `ValueId` minus one marker bit of the
  // `SplitVocabulary` minus the cell bits.
  uint64_t numPositionBits() const {
    return ValueId::numDataBits - 1 - numCellBits();
  }

  // The maximum number of words the geo vocabulary can hold with this grid.
  uint64_t maxNumWords() const { return uint64_t{1} << numPositionBits(); }

  // The index of the cell that contains the point (`lng`, `lat`), with
  // clamping. Only valid for the `Flat` scheme; used by tests and
  // diagnostics.
  CellIndex cellIndexFromPoint(double lng, double lat) const;

  // The cell index for the given bounding box: the smallest regular cell that
  // contains it entirely, or `sentinelCell()` if no such cell exists.
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
  // and take it apart again. The position must be smaller than
  // `maxNumWords()`.
  uint64_t annotateIndex(CellIndex cellIndex, uint64_t position) const {
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
  // two underlying vocabularies. In the unit tests, there are checks that the
  // two are consistent.
  static constexpr uint64_t geoVocabMarkerBit = uint64_t{1}
                                                << (ValueId::numDataBits - 1);
  static constexpr bool isGeoVocabIndex(uint64_t vocabIndexBits) {
    return (vocabIndexBits & geoVocabMarkerBit) != 0;
  }

  // For the given closed range of cell indices, compute the half-open range of
  // vocabulary indices (with the marker bit set) that contains exactly the WKT
  // literals whose cell index is in `[first, last]`.
  //
  // NOTE: The bounds must be computed by addition, not by bitwise or. For
  // the sentinel cell, `(last + 1) << numPositionBits()` is exactly the
  // marker bit, so the addition carries into the bit above it. That is
  // harmless: the upper bound is exclusive and only used in comparisons, so
  // it may be larger than every valid vocabulary index. With a bitwise or,
  // the carry would be lost and the upper bound would be smaller than the
  // lower bound.
  std::pair<uint64_t, uint64_t> vocabIndexRangeForCells(CellIndex first,
                                                        CellIndex last) const {
    return {geoVocabMarkerBit + (first << numPositionBits()),
            geoVocabMarkerBit + ((last + 1) << numPositionBits())};
  }

  bool operator==(const GeoCellGrid&) const = default;

 private:
  // Integer grid coordinates of a normalized coordinate in `[0, 1]`, clamped
  // to `[0, 2^level - 1]`.
  uint64_t gridCoordinate(double normalized) const;

  // Cell assignment and cover computation of the `Flat` scheme, on
  // normalized coordinates in `[0, 1]`.
  CellIndex flatCell(double u1, double v1, double u2, double v2) const;
  void flatCover(double u1, double v1, double u2, double v2,
                 CellRanges& ranges) const;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_GEOCELLGRID_H
