// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/RasterLookup.h"

#include <absl/strings/str_cat.h>

#include "engine/QueryExecutionTree.h"
#include "engine/raster/RasterFile.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "parser/LiteralOrIri.h"
#include "util/ChunkedForLoop.h"
#include "util/Exception.h"
#include "util/OverloadCallOperator.h"
#include "util/ParseException.h"

namespace {

// The pixels of `band` under the geometry in `geometry`: a point stored in the
// `Id`, or a WKT literal from the vocabulary or `localVocab`. Like
// `GeoPointOrWktValueGetter`. Other `Id`s (UNDEF, numbers, IRIs, ...) give no
// pixels.
std::vector<qlever::raster::Pixel> lookupPixels(
    qlever::raster::RasterFile& rasterFile, int band, Id geometry,
    const Index& index, const LocalVocab& localVocab) {
  using enum Datatype;
  switch (geometry.getDatatype()) {
    case GeoPoint: {
      auto pixel = rasterFile.lookupPoint(geometry.getGeoPoint(), band);
      if (!pixel.has_value()) {
        return {};
      }
      return {pixel.value()};
    }
    case VocabIndex:
    case LocalVocabIndex:
    case SecondaryVocabIndex: {
      auto literalOrIri = ql::exportIds::getLiteralOrIriFromVocabIndex(
          index.getImpl(), geometry, localVocab);
      if (literalOrIri.isLiteral() && literalOrIri.hasDatatype() &&
          literalOrIri.getDatatype() == GEO_WKT_LITERAL) {
        return rasterFile.lookupGeometry(literalOrIri.toStringRepresentation(),
                                         band);
      }
      return {};
    }
    // No `default`, so that a new `Datatype` makes the compiler warn here.
    case Bool:
    case Int:
    case Double:
    case Date:
    case Undefined:
    case TextRecordIndex:
    case WordVocabIndex:
    case BlankNodeIndex:
    case EncodedVal:
      return {};
  }
  AD_FAIL();
}

// The number of cells that are written between two cancellation checks.
constexpr size_t CHUNK_SIZE = 10'000;

// The `Id` of a pixel value: Int for integer bands, Double for all others.
// NOTE: `Id::makeFromInt` silently wraps values outside of [-2^59, 2^59), which
// only `Int64` bands can hold.
Id pixelValueToId(const qlever::raster::PixelValue& value) {
  return std::visit(
      ad_utility::OverloadCallOperator{
          [](int64_t intValue) { return Id::makeFromInt(intValue); },
          [](double doubleValue) { return Id::makeFromDouble(doubleValue); }},
      value);
}

}  // namespace

// _____________________________________________________________________________
RasterLookup::RasterLookup(QueryExecutionContext* qec,
                           std::shared_ptr<QueryExecutionTree> subtree,
                           RasterLookupConfiguration config)
    : Operation(qec), subtree_(std::move(subtree)), config_(std::move(config)) {
  // Mistakes in the query, reported as an invalid query (HTTP 400) instead of
  // as a failed internal check later on.
  if (!subtree_->getVariableColumnOrNullopt(config_.geometry_).has_value()) {
    throw InvalidSparqlQueryException(absl::StrCat(
        "The geometry variable ", config_.geometry_.name(),
        " of the raster lookup is not bound by the pattern inside the "
        "SERVICE"));
  }
  auto throwIfBound = [this](const Variable& variable, std::string_view role) {
    if (subtree_->getVariableColumnOrNullopt(variable).has_value()) {
      throw InvalidSparqlQueryException(absl::StrCat(
          "The ", role, " variable ", variable.name(),
          " of the raster lookup is already bound by the pattern inside the "
          "SERVICE"));
    }
  };
  throwIfBound(config_.value_, "value");
  if (config_.center_.has_value()) {
    throwIfBound(config_.center_.value(), "center");
    if (config_.center_.value() == config_.value_) {
      throw InvalidSparqlQueryException(
          "The value and center variables of the raster lookup must be "
          "different");
    }
  }

  // An unknown dataset is also a mistake in the query. `rasterFilePath` does
  // not know about SPARQL, so its error is converted here.
  try {
    path_ = qlever::raster::rasterFilePath(config_.dataset_);
  } catch (const std::runtime_error& error) {
    throw InvalidSparqlQueryException(error.what());
  }
}

// _____________________________________________________________________________
size_t RasterLookup::getResultWidth() const {
  // The value column, and the center column if it is wanted.
  return subtree_->getResultWidth() + (config_.center_.has_value() ? 2 : 1);
}

// _____________________________________________________________________________
VariableToColumnMap RasterLookup::computeVariableToColumnMap() const {
  // The columns of the child stay where they are, the new ones are appended.
  VariableToColumnMap variableColumns = subtree_->getVariableColumns();
  // The new columns are always defined, because a row without a pixel value
  // is not returned at all.
  auto addColumn = [&variableColumns](const Variable& variable,
                                      ColumnIndex column) {
    bool isNew =
        variableColumns.insert({variable, makeAlwaysDefinedColumn(column)})
            .second;
    AD_CORRECTNESS_CHECK(isNew);
  };
  ColumnIndex valueColumn = subtree_->getResultWidth();
  addColumn(config_.value_, valueColumn);
  if (config_.center_.has_value()) {
    addColumn(config_.center_.value(), valueColumn + 1);
  }
  return variableColumns;
}

// _____________________________________________________________________________
qlm::vector<QueryExecutionTree*> RasterLookup::getChildrenImpl() const {
  return {{subtree_.get()}, allocator()};
}

// _____________________________________________________________________________
std::string RasterLookup::getDescriptor() const {
  return absl::StrCat("RasterLookup ", config_.dataset_, " band ",
                      config_.band_);
}

// _____________________________________________________________________________
std::string RasterLookup::getCacheKeyImpl() const {
  // Column numbers instead of variable names, so that renaming a variable
  // keeps the key. The names of the new columns do not change the result.
  return absl::StrCat("RASTER LOOKUP ", config_.dataset_, " band ",
                      config_.band_, " geometry column ",
                      subtree_->getVariableColumn(config_.geometry_),
                      config_.center_.has_value() ? " with center" : "", "\n",
                      subtree_->getCacheKey());
}

// _____________________________________________________________________________
std::unique_ptr<Operation> RasterLookup::cloneImpl() const {
  // Copying keeps `path_`, so it is not looked up again. The subtree is
  // cloned, so that the copy does not share it.
  auto copy = std::make_unique<RasterLookup>(*this);
  copy->subtree_ = subtree_->clone();
  return copy;
}

// _____________________________________________________________________________
uint64_t RasterLookup::getSizeEstimateBeforeLimit() {
  // A point gives at most one row, a polygon often many. Without the size of
  // the polygons, assume one row per input row.
  return subtree_->getSizeEstimate();
}

// _____________________________________________________________________________
size_t RasterLookup::getCostEstimate() {
  // Linear in the number of rows that are produced.
  return subtree_->getCostEstimate() + getSizeEstimateBeforeLimit();
}

// _____________________________________________________________________________
float RasterLookup::getMultiplicity(size_t col) {
  // The new columns, like the one of BIND.
  if (col >= subtree_->getResultWidth()) {
    return 1;
  }
  return subtree_->getMultiplicity(col);
}

// _____________________________________________________________________________
bool RasterLookup::knownEmptyResult() { return subtree_->knownEmptyResult(); }

// _____________________________________________________________________________
qlm::vector<ColumnIndex> RasterLookup::resultSortedOn() const {
  // The pixels of each input row are appended right after each other and the
  // input rows keep their order, so the sorting of the child is kept.
  // `computeResult` must keep this order, also if it ever runs in parallel.
  return subtree_->resultSortedOn();
}

// _____________________________________________________________________________
Result RasterLookup::computeResult([[maybe_unused]] bool requestLaziness) {
  std::shared_ptr<const Result> subResult = subtree_->getResult();
  const IdTableView<0>& input = subResult->idTableView();
  IdTable output{getResultWidth(), allocator()};

  if (!input.empty()) {
    ColumnIndex geometryColumn = subtree_->getVariableColumn(config_.geometry_);
    // Opened once and used for all rows, so GDAL's cache of read blocks stays
    // warm. Closed at the end of this block.
    qlever::raster::RasterFile rasterFile{path_};
    // The number of bands is only known once the file is open.
    if (config_.band_ < 1 || config_.band_ > rasterFile.numBands()) {
      throw InvalidSparqlQueryException(absl::StrCat(
          "The raster \"", config_.dataset_, "\" has ", rasterFile.numBands(),
          " band(s), but band ", config_.band_, " was requested"));
    }
    // Input rows whose geometry is not a point or WKT literal, lies outside
    // the grid, or only covers NoData pixels.
    size_t numRowsWithoutPixels = 0;
    auto checkCancellationCallback = [this]() { checkCancellation(); };
    size_t valueColumn = input.numColumns();
    bool withCenter = config_.center_.has_value();
    for (size_t inputRow = 0; inputRow < input.numRows(); ++inputRow) {
      checkCancellation();
      // NOTE: `pixels` and the temporaries of the lookup are not counted by
      // the memory limit, only `output` is. For a huge polygon, they take
      // about twice the memory of its output rows. Reading large windows in
      // tiles would bound them.
      std::vector<qlever::raster::Pixel> pixels = lookupPixels(
          rasterFile, config_.band_, input(inputRow, geometryColumn),
          getIndex(), subResult->localVocab());
      if (pixels.empty()) {
        ++numRowsWithoutPixels;
        continue;
      }

      // One new row per pixel, written column by column, because an `IdTable`
      // stores each column contiguously. `resize` grows the table
      // geometrically, like `std::vector::resize`.
      size_t firstRow = output.numRows();
      output.resize(firstRow + pixels.size());
      // The columns of the child hold the same `Id` in all new rows.
      for (size_t column = 0; column < input.numColumns(); ++column) {
        ad_utility::chunkedFill(output.getColumn(column).subspan(firstRow),
                                input(inputRow, column), CHUNK_SIZE,
                                checkCancellationCallback);
      }
      ad_utility::chunkedForLoop<CHUNK_SIZE>(
          0, pixels.size(),
          [&](size_t pixelIndex) {
            const qlever::raster::Pixel& pixel = pixels[pixelIndex];
            output(firstRow + pixelIndex, valueColumn) =
                pixelValueToId(pixel.value_);
            if (withCenter) {
              output(firstRow + pixelIndex, valueColumn + 1) =
                  Id::makeFromGeoPoint(pixel.center_);
            }
          },
          checkCancellationCallback);
    }
    runtimeInfo().addDetail("num-input-rows-without-pixels",
                            numRowsWithoutPixels);
  }

  // The pixel values and centers are stored in the `Id`s themselves, so no
  // new words are added and the local vocab of the child can be shared.
  return {std::move(output), resultSortedOn(),
          subResult->getSharedLocalVocab()};
}

// _____________________________________________________________________________
bool RasterLookup::isDeterministicImpl() const {
  // The same file, band, and geometries always give the same pixels.
  return true;
}
