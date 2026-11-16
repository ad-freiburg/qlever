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
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>

#include "engine/QueryExecutionTree.h"
#include "engine/raster/RasterFile.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "parser/LiteralOrIri.h"
#include "util/ChunkedForLoop.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/OverloadCallOperator.h"
#include "util/ParseException.h"

namespace {

// The pixels with their values in `bands` under the geometry in `geometry`: a
// point stored in the `Id`, or a WKT literal from the vocabulary or
// `localVocab`. Like `GeoPointOrWktValueGetter`. Other `Id`s (UNDEF, numbers,
// IRIs, ...) give no pixels. With a `radius` in metres, the geometry is first
// widened by it.
qlever::raster::PixelTable lookupPixels(qlever::raster::RasterFile& rasterFile,
                                        ql::span<const int> bands,
                                        std::optional<double> radius,
                                        Id geometry, const Index& index,
                                        const LocalVocab& localVocab,
                                        const qlm::Allocator<char>& allocator) {
  auto noPixels = [&]() {
    return qlever::raster::PixelTable(bands.size(), allocator);
  };
  using enum Datatype;
  switch (geometry.getDatatype()) {
    case GeoPoint: {
      if (radius.has_value()) {
        return rasterFile.lookupCircle(geometry.getGeoPoint(), bands,
                                       radius.value(), allocator);
      }
      return rasterFile.lookupPoint(geometry.getGeoPoint(), bands, allocator);
    }
    case VocabIndex:
    case LocalVocabIndex:
    case SecondaryVocabIndex: {
      auto literalOrIri = ql::exportIds::getLiteralOrIriFromVocabIndex(
          index.getImpl(), geometry, localVocab);
      if (literalOrIri.isLiteral() && literalOrIri.hasDatatype() &&
          literalOrIri.getDatatype() == GEO_WKT_LITERAL) {
        return rasterFile.lookupGeometry(literalOrIri.toStringRepresentation(),
                                         bands, radius, allocator);
      }
      return noPixels();
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
      return noPixels();
  }
  AD_FAIL();
}

// The number of cells that are written between two cancellation checks.
constexpr size_t CHUNK_SIZE = 10'000;

// The `Id` of a pixel value: Int for integer bands, Double for all others, and
// UNDEF where the band is NoData.
Id pixelValueToId(const std::optional<qlever::raster::PixelValue>& value) {
  if (!value.has_value()) {
    return Id::makeUndefined();
  }
  return std::visit(
      ad_utility::OverloadCallOperator{
          [](int64_t intValue) { return Id::makeFromInt(intValue); },
          [](double doubleValue) { return Id::makeFromDouble(doubleValue); }},
      value.value());
}

// The numbers of `bands`, separated by spaces, for example "1 3".
std::string bandNumbers(
    const std::vector<RasterLookupConfiguration::Band>& bands) {
  return absl::StrJoin(bands, " ", [](std::string* out, const auto& band) {
    absl::StrAppend(out, band.number_);
  });
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
  // The variables of the new columns must be unbound and all different.
  ad_utility::HashSet<Variable> newVariables;
  auto checkNewVariable = [this, &newVariables](const Variable& variable,
                                                std::string_view role) {
    if (subtree_->getVariableColumnOrNullopt(variable).has_value()) {
      throw InvalidSparqlQueryException(absl::StrCat(
          "The ", role, " variable ", variable.name(),
          " of the raster lookup is already bound by the pattern inside the "
          "SERVICE"));
    }
    if (!newVariables.insert(variable).second) {
      throw InvalidSparqlQueryException(
          absl::StrCat("The variable ", variable.name(),
                       " is used more than once in the raster lookup"));
    }
  };
  for (const RasterLookupConfiguration::Band& band : config_.bands_) {
    checkNewVariable(band.variable_, "band");
  }
  if (config_.center_.has_value()) {
    checkNewVariable(config_.center_.value(), "center");
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
  // One column per band, and the center column if it is wanted.
  return subtree_->getResultWidth() + config_.bands_.size() +
         (config_.center_.has_value() ? 1 : 0);
}

// _____________________________________________________________________________
VariableToColumnMap RasterLookup::computeVariableToColumnMap() const {
  // The columns of the child stay where they are, the new ones are appended.
  VariableToColumnMap variableColumns = subtree_->getVariableColumns();
  auto addColumn = [&variableColumns](const Variable& variable,
                                      ColumnIndexAndTypeInfo columnInfo) {
    bool isNew = variableColumns.insert({variable, columnInfo}).second;
    AD_CORRECTNESS_CHECK(isNew);
  };
  ColumnIndex firstBandColumn = subtree_->getResultWidth();
  // A pixel is returned if at least one band has a value there, so with
  // several bands, a band can be UNDEF.
  bool bandsAlwaysDefined = config_.bands_.size() == 1;
  for (size_t bandIndex = 0; bandIndex < config_.bands_.size(); ++bandIndex) {
    ColumnIndex column = firstBandColumn + bandIndex;
    addColumn(config_.bands_[bandIndex].variable_,
              bandsAlwaysDefined ? makeAlwaysDefinedColumn(column)
                                 : makePossiblyUndefinedColumn(column));
  }
  if (config_.center_.has_value()) {
    addColumn(config_.center_.value(),
              makeAlwaysDefinedColumn(firstBandColumn + config_.bands_.size()));
  }
  return variableColumns;
}

// _____________________________________________________________________________
qlm::vector<QueryExecutionTree*> RasterLookup::getChildrenImpl() const {
  return {{subtree_.get()}, allocator()};
}

// _____________________________________________________________________________
std::string RasterLookup::getDescriptor() const {
  return absl::StrCat("RasterLookup ", config_.dataset_, " bands ",
                      bandNumbers(config_.bands_),
                      config_.radius_.has_value()
                          ? absl::StrCat(" radius ", config_.radius_.value())
                          : "");
}

// _____________________________________________________________________________
std::string RasterLookup::getCacheKeyImpl() const {
  // Column numbers instead of variable names, so that renaming a variable
  // keeps the key. The names of the new columns do not change the result, but
  // the order of the bands does.
  return absl::StrCat(
      "RASTER LOOKUP ", config_.dataset_, " bands ",
      bandNumbers(config_.bands_),
      // Full precision, because `StrCat` keeps only 6 digits
      // of a `double`.
      config_.radius_.has_value()
          ? absl::StrFormat(" radius %.17g", config_.radius_.value())
          : "",
      " geometry column ", subtree_->getVariableColumn(config_.geometry_),
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
    // The number of bands is only known once the file is open. The parser
    // already checked that each number is at least 1.
    std::vector<int> bands;
    for (const RasterLookupConfiguration::Band& band : config_.bands_) {
      if (band.number_ > rasterFile.numBands()) {
        throw InvalidSparqlQueryException(absl::StrCat(
            "The raster \"", config_.dataset_, "\" has ", rasterFile.numBands(),
            " band(s), but band ", band.number_, " was requested"));
      }
      bands.push_back(band.number_);
    }
    // Input rows whose geometry is not a point or WKT literal, lies outside
    // the grid, or only covers NoData pixels.
    size_t numRowsWithoutPixels = 0;
    auto checkCancellationCallback = [this]() { checkCancellation(); };
    size_t firstBandColumn = input.numColumns();
    for (size_t inputRow = 0; inputRow < input.numRows(); ++inputRow) {
      checkCancellation();
      // NOTE: `pixels` and the buffers of the lookup count towards the memory
      // limit. GDAL's own memory (its block cache, and the mask image while it
      // is drawn) does not.
      qlever::raster::PixelTable pixels = lookupPixels(
          rasterFile, bands, config_.radius_, input(inputRow, geometryColumn),
          getIndex(), subResult->localVocab(), allocator());
      if (pixels.numPixels() == 0) {
        ++numRowsWithoutPixels;
        continue;
      }

      // One new row per pixel, written column by column, because an `IdTable`
      // stores each column contiguously. `resize` grows the table
      // geometrically, like `std::vector::resize`.
      size_t firstRow = output.numRows();
      output.resize(firstRow + pixels.numPixels());
      // The columns of the child hold the same `Id` in all new rows.
      for (size_t column = 0; column < input.numColumns(); ++column) {
        ad_utility::chunkedFill(output.getColumn(column).subspan(firstRow),
                                input(inputRow, column), CHUNK_SIZE,
                                checkCancellationCallback);
      }
      for (size_t bandIndex = 0; bandIndex < bands.size(); ++bandIndex) {
        const qlever::raster::BandValues& values =
            pixels.valuesPerBand_[bandIndex];
        auto column =
            output.getColumn(firstBandColumn + bandIndex).subspan(firstRow);
        ad_utility::chunkedForLoop<CHUNK_SIZE>(
            0, values.size(),
            [&](size_t pixelIndex) {
              column[pixelIndex] = pixelValueToId(values[pixelIndex]);
            },
            checkCancellationCallback);
      }
      if (config_.center_.has_value()) {
        auto column =
            output.getColumn(firstBandColumn + bands.size()).subspan(firstRow);
        ad_utility::chunkedForLoop<CHUNK_SIZE>(
            0, pixels.numPixels(),
            [&](size_t pixelIndex) {
              column[pixelIndex] =
                  Id::makeFromGeoPoint(pixels.centers_[pixelIndex]);
            },
            checkCancellationCallback);
      }
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
  // The same file, bands, and geometries always give the same pixels.
  return true;
}
