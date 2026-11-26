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
#include "parser/RasterQuery.h"
#include "rdfTypes/LiteralOrIri.h"
#include "util/ChunkedForLoop.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/OverloadCallOperator.h"
#include "util/ParseException.h"

namespace {

// The pixels with their values in `bands` under the geometry in `geometry`: a
// point stored in the `Id`, or a WKT literal from the vocabulary or
// `localVocab`. Like `GeoPointOrWktValueGetter`. Other `Id`s (UNDEF, numbers,
// IRIs, ...) give no pixels. With a `radius` in metres, the geometry is first
// widened by it. The time to fetch a WKT literal is added to
// `timeFetchingGeometries`.
qlever::raster::PixelTable lookupPixels(
    qlever::raster::RasterFile& rasterFile, ql::span<const int> bands,
    std::optional<double> radius, Id geometry, const Index& index,
    const LocalVocab& localVocab, const qlm::Allocator<char>& allocator,
    ad_utility::Timer::Duration& timeFetchingGeometries) {
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
      ad_utility::Timer timer{ad_utility::Timer::Started};
      auto literalOrIri = ql::exportIds::getLiteralOrIriFromVocabIndex(
          index.getImpl(), geometry, localVocab);
      if (literalOrIri.isLiteral() && literalOrIri.hasDatatype() &&
          literalOrIri.getDatatype() == GEO_WKT_LITERAL) {
        std::string wktLiteral = literalOrIri.toStringRepresentation();
        timeFetchingGeometries += timer.value();
        return rasterFile.lookupGeometry(wktLiteral, bands, radius, allocator);
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
    case TripleTermIndex:
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

// The name of the raster in a row of a variable dataset: the content of a
// literal, or the name in a dataset IRI. Nothing for other `Id`s (UNDEF,
// numbers, IRIs outside the namespace, ...).
std::optional<std::string> datasetName(Id dataset, const Index& index,
                                       const LocalVocab& localVocab) {
  std::optional<ql::exportIds::LiteralOrIri> value =
      ql::exportIds::idToLiteralOrIri(index.getImpl(), dataset, localVocab,
                                      /*skipEncodedValues=*/true);
  if (!value.has_value()) {
    return std::nullopt;
  }
  if (value->isLiteral()) {
    return std::string{value->getContent()};
  }
  try {
    return parsedQuery::extractRasterDatasetName(value->getIri());
  } catch (const parsedQuery::RasterException&) {
    return std::nullopt;
  }
}

// The file of the dataset `name`. An unknown dataset is a mistake in the query
// or the data, and `rasterFilePath` does not know about SPARQL, so its error is
// converted.
std::string findRasterFile(std::string_view name) {
  try {
    return qlever::raster::rasterFilePath(name);
  } catch (const std::runtime_error& error) {
    throw InvalidSparqlQueryException(error.what());
  }
}

// Open the raster file at `path` and check that it has all `bands`, which is
// only known once the file is open. `name` is the dataset for the error.
qlever::raster::RasterFile openRasterFile(const std::string& path,
                                          std::string_view name,
                                          ql::span<const int> bands) {
  qlever::raster::RasterFile rasterFile{path};
  for (int band : bands) {
    if (band > rasterFile.numBands()) {
      throw InvalidSparqlQueryException(
          absl::StrCat("The raster \"", name, "\" has ", rasterFile.numBands(),
                       " band(s), but band ", band, " was requested"));
    }
  }
  return rasterFile;
}

// The numbers of `bands`, separated by spaces, for example "1 3".
std::string bandNumbers(
    const std::vector<RasterLookupConfiguration::Band>& bands) {
  return absl::StrJoin(bands, " ", [](std::string* out, const auto& band) {
    absl::StrAppend(out, band.number_);
  });
}

// Return `duration` in whole milliseconds, which `addDetail` accepts.
std::chrono::milliseconds milliseconds(ad_utility::Timer::Duration duration) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(duration);
}

// Add `statistics` to the details of `runtimeInfo`, with times in
// milliseconds. The two counts of lookups are always added. The details of the
// point or shape lookups are only added if there was at least one such lookup,
// and the time to grow the geometries only if the lookup has a radius.
void addStatisticsToDetails(RuntimeInformation& runtimeInfo,
                            const qlever::raster::LookupStatistics& statistics,
                            bool hasRadius) {
  runtimeInfo.addDetail("num-point-lookups", statistics.numPointLookups_);
  runtimeInfo.addDetail("num-shape-lookups", statistics.numShapeLookups_);
  if (statistics.numPointLookups_ > 0) {
    runtimeInfo.addDetail("time-point-lookups",
                          milliseconds(statistics.timePointLookups_));
  }
  if (statistics.numShapeLookups_ > 0) {
    runtimeInfo.addDetail("time-parsing-geometries",
                          milliseconds(statistics.timeParsingGeometries_));
    runtimeInfo.addDetail("time-projecting-geometries",
                          milliseconds(statistics.timeProjectingGeometries_));
    if (hasRadius) {
      runtimeInfo.addDetail("time-growing-geometries",
                            milliseconds(statistics.timeGrowingGeometries_));
    }
    runtimeInfo.addDetail("num-window-pixels", statistics.numWindowPixels_);
    runtimeInfo.addDetail("time-drawing-masks",
                          milliseconds(statistics.timeDrawingMasks_));
    runtimeInfo.addDetail("time-reading-pixels",
                          milliseconds(statistics.timeReadingPixels_));
    runtimeInfo.addDetail("num-pixels-inside-geometry",
                          statistics.numPixelsInsideGeometry_);
    runtimeInfo.addDetail("time-collecting-pixels",
                          milliseconds(statistics.timeCollectingPixels_));
    runtimeInfo.addDetail("time-converting-centers",
                          milliseconds(statistics.timeConvertingCenters_));
  }
}

}  // namespace

// _____________________________________________________________________________
RasterLookup::RasterLookup(QueryExecutionContext* qec,
                           std::shared_ptr<QueryExecutionTree> subtree,
                           RasterLookupConfiguration config)
    : Operation(qec), subtree_(std::move(subtree)), config_(std::move(config)) {
  // Mistakes in the query, reported as an invalid query (HTTP 400) instead of
  // as a failed internal check later on.
  // The variables that are read must be bound by the child.
  auto checkBoundVariable = [this](const Variable& variable,
                                   std::string_view role) {
    if (!subtree_->getVariableColumnOrNullopt(variable).has_value()) {
      throw InvalidSparqlQueryException(absl::StrCat(
          "The ", role, " variable ", variable.name(),
          " of the raster lookup is not bound by the pattern inside the "
          "SERVICE"));
    }
  };
  checkBoundVariable(config_.geometry_, "geometry");
  if (const auto* variable = std::get_if<Variable>(&config_.dataset_)) {
    checkBoundVariable(*variable, "dataset");
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

  // The files of a variable dataset are only found per row, see
  // `computeResult`.
  if (const auto* name = std::get_if<std::string>(&config_.dataset_)) {
    path_ = findRasterFile(*name);
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
  std::string dataset =
      std::visit(ad_utility::OverloadCallOperator{
                     [](const std::string& name) { return name; },
                     [](const Variable& variable) { return variable.name(); }},
                 config_.dataset_);
  return absl::StrCat("RasterLookup ", dataset, " bands ",
                      bandNumbers(config_.bands_),
                      config_.radius_.has_value()
                          ? absl::StrCat(" radius ", config_.radius_.value())
                          : "");
}

// _____________________________________________________________________________
qlm::string RasterLookup::getCacheKeyImpl() const {
  // Column numbers instead of variable names, so that renaming a variable
  // keeps the key. The names of the new columns do not change the result, but
  // the order of the bands does. A name is quoted, so that it never looks like
  // a column.
  std::string dataset = std::visit(
      ad_utility::OverloadCallOperator{
          [](const std::string& name) {
            return absl::StrCat("dataset \"", name, "\"");
          },
          [this](const Variable& variable) {
            return absl::StrCat("dataset column ",
                                subtree_->getVariableColumn(variable));
          }},
      config_.dataset_);
  return qlm::StrCat(
      allocator(), "RASTER LOOKUP ", dataset, " bands ",
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
    // The parser already checked that each number is at least 1.
    std::vector<int> bands;
    for (const RasterLookupConfiguration::Band& band : config_.bands_) {
      bands.push_back(band.number_);
    }
    // A fixed dataset is opened once and used for all rows, so GDAL's cache of
    // read blocks stays warm. Closed at the end of this block.
    std::optional<qlever::raster::RasterFile> fixedRasterFile;
    if (const auto* name = std::get_if<std::string>(&config_.dataset_)) {
      fixedRasterFile = openRasterFile(path_.value(), *name, bands);
    }
    // A variable dataset opens each file when a row first names it, keyed by
    // the `Id`, so that the name is only read once per value.
    std::optional<ColumnIndex> datasetColumn;
    if (const auto* variable = std::get_if<Variable>(&config_.dataset_)) {
      datasetColumn = subtree_->getVariableColumn(*variable);
    }
    // Empty for a value without a usable raster, so that it is only checked
    // once.
    ad_utility::HashMap<Id, std::optional<qlever::raster::RasterFile>>
        rasterFilesPerDataset;
    // The file of a row, or `nullptr` if its dataset has no usable raster.
    // Only valid until the next call, as the map moves its entries when it
    // grows.
    auto rasterFileForRow =
        [&](size_t inputRow) -> qlever::raster::RasterFile* {
      if (fixedRasterFile.has_value()) {
        return &fixedRasterFile.value();
      }
      Id dataset = input(inputRow, datasetColumn.value());
      auto it = rasterFilesPerDataset.find(dataset);
      if (it == rasterFilesPerDataset.end()) {
        std::optional<qlever::raster::RasterFile> rasterFile;
        std::optional<std::string> name =
            datasetName(dataset, getIndex(), subResult->localVocab());
        // An unknown name or a missing band is a mistake in the data, so the
        // rows with this value are skipped. A file that cannot be read is
        // still an error, because `RasterFile` throws a different exception.
        if (name.has_value()) {
          try {
            rasterFile = openRasterFile(findRasterFile(name.value()),
                                        name.value(), bands);
          } catch (const InvalidSparqlQueryException&) {
          }
        }
        it = rasterFilesPerDataset.try_emplace(dataset, std::move(rasterFile))
                 .first;
      }
      return it->second.has_value() ? &it->second.value() : nullptr;
    };
    // Input rows whose dataset names no usable raster. Only possible for a
    // variable dataset.
    size_t numRowsWithoutRaster = 0;
    // Input rows whose geometry is not a point or WKT literal, lies outside the
    // grid, or only covers NoData pixels.
    size_t numRowsWithoutPixels = 0;
    // The time to fetch the WKT literals of all input rows.
    ad_utility::Timer::Duration timeFetchingGeometries{};
    // The time to write the output rows of all input rows.
    ad_utility::Timer::Duration timeWritingRows{};
    auto checkCancellationCallback = [this]() { checkCancellation(); };
    size_t firstBandColumn = input.numColumns();
    for (size_t inputRow = 0; inputRow < input.numRows(); ++inputRow) {
      checkCancellation();
      qlever::raster::RasterFile* rasterFile = rasterFileForRow(inputRow);
      if (rasterFile == nullptr) {
        ++numRowsWithoutRaster;
        continue;
      }
      // NOTE: `pixels` and the buffers of the lookup count towards the memory
      // limit. GDAL's own memory (its block cache, and the mask image while it
      // is drawn) does not.
      qlever::raster::PixelTable pixels = lookupPixels(
          *rasterFile, bands, config_.radius_, input(inputRow, geometryColumn),
          getIndex(), subResult->localVocab(), allocator(),
          timeFetchingGeometries);
      if (pixels.numPixels() == 0) {
        ++numRowsWithoutPixels;
        continue;
      }

      ad_utility::Timer timer{ad_utility::Timer::Started};
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
      timeWritingRows += timer.value();
    }
    runtimeInfo().addDetail("num-input-rows-without-pixels",
                            numRowsWithoutPixels);
    // A fixed dataset always has a raster, so the counter would always be 0.
    if (datasetColumn.has_value()) {
      runtimeInfo().addDetail("num-input-rows-without-raster",
                              numRowsWithoutRaster);
    }
    runtimeInfo().addDetail("time-fetching-geometries",
                            milliseconds(timeFetchingGeometries));
    runtimeInfo().addDetail("time-writing-rows", milliseconds(timeWritingRows));
    // The statistics of all files that this lookup opened.
    qlever::raster::LookupStatistics statistics;
    if (fixedRasterFile.has_value()) {
      statistics += fixedRasterFile->statistics();
    }
    for (const auto& [dataset, rasterFile] : rasterFilesPerDataset) {
      if (rasterFile.has_value()) {
        statistics += rasterFile->statistics();
      }
    }
    addStatisticsToDetails(runtimeInfo(), statistics,
                           config_.radius_.has_value());
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
