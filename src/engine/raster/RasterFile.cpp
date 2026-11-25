// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/raster/RasterFile.h"

#include <absl/strings/str_cat.h>
#include <gdal_priv.h>
#include <ogr_spatialref.h>

#include <array>
#include <cmath>
#include <optional>
#include <stdexcept>
#include <variant>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace qlever::raster {

namespace {

// GDAL's drivers must be registered once per process, before the first file
// is opened.
void registerGdalDrivers() {
  [[maybe_unused]] static const bool registered = [] {
    GDALAllRegister();
    return true;
  }();
}

// Return the NoData value of `band` (`int64_t` for integer bands, `double`
// otherwise), or nothing if it has none. Throws if no pixel of the band's type
// can hold it.
std::optional<PixelValue> readNoData(GDALRasterBand& band) {
  int hasNoData = 0;
  double noData = band.GetNoDataValue(&hasNoData);
  if (!hasNoData) {
    return std::nullopt;
  }
  // This is the value that a pixel of this type holds if `noData` is written
  // to it.
  GDALDataType type = band.GetRasterDataType();
  int clamped = 0;
  int rounded = 0;
  double storedNoData =
      GDALAdjustValueToDataType(type, noData, &clamped, &rounded);
  if (clamped || rounded) {
    throw std::runtime_error(absl::StrCat(
        "Band ", band.GetBand(), " of raster file \"",
        band.GetDataset()->GetDescription(), "\" has the NoData value ", noData,
        ", which its type ", GDALGetDataTypeName(type), " cannot hold"));
  }
  if (GDALDataTypeIsInteger(type)) {
    return PixelValue{static_cast<int64_t>(storedNoData)};
  }
  return PixelValue{storedNoData};
}

}  // namespace

// _____________________________________________________________________________
PixelTable::PixelTable(size_t numBands, const qlm::Allocator<char>& allocator)
    : centers_{allocator}, valuesPerBand_(numBands, BandValues{allocator}) {}

// _____________________________________________________________________________
PixelTable::PixelTable(qlm::vector<GeoPoint> centers,
                       std::vector<BandValues> valuesPerBand)
    : centers_{std::move(centers)}, valuesPerBand_{std::move(valuesPerBand)} {
  for (const BandValues& values : valuesPerBand_) {
    AD_CONTRACT_CHECK(values.size() == centers_.size());
  }
}

// _____________________________________________________________________________
struct RasterFile::Impl {
  // This holds the information about one band that the lookups need.
  struct Band {
    // The band is owned by `dataset_` and stays valid as long as it is open.
    GDALRasterBand* handle_;
    // Integer bands are read as `int64_t`, all others as `double`.
    bool isInteger_;
    // The NoData value has the same type as the values read from this band.
    std::optional<PixelValue> noData_;

    // Return the value of the pixel at `column`, `row`.
    PixelValue readPixel(int column, int row) const;

    // Return whether `value`, which was read from this band, is its NoData
    // value.
    bool isNoData(const PixelValue& value) const;
  };

  GDALDatasetUniquePtr dataset_;
  // These convert pixel positions to coordinates in the CRS of the file, and
  // back.
  std::array<double, 6> pixelToRaster_{};
  std::array<double, 6> rasterToPixel_{};
  // These transform coordinates from CRS84 to the CRS of the file, and back.
  std::unique_ptr<OGRCoordinateTransformation> crs84ToRaster_;
  std::unique_ptr<OGRCoordinateTransformation> rasterToCrs84_;
  // There is one entry per band. Band `n` is at index `n - 1`.
  std::vector<Band> bands_;

  explicit Impl(const std::string& path);

  // Check that `bands` is not empty and that each band exists.
  void checkBands(ql::span<const int> bands) const;

  // Return the center of the pixel at `column`, `row`. It is not `const`,
  // because GDAL 3.8 needs a non-const geotransform.
  GeoPoint pixelCenter(int column, int row);
};

// _____________________________________________________________________________
RasterFile::Impl::Impl(const std::string& path) {
  registerGdalDrivers();
  dataset_.reset(
      GDALDataset::Open(path.c_str(), GDAL_OF_RASTER | GDAL_OF_READONLY));
  if (!dataset_) {
    throw std::runtime_error(absl::StrCat("Could not open raster file \"", path,
                                          "\": ", CPLGetLastErrorMsg()));
  }

  // The C function and not the member function of `GDALDataset`, because that
  // is deprecated in GDAL 3.12, and its replacement does not exist in GDAL 3.8.
  if (GDALGetGeoTransform(GDALDataset::ToHandle(dataset_.get()),
                          pixelToRaster_.data()) != CE_None ||
      !GDALInvGeoTransform(pixelToRaster_.data(), rasterToPixel_.data())) {
    throw std::runtime_error(
        absl::StrCat("Raster file \"", path, "\" has no valid geotransform"));
  }

  const OGRSpatialReference* fileCrs = dataset_->GetSpatialRef();
  if (fileCrs == nullptr) {
    throw std::runtime_error(
        absl::StrCat("Raster file \"", path, "\" has no CRS"));
  }
  OGRSpatialReference rasterCrs{*fileCrs};
  OGRSpatialReference crs84;
  AD_CORRECTNESS_CHECK(crs84.importFromEPSG(4326) == OGRERR_NONE);
  // Longitude (or easting) first, as in WKT. Otherwise, GDAL follows the axis
  // order of the CRS definition, which is latitude first for EPSG:4326.
  rasterCrs.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
  crs84.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
  // The transformations keep their own copies of both CRSs.
  crs84ToRaster_.reset(OGRCreateCoordinateTransformation(&crs84, &rasterCrs));
  rasterToCrs84_.reset(OGRCreateCoordinateTransformation(&rasterCrs, &crs84));
  if (!crs84ToRaster_ || !rasterToCrs84_) {
    throw std::runtime_error(absl::StrCat(
        "Could not transform between CRS84 and the CRS of raster file \"", path,
        "\": ", CPLGetLastErrorMsg()));
  }

  for (int bandNumber = 1; bandNumber <= dataset_->GetRasterCount();
       ++bandNumber) {
    GDALRasterBand* band = dataset_->GetRasterBand(bandNumber);
    GDALDataType type = band->GetRasterDataType();
    // A `double` must hold every value of the type exactly, because GDAL
    // returns the NoData value as a `double`, see `readNoData`.
    if (GDALDataTypeIsComplex(type) || type == GDT_Int64 ||
        type == GDT_UInt64) {
      throw std::runtime_error(absl::StrCat(
          "Band ", bandNumber, " of raster file \"", path,
          "\" has the unsupported type ", GDALGetDataTypeName(type)));
    }
    // Only a NoData value may mark invalid pixels, see `readNoData`.
    int maskFlags = band->GetMaskFlags();
    if (maskFlags != GMF_ALL_VALID && maskFlags != GMF_NODATA) {
      throw std::runtime_error(absl::StrCat(
          "Band ", bandNumber, " of raster file \"", path,
          "\" marks invalid pixels with a mask, not with a NoData value"));
    }
    bands_.push_back(
        Band{band, GDALDataTypeIsInteger(type) != 0, readNoData(*band)});
  }
}

// _____________________________________________________________________________
PixelValue RasterFile::Impl::Band::readPixel(int column, int row) const {
  // GDAL converts the pixel to `type` while reading it into `value`.
  auto read = [this, column, row](void* value, GDALDataType type) {
    if (handle_->RasterIO(GF_Read, column, row, /*nXSize=*/1, /*nYSize=*/1,
                          value, /*nBufXSize=*/1, /*nBufYSize=*/1, type,
                          /*nPixelSpace=*/0, /*nLineSpace=*/0) != CE_None) {
      throw std::runtime_error(absl::StrCat(
          "Could not read pixel (", column, ", ", row, ") of raster file \"",
          handle_->GetDataset()->GetDescription(),
          "\": ", CPLGetLastErrorMsg()));
    }
  };
  if (isInteger_) {
    int64_t value = 0;
    read(&value, GDT_Int64);
    return PixelValue{value};
  }
  double value = 0.0;
  read(&value, GDT_Float64);
  return PixelValue{value};
}

// _____________________________________________________________________________
bool RasterFile::Impl::Band::isNoData(const PixelValue& value) const {
  if (!noData_.has_value()) {
    return false;
  }
  // NaN is not equal to anything, not even to itself.
  const auto* noDataAsDouble = std::get_if<double>(&noData_.value());
  if (noDataAsDouble != nullptr && std::isnan(*noDataAsDouble)) {
    return std::isnan(std::get<double>(value));
  }
  return value == noData_.value();
}

// _____________________________________________________________________________
void RasterFile::Impl::checkBands(ql::span<const int> bands) const {
  AD_CONTRACT_CHECK(!bands.empty());
  for (int band : bands) {
    AD_CONTRACT_CHECK(1 <= band && band <= static_cast<int>(bands_.size()));
  }
}

// _____________________________________________________________________________
GeoPoint RasterFile::Impl::pixelCenter(int column, int row) {
  double x = 0.0;
  double y = 0.0;
  GDALApplyGeoTransform(pixelToRaster_.data(), column + 0.5, row + 0.5, &x, &y);
  AD_CORRECTNESS_CHECK(rasterToCrs84_->Transform(/*nCount=*/1, &x, &y));
  // `GeoPoint` takes the latitude first.
  return GeoPoint{y, x};
}

// _____________________________________________________________________________
RasterFile::RasterFile(const std::string& path)
    : impl_(std::make_unique<Impl>(path)) {}

// The following are defined here and not in the header, because they need the
// complete type `Impl`.
RasterFile::~RasterFile() = default;
RasterFile::RasterFile(RasterFile&&) noexcept = default;
RasterFile& RasterFile::operator=(RasterFile&&) noexcept = default;

// _____________________________________________________________________________
int RasterFile::numBands() const { return impl_->dataset_->GetRasterCount(); }

// _____________________________________________________________________________
PixelTable RasterFile::lookupPoint(const GeoPoint& point,
                                   ql::span<const int> bands,
                                   const qlm::Allocator<char>& allocator) {
  Impl& impl = *impl_;
  impl.checkBands(bands);
  auto noPixels = [&]() { return PixelTable(bands.size(), allocator); };

  // Transform the point to the CRS of the file.
  double x = point.getLng();
  double y = point.getLat();
  if (!impl.crs84ToRaster_->Transform(/*nCount=*/1, &x, &y)) {
    return noPixels();
  }

  // Find the pixel that contains the point. Use `floor` and not a cast to
  // `int`, because a cast turns -0.5 into 0 and accepts a point just outside
  // the grid.
  double column = 0.0;
  double row = 0.0;
  GDALApplyGeoTransform(impl.rasterToPixel_.data(), x, y, &column, &row);
  column = std::floor(column);
  row = std::floor(row);
  // The condition checks for "inside", so that NaN counts as outside.
  if (!(0 <= column && column < impl.dataset_->GetRasterXSize() && 0 <= row &&
        row < impl.dataset_->GetRasterYSize())) {
    return noPixels();
  }
  int pixelColumn = static_cast<int>(column);
  int pixelRow = static_cast<int>(row);

  // Read the pixel in each band. The value is empty where the band is NoData.
  std::vector<std::optional<PixelValue>> pixelValues;
  pixelValues.reserve(bands.size());
  for (int band : bands) {
    const Impl::Band& bandInfo = impl.bands_[band - 1];
    PixelValue value = bandInfo.readPixel(pixelColumn, pixelRow);
    pixelValues.push_back(bandInfo.isNoData(value)
                              ? std::nullopt
                              : std::optional<PixelValue>{value});
  }
  if (!ql::ranges::any_of(
          pixelValues, [](const auto& value) { return value.has_value(); })) {
    return noPixels();
  }

  // Build a table with one row.
  qlm::vector<GeoPoint> centers({impl.pixelCenter(pixelColumn, pixelRow)},
                                allocator);
  std::vector<BandValues> valuesPerBand;
  valuesPerBand.reserve(bands.size());
  for (const std::optional<PixelValue>& value : pixelValues) {
    valuesPerBand.push_back(BandValues({value}, allocator));
  }
  return PixelTable{std::move(centers), std::move(valuesPerBand)};
}

// _____________________________________________________________________________
std::string rasterFilePath(const ql::filesystem::path& folder,
                           std::string_view name) {
  for (const auto& entry : ql::filesystem::directory_iterator(folder)) {
    const ql::filesystem::path& file = entry.path();
    if (ql::filesystem::is_regular_file(file) && file.extension() == ".tif" &&
        file.stem().string() == name) {
      return file.string();
    }
  }
  throw std::runtime_error(absl::StrCat("There is no raster file \"", name,
                                        ".tif\" in the folder \"",
                                        folder.string(), "\""));
}

}  // namespace qlever::raster
