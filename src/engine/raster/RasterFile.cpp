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
#include <stdexcept>

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

}  // namespace

// _____________________________________________________________________________
struct RasterFile::Impl {
  GDALDatasetUniquePtr dataset_;
  // Pixel position to coordinates in the CRS of the file, and vice versa
  std::array<double, 6> pixelToRaster_{};
  std::array<double, 6> rasterToPixel_{};
  // CRS84 to the CRS of the file, and vice versa
  std::unique_ptr<OGRCoordinateTransformation> crs84ToRaster_;
  std::unique_ptr<OGRCoordinateTransformation> rasterToCrs84_;

  explicit Impl(const std::string& path);
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
    GDALDataType type =
        dataset_->GetRasterBand(bandNumber)->GetRasterDataType();
    // Not every `UInt64` value fits into an `int64_t`.
    if (GDALDataTypeIsComplex(type) || type == GDT_UInt64 ||
        type == GDT_Unknown) {
      throw std::runtime_error(absl::StrCat(
          "Band ", bandNumber, " of raster file \"", path,
          "\" has the unsupported type ", GDALGetDataTypeName(type)));
    }
  }
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

}  // namespace qlever::raster
