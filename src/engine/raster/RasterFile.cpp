// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/raster/RasterFile.h"

#include <gdal_priv.h>

#include <array>
#include <stdexcept>

#include "absl/strings/str_cat.h"

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
  std::array<double, 6> pixelToRaster_;
  std::array<double, 6> rasterToPixel_;

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
}

// _____________________________________________________________________________
RasterFile::RasterFile(const std::string& path)
    : impl_(std::make_unique<Impl>(path)) {}

// The following are defined here and not in the header, because they need the
// complete type `Impl`.
RasterFile::~RasterFile() = default;
RasterFile::RasterFile(RasterFile&&) noexcept = default;
RasterFile& RasterFile::operator=(RasterFile&&) noexcept = default;

}  // namespace qlever::raster
