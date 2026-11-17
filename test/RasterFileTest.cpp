// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gdal_priv.h>
#include <gmock/gmock.h>
#include <ogr_spatialref.h>

#include <array>
#include <fstream>
#include <optional>
#include <string_view>

#include "engine/raster/RasterFile.h"
#include "util/FileTestHelpers.h"
#include "util/GTestHelpers.h"

using qlever::raster::RasterFile;
using ::testing::AllOf;
using ::testing::HasSubstr;

namespace {

// A geotransform, in GDAL's order: {originX, pixelWidth, rotationX, originY,
// rotationY, pixelHeight}. Here: pixels of 10 x 10 units, north up, with the
// top-left corner at (400000, 5300000).
constexpr std::array<double, 6> NORTH_UP{400000, 10, 0, 5300000, 0, -10};

// CRSs, as accepted by GDAL's `SetFromUserInput`.
// UTM zone 32N, in metres. `NORTH_UP` is near Freiburg in it.
constexpr std::string_view UTM_ZONE_32N = "EPSG:25832";
// A local grid in metres, which is not tied to positions on Earth, so it
// cannot be transformed to CRS84.
constexpr std::string_view LOCAL_GRID =
    R"(LOCAL_CS["engineering",UNIT["metre",1]])";

// Write a GeoTIFF with one band of 2 x 2 pixels to `path`. Without a
// `geoTransform` or a `crs`, the file has none.
void writeTestRaster(
    const std::string& path,
    std::optional<std::array<double, 6>> geoTransform = NORTH_UP,
    std::optional<std::string_view> crs = UTM_ZONE_32N) {
  GDALAllRegister();
  GDALDriver* driver = GetGDALDriverManager()->GetDriverByName("GTiff");
  ASSERT_NE(driver, nullptr);
  GDALDatasetUniquePtr dataset{driver->Create(path.c_str(), /*nXSize=*/2,
                                              /*nYSize=*/2, /*nBands=*/1,
                                              GDT_Byte, nullptr)};
  ASSERT_NE(dataset, nullptr);
  if (geoTransform.has_value()) {
    ASSERT_EQ(GDALSetGeoTransform(GDALDataset::ToHandle(dataset.get()),
                                  geoTransform->data()),
              CE_None);
  }
  if (crs.has_value()) {
    OGRSpatialReference spatialRef;
    ASSERT_EQ(spatialRef.SetFromUserInput(std::string{*crs}.c_str()),
              OGRERR_NONE);
    ASSERT_EQ(dataset->SetSpatialRef(&spatialRef), CE_None);
  }
  // The file is complete once `dataset` is closed at the end of this scope.
}

}  // namespace

// _____________________________________________________________________________
TEST(RasterFile, opensGeoTiff) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  EXPECT_NO_THROW(RasterFile{path.string()});
}

// _____________________________________________________________________________
TEST(RasterFile, throwsForMissingFile) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  AD_EXPECT_THROW_WITH_MESSAGE(
      RasterFile{path.string()},
      AllOf(HasSubstr("Could not open raster file"), HasSubstr(path.string())));
}

// _____________________________________________________________________________
TEST(RasterFile, throwsForFileThatIsNotARaster) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  std::ofstream{path} << "this is not a raster";
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("Could not open raster file"));
}

// _____________________________________________________________________________
TEST(RasterFile, throwsForMissingGeoTransform) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), std::nullopt);
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("has no valid geotransform"));
}

// _____________________________________________________________________________
TEST(RasterFile, throwsForGeoTransformThatCannotBeInverted) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  // A pixel width and height of 0, which maps all pixels to the same point.
  writeTestRaster(path.string(),
                  std::array<double, 6>{400000, 0, 0, 5300000, 0, 0});
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("has no valid geotransform"));
}

// _____________________________________________________________________________
TEST(RasterFile, throwsForMissingCrs) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, std::nullopt);
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("has no CRS"));
}

// _____________________________________________________________________________
TEST(RasterFile, throwsForCrsWithoutTransformationToCrs84) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, LOCAL_GRID);
  AD_EXPECT_THROW_WITH_MESSAGE(
      RasterFile{path.string()},
      HasSubstr("Could not transform between CRS84 and the CRS"));
}
