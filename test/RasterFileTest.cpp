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

#include <fstream>

#include "engine/raster/RasterFile.h"
#include "util/FileTestHelpers.h"
#include "util/GTestHelpers.h"

using qlever::raster::RasterFile;
using ::testing::AllOf;
using ::testing::HasSubstr;

namespace {

// Write a GeoTIFF with one band of 2 x 2 pixels to `path`.
void writeTestRaster(const std::string& path) {
  GDALAllRegister();
  GDALDriver* driver = GetGDALDriverManager()->GetDriverByName("GTiff");
  ASSERT_NE(driver, nullptr);
  GDALDatasetUniquePtr dataset{driver->Create(path.c_str(), /*nXSize=*/2,
                                              /*nYSize=*/2, /*nBands=*/1,
                                              GDT_Byte, nullptr)};
  ASSERT_NE(dataset, nullptr);
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
