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
#include <limits>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "engine/raster/RasterFile.h"
#include "util/Allocator.h"
#include "util/FileTestHelpers.h"
#include "util/GTestHelpers.h"

using qlever::raster::BandValues;
using qlever::raster::PixelTable;
using qlever::raster::RasterFile;
using qlever::raster::rasterFilePath;
using ::testing::AllOf;
using ::testing::DoubleNear;
using ::testing::Each;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Optional;
using ::testing::SizeIs;
using ::testing::VariantWith;

namespace {

// This is a geotransform, in GDAL's order: {originX, pixelWidth, rotationX,
// originY, rotationY, pixelHeight}. Its pixels are 10 x 10 units, north up,
// and the top-left corner is at (400000, 5300000).
constexpr std::array<double, 6> NORTH_UP{400000, 10, 0, 5300000, 0, -10};

// The following CRSs are written as GDAL's `SetFromUserInput` accepts them.
// UTM zone 32N is in metres, and `NORTH_UP` is near Freiburg in it.
constexpr std::string_view UTM_ZONE_32N = "EPSG:25832";
// A local grid in metres is not tied to positions on Earth, so it cannot be
// transformed to CRS84.
constexpr std::string_view LOCAL_GRID =
    R"(LOCAL_CS["engineering",UNIT["metre",1]])";

// Write a GeoTIFF of 2 x 2 pixels with `numBands` bands of `type` to `path`.
// Without a `geoTransform` or a `crs`, the file has none.
void writeTestRaster(
    const std::string& path,
    std::optional<std::array<double, 6>> geoTransform = NORTH_UP,
    std::optional<std::string_view> crs = UTM_ZONE_32N, int numBands = 1,
    GDALDataType type = GDT_Byte) {
  GDALAllRegister();
  GDALDriver* driver = GetGDALDriverManager()->GetDriverByName("GTiff");
  ASSERT_NE(driver, nullptr);
  GDALDatasetUniquePtr dataset{driver->Create(path.c_str(), /*nXSize=*/2,
                                              /*nYSize=*/2, numBands, type,
                                              nullptr)};
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

// Open the raster file at `path` for changes. They are written when the result
// is closed.
GDALDatasetUniquePtr openRasterForUpdate(const std::string& path) {
  return GDALDatasetUniquePtr{
      GDALDataset::Open(path.c_str(), GDAL_OF_RASTER | GDAL_OF_UPDATE)};
}

// Write the values of the 2 x 2 pixels of band `bandNumber`, row by row. GDAL
// converts them to the type of the band.
void writePixelValues(const std::string& path, int bandNumber,
                      std::array<double, 4> values) {
  auto dataset = openRasterForUpdate(path);
  ASSERT_EQ(dataset->GetRasterBand(bandNumber)
                ->RasterIO(GF_Write, /*nXOff=*/0, /*nYOff=*/0, /*nXSize=*/2,
                           /*nYSize=*/2, values.data(), /*nBufXSize=*/2,
                           /*nBufYSize=*/2, GDT_Float64, /*nPixelSpace=*/0,
                           /*nLineSpace=*/0),
            CE_None);
}

// This point is the center of pixel (1, 0) of a raster with `NORTH_UP` in
// `UTM_ZONE_32N`, computed with GDAL. `GeoPoint` takes the latitude first.
const GeoPoint CENTER_OF_PIXEL_1_0{47.845518612, 7.663610613};

// Match a `GeoPoint` whose latitude and longitude differ from those of
// `expected` by at most 1e-6 degrees (about 10 cm). That is much less than the
// distance between two pixel centers.
auto pixelCenterNear(const GeoPoint& expected) {
  constexpr double maxError = 1e-6;
  return AllOf(
      AD_PROPERTY(GeoPoint, getLat, DoubleNear(expected.getLat(), maxError)),
      AD_PROPERTY(GeoPoint, getLng, DoubleNear(expected.getLng(), maxError)));
}

// Match a `PixelTable` without pixels, which still has one empty column for
// each of its `numBands` bands.
auto isEmptyPixelTable(size_t numBands) {
  return AllOf(AD_PROPERTY(PixelTable, numPixels, 0),
               AD_FIELD(PixelTable, valuesPerBand_,
                        AllOf(SizeIs(numBands), Each(IsEmpty()))));
}

}  // namespace

// _____________________________________________________________________________
// Test that a valid GeoTIFF can be opened.
TEST(RasterFile, opensGeoTiff) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  EXPECT_NO_THROW(RasterFile{path.string()});
}

// _____________________________________________________________________________
// Test that opening a file that does not exist throws, and that the message
// names the file.
TEST(RasterFile, throwsForMissingFile) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  AD_EXPECT_THROW_WITH_MESSAGE(
      RasterFile{path.string()},
      AllOf(HasSubstr("Could not open raster file"), HasSubstr(path.string())));
}

// _____________________________________________________________________________
// Test that opening a file that exists but is not a raster throws.
TEST(RasterFile, throwsForFileThatIsNotARaster) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  std::ofstream{path} << "this is not a raster";
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("Could not open raster file"));
}

// _____________________________________________________________________________
// Test that a raster without a geotransform is rejected.
TEST(RasterFile, throwsForMissingGeoTransform) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), std::nullopt);
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("has no valid geotransform"));
}

// _____________________________________________________________________________
// Test that a raster whose geotransform cannot be inverted is rejected.
TEST(RasterFile, throwsForGeoTransformThatCannotBeInverted) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  // A pixel width and height of 0 maps all pixels to the same point.
  writeTestRaster(path.string(),
                  std::array<double, 6>{400000, 0, 0, 5300000, 0, 0});
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("has no valid geotransform"));
}

// _____________________________________________________________________________
// Test that a raster without a CRS is rejected.
TEST(RasterFile, throwsForMissingCrs) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, std::nullopt);
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("has no CRS"));
}

// _____________________________________________________________________________
// Test that a raster whose CRS cannot be transformed to CRS84 is rejected.
TEST(RasterFile, throwsForCrsWithoutTransformationToCrs84) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, LOCAL_GRID);
  AD_EXPECT_THROW_WITH_MESSAGE(
      RasterFile{path.string()},
      HasSubstr("Could not transform between CRS84 and the CRS"));
}

// _____________________________________________________________________________
// Test that a raster whose CRS can be transformed from CRS84, but not back, is
// rejected.
TEST(RasterFile, throwsForCrsWithoutTransformationFromCrs84) {
  // PROJ can compute the Airy projection, but not its inverse.
  constexpr std::string_view airy =
      "+proj=airy +datum=WGS84 +units=m +type=crs";
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  // GeoTIFF cannot store this CRS itself, so GDAL writes it to a second file.
  absl::Cleanup removeAuxFile{[auxPath = path.string() + ".aux.xml"] {
    ql::filesystem::remove(auxPath);
  }};
  writeTestRaster(path.string(), NORTH_UP, airy);
  AD_EXPECT_THROW_WITH_MESSAGE(
      RasterFile{path.string()},
      HasSubstr("Could not transform between CRS84 and the CRS"));
}

// _____________________________________________________________________________
// Test that `numBands` returns the number of bands of the file.
TEST(RasterFile, numBandsCountsAllBands) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/3);
  EXPECT_EQ(RasterFile{path.string()}.numBands(), 3);
}

// _____________________________________________________________________________
// Test that a `RasterFile` that is moved into, by construction or assignment,
// holds the file of the one it was moved from.
TEST(RasterFile, movesKeepTheFile) {
  auto [twoBandsPath, cleanup1] = ad_utility::testing::filenameForTesting();
  auto [oneBandPath, cleanup2] = ad_utility::testing::filenameForTesting();
  writeTestRaster(twoBandsPath.string(), NORTH_UP, UTM_ZONE_32N,
                  /*numBands=*/2);
  writeTestRaster(oneBandPath.string());

  RasterFile original{twoBandsPath.string()};
  RasterFile moved{std::move(original)};
  EXPECT_EQ(moved.numBands(), 2);

  RasterFile assigned{oneBandPath.string()};
  assigned = std::move(moved);
  EXPECT_EQ(assigned.numBands(), 2);
}

// _____________________________________________________________________________
// Test that a band with complex values is rejected, and that the message names
// the band.
TEST(RasterFile, throwsForComplexBand) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/1,
                  GDT_CInt16);
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               AllOf(HasSubstr("Band 1 of raster file"),
                                     HasSubstr("unsupported type CInt16")));
}

// _____________________________________________________________________________
// Test that a band of type `UInt64` is rejected.
TEST(RasterFile, throwsForUInt64Band) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/1,
                  GDT_UInt64);
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("unsupported type UInt64"));
}

// _____________________________________________________________________________
// Test that a band of type `Int64` is rejected.
TEST(RasterFile, throwsForInt64Band) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/1,
                  GDT_Int64);
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("unsupported type Int64"));
}

// _____________________________________________________________________________
// Test that a NoData value outside the range of the band's type is rejected,
// and that the message names the band and the file.
TEST(RasterFile, throwsForNoDataOutsideTheTypesRange) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  {
    // A Byte holds 0 to 255.
    auto dataset = openRasterForUpdate(path.string());
    ASSERT_EQ(dataset->GetRasterBand(1)->SetNoDataValue(256), CE_None);
  }
  AD_EXPECT_THROW_WITH_MESSAGE(
      RasterFile{path.string()},
      AllOf(HasSubstr("Band 1 of raster file"), HasSubstr(path.string()),
            HasSubstr("which its type Byte cannot hold")));
}

// _____________________________________________________________________________
// Test that a NoData value that is not a whole number is rejected for an
// integer band.
TEST(RasterFile, throwsForNoDataThatIsNotAWholeNumber) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  {
    auto dataset = openRasterForUpdate(path.string());
    ASSERT_EQ(dataset->GetRasterBand(1)->SetNoDataValue(1.5), CE_None);
  }
  AD_EXPECT_THROW_WITH_MESSAGE(RasterFile{path.string()},
                               HasSubstr("which its type Byte cannot hold"));
}

// _____________________________________________________________________________
// Test that a band that marks invalid pixels with a mask is rejected.
TEST(RasterFile, throwsForBandWithMask) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  {
    auto dataset = openRasterForUpdate(path.string());
    ASSERT_EQ(dataset->CreateMaskBand(GMF_PER_DATASET), CE_None);
  }
  AD_EXPECT_THROW_WITH_MESSAGE(
      RasterFile{path.string()},
      HasSubstr("marks invalid pixels with a mask, not with a NoData value"));
}

// _____________________________________________________________________________
// Test that `lookupPoint` returns the value and the center of the pixel under
// the point. Pixel (1, 0) is in column 1 and row 0, so mixing up columns and
// rows, or latitude and longitude, would give another pixel or none.
TEST(RasterFile, readsThePixelUnderAPoint) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  writePixelValues(path.string(), /*bandNumber=*/1, {1, 2, 3, 4});
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  EXPECT_THAT(
      file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector{1}, allocator),
      AllOf(AD_FIELD(PixelTable, centers_,
                     ElementsAre(pixelCenterNear(CENTER_OF_PIXEL_1_0))),
            AD_FIELD(
                PixelTable, valuesPerBand_,
                ElementsAre(ElementsAre(Optional(VariantWith<int64_t>(2)))))));
}

// _____________________________________________________________________________
// Test that `lookupPoint` returns one column per entry of `bands`, in the order
// of `bands`, also if a band is requested more than once.
TEST(RasterFile, returnsBandsInTheRequestedOrder) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/2);
  writePixelValues(path.string(), /*bandNumber=*/1, {1, 2, 3, 4});
  writePixelValues(path.string(), /*bandNumber=*/2, {10, 20, 30, 40});
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  EXPECT_THAT(
      file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector{2, 1, 2}, allocator),
      AD_FIELD(PixelTable, valuesPerBand_,
               ElementsAre(ElementsAre(Optional(VariantWith<int64_t>(20))),
                           ElementsAre(Optional(VariantWith<int64_t>(2))),
                           ElementsAre(Optional(VariantWith<int64_t>(20))))));
}

// _____________________________________________________________________________
// Test that `lookupPoint` returns no pixel for points just outside each side of
// the grid.
TEST(RasterFile, returnsNothingOutsideTheGrid) {
  // These points lie 1 m outside each side of the grid. They were computed
  // with GDAL, like `CENTER_OF_PIXEL_1_0`.
  const GeoPoint leftOfGrid{47.845516123, 7.663396819};
  const GeoPoint rightOfGrid{47.845519546, 7.663690786};
  const GeoPoint aboveGrid{47.845571026, 7.663475605};
  const GeoPoint belowGrid{47.845373137, 7.663480689};
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  writePixelValues(path.string(), /*bandNumber=*/1, {1, 2, 3, 4});
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  for (const GeoPoint& point :
       {leftOfGrid, rightOfGrid, aboveGrid, belowGrid}) {
    SCOPED_TRACE(point.toStringRepresentation());
    EXPECT_THAT(file.lookupPoint(point, std::vector{1}, allocator),
                isEmptyPixelTable(/*numBands=*/1));
  }
}

// _____________________________________________________________________________
// Test that `lookupPoint` returns no pixel for a point that cannot be
// transformed to the CRS of the file. UTM zone 32N is centered on longitude 9,
// and its transformation fails at longitude 100, which is 91 degrees away.
TEST(RasterFile, returnsNothingWhereTheTransformFails) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  writePixelValues(path.string(), /*bandNumber=*/1, {1, 2, 3, 4});
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  EXPECT_THAT(file.lookupPoint(GeoPoint{0, 100}, std::vector{1}, allocator),
              isEmptyPixelTable(/*numBands=*/1));
}

// _____________________________________________________________________________
// Test that `lookupPoint` keeps a pixel that is NoData in only some of the
// requested bands, and leaves its value empty in those bands.
TEST(RasterFile, keepsAPixelThatIsNoDataInSomeBands) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/2);
  writePixelValues(path.string(), /*bandNumber=*/1, {1, 2, 3, 4});
  writePixelValues(path.string(), /*bandNumber=*/2, {10, 20, 30, 40});
  {
    // GeoTIFF stores one NoData value for all bands. It is 2, the value of
    // pixel (1, 0) in band 1, while band 2 has 20 there.
    auto dataset = openRasterForUpdate(path.string());
    ASSERT_EQ(dataset->GetRasterBand(1)->SetNoDataValue(2), CE_None);
  }
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  EXPECT_THAT(
      file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector{1, 2}, allocator),
      AD_FIELD(PixelTable, valuesPerBand_,
               ElementsAre(ElementsAre(std::nullopt),
                           ElementsAre(Optional(VariantWith<int64_t>(20))))));
}

// _____________________________________________________________________________
// Test that `lookupPoint` leaves out a pixel that is NoData in all requested
// bands.
TEST(RasterFile, skipsAPixelThatIsNoDataInAllBands) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/2);
  writePixelValues(path.string(), /*bandNumber=*/1, {1, 2, 3, 4});
  writePixelValues(path.string(), /*bandNumber=*/2, {10, 2, 30, 40});
  {
    // GeoTIFF stores one NoData value for all bands. The value 2 of pixel
    // (1, 0) is NoData in both bands.
    auto dataset = openRasterForUpdate(path.string());
    ASSERT_EQ(dataset->GetRasterBand(1)->SetNoDataValue(2), CE_None);
    ASSERT_EQ(dataset->GetRasterBand(2)->SetNoDataValue(2), CE_None);
  }
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  EXPECT_THAT(
      file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector{1, 2}, allocator),
      isEmptyPixelTable(/*numBands=*/2));
}

// _____________________________________________________________________________
// Test that `lookupPoint` reads floating point bands as `double`, and that a
// pixel is NoData if both the pixel and the NoData value are NaN (although NaN
// is not equal to NaN).
TEST(RasterFile, readsFloatBandsWithNaNAsNoData) {
  constexpr double nan = std::numeric_limits<double>::quiet_NaN();
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/2,
                  GDT_Float32);
  writePixelValues(path.string(), /*bandNumber=*/1, {1.5, nan, 2.5, 3.5});
  writePixelValues(path.string(), /*bandNumber=*/2, {0.25, 0.5, 0.75, 1.0});
  {
    auto dataset = openRasterForUpdate(path.string());
    ASSERT_EQ(dataset->GetRasterBand(1)->SetNoDataValue(nan), CE_None);
    ASSERT_EQ(dataset->GetRasterBand(2)->SetNoDataValue(nan), CE_None);
  }
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  EXPECT_THAT(
      file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector{1, 2}, allocator),
      AD_FIELD(PixelTable, valuesPerBand_,
               ElementsAre(ElementsAre(std::nullopt),
                           ElementsAre(Optional(VariantWith<double>(0.5))))));
}

// _____________________________________________________________________________
// Test that a pixel of a floating point band is NoData if it equals a NoData
// value that is not NaN.
TEST(RasterFile, readsFloatBandsWithNumericNoData) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string(), NORTH_UP, UTM_ZONE_32N, /*numBands=*/2,
                  GDT_Float32);
  writePixelValues(path.string(), /*bandNumber=*/1, {1.5, 2.5, 3.5, 4.5});
  writePixelValues(path.string(), /*bandNumber=*/2, {0.25, 0.5, 0.75, 1.0});
  {
    // GeoTIFF stores one NoData value for all bands.
    auto dataset = openRasterForUpdate(path.string());
    ASSERT_EQ(dataset->GetRasterBand(1)->SetNoDataValue(2.5), CE_None);
    ASSERT_EQ(dataset->GetRasterBand(2)->SetNoDataValue(2.5), CE_None);
  }
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  EXPECT_THAT(
      file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector{1, 2}, allocator),
      AD_FIELD(PixelTable, valuesPerBand_,
               ElementsAre(ElementsAre(std::nullopt),
                           ElementsAre(Optional(VariantWith<double>(0.5))))));
}

// _____________________________________________________________________________
// Test that `lookupPoint` rejects an empty list of bands and band numbers that
// do not exist. Bands are numbered from 1, and the file has only band 1.
TEST(RasterFile, rejectsInvalidBandNumbers) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  writeTestRaster(path.string());
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  RasterFile file{path.string()};
  EXPECT_THROW(
      file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector<int>{}, allocator),
      ad_utility::Exception);
  EXPECT_THROW(file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector{0}, allocator),
               ad_utility::Exception);
  EXPECT_THROW(file.lookupPoint(CENTER_OF_PIXEL_1_0, std::vector{2}, allocator),
               ad_utility::Exception);
}

// _____________________________________________________________________________
// Test that a `PixelTable` cannot be built from columns of different lengths.
TEST(PixelTable, rejectsColumnsOfDifferentLengths) {
  auto allocator = qlever::makeUnlimitedAllocator<char>();
  qlm::vector<GeoPoint> centers({CENTER_OF_PIXEL_1_0}, allocator);
  std::vector<BandValues> valuesPerBand{BandValues{allocator}};
  EXPECT_THROW(PixelTable(std::move(centers), std::move(valuesPerBand)),
               ad_utility::Exception);
}

// _____________________________________________________________________________
// Test that a name gives the path of the file with that name, and not of
// another file in the folder.
TEST(RasterFilePath, findsTheFileWithTheGivenName) {
  auto [folder, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("rasterFilePathFinds");
  std::ofstream{folder + "/elevation.tif"}.close();
  std::ofstream{folder + "/slope.tif"}.close();
  EXPECT_EQ(rasterFilePath(folder, "elevation"), folder + "/elevation.tif");
}

// _____________________________________________________________________________
// Test that a name without a file throws, and that the message names the file
// and the folder.
TEST(RasterFilePath, throwsForAnUnknownName) {
  auto [folder, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("rasterFilePathUnknown");
  std::ofstream{folder + "/elevation.tif"}.close();
  AD_EXPECT_THROW_WITH_MESSAGE(
      rasterFilePath(folder, "slope"),
      AllOf(HasSubstr("There is no raster file \"slope.tif\""),
            HasSubstr(folder)));
}

// _____________________________________________________________________________
// Test that a name cannot reach a file outside the folder, even if the file
// exists.
TEST(RasterFilePath, throwsForANameThatIsAPath) {
  auto [folder, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("rasterFilePathOutside");
  ql::filesystem::create_directory(folder + "/rasters");
  std::ofstream{folder + "/outside.tif"}.close();
  AD_EXPECT_THROW_WITH_MESSAGE(
      rasterFilePath(folder + "/rasters", "../outside"),
      HasSubstr("There is no raster file"));
}

// _____________________________________________________________________________
// Test that files with another extension than `.tif` are not found.
TEST(RasterFilePath, ignoresFilesThatAreNotTif) {
  auto [folder, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("rasterFilePathNotTif");
  std::ofstream{folder + "/elevation.txt"}.close();
  AD_EXPECT_THROW_WITH_MESSAGE(rasterFilePath(folder, "elevation"),
                               HasSubstr("There is no raster file"));
}
