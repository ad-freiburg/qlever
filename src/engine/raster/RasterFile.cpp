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
#include <absl/strings/str_join.h>
#include <gdal_alg.h>
#include <gdal_priv.h>
#include <ogr_spatialref.h>

#include <array>
#include <cmath>
#include <map>
#include <stdexcept>
#include <vector>

#include "backports/filesystem.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/Exception.h"

namespace qlever::raster {

namespace {

// The longest polygon edge, in degrees, before a polygon is transformed to the
// CRS of the file. This is about 1 km, which keeps the error of a transformed
// edge at a few centimeters.
constexpr double MAX_SEGMENT_LENGTH_DEGREES = 0.01;

// GDAL's drivers must be registered once per process, before the first file
// is opened.
void registerGdalDrivers() {
  [[maybe_unused]] static const bool registered = [] {
    GDALAllRegister();
    return true;
  }();
}

// The NoData value of `band` as an `int64_t` for integer bands and as a
// `double` otherwise. Returns nothing if the band has no NoData value. A NoData
// value that the band's type cannot hold (for example -3.4e38 on a Byte band)
// can never occur, so it also counts as none.
std::optional<PixelValue> readNoData(GDALRasterBand& band) {
  int hasNoData = 0;
  GDALDataType type = band.GetRasterDataType();

  // A `double` cannot hold every `int64_t` exactly.
  if (type == GDT_Int64) {
    int64_t noData = band.GetNoDataValueAsInt64(&hasNoData);
    if (!hasNoData) {
      return std::nullopt;
    }
    return PixelValue{noData};
  }

  double noData = band.GetNoDataValue(&hasNoData);
  if (!hasNoData) {
    return std::nullopt;
  }
  // The value that a pixel of this type holds if `noData` is written to it.
  int clamped = 0;
  int rounded = 0;
  double storedNoData =
      GDALAdjustValueToDataType(type, noData, &clamped, &rounded);
  // If GDAL had to change `noData` to fit the type, no pixel can hold it.
  if (clamped || rounded) {
    return std::nullopt;
  }
  if (GDALDataTypeIsInteger(type)) {
    return PixelValue{static_cast<int64_t>(storedNoData)};
  }
  return PixelValue{storedNoData};
}

// Converts a geometry parsed by QLever to the same geometry in OGR. One call
// operator per type, so that `visitAnyGeometry` can apply it to the members of
// a collection. All return the base class, because a collection mixes types.
struct ToOgrGeometry {
  using OgrGeometryPtr = std::unique_ptr<OGRGeometry>;

  OgrGeometryPtr operator()(const ::util::geo::DPoint& point) const {
    return std::make_unique<OGRPoint>(point.getX(), point.getY());
  }

  OgrGeometryPtr operator()(const ::util::geo::DLine& line) const {
    auto ogrLine = std::make_unique<OGRLineString>();
    addPoints(*ogrLine, line);
    return ogrLine;
  }

  OgrGeometryPtr operator()(const ::util::geo::DPolygon& polygon) const {
    auto ogrPolygon = std::make_unique<OGRPolygon>();
    auto addRing = [&ogrPolygon](const ::util::geo::Ring<double>& points) {
      OGRLinearRing ring;
      addPoints(ring, points);
      ogrPolygon->addRing(&ring);
    };
    // The first ring is the outer one, the others are holes.
    addRing(polygon.getOuter());
    for (const auto& inner : polygon.getInners()) {
      addRing(inner);
    }
    // QLever stores rings without repeating the first point at the end, but
    // OGR needs closed rings.
    ogrPolygon->closeRings();
    return ogrPolygon;
  }

  OgrGeometryPtr operator()(const ::util::geo::DMultiPoint& points) const {
    return toContainer<OGRMultiPoint>(points);
  }

  OgrGeometryPtr operator()(const ::util::geo::DMultiLine& lines) const {
    return toContainer<OGRMultiLineString>(lines);
  }

  OgrGeometryPtr operator()(const ::util::geo::DMultiPolygon& polygons) const {
    return toContainer<OGRMultiPolygon>(polygons);
  }

  OgrGeometryPtr operator()(const ::util::geo::DCollection& members) const {
    return toContainer<OGRGeometryCollection>(members);
  }

  // A member of a collection, which can hold any of the types above.
  OgrGeometryPtr operator()(
      const ::util::geo::AnyGeometry<double>& geometry) const {
    return ad_utility::detail::visitAnyGeometry(*this, geometry);
  }

 private:
  // Lines and polygon rings are both curves of points in OGR.
  static void addPoints(OGRSimpleCurve& curve,
                        const ::util::geo::DLine& points) {
    for (const auto& point : points) {
      curve.addPoint(point.getX(), point.getY());
    }
  }

  // An `OgrContainer` with all `members`, each converted by this struct.
  template <typename OgrContainer, typename Members>
  OgrGeometryPtr toContainer(const Members& members) const {
    auto container = std::make_unique<OgrContainer>();
    for (const auto& member : members) {
      // The container takes ownership. It only refuses a member of the wrong
      // type, which the overloads above never create.
      OGRErr error = container->addGeometryDirectly((*this)(member).release());
      AD_CORRECTNESS_CHECK(error == OGRERR_NONE);
    }
    return container;
  }
};

}  // namespace

// _____________________________________________________________________________
struct RasterFile::Impl {
  // A rectangle of whole pixels, for example the pixels under a polygon.
  struct PixelWindow {
    // The top-left pixel.
    int column_;
    int row_;
    // The size in pixels.
    int width_;
    int height_;
  };

  // The information about one band that the lookups need.
  struct Band {
    // Owned by `dataset_`, valid as long as it is open.
    GDALRasterBand* handle_;
    // Integer bands are read as `int64_t`, all others as `double`.
    bool isInteger_;
    // Has the same type as the values that are read from this band.
    std::optional<PixelValue> noData_;

    // Return the value of the pixel at `column`, `row`.
    PixelValue readPixel(int column, int row) const;

    // Return the values of all pixels in `window`, row by row.
    std::variant<std::vector<int64_t>, std::vector<double>> readWindow(
        const PixelWindow& window) const;

    // Return whether `value`, which was read from this band, is its NoData
    // value.
    bool isNoData(const PixelValue& value) const;
  };

  GDALDatasetUniquePtr dataset_;
  int width_;
  int height_;
  // Pixel position to coordinates in the CRS of the file, and back.
  std::array<double, 6> pixelToRaster_;
  std::array<double, 6> rasterToPixel_;
  // CRS84 to the CRS of the file, and back.
  std::unique_ptr<OGRCoordinateTransformation> crs84ToRaster_;
  std::unique_ptr<OGRCoordinateTransformation> rasterToCrs84_;
  // How many metres one unit of the CRS of the file is: 1 for metres, 0.3048
  // for feet. Empty for a CRS in degrees, where a radius cannot be used.
  std::optional<double> metresPerUnit_;
  std::vector<Band> bands_;

  explicit Impl(const std::string& path);

  // Return the center of the pixel at `column`, `row`.
  GeoPoint pixelCenter(int column, int row);

  // Return the pixels under `envelope`, which is in the CRS of the file,
  // clipped to the grid. Return nothing if `envelope` misses the grid.
  std::optional<PixelWindow> pixelWindow(const OGREnvelope& envelope) const;

  // Return one byte per pixel of `window`, row by row. A byte is 1 if its
  // pixel belongs to `geometry`, which is in the CRS of the file, by GDAL's
  // default rule: the pixel center lies inside a polygon, the pixel is on the
  // thin line drawn along a line, or the pixel contains a point. `geometry`
  // is not `const`, because GDAL takes the geometries as non-const handles.
  std::vector<uint8_t> rasterizeMask(OGRGeometry& geometry,
                                     const PixelWindow& window) const;

  // Return the pixels of `band` that belong to `geometry`, which is in CRS84
  // (see `rasterizeMask`). With a `radius` in metres, `geometry` is first
  // widened by that radius. Takes ownership, because `geometry` is changed in
  // place.
  std::vector<Pixel> lookupOgrGeometry(std::unique_ptr<OGRGeometry> geometry,
                                       const Band& band,
                                       std::optional<double> radius);
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
  width_ = dataset_->GetRasterXSize();
  height_ = dataset_->GetRasterYSize();

  // The member function `GetGeoTransform(double*)` is deprecated in GDAL 3.12,
  // and its replacement does not exist in GDAL 3.8.
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
  // Longitude (or easting) first, as in WKT. This turns EPSG:4326 into CRS84.
  // Otherwise GDAL follows the axis order of the CRS definition, which is
  // latitude first for EPSG:4326.
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
  // A CRS in degrees also reports 1 here, so check that it is projected.
  if (rasterCrs.IsProjected()) {
    metresPerUnit_ = rasterCrs.GetLinearUnits();
  }

  for (int index = 1; index <= dataset_->GetRasterCount(); ++index) {
    GDALRasterBand* band = dataset_->GetRasterBand(index);
    GDALDataType type = band->GetRasterDataType();
    // Not every `UInt64` value fits into an `int64_t`.
    if (GDALDataTypeIsComplex(type) || type == GDT_UInt64 ||
        type == GDT_Unknown) {
      throw std::runtime_error(absl::StrCat(
          "Band ", index, " of raster file \"", path,
          "\" has the unsupported type ", GDALGetDataTypeName(type)));
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
std::variant<std::vector<int64_t>, std::vector<double>>
RasterFile::Impl::Band::readWindow(const PixelWindow& window) const {
  // GDAL converts the pixels to `type` while reading them into `values`.
  auto read = [this, &window](auto& values, GDALDataType type) {
    values.resize(static_cast<size_t>(window.width_) * window.height_);
    if (handle_->RasterIO(GF_Read, window.column_, window.row_, window.width_,
                          window.height_, values.data(), window.width_,
                          window.height_, type, /*nPixelSpace=*/0,
                          /*nLineSpace=*/0) != CE_None) {
      throw std::runtime_error(absl::StrCat(
          "Could not read ", window.width_, " x ", window.height_,
          " pixels at (", window.column_, ", ", window.row_,
          ") of raster file \"", handle_->GetDataset()->GetDescription(),
          "\": ", CPLGetLastErrorMsg()));
    }
  };
  if (isInteger_) {
    std::vector<int64_t> values;
    read(values, GDT_Int64);
    return values;
  }
  std::vector<double> values;
  read(values, GDT_Float64);
  return values;
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
GeoPoint RasterFile::Impl::pixelCenter(int column, int row) {
  double x = 0.0;
  double y = 0.0;
  GDALApplyGeoTransform(pixelToRaster_.data(), column + 0.5, row + 0.5, &x, &y);
  AD_CORRECTNESS_CHECK(rasterToCrs84_->Transform(1, &x, &y));
  // `GeoPoint` takes the latitude first.
  return GeoPoint{y, x};
}

// _____________________________________________________________________________
std::optional<RasterFile::Impl::PixelWindow> RasterFile::Impl::pixelWindow(
    const OGREnvelope& envelope) const {
  // The window must contain all four corners. In a rotated grid, any corner
  // can have the smallest or largest column or row.
  std::array<double, 4> cornersX{envelope.MinX, envelope.MinX, envelope.MaxX,
                                 envelope.MaxX};
  std::array<double, 4> cornersY{envelope.MinY, envelope.MaxY, envelope.MinY,
                                 envelope.MaxY};
  std::array<double, 4> columns{};
  std::array<double, 4> rows{};
  for (size_t i = 0; i < 4; ++i) {
    GDALApplyGeoTransform(rasterToPixel_.data(), cornersX[i], cornersY[i],
                          &columns[i], &rows[i]);
  }
  double minColumn = columns[0];
  double maxColumn = columns[0];
  double minRow = rows[0];
  double maxRow = rows[0];
  for (size_t i = 1; i < 4; ++i) {
    minColumn = std::fmin(minColumn, columns[i]);
    maxColumn = std::fmax(maxColumn, columns[i]);
    minRow = std::fmin(minRow, rows[i]);
    maxRow = std::fmax(maxRow, rows[i]);
  }

  // `fmin` and `fmax` ignore NaN, so it has to be checked here.
  if (std::isnan(minColumn) || std::isnan(maxColumn) || std::isnan(minRow) ||
      std::isnan(maxRow)) {
    return std::nullopt;
  }

  // Widen to whole pixels, then clip to the grid. The end is one past the
  // pixel that contains the maximum, also if it lies exactly on a border.
  double columnStart = std::fmax(std::floor(minColumn), 0.0);
  double columnEnd = std::fmin(std::floor(maxColumn) + 1, width_);
  double rowStart = std::fmax(std::floor(minRow), 0.0);
  double rowEnd = std::fmin(std::floor(maxRow) + 1, height_);
  if (columnStart >= columnEnd || rowStart >= rowEnd) {
    return std::nullopt;
  }
  return PixelWindow{static_cast<int>(columnStart), static_cast<int>(rowStart),
                     static_cast<int>(columnEnd - columnStart),
                     static_cast<int>(rowEnd - rowStart)};
}

// _____________________________________________________________________________
std::vector<uint8_t> RasterFile::Impl::rasterizeMask(
    OGRGeometry& geometry, const PixelWindow& window) const {
  // An image in memory that covers exactly the window, all pixels 0.
  GDALDriver* memDriver = GetGDALDriverManager()->GetDriverByName("MEM");
  GDALDatasetUniquePtr mask{memDriver->Create("", window.width_, window.height_,
                                              /*nBands=*/1, GDT_Byte,
                                              /*papszOptions=*/nullptr)};
  AD_CORRECTNESS_CHECK(mask != nullptr);

  // Our geotransform, but with its origin at the top-left pixel of the
  // window. This places the geometry on the right pixels of the mask.
  std::array<double, 6> windowToRaster = pixelToRaster_;
  GDALApplyGeoTransform(pixelToRaster_.data(), window.column_, window.row_,
                        &windowToRaster[0], &windowToRaster[3]);
  GDALSetGeoTransform(GDALDataset::ToHandle(mask.get()), windowToRaster.data());

  // Write 1 into every pixel that belongs to `geometry`.
  int bandNumber = 1;
  OGRGeometryH geometryHandle = OGRGeometry::ToHandle(&geometry);
  double burnValue = 1.0;
  AD_CORRECTNESS_CHECK(
      GDALRasterizeGeometries(
          GDALDataset::ToHandle(mask.get()), /*nBandCount=*/1, &bandNumber,
          /*nGeomCount=*/1, &geometryHandle, /*pfnTransformer=*/nullptr,
          /*pTransformArg=*/nullptr, &burnValue, /*papszOptions=*/nullptr,
          /*pfnProgress=*/nullptr, /*pProgressArg=*/nullptr) == CE_None);

  std::vector<uint8_t> belongsToGeometry(static_cast<size_t>(window.width_) *
                                         window.height_);
  AD_CORRECTNESS_CHECK(mask->GetRasterBand(1)->RasterIO(
                           GF_Read, 0, 0, window.width_, window.height_,
                           belongsToGeometry.data(), window.width_,
                           window.height_, GDT_Byte, /*nPixelSpace=*/0,
                           /*nLineSpace=*/0) == CE_None);
  return belongsToGeometry;
}

// _____________________________________________________________________________
std::vector<Pixel> RasterFile::Impl::lookupOgrGeometry(
    std::unique_ptr<OGRGeometry> geometry, const Band& band,
    std::optional<double> radius) {
  // An edge that is straight in CRS84 is curved in the CRS of the file. Extra
  // vertices let the transformed edge follow that curve.
  geometry->segmentize(MAX_SEGMENT_LENGTH_DEGREES);
  if (geometry->transform(crs84ToRaster_.get()) != OGRERR_NONE) {
    return {};
  }
  // Widen in the CRS of the file, where distances are in its own unit, not in
  // degrees.
  if (radius.has_value()) {
    if (!metresPerUnit_.has_value()) {
      throw std::runtime_error(absl::StrCat(
          "The parameter `<radius>` needs a raster in a projected CRS (for "
          "example in metres), but the CRS of this raster is \"",
          dataset_->GetSpatialRef()->GetName(), "\""));
    }
    // `Buffer` returns a new geometry, which `geometry` now owns.
    geometry.reset(geometry->Buffer(radius.value() / metresPerUnit_.value()));
    if (!geometry) {
      return {};
    }
  }
  OGREnvelope envelope;
  geometry->getEnvelope(&envelope);
  std::optional<PixelWindow> window = pixelWindow(envelope);
  if (!window.has_value()) {
    return {};
  }
  std::vector<uint8_t> belongsToGeometry =
      rasterizeMask(*geometry, window.value());

  // The values of the pixels to return, and their centers in the CRS of the
  // file.
  std::vector<PixelValue> values;
  std::vector<double> centersX;
  std::vector<double> centersY;
  std::visit(
      [&](const auto& windowValues) {
        for (int row = 0; row < window->height_; ++row) {
          for (int column = 0; column < window->width_; ++column) {
            size_t index = static_cast<size_t>(row) * window->width_ + column;
            if (!belongsToGeometry[index]) {
              continue;
            }
            PixelValue value{windowValues[index]};
            if (band.isNoData(value)) {
              continue;
            }
            double x = 0.0;
            double y = 0.0;
            GDALApplyGeoTransform(pixelToRaster_.data(),
                                  window->column_ + column + 0.5,
                                  window->row_ + row + 0.5, &x, &y);
            values.push_back(value);
            centersX.push_back(x);
            centersY.push_back(y);
          }
        }
      },
      band.readWindow(window.value()));

  // Transform all centers to CRS84 in one call.
  if (!values.empty()) {
    AD_CORRECTNESS_CHECK(rasterToCrs84_->Transform(
        centersX.size(), centersX.data(), centersY.data()));
  }
  std::vector<Pixel> pixels;
  pixels.reserve(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    // `GeoPoint` takes the latitude first.
    pixels.push_back(Pixel{GeoPoint{centersY[i], centersX[i]}, values[i]});
  }
  return pixels;
}

// _____________________________________________________________________________
RasterFile::RasterFile(const std::string& path)
    : impl_{std::make_unique<Impl>(path)} {}

// The following are defined here and not in the header, because they need the
// complete type `Impl`.
RasterFile::~RasterFile() = default;
RasterFile::RasterFile(RasterFile&&) noexcept = default;
RasterFile& RasterFile::operator=(RasterFile&&) noexcept = default;

// _____________________________________________________________________________
int RasterFile::numBands() const {
  return static_cast<int>(impl_->bands_.size());
}

// _____________________________________________________________________________
std::optional<Pixel> RasterFile::lookupPoint(const GeoPoint& point, int band) {
  AD_CONTRACT_CHECK(1 <= band && band <= numBands());
  Impl& impl = *impl_;

  double x = point.getLng();
  double y = point.getLat();
  if (!impl.crs84ToRaster_->Transform(1, &x, &y)) {
    return std::nullopt;
  }

  double column = 0.0;
  double row = 0.0;
  GDALApplyGeoTransform(impl.rasterToPixel_.data(), x, y, &column, &row);
  // Use `floor` and not a cast to `int`. A cast turns -0.5 into 0, which would
  // accept a point just outside the grid.
  column = std::floor(column);
  row = std::floor(row);
  // Written as "inside" so that NaN counts as outside.
  if (!(0 <= column && column < impl.width_ && 0 <= row &&
        row < impl.height_)) {
    return std::nullopt;
  }
  int pixelColumn = static_cast<int>(column);
  int pixelRow = static_cast<int>(row);

  const Impl::Band& bandInfo = impl.bands_[band - 1];
  PixelValue value = bandInfo.readPixel(pixelColumn, pixelRow);
  if (bandInfo.isNoData(value)) {
    return std::nullopt;
  }
  return Pixel{impl.pixelCenter(pixelColumn, pixelRow), value};
}

// _____________________________________________________________________________
std::vector<Pixel> RasterFile::lookupCircle(const GeoPoint& center, int band,
                                            double radius) {
  AD_CONTRACT_CHECK(1 <= band && band <= numBands());
  return impl_->lookupOgrGeometry(
      std::make_unique<OGRPoint>(center.getLng(), center.getLat()),
      impl_->bands_[band - 1], radius);
}

// _____________________________________________________________________________
std::vector<Pixel> RasterFile::lookupGeometry(std::string_view wktLiteral,
                                              int band,
                                              std::optional<double> radius) {
  AD_CONTRACT_CHECK(1 <= band && band <= numBands());
  std::optional<ad_utility::detail::ParsedWkt> geometry =
      ad_utility::detail::parseWkt(wktLiteral).parsedWkt_;
  if (!geometry.has_value()) {
    return {};
  }
  const Impl::Band& bandInfo = impl_->bands_[band - 1];

  return std::visit(
      [this, band, &bandInfo,
       radius](const auto& parsed) -> std::vector<Pixel> {
        using T = std::decay_t<decltype(parsed)>;
        // A point without a radius is one pixel, read directly.
        if constexpr (std::is_same_v<T, ::util::geo::DPoint>) {
          if (!radius.has_value()) {
            std::optional<GeoPoint> point;
            try {
              point = ad_utility::detail::utilPointToGeoPoint(parsed);
            } catch (const CoordinateOutOfRangeException&) {
              return {};
            }
            std::optional<Pixel> pixel = lookupPoint(point.value(), band);
            if (!pixel.has_value()) {
              return {};
            }
            return {pixel.value()};
          }
        }
        // Lines, polygons, their multi types and collections, and every
        // geometry widened by a radius, each by the rule of `rasterizeMask`.
        return impl_->lookupOgrGeometry(ToOgrGeometry{}(parsed), bandInfo,
                                        radius);
      },
      geometry.value());
}

// _____________________________________________________________________________
std::string rasterFilePath(std::string_view datasetName) {
  // The folder is scanned only on the first call. A `std::map` keeps the names
  // sorted for the error message.
  static const std::map<std::string, std::string> pathsByName = [] {
    std::map<std::string, std::string> result;
    // Without the folder there are no rasters. Throwing here instead would
    // repeat the initialization (and the exception) on every call.
    if (!ql::filesystem::exists(QLEVER_RASTER_DIR)) {
      return result;
    }
    for (const auto& entry : ql::directoryRange(QLEVER_RASTER_DIR)) {
      const ql::filesystem::path& path = entry.path();
      if (path.extension() == ".tif") {
        result[path.stem().string()] = path.string();
      }
    }
    return result;
  }();

  // The returned path always comes from the folder, never from `datasetName`,
  // so a name like "../secret" cannot reach a file outside the folder.
  auto match = pathsByName.find(std::string{datasetName});
  if (match != pathsByName.end()) {
    return match->second;
  }
  if (pathsByName.empty()) {
    throw std::runtime_error(absl::StrCat(
        "Unknown raster \"", datasetName,
        "\": no raster files are available (the \"rasters\" folder of the "
        "QLever source tree is missing or has no .tif files)"));
  }
  throw std::runtime_error(
      absl::StrCat("Unknown raster \"", datasetName, "\", available: ",
                   absl::StrJoin(pathsByName, ", ",
                                 [](std::string* out, const auto& nameAndPath) {
                                   out->append(nameAndPath.first);
                                 })));
}

}  // namespace qlever::raster
