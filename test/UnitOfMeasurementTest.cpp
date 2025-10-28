// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gtest/gtest.h>

#include "./printers/UnitOfMeasurementPrinters.h"
#include "./util/GTestHelpers.h"
#include "util/SourceLocation.h"
#include "util/UnitOfMeasurement.h"

namespace {

using ad_utility::source_location;

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, KmToUnit) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;
  auto kmToUnit = kilometerToUnit;
  const double error = 0.0001;

  EXPECT_NEAR(kmToUnit(0.0, std::nullopt), 0.0, error);
  EXPECT_NEAR(kmToUnit(0.0, KILOMETERS), 0.0, error);
  EXPECT_NEAR(kmToUnit(0.0, METERS), 0.0, error);
  EXPECT_NEAR(kmToUnit(0.0, MILES), 0.0, error);
  EXPECT_NEAR(kmToUnit(-500.0, KILOMETERS), -500.0, error);
  EXPECT_NEAR(kmToUnit(-500.0, std::nullopt), -500.0, error);
  EXPECT_NEAR(kmToUnit(500.0, METERS), 500000.0, error);
  EXPECT_NEAR(kmToUnit(500.0, MILES), 310.685595, error);
  EXPECT_NEAR(kmToUnit(1.0, MILES), 0.62137119, error);

  auto checkUnsupported = [&](UnitOfMeasurement unit,
                              source_location sourceLocation =
                                  AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    AD_EXPECT_THROW_WITH_MESSAGE(kmToUnit(1.0, unit),
                                 ::testing::HasSubstr("Unsupported unit"));
  };

  checkUnsupported(UNKNOWN);
  checkUnsupported(SQUARE_METERS);
  checkUnsupported(SQUARE_KILOMETERS);
  checkUnsupported(SQUARE_MILES);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, UnitToKm) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;
  auto toKm = valueInUnitToKilometer;
  const double error = 0.0001;

  EXPECT_NEAR(toKm(0.0, std::nullopt), 0.0, error);
  EXPECT_NEAR(toKm(0.0, KILOMETERS), 0.0, error);
  EXPECT_NEAR(toKm(0.0, METERS), 0.0, error);
  EXPECT_NEAR(toKm(0.0, MILES), 0.0, error);

  EXPECT_NEAR(toKm(-500.0, KILOMETERS), -500.0, error);
  EXPECT_NEAR(toKm(-500.0, std::nullopt), -500.0, error);

  EXPECT_NEAR(toKm(500000.0, METERS), 500.0, error);
  EXPECT_NEAR(toKm(310.685595, MILES), 500.0, error);
  EXPECT_NEAR(toKm(0.62137119, MILES), 1.0, error);

  auto checkUnsupported = [&](UnitOfMeasurement unit,
                              source_location sourceLocation =
                                  AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    AD_EXPECT_THROW_WITH_MESSAGE(toKm(1.0, unit),
                                 ::testing::HasSubstr("Unsupported unit"));
  };

  checkUnsupported(UNKNOWN);
  checkUnsupported(SQUARE_METERS);
  checkUnsupported(SQUARE_KILOMETERS);
  checkUnsupported(SQUARE_MILES);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, SqMeterToUnit) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;
  auto m2ToUnit = squareMeterToUnit;
  const double error = 0.0001;

  EXPECT_NEAR(m2ToUnit(0.0, std::nullopt), 0.0, error);
  EXPECT_NEAR(m2ToUnit(0.0, SQUARE_METERS), 0.0, error);
  EXPECT_NEAR(m2ToUnit(0.0, SQUARE_KILOMETERS), 0.0, error);
  EXPECT_NEAR(m2ToUnit(0.0, SQUARE_MILES), 0.0, error);

  // TODO<ullingerc> !!! tests for other values

  auto checkUnsupported = [&](UnitOfMeasurement unit,
                              source_location sourceLocation =
                                  AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    AD_EXPECT_THROW_WITH_MESSAGE(m2ToUnit(1.0, unit),
                                 ::testing::HasSubstr("Unsupported unit"));
  };

  checkUnsupported(UNKNOWN);
  checkUnsupported(METERS);
  checkUnsupported(KILOMETERS);
  checkUnsupported(MILES);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, IsLengthUnit) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;

  EXPECT_TRUE(isLengthUnit(METERS));
  EXPECT_TRUE(isLengthUnit(KILOMETERS));
  EXPECT_TRUE(isLengthUnit(MILES));

  EXPECT_FALSE(isLengthUnit(SQUARE_METERS));
  EXPECT_FALSE(isLengthUnit(SQUARE_KILOMETERS));
  EXPECT_FALSE(isLengthUnit(SQUARE_MILES));
  EXPECT_FALSE(isLengthUnit(UNKNOWN));
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, IsAreaUnit) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;

  EXPECT_TRUE(isAreaUnit(SQUARE_METERS));
  EXPECT_TRUE(isAreaUnit(SQUARE_KILOMETERS));
  EXPECT_TRUE(isAreaUnit(SQUARE_MILES));

  EXPECT_FALSE(isAreaUnit(METERS));
  EXPECT_FALSE(isAreaUnit(KILOMETERS));
  EXPECT_FALSE(isAreaUnit(MILES));
  EXPECT_FALSE(isAreaUnit(UNKNOWN));
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, IriToUnit) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;
  auto iriToUnit = iriToUnitOfMeasurement;

  EXPECT_EQ(iriToUnit(""), UNKNOWN);
  EXPECT_EQ(iriToUnit("http://example.com"), UNKNOWN);
  EXPECT_EQ(iriToUnit("http://qudt.org/vocab/unit/"), UNKNOWN);

  EXPECT_EQ(iriToUnit("http://qudt.org/vocab/unit/M"), METERS);
  EXPECT_EQ(iriToUnit("http://qudt.org/vocab/unit/KiloM"), KILOMETERS);
  EXPECT_EQ(iriToUnit("http://qudt.org/vocab/unit/MI"), MILES);

  EXPECT_EQ(iriToUnit("http://qudt.org/vocab/unit/M2"), SQUARE_METERS);
  EXPECT_EQ(iriToUnit("http://qudt.org/vocab/unit/KiloM2"), SQUARE_KILOMETERS);
  EXPECT_EQ(iriToUnit("http://qudt.org/vocab/unit/MI2"), SQUARE_MILES);
}

}  // namespace
