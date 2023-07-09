// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <ostream>

#include "../test/util/GTestHelpers.h"
#include "gtest/gtest.h"
#include "util/File.h"
#include "util/json.h"

TEST(JsonUtilityFunctionTests, fileToJson) {
  // The helper function only wants `.json` files.
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJson("NotAJsonFile.txt"),
      ::testing::ContainsRegex(R"(NotAJsonFile\.txt.*json file)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJson("NotAJsonFile.md"),
      ::testing::ContainsRegex(R"(NotAJsonFile\.md.*json file)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJson("NotAJsonFile.mp4"),
      ::testing::ContainsRegex(R"(NotAJsonFile\.mp4.*json file)"));

  // File doesn't exist.
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJson("FileINeverCreated.json"),
      ::testing::ContainsRegex(R"(Could not open file)"));

  // Creates a temporare file, containing the given json object, and checks, if
  // `fileToJson` recreates it correctly.
  auto makeTempFileAndCompare = [](const nlohmann::json& j) {
    // Creating the file.
    constexpr std::string_view fileName{"TempTestFile.json"};
    std::ofstream tempStream{ad_utility::makeOfstream(fileName)};
    tempStream << j << std::endl;
    tempStream.close();

    EXPECT_EQ(j, fileToJson(fileName));

    // Deleting the file.
    ad_utility::deleteFile(fileName);
  };

  makeTempFileAndCompare(nlohmann::json::parse(R"({ "name"   : "John Smith",
  "sku"    : "20223",
  "price"  : 23.95,
  "shipTo" : { "name" : "Jane Smith",
               "address" : "123 Maple Street",
               "city" : "Pretendville",
               "state" : "NY",
               "zip"   : "12345" },
  "billTo" : { "name" : "John Smith",
               "address" : "123 Maple Street",
               "city" : "Pretendville",
               "state" : "NY",
               "zip"   : "12345" }
})"));
}
