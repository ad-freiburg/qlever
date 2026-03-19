// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <ostream>

#include "../test/util/GTestHelpers.h"
#include "gtest/gtest.h"
#include "util/File.h"
#include "util/json.h"

/*
@brief Do the `fileToJson` test with the wanted json class type.
*/
CPP_template(typename WantedJsonClassType)(
    requires OrderedOrUnorderedJson<
        WantedJsonClassType>) static void doFileToJsonTest(ad_utility::
                                                               source_location l =
                                                                   AD_CURRENT_SOURCE_LOC()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "doFileToJsonTest")};

  // The `fileToJson` function with the wanted json class type.
  const auto& fileToJsonWithWantedType = [](std::string_view jsonFileName) {
    return fileToJson<WantedJsonClassType>(jsonFileName);
  };

  // Creates a file with the given name and content. Can be deleted with
  // `ad_utility::deleteFile(fileName)`.
  auto createFile = [](std::string_view fileName, auto fileContent) {
    std::ofstream tempStream{ad_utility::makeOfstream(fileName)};
    tempStream << fileContent << std::endl;
    tempStream.close();
  };

  // The helper function only wants `.json` files.
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJsonWithWantedType("NotAJsonFile.txt"),
      ::testing::ContainsRegex(R"(NotAJsonFile\.txt.*json file)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJsonWithWantedType("NotAJsonFile.md"),
      ::testing::ContainsRegex(R"(NotAJsonFile\.md.*json file)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJsonWithWantedType("NotAJsonFile.mp4"),
      ::testing::ContainsRegex(R"(NotAJsonFile\.mp4.*json file)"));

  // File doesn't exist.
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJsonWithWantedType("FileINeverCreated.json"),
      ::testing::ContainsRegex(R"(Could not open file)"));

  // File exists, but doesn't contain valid json.
  createFile("NotJson.json", R"("d":4)");
  AD_EXPECT_THROW_WITH_MESSAGE(
      fileToJsonWithWantedType("NotJson.json"),
      ::testing::ContainsRegex("could not be parsed as JSON"));
  ad_utility::deleteFile("NotJson.json");

  // Creates a temporare file, containing the given json object, and checks, if
  // `fileToJson` recreates it correctly.
  auto makeTempFileAndCompare =
      [&createFile, &fileToJsonWithWantedType](const WantedJsonClassType& j) {
        // Creating the file.
        constexpr std::string_view fileName{"TempTestFile.json"};
        createFile(fileName, j);

        EXPECT_EQ(j, fileToJsonWithWantedType(fileName));

        // Deleting the file.
        ad_utility::deleteFile(fileName);
      };

  makeTempFileAndCompare(
      WantedJsonClassType::parse(R"({ "name"   : "John Smith",
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
TEST(JsonUtilityFunctionTests, fileToJson) {
  doFileToJsonTest<nlohmann::json>();
  doFileToJsonTest<nlohmann::ordered_json>();
}

TEST(JsonUtilTests, JsonToTypeString) {
  // All official data types in json.
  ASSERT_EQ("array",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::array)));
  ASSERT_EQ("boolean",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::boolean)));
  ASSERT_EQ("null",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::null)));
  ASSERT_EQ(
      "number",
      jsonToTypeString(nlohmann::json(nlohmann::json::value_t::number_float)));
  ASSERT_EQ("number", jsonToTypeString(nlohmann::json(
                          nlohmann::json::value_t::number_integer)));
  ASSERT_EQ("number", jsonToTypeString(nlohmann::json(
                          nlohmann::json::value_t::number_unsigned)));
  ASSERT_EQ("object",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::object)));
  ASSERT_EQ("string",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::string)));

  // Unofficial types shouldn't work.
  ASSERT_ANY_THROW(
      jsonToTypeString(nlohmann::json(nlohmann::json::value_t::discarded)));
}
