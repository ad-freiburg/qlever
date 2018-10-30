// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>
#include <cstdio>
#include "../src/engine/GroupBy.h"

// This fixture is used to create an Index for the tests.
// The full index creation is required for initialization of the vocabularies.
class GroupByTest : public ::testing::Test {
 public:
  GroupByTest() {
    // Create the index. The full index creation is run here to allow for
    // loading a docsDb file, which is not otherwise accessible
    std::string docsFileContent = "0\tExert 1\n1\tExert 2\n2\tExert3";
    std::string wordsFileContent =
        "Exert\t0\t0\t0\n"
        "1\t0\t0\t0\n"
        "Exert\t1\t0\t0\n"
        "2\t1\t0\t0\n"
        "Exert\t2\t0\t0\n"
        "3\t2\t0\t0\n";
    std::string ntFileContent = "<a>\t<b>\t<c>\t.";
    ad_utility::File docsFile("group_by_test.documents", "w");
    ad_utility::File wordsFile("group_by_test.words", "w");
    ad_utility::File ntFile("group_by_test.nt", "w");
    docsFile.write(docsFileContent.c_str(), docsFileContent.size());
    wordsFile.write(wordsFileContent.c_str(), wordsFileContent.size());
    ntFile.write(ntFileContent.c_str(), ntFileContent.size());
    docsFile.close();
    wordsFile.close();
    ntFile.close();
    try {
      _index.setKbName("group_by_test");
      _index.setTextName("group_by_test");
      _index.setOnDiskBase("group_ty_test");
      _index.createFromFile<NTriplesParser>("group_by_test.nt", false);
      _index.addTextFromContextFile("group_by_test.words");
      _index.buildDocsDB("group_by_test.documents");

      _index.addTextFromOnDiskIndex();
    } catch (const ad_semsearch::Exception& e) {
      std::cout << "semsearch exception: " << e.getErrorMessage()
                << e.getErrorDetails() << std::endl;
    } catch (std::exception& e) {
      std::cout << "std exception" << e.what() << std::endl;
    }
  }

  virtual ~GroupByTest() {
    // delete all files created during index creation
    std::remove("group_by_test.documents");
    std::remove("group_by_test.words");
    std::remove("group_by_test.text.vocabulary");
    std::remove("group_by_test.vocabulary");
    std::remove("group_by_test.text.index");
    std::remove("group_by_test.text.docsDB");
    std::remove("group_by_test.index.pso");
    std::remove("group_by_test.index.pos");
    std::remove("group_by_test.nt");
  }

  Index _index;
};

TEST_F(GroupByTest, doGroupBy) {
  using std::string;
  using std::vector;

  // There are 7 different aggregates, of which 5 (all apart from SAMPLE and
  // COUNT) react different to the 5 different ResultTypes.

  Id floatBuffers[3];
  float floatValues[3] = {-3, 2, 1231};
  for (int i = 0; i < 3; i++) {
    std::memcpy(&floatBuffers[i], &floatValues[i], sizeof(float));
  }

  // add some words to the index's vocabulary
  Vocabulary<CompressedString>& vocab =
      const_cast<Vocabulary<CompressedString>&>(_index.getVocab());
  vocab.push_back("<entity1>");
  vocab.push_back("<entity2>");
  vocab.push_back("<entity3>");
  vocab.push_back(ad_utility::convertFloatToIndexWord("1.1231", 10, 20));
  vocab.push_back(ad_utility::convertFloatToIndexWord("-5", 10, 20));
  vocab.push_back(ad_utility::convertFloatToIndexWord("17", 10, 20));

  // create an input result table with a local vocabulary
  ResultTable inTable;
  inTable._localVocab->push_back("<local1>");
  inTable._localVocab->push_back("<local2>");
  inTable._localVocab->push_back("<local3>");

  vector<vector<Id>> inputData;
  // The input data types are
  //                   KB, KB, VERBATIM, TEXT, FLOAT,           STRING
  inputData.push_back({0, 3, 123, 0, floatBuffers[0], 0});
  inputData.push_back({0, 4, 0, 1, floatBuffers[1], 1});

  inputData.push_back({1, 5, 41223, 2, floatBuffers[2], 2});
  inputData.push_back({1, 6, 123, 0, floatBuffers[0], 0});
  inputData.push_back({1, 6, 123, 0, floatBuffers[0], 0});

  inputData.push_back({2, 7, 0, 1, floatBuffers[1], 1});
  inputData.push_back({2, 8, 41223, 2, floatBuffers[2], 2});

  std::vector<ResultTable::ResultType> inputTypes = {
      ResultTable::ResultType::KB,       ResultTable::ResultType::KB,
      ResultTable::ResultType::VERBATIM, ResultTable::ResultType::TEXT,
      ResultTable::ResultType::FLOAT,    ResultTable::ResultType::LOCAL_VOCAB};

  /*
    COUNT,
    GROUP_CONCAT,
    FIRST,
    LAST,
    SAMPLE,
    MIN,
    MAX,
    SUM,
    AVG
   */

  std::vector<size_t> groupByCols = {0};
  std::string delim1(", ");
  std::vector<GroupBy::Aggregate> aggregates = {
      // type                                in out userdata
      {GroupBy::AggregateType::COUNT, 1, 1, nullptr},

      {GroupBy::AggregateType::GROUP_CONCAT, 1, 2, &delim1},
      {GroupBy::AggregateType::GROUP_CONCAT, 2, 3, &delim1},
      {GroupBy::AggregateType::GROUP_CONCAT, 3, 4, &delim1},
      {GroupBy::AggregateType::GROUP_CONCAT, 4, 5, &delim1},
      {GroupBy::AggregateType::GROUP_CONCAT, 5, 6, &delim1},

      {GroupBy::AggregateType::SAMPLE, 1, 7, nullptr},

      {GroupBy::AggregateType::MIN, 1, 8, nullptr},
      {GroupBy::AggregateType::MIN, 2, 9, nullptr},
      {GroupBy::AggregateType::MIN, 3, 10, nullptr},
      {GroupBy::AggregateType::MIN, 4, 11, nullptr},

      {GroupBy::AggregateType::MAX, 1, 12, nullptr},
      {GroupBy::AggregateType::MAX, 2, 13, nullptr},
      {GroupBy::AggregateType::MAX, 3, 14, nullptr},
      {GroupBy::AggregateType::MAX, 4, 15, nullptr},

      {GroupBy::AggregateType::SUM, 1, 16, nullptr},
      {GroupBy::AggregateType::SUM, 2, 17, nullptr},
      {GroupBy::AggregateType::SUM, 3, 18, nullptr},
      {GroupBy::AggregateType::SUM, 4, 19, nullptr},

      {GroupBy::AggregateType::AVG, 1, 20, nullptr},
      {GroupBy::AggregateType::AVG, 2, 21, nullptr},
      {GroupBy::AggregateType::AVG, 3, 22, nullptr},
      {GroupBy::AggregateType::AVG, 4, 23, nullptr}};

  ResultTable outTable;

  doGroupBy<vector<Id>, vector<Id>>(&inputData, inputTypes, groupByCols,
                                    aggregates, &outTable._varSizeData,
                                    &inTable, &outTable, this->_index);

  ASSERT_EQ(3u, outTable._varSizeData.size());

  ASSERT_EQ(24u, outTable._varSizeData[0].size());
  ASSERT_EQ(24u, outTable._varSizeData[1].size());
  ASSERT_EQ(24u, outTable._varSizeData[2].size());

  // COUNT CHECKS
  ASSERT_EQ(2u, outTable._varSizeData[0][1]);
  ASSERT_EQ(3u, outTable._varSizeData[1][1]);
  ASSERT_EQ(2u, outTable._varSizeData[2][1]);

  // GROUP CONCAT CHECKS
  // check that the local vocab ids are ascending
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(0u + i, outTable._varSizeData[0][2 + i]);
    ASSERT_EQ(0u + i + 5, outTable._varSizeData[1][2 + i]);
    ASSERT_EQ(0u + i + 10, outTable._varSizeData[2][2 + i]);
  }
  // check for a local vocab entry for each of the 5 input cols
  ASSERT_EQ(std::string("<entity1>, <entity2>"), (*outTable._localVocab)[0]);
  ASSERT_EQ(std::string("123, 0"), (*outTable._localVocab)[1]);
  ASSERT_EQ(std::string("Exert 1, Exert 2"), (*outTable._localVocab)[2]);
  std::ostringstream groupConcatFloatString;
  groupConcatFloatString << floatValues[0] << ", " << floatValues[1];
  ASSERT_EQ(groupConcatFloatString.str(), (*outTable._localVocab)[3]);
  ASSERT_EQ(std::string("<local1>, <local2>"), (*outTable._localVocab)[4]);

  // SAMPLE CHECKS
  ASSERT_EQ(4u, outTable._varSizeData[0][7]);
  ASSERT_EQ(6u, outTable._varSizeData[1][7]);
  ASSERT_EQ(8u, outTable._varSizeData[2][7]);

  // MIN CHECKS
  float buffer;
  ASSERT_EQ(3u, outTable._varSizeData[0][8]);
  ASSERT_EQ(5u, outTable._varSizeData[1][8]);
  ASSERT_EQ(7u, outTable._varSizeData[2][8]);

  ASSERT_EQ(0u, outTable._varSizeData[0][9]);
  ASSERT_EQ(123u, outTable._varSizeData[1][9]);
  ASSERT_EQ(0u, outTable._varSizeData[2][9]);

  ASSERT_EQ(ID_NO_VALUE, outTable._varSizeData[0][10]);
  ASSERT_EQ(ID_NO_VALUE, outTable._varSizeData[1][10]);
  ASSERT_EQ(ID_NO_VALUE, outTable._varSizeData[2][10]);

  std::memcpy(&buffer, &outTable._varSizeData[0][11], sizeof(float));
  ASSERT_FLOAT_EQ(-3, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[1][11], sizeof(float));
  ASSERT_FLOAT_EQ(-3, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[2][11], sizeof(float));
  ASSERT_FLOAT_EQ(2, buffer);

  // MAX CHECKS
  ASSERT_EQ(4u, outTable._varSizeData[0][12]);
  ASSERT_EQ(6u, outTable._varSizeData[1][12]);
  ASSERT_EQ(8u, outTable._varSizeData[2][12]);

  ASSERT_EQ(123u, outTable._varSizeData[0][13]);
  ASSERT_EQ(41223u, outTable._varSizeData[1][13]);
  ASSERT_EQ(41223u, outTable._varSizeData[2][13]);

  ASSERT_EQ(ID_NO_VALUE, outTable._varSizeData[0][14]);
  ASSERT_EQ(ID_NO_VALUE, outTable._varSizeData[1][14]);
  ASSERT_EQ(ID_NO_VALUE, outTable._varSizeData[2][14]);

  std::memcpy(&buffer, &outTable._varSizeData[0][15], sizeof(float));
  ASSERT_FLOAT_EQ(2, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[1][15], sizeof(float));
  ASSERT_FLOAT_EQ(1231, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[2][15], sizeof(float));
  ASSERT_FLOAT_EQ(1231, buffer);

  // SUM CHECKS
  std::memcpy(&buffer, &outTable._varSizeData[0][16], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._varSizeData[1][16], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._varSizeData[2][16], sizeof(float));
  ASSERT_FLOAT_EQ(12, buffer);

  std::memcpy(&buffer, &outTable._varSizeData[0][17], sizeof(float));
  ASSERT_FLOAT_EQ(123, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[1][17], sizeof(float));
  ASSERT_FLOAT_EQ(41469, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[2][17], sizeof(float));
  ASSERT_FLOAT_EQ(41223, buffer);

  std::memcpy(&buffer, &outTable._varSizeData[0][18], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._varSizeData[1][18], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._varSizeData[2][18], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));

  std::memcpy(&buffer, &outTable._varSizeData[0][19], sizeof(float));
  ASSERT_FLOAT_EQ(-1, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[1][19], sizeof(float));
  ASSERT_FLOAT_EQ(1225, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[2][19], sizeof(float));
  ASSERT_FLOAT_EQ(1233, buffer);

  // AVG CHECKS
  std::memcpy(&buffer, &outTable._varSizeData[0][20], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._varSizeData[1][20], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._varSizeData[2][20], sizeof(float));
  ASSERT_FLOAT_EQ(6, buffer);

  std::memcpy(&buffer, &outTable._varSizeData[0][21], sizeof(float));
  ASSERT_FLOAT_EQ(61.5, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[1][21], sizeof(float));
  ASSERT_FLOAT_EQ(13823, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[2][21], sizeof(float));
  ASSERT_FLOAT_EQ(20611.5, buffer);

  std::memcpy(&buffer, &outTable._varSizeData[0][22], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._varSizeData[1][22], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));
  std::memcpy(&buffer, &outTable._varSizeData[2][22], sizeof(float));
  ASSERT_TRUE(std::isnan(buffer));

  std::memcpy(&buffer, &outTable._varSizeData[0][23], sizeof(float));
  ASSERT_FLOAT_EQ(-0.5, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[1][23], sizeof(float));
  ASSERT_FLOAT_EQ(408.3333333333333, buffer);
  std::memcpy(&buffer, &outTable._varSizeData[2][23], sizeof(float));
  ASSERT_FLOAT_EQ(616.5, buffer);
}
