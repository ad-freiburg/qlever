// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "../src/global/Pattern.h"
#include "../src/index/Index.h"
#include "./IndexTestHelpers.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](auto id) { return Id::make(id); };

string getStxxlConfigFileName(const string& location) {
  std::ostringstream os;
  os << location << ".stxxl";
  return std::move(os).str();
}

string getStxxlDiskFileName(const string& location, const string& tail) {
  std::ostringstream os;
  os << location << tail << "-stxxl.disk";
  return std::move(os).str();
}

// Write a .stxxl config-file.
// All we want is sufficient space somewhere with enough space.
// We can use the location of input files and use a constant size for now.
// The required size can only be estimated anyway, since index size
// depends on the structure of words files rather than their size only,
// because of the "multiplications" performed.
void writeStxxlConfigFile(const string& location, const string& tail) {
  string stxxlConfigFileName = getStxxlConfigFileName(location);
  ad_utility::File stxxlConfig(stxxlConfigFileName, "w");
  // Inform stxxl about .stxxl location
  setenv("STXXLCFG", stxxlConfigFileName.c_str(), true);
  std::ostringstream config;
  config << "disk=" << getStxxlDiskFileName(location, tail) << ","
         << STXXL_DISK_SIZE_INDEX_TEST << ",syscall";
  stxxlConfig.writeLine(std::move(config).str());
}

TEST(IndexTest, createFromTurtleTest) {
  string location = "./";
  string tail = "";
  writeStxxlConfigFile(location, tail);
  string stxxlFileName = getStxxlDiskFileName(location, tail);

  std::string filename = "_testtmp2.ttl";

  {
    std::fstream f(filename, std::ios_base::out);

    // Vocab:
    // 0: a
    // 1: a2
    // 2: b
    // 3: b2
    // 4: c
    // 5: c2
    f << "<a>\t<b>\t<c>\t.\n"
         "<a>\t<b>\t<c2>\t.\n"
         "<a>\t<b2>\t<c>\t.\n"
         "<a2>\t<b2>\t<c2>\t.";
    f.close();

    {
      Index index = makeIndexWithTestSettings();
      index.setOnDiskBase("_testindex");
      index.createFromFile<TurtleParserAuto>(filename);
    }
    Index index;
    index.createFromOnDiskIndex("_testindex");

    ASSERT_TRUE(index._PSO.metaData().col0IdExists(I(2)));
    ASSERT_TRUE(index._PSO.metaData().col0IdExists(I(3)));
    ASSERT_FALSE(index._PSO.metaData().col0IdExists(I(1)));
    ASSERT_FALSE(index._PSO.metaData().col0IdExists(I(0)));
    ASSERT_FALSE(index._PSO.metaData().getMetaData(I(2)).isFunctional());
    ASSERT_TRUE(index._PSO.metaData().getMetaData(I(3)).isFunctional());

    ASSERT_TRUE(index.POS().metaData().col0IdExists(I(2)));
    ASSERT_TRUE(index.POS().metaData().col0IdExists(I(3)));
    ASSERT_FALSE(index.POS().metaData().col0IdExists(I(1)));
    ASSERT_FALSE(index.POS().metaData().col0IdExists(I(4)));
    ASSERT_TRUE(index.POS().metaData().getMetaData(I(2)).isFunctional());
    ASSERT_TRUE(index.POS().metaData().getMetaData(I(3)).isFunctional());

    // Relation b
    // Pair index
    std::vector<std::array<Id, 2>> buffer;
    CompressedRelationMetaData::scan(I(2), &buffer, index.PSO());
    ASSERT_EQ(2ul, buffer.size());
    ASSERT_EQ(I(0), buffer[0][0]);
    ASSERT_EQ(I(4), buffer[0][1]);
    ASSERT_EQ(I(0u), buffer[1][0]);
    ASSERT_EQ(I(5u), buffer[1][1]);

    // Relation b2
    CompressedRelationMetaData::scan(I(3), &buffer, index.PSO());
    ASSERT_EQ(2ul, buffer.size());
    ASSERT_EQ(I(0), buffer[0][0]);
    ASSERT_EQ(I(4u), buffer[0][1]);
    ASSERT_EQ(I(1u), buffer[1][0]);
    ASSERT_EQ(I(5u), buffer[1][1]);

    ad_utility::deleteFile(filename);
    std::remove(stxxlFileName.c_str());
    remove("_testindex.index.pso");
    remove("_testindex.index.pos");
  }
  {
    // a       is-a    1       .
    // a       is-a    2       .
    // a       is-a    0       .
    // b       is-a    3       .
    // b       is-a    0       .
    // c       is-a    1       .
    // c       is-a    2       .
    std::fstream f(filename, std::ios_base::out);

    // Vocab:
    // 0: 0
    // 1: 1
    // 2: 2
    // 3: 3
    // 4: a
    // 5: b
    // 6: c
    // 7: is-a
    // 8: ql:langtag
    f << "<a>\t<is-a>\t<1>\t.\n"
         "<a>\t<is-a>\t<2>\t.\n"
         "<a>\t<is-a>\t<0>\t.\n"
         "<b>\t<is-a>\t<3>\t.\n"
         "<b>\t<is-a>\t<0>\t.\n"
         "<c>\t<is-a>\t<1>\t.\n"
         "<c>\t<is-a>\t<2>\t.\n";
    f.close();

    {
      Index index = makeIndexWithTestSettings();
      index.setOnDiskBase("_testindex");
      index.createFromFile<TurtleParserAuto>(filename);
    }
    Index index;
    index.createFromOnDiskIndex("_testindex");

    ASSERT_TRUE(index._PSO.metaData().col0IdExists(I(7)));
    ASSERT_FALSE(index._PSO.metaData().col0IdExists(I(1)));

    ASSERT_FALSE(index._PSO.metaData().getMetaData(I(7)).isFunctional());

    ASSERT_TRUE(index.POS().metaData().col0IdExists(I(7)));
    ASSERT_FALSE(index.POS().metaData().getMetaData(I(7)).isFunctional());

    std::vector<std::array<Id, 2>> buffer;
    // is-a
    CompressedRelationMetaData::scan(I(7), &buffer, index.PSO());
    ASSERT_EQ(7ul, buffer.size());
    // Pair index
    ASSERT_EQ(I(4u), buffer[0][0]);
    ASSERT_EQ(I(0u), buffer[0][1]);
    ASSERT_EQ(I(4u), buffer[1][0]);
    ASSERT_EQ(I(1u), buffer[1][1]);
    ASSERT_EQ(I(4u), buffer[2][0]);
    ASSERT_EQ(I(2u), buffer[2][1]);
    ASSERT_EQ(I(5u), buffer[3][0]);
    ASSERT_EQ(I(0u), buffer[3][1]);
    ASSERT_EQ(I(5u), buffer[4][0]);
    ASSERT_EQ(I(3u), buffer[4][1]);
    ASSERT_EQ(I(6u), buffer[5][0]);
    ASSERT_EQ(I(1u), buffer[5][1]);
    ASSERT_EQ(I(6u), buffer[6][0]);
    ASSERT_EQ(I(2u), buffer[6][1]);

    // is-a for POS
    CompressedRelationMetaData::scan(I(7), &buffer, index.POS());
    ASSERT_EQ(7ul, buffer.size());
    // Pair index
    ASSERT_EQ(I(0u), buffer[0][0]);
    ASSERT_EQ(I(4u), buffer[0][1]);
    ASSERT_EQ(I(0u), buffer[1][0]);
    ASSERT_EQ(I(5u), buffer[1][1]);
    ASSERT_EQ(I(1u), buffer[2][0]);
    ASSERT_EQ(I(4u), buffer[2][1]);
    ASSERT_EQ(I(1u), buffer[3][0]);
    ASSERT_EQ(I(6u), buffer[3][1]);
    ASSERT_EQ(I(2u), buffer[4][0]);
    ASSERT_EQ(I(4u), buffer[4][1]);
    ASSERT_EQ(I(2u), buffer[5][0]);
    ASSERT_EQ(I(6u), buffer[5][1]);
    ASSERT_EQ(I(3u), buffer[6][0]);
    ASSERT_EQ(I(5u), buffer[6][1]);

    ad_utility::deleteFile(filename);
    std::remove(stxxlFileName.c_str());
    remove("_testindex.index.pso");
    remove("_testindex.index.pos");
  }
}

// used to ensure all files will be deleted even if the test fails
class CreatePatternsFixture : public testing::Test {
 public:
  CreatePatternsFixture() {
    string location = "./";
    string tail = "";
    stxxlFileName = getStxxlDiskFileName(location, tail);
    writeStxxlConfigFile(location, tail);
  }

  virtual ~CreatePatternsFixture() {
    ad_utility::deleteFile(inputFilename);
    ad_utility::deleteFile(stxxlFileName);
    ad_utility::deleteFile("_testindex.index.pso");
    ad_utility::deleteFile("_testindex.index.pos");
    ad_utility::deleteFile("_testindex.index.patterns");
    ad_utility::deleteFile("stxxl.log");
    ad_utility::deleteFile("stxxl.errorlog");
  }

  string stxxlFileName;
  string inputFilename = "_testtmppatterns.ttl";
};

TEST_F(CreatePatternsFixture, createPatterns) {
  {
    LOG(DEBUG) << "Testing createPatterns with ttl file..." << std::endl;
    std::ofstream f(inputFilename);

    f << "<a>\t<b>\t<c>\t.\n"
         "<a>\t<b>\t<c2>\t.\n"
         "<a>\t<b2>\t<c>\t.\n"
         "<a2>\t<b2>\t<c2>\t.\n"
         "<a2>\t<d>\t<c2>\t.";
    f.close();

    {
      Index index = makeIndexWithTestSettings();
      index.setUsePatterns(true);
      index.setOnDiskBase("_testindex");
      index.createFromFile<TurtleParserAuto>(inputFilename);
    }
    Index index;
    index.setUsePatterns(true);
    index.createFromOnDiskIndex("_testindex");

    ASSERT_EQ(2u, index.getHasPattern().size());
    ASSERT_EQ(0u, index.getHasPredicate().size());
    ASSERT_EQ(2u, index._patterns.size());
    std::vector<VocabIndex> p0;
    std::vector<VocabIndex> p1;
    VocabIndex idx;
    // Pattern p0 (for subject <a>) consists of <b> and <b2)
    ASSERT_TRUE(index.getVocab().getId("<b>", &idx));
    p0.push_back(idx);
    ASSERT_TRUE(index.getVocab().getId("<b2>", &idx));
    p0.push_back(idx);

    // Pattern p1 (for subject <as>) consists of <b2> and <d>)
    p1.push_back(idx);
    ASSERT_TRUE(index.getVocab().getId("<d>", &idx));
    p1.push_back(idx);

    auto checkPattern = [](const auto& expected, const auto& actual) {
      for (size_t i = 0; i < actual.size(); i++) {
        ASSERT_EQ(Id::make(expected[i].get()), actual[i]);
      }
    };

    ASSERT_TRUE(index.getVocab().getId("<a>", &idx));
    LOG(INFO) << idx << std::endl;
    for (size_t i = 0; i < index.getHasPattern().size(); ++i) {
      LOG(INFO) << index.getHasPattern()[i] << std::endl;
    }
    checkPattern(p0, index.getPatterns()[index.getHasPattern()[idx.get()]]);
    ASSERT_TRUE(index.getVocab().getId("<a2>", &idx));
    checkPattern(p1, index.getPatterns()[index.getHasPattern()[idx.get()]]);
  }
}

TEST(IndexTest, createFromOnDiskIndexTest) {
  string location = "./";
  string tail = "";
  writeStxxlConfigFile(location, tail);
  string stxxlFileName = getStxxlDiskFileName(location, tail);
  string filename = "_testtmp3.ttl";

  std::ofstream f(filename);

  // Vocab:
  // 0: a
  // 1: a2
  // 2: b
  // 3: b2
  // 4: c
  // 5: c2
  f << "<a>\t<b>\t<c>\t.\n"
       "<a>\t<b>\t<c2>\t.\n"
       "<a>\t<b2>\t<c>\t.\n"
       "<a2>\t<b2>\t<c2>\t.";
  f.close();

  {
    Index indexPrim = makeIndexWithTestSettings();
    indexPrim.setOnDiskBase("_testindex2");
    indexPrim.createFromFile<TurtleParserAuto>(filename);
  }

  Index index;
  index.createFromOnDiskIndex("_testindex2");

  ASSERT_TRUE(index.PSO().metaData().col0IdExists(I(2)));
  ASSERT_TRUE(index.PSO().metaData().col0IdExists(I(3)));
  ASSERT_FALSE(index.PSO().metaData().col0IdExists(I(1)));
  ASSERT_FALSE(index.PSO().metaData().col0IdExists(I(4)));
  ASSERT_FALSE(index.PSO().metaData().getMetaData(I(2)).isFunctional());
  ASSERT_TRUE(index.PSO().metaData().getMetaData(I(3)).isFunctional());

  ASSERT_TRUE(index.POS().metaData().col0IdExists(I(2)));
  ASSERT_TRUE(index.POS().metaData().col0IdExists(I(3)));
  ASSERT_FALSE(index.POS().metaData().col0IdExists(I(1)));
  ASSERT_FALSE(index.POS().metaData().col0IdExists(I(4)));
  ASSERT_TRUE(index.POS().metaData().getMetaData(I(2)).isFunctional());
  ASSERT_TRUE(index.POS().metaData().getMetaData(I(3)).isFunctional());

  ad_utility::deleteFile(filename);
  ad_utility::deleteFile("_testindex2.index.pso");
  ad_utility::deleteFile("_testindex2.index.pos");
  ad_utility::deleteFile(stxxlFileName);
};

TEST(IndexTest, scanTest) {
  string location = "./";
  string tail = "";
  writeStxxlConfigFile(location, tail);
  string stxxlFileName = getStxxlDiskFileName(location, tail);
  string filename = "_testtmp2.ttl";
  std::ofstream f(filename);

  // Vocab:
  // 0: a
  // 1: a2
  // 2: b
  // 3: b2
  // 4: c
  // 5: c2
  f << "<a>\t<b>\t<c>\t.\n"
       "<a>\t<b>\t<c2>\t.\n"
       "<a>\t<b2>\t<c>\t.\n"
       "<a2>\t<b2>\t<c2>\t.";
  f.close();
  {
    {
      Index index = makeIndexWithTestSettings();
      index.setOnDiskBase("_testindex");
      index.createFromFile<TurtleParserAuto>(filename);
    }

    Index index;
    index.createFromOnDiskIndex("_testindex");

    IdTable wol(1, allocator());
    IdTable wtl(2, allocator());

    index.scan("<b>", &wtl, index._PSO);
    ASSERT_EQ(2u, wtl.size());
    ASSERT_EQ(I(0u), wtl[0][0]);
    ASSERT_EQ(I(4u), wtl[0][1]);
    ASSERT_EQ(I(0u), wtl[1][0]);
    ASSERT_EQ(I(5u), wtl[1][1]);
    wtl.clear();
    index.scan("<x>", &wtl, index._PSO);
    ASSERT_EQ(0u, wtl.size());

    index.scan("<c>", &wtl, index._PSO);
    ASSERT_EQ(0u, wtl.size());

    index.scan("<b>", &wtl, index._POS);
    ASSERT_EQ(2u, wtl.size());
    ASSERT_EQ(I(4u), wtl[0][0]);
    ASSERT_EQ(I(0u), wtl[0][1]);
    ASSERT_EQ(I(5u), wtl[1][0]);
    ASSERT_EQ(I(0u), wtl[1][1]);
    wtl.clear();
    index.scan("<x>", &wtl, index._POS);
    ASSERT_EQ(0u, wtl.size());

    index.scan("<c>", &wtl, index._POS);
    ASSERT_EQ(0u, wtl.size());

    index.scan("<b>", "<a>", &wol, index._PSO);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(I(4u), wol[0][0]);
    ASSERT_EQ(I(5u), wol[1][0]);
    wol.clear();
    index.scan("<b>", "<c>", &wol, index._PSO);
    ASSERT_EQ(0u, wol.size());

    index.scan("<b2>", "<c2>", &wol, index._POS);
    ASSERT_EQ(1u, wol.size());
    ASSERT_EQ(I(1u), wol[0][0]);
  }

  ad_utility::deleteFile(filename);
  ad_utility::deleteFile(stxxlFileName);
  ad_utility::deleteFile("_testindex.index.pso");
  ad_utility::deleteFile("_testindex.index.pos");

  // a       is-a    1       .
  // a       is-a    2       .
  // a       is-a    0       .
  // b       is-a    3       .
  // b       is-a    0       .
  // c       is-a    1       .
  // c       is-a    2       .
  std::ofstream f2(filename);

  // Vocab:
  // 0: 0
  // 1: 1
  // 2: 2
  // 3: 3
  // 4: a
  // 5: b
  // 6: c
  // 7: is-a
  // 8: ql:langtag
  f2 << "<a>\t<is-a>\t<1>\t.\n"
        "<a>\t<is-a>\t<2>\t.\n"
        "<a>\t<is-a>\t<0>\t.\n"
        "<b>\t<is-a>\t<3>\t.\n"
        "<b>\t<is-a>\t<0>\t.\n"
        "<c>\t<is-a>\t<1>\t.\n"
        "<c>\t<is-a>\t<2>\t.\n";
  f2.close();

  {
    {
      Index index = makeIndexWithTestSettings();
      index.setOnDiskBase("_testindex");
      index.createFromFile<TurtleParserAuto>(filename);
    }
    Index index;
    index.createFromOnDiskIndex("_testindex");

    IdTable wol(1, allocator());
    IdTable wtl(2, allocator());

    index.scan("<is-a>", &wtl, index._PSO);
    ASSERT_EQ(7u, wtl.size());
    ASSERT_EQ(I(4u), wtl[0][0]);
    ASSERT_EQ(I(0u), wtl[0][1]);
    ASSERT_EQ(I(4u), wtl[1][0]);
    ASSERT_EQ(I(1u), wtl[1][1]);
    ASSERT_EQ(I(4u), wtl[2][0]);
    ASSERT_EQ(I(2u), wtl[2][1]);
    ASSERT_EQ(I(5u), wtl[3][0]);
    ASSERT_EQ(I(0u), wtl[3][1]);
    ASSERT_EQ(I(5u), wtl[4][0]);
    ASSERT_EQ(I(3u), wtl[4][1]);
    ASSERT_EQ(I(6u), wtl[5][0]);
    ASSERT_EQ(I(1u), wtl[5][1]);
    ASSERT_EQ(I(6u), wtl[6][0]);
    ASSERT_EQ(I(2u), wtl[6][1]);

    index.scan("<is-a>", &wtl, index._POS);
    ASSERT_EQ(7u, wtl.size());
    ASSERT_EQ(I(0u), wtl[0][0]);
    ASSERT_EQ(I(4u), wtl[0][1]);
    ASSERT_EQ(I(0u), wtl[1][0]);
    ASSERT_EQ(I(5u), wtl[1][1]);
    ASSERT_EQ(I(1u), wtl[2][0]);
    ASSERT_EQ(I(4u), wtl[2][1]);
    ASSERT_EQ(I(1u), wtl[3][0]);
    ASSERT_EQ(I(6u), wtl[3][1]);
    ASSERT_EQ(I(2u), wtl[4][0]);
    ASSERT_EQ(I(4u), wtl[4][1]);
    ASSERT_EQ(I(2u), wtl[5][0]);
    ASSERT_EQ(I(6u), wtl[5][1]);
    ASSERT_EQ(I(3u), wtl[6][0]);
    ASSERT_EQ(I(5u), wtl[6][1]);

    index.scan("<is-a>", "<0>", &wol, index._POS);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(I(4u), wol[0][0]);
    ASSERT_EQ(I(5u), wol[1][0]);

    wol.clear();
    index.scan("<is-a>", "<1>", &wol, index._POS);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(I(4u), wol[0][0]);
    ASSERT_EQ(I(6u), wol[1][0]);

    wol.clear();
    index.scan("<is-a>", "<2>", &wol, index._POS);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(I(4u), wol[0][0]);
    ASSERT_EQ(I(6u), wol[1][0]);

    wol.clear();
    index.scan("<is-a>", "<3>", &wol, index._POS);
    ASSERT_EQ(1u, wol.size());
    ASSERT_EQ(I(5u), wol[0][0]);

    wol.clear();
    index.scan("<is-a>", "<a>", &wol, index._PSO);
    ASSERT_EQ(3u, wol.size());
    ASSERT_EQ(I(0u), wol[0][0]);
    ASSERT_EQ(I(1u), wol[1][0]);
    ASSERT_EQ(I(2u), wol[2][0]);

    wol.clear();
    index.scan("<is-a>", "<b>", &wol, index._PSO);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(I(0u), wol[0][0]);
    ASSERT_EQ(I(3u), wol[1][0]);

    wol.clear();
    index.scan("<is-a>", "<c>", &wol, index._PSO);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(I(1u), wol[0][0]);
    ASSERT_EQ(I(2u), wol[1][0]);
  }
  ad_utility::deleteFile(filename);
  ad_utility::deleteFile(stxxlFileName);
  ad_utility::deleteFile("_testindex.index.pso");
  ad_utility::deleteFile("_testindex.index.pos");
};
