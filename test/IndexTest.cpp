// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "./IndexTestHelpers.h"
#include "global/Pattern.h"
#include "index/Index.h"
#include "index/IndexImpl.h"

using namespace ad_utility::testing;

auto I = [](auto id) { return Id::makeFromVocabIndex(VocabIndex::make(id)); };

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
  FILE_BUFFER_SIZE() = 1000;  // Increase performance in debug mode.
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
    IndexImpl index;
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

    {
      // Test for a previous bug in the scan of two fixed elements: An assertion
      // wrongly failed if the first Id existed in the permutation, but no
      // compressed block was found via binary search that could possibly
      // contain the combination of the ids. In this example <b2> is the largest
      // predicate that occurs and <c2> is larger than the largest subject that
      // appears with <b2>.
      IdTable oneColBuffer{makeAllocator()};
      oneColBuffer.setNumColumns(1);
      index.scan("<b2>", "<c2>", &oneColBuffer, index.PSO());
      ASSERT_EQ(oneColBuffer.size(), 0u);
    }

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
    IndexImpl index;
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
  FILE_BUFFER_SIZE() = 1000;  // Increase performance in debug mode.
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
    IndexImpl index;
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
        ASSERT_EQ(Id::makeFromVocabIndex(expected[i]), actual[i]);
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
  FILE_BUFFER_SIZE() = 1000;  // Increase performance in debug mode.
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

  IndexImpl index;
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
  FILE_BUFFER_SIZE() = 1000;  // Increase performance in debug mode.
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

    IndexImpl index;
    index.createFromOnDiskIndex("_testindex");

    IdTable wol(1, makeAllocator());
    IdTable wtl(2, makeAllocator());

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
    IndexImpl index;
    index.createFromOnDiskIndex("_testindex");

    IdTable wol(1, makeAllocator());
    IdTable wtl(2, makeAllocator());

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

// Returns true iff `arg` (the first argument of `EXPECT_THAT` below) holds a
// `PossiblyExternalizedIriOrLiteral` that matches the string `content` and the
// bool `isExternal`.
MATCHER_P2(IsPossiblyExternalString, content, isExternal, "") {
  if (!std::holds_alternative<PossiblyExternalizedIriOrLiteral>(arg)) {
    return false;
  }
  const auto& el = std::get<PossiblyExternalizedIriOrLiteral>(arg);
  return (el._iriOrLiteral == content) && (isExternal == el._isExternal);
}

TEST(IndexTest, TripleToInternalRepresentation) {
  {
    IndexImpl index;
    TurtleTriple turtleTriple{"<subject>", "<predicate>", "\"literal\""};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_TRUE(res._langtag.empty());
    EXPECT_THAT(res._triple[0], IsPossiblyExternalString("<subject>", false));
    EXPECT_THAT(res._triple[1], IsPossiblyExternalString("<predicate>", false));
    EXPECT_THAT(res._triple[2], IsPossiblyExternalString("\"literal\"", false));
  }
  {
    IndexImpl index;
    index.getNonConstVocabForTesting().initializeExternalizePrefixes(
        std::vector{"<subj"s});
    TurtleTriple turtleTriple{"<subject>", "<predicate>", "\"literal\"@fr"};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_EQ(res._langtag, "fr");
    EXPECT_THAT(res._triple[0], IsPossiblyExternalString("<subject>", true));
    EXPECT_THAT(res._triple[1], IsPossiblyExternalString("<predicate>", false));
    // By default all languages other than English are externalized.
    EXPECT_THAT(res._triple[2],
                IsPossiblyExternalString("\"literal\"@fr", true));
  }
  {
    IndexImpl index;
    TurtleTriple turtleTriple{"<subject>", "<predicate>", 42.0};
    LangtagAndTriple res =
        index.tripleToInternalRepresentation(std::move(turtleTriple));
    ASSERT_EQ(Id::makeFromDouble(42.0), std::get<Id>(res._triple[2]));
  }
}

TEST(IndexTest, getIgnoredIdRanges) {
  const IndexImpl& index = getQec()->getIndex().getImpl();

  // First manually get the IDs of the vocabulary elements that might appear
  // in added triples.
  auto getId = [&index](const std::string& s) {
    Id id;
    bool success = index.getId(s, &id);
    AD_CHECK(success);
    return id;
  };

  Id qlLangtag = getId("<QLever-internal-function/langtag>");
  Id en = getId("<QLever-internal-function/@en>");
  Id enLabel = getId("@en@<label>");
  Id label = getId("<label>");
  Id firstLiteral = getId("\"A\"");
  Id lastLiteral = getId("\"zz\"@en");
  Id x = getId("<x>");

  auto increment = [](Id id) {
    return Id::makeFromVocabIndex(id.getVocabIndex().incremented());
  };

  // The range of all entities that start with "<QLever-internal-function/"
  auto internalEntities = std::pair{en, increment(qlLangtag)};
  // The range of all entities that start with @ (like `@en@<label>`)
  auto predicatesWithLangtag = std::pair{enLabel, increment(enLabel)};
  // The range of all literals;
  auto literals = std::pair{firstLiteral, increment(lastLiteral)};

  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::POS);
    ASSERT_FALSE(lambda(std::array{label, firstLiteral, x}));

    // Note: The following triple is added, but it should be filtered out via
    // the ranges and not via the lambda, because it can be retrieved using the
    // `ranges`.
    ASSERT_FALSE(lambda(std::array{enLabel, firstLiteral, x}));
    ASSERT_FALSE(lambda(std::array{x, x, x}));
    ASSERT_EQ(2u, ranges.size());

    ASSERT_EQ(ranges[0], internalEntities);
    ASSERT_EQ(ranges[1], predicatesWithLangtag);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::PSO);
    ASSERT_FALSE(lambda(std::array{label, x, firstLiteral}));

    // Note: The following triple is added, but it should be filtered out via
    // the ranges and not via the lambda, because it can be retrieved using the
    // `ranges`.
    ASSERT_FALSE(lambda(std::array{enLabel, x, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, x, x}));
    ASSERT_EQ(2u, ranges.size());

    ASSERT_EQ(ranges[0], internalEntities);
    ASSERT_EQ(ranges[1], predicatesWithLangtag);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::SOP);
    ASSERT_TRUE(lambda(std::array{x, firstLiteral, enLabel}));
    ASSERT_FALSE(lambda(std::array{x, firstLiteral, label}));
    ASSERT_FALSE(lambda(std::array{x, x, label}));
    ASSERT_EQ(2u, ranges.size());

    ASSERT_EQ(ranges[0], internalEntities);
    ASSERT_EQ(ranges[1], literals);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::SPO);
    ASSERT_TRUE(lambda(std::array{x, enLabel, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, label, firstLiteral}));
    ASSERT_FALSE(lambda(std::array{x, label, x}));
    ASSERT_EQ(2u, ranges.size());

    ASSERT_EQ(ranges[0], internalEntities);
    ASSERT_EQ(ranges[1], literals);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::OSP);
    ASSERT_TRUE(lambda(std::array{firstLiteral, x, enLabel}));
    ASSERT_FALSE(lambda(std::array{firstLiteral, x, label}));
    ASSERT_FALSE(lambda(std::array{x, x, label}));
    ASSERT_EQ(1u, ranges.size());
    ASSERT_EQ(ranges[0], internalEntities);
  }
  {
    auto [ranges, lambda] = index.getIgnoredIdRanges(Index::Permutation::OPS);
    ASSERT_TRUE(lambda(std::array{firstLiteral, enLabel, x}));
    ASSERT_FALSE(lambda(std::array{firstLiteral, label, x}));
    ASSERT_FALSE(lambda(std::array{x, label, x}));
    ASSERT_EQ(1u, ranges.size());
    ASSERT_EQ(ranges[0], internalEntities);
  }
}

TEST(IndexTest, NumDistinctEntities) {
  const IndexImpl& index = getQec()->getIndex().getImpl();
  // Note: Those numbers might change as the triples of the test index in
  // `IndexTestHelpers.cpp` change.
  // TODO<joka921> Also check the number of triples and the number of
  // added things.
  auto subjects = index.numDistinctSubjects();
  EXPECT_EQ(subjects.normal_, 3);
  // All literals with language tags are added subjects.
  EXPECT_EQ(subjects.internal_, 1);

  auto predicates = index.numDistinctPredicates();
  EXPECT_EQ(predicates.normal_, 2);
  // One added predicate is `ql:langtag` and one added predicate for
  // each combination of predicate+language that is actually used (e.g.
  // `@en@label`).
  EXPECT_EQ(predicates.internal_, 2);

  auto objects = index.numDistinctObjects();
  EXPECT_EQ(objects.normal_, 7);
  // One added object for each language that is used
  EXPECT_EQ(objects.internal_, 1);

  auto numTriples = index.numTriples();
  EXPECT_EQ(numTriples.normal_, 7);
  // Two added triples for each triple that has an object with a language tag.
  EXPECT_EQ(numTriples.internal_, 2);
}
