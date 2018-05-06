// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>
#include <cstdio>
#include <fstream>
#include "../src/global/Pattern.h"
#include "../src/index/Index.h"

string getStxxlDiskFileName(const string& location, const string& tail) {
  std::ostringstream os;
  os << location << tail << "-stxxl.disk";
  return os.str();
}

// Write a .stxxl config-file.
// All we want is sufficient space somewhere with enough space.
// We can use the location of input files and use a constant size for now.
// The required size can only ben estimation anyway, since index size
// depends on the structure of words files rather than their size only,
// because of the "multiplications" performed.
void writeStxxlConfigFile(const string& location, const string& tail) {
  ad_utility::File stxxlConfig(".stxxl", "w");
  std::ostringstream config;
  config << "disk=" << getStxxlDiskFileName(location, tail) << ","
         << STXXL_DISK_SIZE_INDEX_TEST << ",syscall";
  stxxlConfig.writeLine(config.str());
}

TEST(IndexTest, createFromTsvTest) {
  string location = "./";
  string tail = "";
  writeStxxlConfigFile(location, tail);
  string stxxlFileName = getStxxlDiskFileName(location, tail);

  {
    std::fstream f("_testtmp2.tsv", std::ios_base::out);

    // Vocab:
    // 0: a
    // 1: a2
    // 2: b
    // 3: b2
    // 4: c
    // 5: c2
    f << "a\tb\tc\t.\n"
         "a\tb\tc2\t.\n"
         "a\tb2\tc\t.\n"
         "a2\tb2\tc2\t.";
    f.close();

    Index index;
    index.createFromTsvFile("_testtmp2.tsv", "_testindex", false);

    ASSERT_TRUE(index._psoMeta.relationExists(2));
    ASSERT_TRUE(index._psoMeta.relationExists(3));
    ASSERT_FALSE(index._psoMeta.relationExists(1));
    ASSERT_FALSE(index._psoMeta.relationExists(4));
    ASSERT_FALSE(index._psoMeta.getRmd(2).isFunctional());
    ASSERT_TRUE(index._psoMeta.getRmd(3).isFunctional());
    ASSERT_FALSE(index._psoMeta.getRmd(2).hasBlocks());

    ASSERT_TRUE(index._posMeta.relationExists(2));
    ASSERT_TRUE(index._posMeta.relationExists(3));
    ASSERT_FALSE(index._posMeta.relationExists(1));
    ASSERT_FALSE(index._posMeta.relationExists(4));
    ASSERT_TRUE(index._posMeta.getRmd(2).isFunctional());
    ASSERT_TRUE(index._posMeta.getRmd(3).isFunctional());

    ad_utility::File psoFile("_testindex.index.pso", "r");
    size_t nofbytes = static_cast<size_t>(index._psoMeta.getOffsetAfter());
    unsigned char* buf = new unsigned char[nofbytes];
    psoFile.read(buf, nofbytes);

    off_t bytesDone = 0;
    // Relation b
    // Pair index
    ASSERT_EQ(Id(0), *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    // No (more) block info because of the change from Aug2016 and
    // due to the reasons that the test relation is very small.
    //    // Lhs info
    //    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(static_cast<off_t>(bytesDone + sizeof(off_t)),
    //              *reinterpret_cast<off_t*>(buf + bytesDone));
    //    bytesDone += sizeof(off_t);
    //    // Rhs list
    //    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    // Relation b2
    ASSERT_EQ(Id(0), *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(1u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    // No LHS & RHS
    ASSERT_EQ(index._psoMeta.getOffsetAfter(), bytesDone);

    delete[] buf;
    psoFile.close();

    remove("_testtmp2.tsv");
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
    std::fstream f("_testtmp2.tsv", std::ios_base::out);

    // Vocab:
    // 0: 0
    // 1: 1
    // 2: 2
    // 3: 3
    // 4: a
    // 5: b
    // 6: c
    // 7: is-a
    f << "a\tis-a\t1\t.\n"
         "a\tis-a\t2\t.\n"
         "a\tis-a\t0\t.\n"
         "b\tis-a\t3\t.\n"
         "b\tis-a\t0\t.\n"
         "c\tis-a\t1\t.\n"
         "c\tis-a\t2\t.\n";
    f.close();

    Index index;
    index.createFromTsvFile("_testtmp2.tsv", "_testindex", false);

    ASSERT_TRUE(index._psoMeta.relationExists(7));
    ASSERT_FALSE(index._psoMeta.relationExists(1));

    ASSERT_FALSE(index._psoMeta.getRmd(7).isFunctional());
    ASSERT_FALSE(index._psoMeta.getRmd(7).hasBlocks());

    ASSERT_TRUE(index._posMeta.relationExists(7));
    ASSERT_FALSE(index._posMeta.getRmd(7).isFunctional());

    ad_utility::File psoFile("_testindex.index.pso", "r");
    size_t nofbytes = static_cast<size_t>(index._psoMeta.getOffsetAfter());
    unsigned char* buf = new unsigned char[nofbytes];
    psoFile.read(buf, nofbytes);

    off_t bytesDone = 0;

    // Pair index
    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(1u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(2u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(3u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(6u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(1u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(6u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(2u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    // Lhs info

    //    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(index._psoMeta.getRmd(7)._rmdBlocks->_startRhs,
    //              *reinterpret_cast<off_t*>(buf + bytesDone));
    //    bytesDone += sizeof(off_t);
    //    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(
    //        static_cast<off_t>(index._psoMeta.getRmd(7)._rmdBlocks->_startRhs
    //        +
    //                           3 * sizeof(Id)),
    //        *reinterpret_cast<off_t*>(buf + bytesDone));
    //    bytesDone += sizeof(off_t);
    //    ASSERT_EQ(6u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(
    //        static_cast<off_t>(index._psoMeta.getRmd(7)._rmdBlocks->_startRhs
    //        +
    //                           5 * sizeof(Id)),
    //        *reinterpret_cast<off_t*>(buf + bytesDone));
    //    bytesDone += sizeof(off_t);
    //
    //    // Rhs list
    //    ASSERT_EQ(bytesDone, index._psoMeta.getRmd(7)._rmdBlocks->_startRhs);
    //    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(1u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(2u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(3u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(1u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(2u, *reinterpret_cast<Id*>(buf + bytesDone));

    delete[] buf;
    psoFile.close();

    ad_utility::File posFile("_testindex.index.pos", "r");
    nofbytes = static_cast<size_t>(index._posMeta.getOffsetAfter());
    buf = new unsigned char[nofbytes];
    posFile.read(buf, nofbytes);

    bytesDone = 0;

    // Pair index
    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(1u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(1u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(6u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(2u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(2u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(6u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(3u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);
    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    bytesDone += sizeof(Id);

    // Lhs info
    //    ASSERT_EQ(0u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(index._posMeta.getRmd(7)._rmdBlocks->_startRhs,
    //              *reinterpret_cast<off_t*>(buf + bytesDone));
    //    bytesDone += sizeof(off_t);
    //    ASSERT_EQ(1u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(
    //        static_cast<off_t>(index._posMeta.getRmd(7)._rmdBlocks->_startRhs
    //        +
    //                           2 * sizeof(Id)),
    //        *reinterpret_cast<off_t*>(buf + bytesDone));
    //    bytesDone += sizeof(off_t);
    //    ASSERT_EQ(2u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(
    //        static_cast<off_t>(index._posMeta.getRmd(7)._rmdBlocks->_startRhs
    //        +
    //                           4 * sizeof(Id)),
    //        *reinterpret_cast<off_t*>(buf + bytesDone));
    //    bytesDone += sizeof(off_t);
    //    ASSERT_EQ(3u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(
    //        static_cast<off_t>(index._posMeta.getRmd(7)._rmdBlocks->_startRhs
    //        +
    //                           6 * sizeof(Id)),
    //        *reinterpret_cast<off_t*>(buf + bytesDone));
    //    bytesDone += sizeof(off_t);
    //
    //    // Rhs list
    //    ASSERT_EQ(bytesDone, index._posMeta.getRmd(7)._rmdBlocks->_startRhs);
    //    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(6u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(4u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(6u, *reinterpret_cast<Id*>(buf + bytesDone));
    //    bytesDone += sizeof(Id);
    //    ASSERT_EQ(5u, *reinterpret_cast<Id*>(buf + bytesDone));

    delete[] buf;
    psoFile.close();

    remove("_testtmp2.tsv");
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
    remove("_testtmppatterns.tsv");
    std::remove(stxxlFileName.c_str());
    remove("_testindex.index.pso");
    remove("_testindex.index.pos");
    remove("_testindex.index.patterns");
    remove("stxxl.log");
    remove("stxxl.errorlog");
  }

  string stxxlFileName;
};

TEST_F(CreatePatternsFixture, createPatterns) {
  {
    LOG(DEBUG) << "Testing createPatterns with tsv file..." << std::endl;
    std::fstream f("_testtmppatterns.tsv", std::ios_base::out);

    f << "a\tb\tc\t.\n"
         "a\tb\tc2\t.\n"
         "a\tb2\tc\t.\n"
         "a2\tb2\tc2\t.\n"
         "a2\td\tc2\t.";
    f.close();

    Index index;
    index.setUsePatterns(true);
    index._maxNumPatterns = 1;
    index.createFromTsvFile("_testtmppatterns.tsv", "_testindex", false);

    ASSERT_EQ(2u, index.getHasPattern().size());
    ASSERT_EQ(1u, index.getHasRelation().size());
    ASSERT_EQ(1u, index._patterns.size());
    Pattern p;
    p.push_back(3);
    p.push_back(6);
    std::pair<Id*, size_t> ip = index._patterns[0];
    for (size_t i = 0; i < ip.second; i++) {
      ASSERT_EQ(p[i], ip.first[i]);
    }
    ASSERT_EQ(0, index.getHasPattern()[1]);
    ASSERT_EQ(NO_PATTERN, index.getHasPattern()[0]);
  }
  {
    LOG(DEBUG) << "Testing createPatterns with existing index..." << std::endl;
    Index index;
    index.setUsePatterns(true);
    index._maxNumPatterns = 1;
    index.createFromOnDiskIndex("_testindex");

    ASSERT_EQ(2u, index.getHasPattern().size());
    ASSERT_EQ(1u, index.getHasRelation().size());
    ASSERT_EQ(1u, index._patterns.size());
    Pattern p;
    p.push_back(3);
    p.push_back(6);
    std::pair<Id*, size_t> ip = index._patterns[0];
    for (size_t i = 0; i < ip.second; i++) {
      ASSERT_EQ(p[i], ip.first[i]);
    }
    ASSERT_EQ(0, index.getHasPattern()[1]);
    ASSERT_EQ(NO_PATTERN, index.getHasPattern()[0]);
  }
}

TEST(IndexTest, createFromOnDiskIndexTest) {
  string location = "./";
  string tail = "";
  writeStxxlConfigFile(location, tail);
  string stxxlFileName = getStxxlDiskFileName(location, tail);

  std::fstream f("_testtmp3.tsv", std::ios_base::out);

  // Vocab:
  // 0: a
  // 1: a2
  // 2: b
  // 3: b2
  // 4: c
  // 5: c2
  f << "a\tb\tc\t.\n"
       "a\tb\tc2\t.\n"
       "a\tb2\tc\t.\n"
       "a2\tb2\tc2\t.";
  f.close();

  {
    Index indexPrim;
    indexPrim.createFromTsvFile("_testtmp3.tsv", "_testindex2", false);
  }

  Index index;
  index.createFromOnDiskIndex("_testindex2");

  ASSERT_TRUE(index._psoMeta.relationExists(2));
  ASSERT_TRUE(index._psoMeta.relationExists(3));
  ASSERT_FALSE(index._psoMeta.relationExists(1));
  ASSERT_FALSE(index._psoMeta.relationExists(4));
  ASSERT_FALSE(index._psoMeta.getRmd(2).isFunctional());
  ASSERT_TRUE(index._psoMeta.getRmd(3).isFunctional());
  ASSERT_FALSE(index._psoMeta.getRmd(2).hasBlocks());
  ASSERT_FALSE(index._psoMeta.getRmd(3).hasBlocks());

  ASSERT_TRUE(index._posMeta.relationExists(2));
  ASSERT_TRUE(index._posMeta.relationExists(3));
  ASSERT_FALSE(index._posMeta.relationExists(1));
  ASSERT_FALSE(index._posMeta.relationExists(4));
  ASSERT_TRUE(index._posMeta.getRmd(2).isFunctional());
  ASSERT_TRUE(index._posMeta.getRmd(3).isFunctional());

  remove("_testtmp3.tsv");
  remove("_testindex2.index.pso");
  remove("_testindex2.index.pos");
  std::remove(stxxlFileName.c_str());
};

TEST(IndexTest, scanTest) {
  string location = "./";
  string tail = "";
  writeStxxlConfigFile(location, tail);
  string stxxlFileName = getStxxlDiskFileName(location, tail);

  std::fstream f("_testtmp2.tsv", std::ios_base::out);

  // Vocab:
  // 0: a
  // 1: a2
  // 2: b
  // 3: b2
  // 4: c
  // 5: c2
  f << "a\tb\tc\t.\n"
       "a\tb\tc2\t.\n"
       "a\tb2\tc\t.\n"
       "a2\tb2\tc2\t.";
  f.close();
  {
    Index index;
    index.createFromTsvFile("_testtmp2.tsv", "_testindex", false);

    Index::WidthOneList wol;
    Index::WidthTwoList wtl;

    index.scanPSO("b", &wtl);
    ASSERT_EQ(2u, wtl.size());
    ASSERT_EQ(0u, wtl[0][0]);
    ASSERT_EQ(4u, wtl[0][1]);
    ASSERT_EQ(0u, wtl[1][0]);
    ASSERT_EQ(5u, wtl[1][1]);
    wtl.clear();
    index.scanPSO("x", &wtl);
    ASSERT_EQ(0u, wtl.size());

    index.scanPSO("c", &wtl);
    ASSERT_EQ(0u, wtl.size());

    index.scanPOS("b", &wtl);
    ASSERT_EQ(2u, wtl.size());
    ASSERT_EQ(4u, wtl[0][0]);
    ASSERT_EQ(0u, wtl[0][1]);
    ASSERT_EQ(5u, wtl[1][0]);
    ASSERT_EQ(0u, wtl[1][1]);
    wtl.clear();
    index.scanPOS("x", &wtl);
    ASSERT_EQ(0u, wtl.size());

    index.scanPOS("c", &wtl);
    ASSERT_EQ(0u, wtl.size());

    index.scanPSO("b", "a", &wol);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(4u, wol[0][0]);
    ASSERT_EQ(5u, wol[1][0]);
    wol.clear();
    index.scanPSO("b", "c", &wol);
    ASSERT_EQ(0u, wol.size());

    index.scanPOS("b2", "c2", &wol);
    ASSERT_EQ(1u, wol.size());
    ASSERT_EQ(1u, wol[0][0]);
  }

  remove("_testtmp2.tsv");
  std::remove(stxxlFileName.c_str());
  remove("_testindex.index.pso");
  remove("_testindex.index.pos");

  // a       is-a    1       .
  // a       is-a    2       .
  // a       is-a    0       .
  // b       is-a    3       .
  // b       is-a    0       .
  // c       is-a    1       .
  // c       is-a    2       .
  std::fstream f2("_testtmp2.tsv", std::ios_base::out);

  // Vocab:
  // 0: 0
  // 1: 1
  // 2: 2
  // 3: 3
  // 4: a
  // 5: b
  // 6: c
  // 7: is-a
  f2 << "a\tis-a\t1\t.\n"
        "a\tis-a\t2\t.\n"
        "a\tis-a\t0\t.\n"
        "b\tis-a\t3\t.\n"
        "b\tis-a\t0\t.\n"
        "c\tis-a\t1\t.\n"
        "c\tis-a\t2\t.\n";
  f2.close();

  {
    Index index;
    index.createFromTsvFile("_testtmp2.tsv", "_testindex", false);

    Index::WidthOneList wol;
    Index::WidthTwoList wtl;

    index.scanPSO("is-a", &wtl);
    ASSERT_EQ(7u, wtl.size());
    ASSERT_EQ(4u, wtl[0][0]);
    ASSERT_EQ(0u, wtl[0][1]);
    ASSERT_EQ(4u, wtl[1][0]);
    ASSERT_EQ(1u, wtl[1][1]);
    ASSERT_EQ(4u, wtl[2][0]);
    ASSERT_EQ(2u, wtl[2][1]);
    ASSERT_EQ(5u, wtl[3][0]);
    ASSERT_EQ(0u, wtl[3][1]);
    ASSERT_EQ(5u, wtl[4][0]);
    ASSERT_EQ(3u, wtl[4][1]);
    ASSERT_EQ(6u, wtl[5][0]);
    ASSERT_EQ(1u, wtl[5][1]);
    ASSERT_EQ(6u, wtl[6][0]);
    ASSERT_EQ(2u, wtl[6][1]);

    index.scanPOS("is-a", &wtl);
    ASSERT_EQ(7u, wtl.size());
    ASSERT_EQ(0u, wtl[0][0]);
    ASSERT_EQ(4u, wtl[0][1]);
    ASSERT_EQ(0u, wtl[1][0]);
    ASSERT_EQ(5u, wtl[1][1]);
    ASSERT_EQ(1u, wtl[2][0]);
    ASSERT_EQ(4u, wtl[2][1]);
    ASSERT_EQ(1u, wtl[3][0]);
    ASSERT_EQ(6u, wtl[3][1]);
    ASSERT_EQ(2u, wtl[4][0]);
    ASSERT_EQ(4u, wtl[4][1]);
    ASSERT_EQ(2u, wtl[5][0]);
    ASSERT_EQ(6u, wtl[5][1]);
    ASSERT_EQ(3u, wtl[6][0]);
    ASSERT_EQ(5u, wtl[6][1]);

    index.scanPOS("is-a", "0", &wol);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(4u, wol[0][0]);
    ASSERT_EQ(5u, wol[1][0]);

    wol.clear();
    index.scanPOS("is-a", "1", &wol);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(4u, wol[0][0]);
    ASSERT_EQ(6u, wol[1][0]);

    wol.clear();
    index.scanPOS("is-a", "2", &wol);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(4u, wol[0][0]);
    ASSERT_EQ(6u, wol[1][0]);

    wol.clear();
    index.scanPOS("is-a", "3", &wol);
    ASSERT_EQ(1u, wol.size());
    ASSERT_EQ(5u, wol[0][0]);

    wol.clear();
    index.scanPSO("is-a", "a", &wol);
    ASSERT_EQ(3u, wol.size());
    ASSERT_EQ(0u, wol[0][0]);
    ASSERT_EQ(1u, wol[1][0]);
    ASSERT_EQ(2u, wol[2][0]);

    wol.clear();
    index.scanPSO("is-a", "b", &wol);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(0u, wol[0][0]);
    ASSERT_EQ(3u, wol[1][0]);

    wol.clear();
    index.scanPSO("is-a", "c", &wol);
    ASSERT_EQ(2u, wol.size());
    ASSERT_EQ(1u, wol[0][0]);
    ASSERT_EQ(2u, wol[1][0]);
  }
  remove("_testtmp2.tsv");
  std::remove(stxxlFileName.c_str());
  remove("_testindex.index.pso");
  remove("_testindex.index.pos");
};

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
