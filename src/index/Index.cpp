// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <cmath>
#include <algorithm>
#include <stxxl/algorithm>
#include "../parser/TsvParser.h"
#include "./Index.h"
#include "../parser/NTriplesParser.h"
#include "../util/Conversions.h"

using std::array;

// _____________________________________________________________________________
void Index::createFromTsvFile(const string& tsvFile, const string& onDiskBase,
                              bool allPermutations, bool onDiskLiterals) {
  _onDiskBase = onDiskBase;
  string indexFilename = _onDiskBase + ".index";
  size_t nofLines = passTsvFileForVocabulary(tsvFile, onDiskLiterals);
  ExtVec v(nofLines);
  passTsvFileIntoIdVector(tsvFile, v, onDiskLiterals);
  if (onDiskLiterals) {
    _vocab.externalizeLiterals(_onDiskBase + ".literals-index");
  }
  _vocab.writeToFile(onDiskBase + ".vocabulary");
  // PSO permutation
  LOG(INFO) << "Sorting for PSO permutation..." << std::endl;
  stxxl::sort(begin(v), end(v), SortByPSO(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
  LOG(INFO) << "Performing unique to ensure RDF semantics..." << std::endl;
  LOG(INFO) << "Size before: " << v.size() << std::endl;
  auto last = std::unique(begin(v), end(v));
  v.resize(size_t(last - v.begin()));
  LOG(INFO) << "Done: unique." << std::endl;
  LOG(INFO) << "Size after: " << v.size() << std::endl;
  createPermutation(indexFilename + ".pso", v, _psoMeta, 1, 0, 2);
  // POS permutation
  LOG(INFO) << "Sorting for POS permutation..." << std::endl;;
  stxxl::sort(begin(v), end(v), SortByPOS(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
  createPermutation(indexFilename + ".pos", v, _posMeta, 1, 2, 0);
  if (allPermutations) {
    // SPO permutation
    LOG(INFO) << "Sorting for SPO permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortBySPO(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".spo", v, _spoMeta, 0, 1, 2);
    // SOP permutation
    LOG(INFO) << "Sorting for SOP permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortBySOP(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".sop", v, _sopMeta, 0, 2, 1);
    // OSP permutation
    LOG(INFO) << "Sorting for OSP permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortByOSP(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".osp", v, _ospMeta, 2, 0, 1);
    // OPS permutation
    LOG(INFO) << "Sorting for OPS permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortByOPS(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".ops", v, _opsMeta, 2, 1, 0);
  }
  openFileHandles();
}

// _____________________________________________________________________________
void Index::createFromNTriplesFile(const string& ntFile,
                                   const string& onDiskBase,
                                   bool allPermutations, bool onDiskLiterals) {
  _onDiskBase = onDiskBase;
  string indexFilename = _onDiskBase + ".index";
  size_t nofLines = passNTriplesFileForVocabulary(ntFile, onDiskLiterals);
  ExtVec v(nofLines);
  passNTriplesFileIntoIdVector(ntFile, v, onDiskLiterals);
  if (onDiskLiterals) {
    _vocab.externalizeLiterals(onDiskBase + ".literals-index");
  }
  _vocab.writeToFile(onDiskBase + ".vocabulary");
  LOG(INFO) << "Sorting for PSO permutation..." << std::endl;
  stxxl::sort(begin(v), end(v), SortByPSO(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
  LOG(INFO) << "Performing unique to ensure RDF semantics..." << std::endl;
  LOG(INFO) << "Size before: " << v.size() << std::endl;
  auto last = std::unique(begin(v), end(v));
  v.resize(size_t(last - v.begin()));
  LOG(INFO) << "Done: unique." << std::endl;
  LOG(INFO) << "Size after: " << v.size() << std::endl;
  createPermutation(indexFilename + ".pso", v, _psoMeta, 1, 0, 2);
  LOG(INFO) << "Sorting for POS permutation..." << std::endl;;
  stxxl::sort(begin(v), end(v), SortByPOS(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;;
  createPermutation(indexFilename + ".pos", v, _posMeta, 1, 2, 0);
  if (allPermutations) {
    // SPO permutation
    LOG(INFO) << "Sorting for SPO permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortBySPO(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".spo", v, _spoMeta, 0, 1, 2);
    // SOP permutation
    LOG(INFO) << "Sorting for SOP permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortBySOP(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".sop", v, _sopMeta, 0, 2, 1);
    // OSP permutation
    LOG(INFO) << "Sorting for OSP permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortByOSP(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".osp", v, _ospMeta, 2, 0, 1);
    // OPS permutation
    LOG(INFO) << "Sorting for OPS permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortByOPS(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".ops", v, _opsMeta, 2, 1, 0);
  }
  openFileHandles();
}

// _____________________________________________________________________________
size_t
Index::passTsvFileForVocabulary(const string& tsvFile, bool onDiskLiterals) {
  LOG(INFO) << "Making pass over TsvFile " << tsvFile << " for vocabulary."
            << std::endl;
  array<string, 3> spo;
  TsvParser p(tsvFile);
  ad_utility::HashSet<string> items;
  size_t i = 0;
  while (p.getLine(spo)) {
    if (ad_utility::isXsdValue(spo[2])) {
      spo[2] = ad_utility::convertValueLiteralToIndexWord(spo[2]);
    }
    if (onDiskLiterals && isLiteral(spo[2]) && shouldBeExternalized(spo[2])) {
      spo[2] = string({EXTERNALIZED_LITERALS_PREFIX}) + spo[2];
    }
    items.insert(spo[0]);
    items.insert(spo[1]);
    items.insert(spo[2]);
    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }
  }
  LOG(INFO) << "Pass done.\n";
  _vocab.createFromSet(items);
  return i;
}

// _____________________________________________________________________________
void Index::passTsvFileIntoIdVector(const string& tsvFile, ExtVec& data,
                                    bool onDiskLiterals) {
  LOG(INFO) << "Making pass over TsvFile " << tsvFile
            << " and creating stxxl vector.\n";
  array<string, 3> spo;
  TsvParser p(tsvFile);
  auto vocabMap = _vocab.asMap();
  size_t i = 0;
  // write using vector_bufwriter
  ExtVec::bufwriter_type writer(data);
  while (p.getLine(spo)) {
    if (ad_utility::isXsdValue(spo[2])) {
      spo[2] = ad_utility::convertValueLiteralToIndexWord(spo[2]);
    }
    if (onDiskLiterals && isLiteral(spo[2]) && shouldBeExternalized(spo[2])) {
      spo[2] = string({EXTERNALIZED_LITERALS_PREFIX}) + spo[2];
    }
    writer << array<Id, 3>{{
                               vocabMap.find(spo[0])->second,
                               vocabMap.find(spo[1])->second,
                               vocabMap.find(spo[2])->second
                           }};
    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }
  }
  writer.finish();
  LOG(INFO) << "Pass done.\n";
}

// _____________________________________________________________________________
size_t Index::passNTriplesFileForVocabulary(const string& ntFile,
                                            bool onDiskLiterals) {
  LOG(INFO) << "Making pass over NTriples " << ntFile << " for vocabulary."
            << std::endl;
  array<string, 3> spo;
  NTriplesParser p(ntFile);
  ad_utility::HashSet<string> items;
  size_t i = 0;
  while (p.getLine(spo)) {
    if (ad_utility::isXsdValue(spo[2])) {
      spo[2] = ad_utility::convertValueLiteralToIndexWord(spo[2]);
    }
    if (onDiskLiterals && isLiteral(spo[2]) && shouldBeExternalized(spo[2])) {
      spo[2] = string({EXTERNALIZED_LITERALS_PREFIX}) + spo[2];
    }
    items.insert(spo[0]);
    items.insert(spo[1]);
    items.insert(spo[2]);
    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }
  }
  LOG(INFO) << "Pass done.\n";
  _vocab.createFromSet(items);
  items.clear();
  return i;
}

// _____________________________________________________________________________
void Index::passNTriplesFileIntoIdVector(const string& ntFile, ExtVec& data,
                                         bool onDiskLiterals) {
  LOG(INFO) << "Making pass over NTriples " << ntFile
            << " and creating stxxl vector.\n";
  array<string, 3> spo;
  NTriplesParser p(ntFile);
  google::sparse_hash_map<string, Id> vocabMap = _vocab.asMap();
  size_t i = 0;
  // write using vector_bufwriter
  ExtVec::bufwriter_type writer(data);
  while (p.getLine(spo)) {
    if (ad_utility::isXsdValue(spo[2])) {
      spo[2] = ad_utility::convertValueLiteralToIndexWord(spo[2]);
    }
    if (onDiskLiterals && isLiteral(spo[2]) && shouldBeExternalized(spo[2])) {
      spo[2] = string({EXTERNALIZED_LITERALS_PREFIX}) + spo[2];
    }
    writer << array<Id, 3>{{
                               vocabMap.find(spo[0])->second,
                               vocabMap.find(spo[1])->second,
                               vocabMap.find(spo[2])->second
                           }};
    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }
  }
  writer.finish();
  LOG(INFO) << "Pass done.\n";
}

// _____________________________________________________________________________
void Index::createPermutation(const string& fileName, Index::ExtVec const& vec,
                              IndexMetaData& metaData, size_t c0, size_t c1,
                              size_t c2) {
  if (vec.size() == 0) {
    LOG(WARN) << "Attempt to write an empty index!" << std::endl;
    return;
  }
  ad_utility::File out(fileName.c_str(), "w");
  LOG(INFO) << "Creating an on-disk index permutation of " << vec.size()
            << " elements / facts." << std::endl;
  // Iterate over the vector and identify relation boundaries
  size_t from = 0;
  Id currentRel = vec[0][c0];
  off_t lastOffset = 0;
  vector<array<Id, 2>> buffer;
  bool functional = true;
  Id lastLhs = std::numeric_limits<Id>::max();
  for (ExtVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    if ((*reader)[c0] != currentRel) {
      auto md = writeRel(out, lastOffset, currentRel, buffer, functional);
      metaData.add(md.first, md.second);
      buffer.clear();
      lastOffset = metaData.getOffsetAfter();
      currentRel = (*reader)[c0];
      functional = true;
    } else {
      if ((*reader)[c1] == lastLhs) {
        functional = false;
      }
    }
    buffer.emplace_back(array<Id, 2>{{(*reader)[c1], (*reader)[c2]}});
    lastLhs = (*reader)[c1];
  }
  if (from < vec.size()) {
    auto md = writeRel(out, lastOffset, currentRel, buffer, functional);
    metaData.add(md.first, md.second);
  }

  LOG(INFO) << "Done creating index permutation." << std::endl;
  LOG(INFO) << "Writing statistics for this permutation:\n"
            << metaData.statistics() << std::endl;

  LOG(INFO) << "Writing Meta data to index file...\n";
  out << metaData;
  off_t startOfMeta = metaData.getOffsetAfter();
  out.write(&startOfMeta, sizeof(startOfMeta));
  out.close();
  LOG(INFO) << "Permutation done.\n";
}

// _____________________________________________________________________________
pair<FullRelationMetaData, BlockBasedRelationMetaData>
Index::writeRel(ad_utility::File& out, off_t currentOffset,
                Id relId, const vector<array<Id, 2>>& data,
                bool functional) {
  LOG(TRACE) << "Writing a relation ...\n";
  AD_CHECK_GT(data.size(), 0);
  LOG(TRACE) << "Calculating multiplicities ...\n";
  ad_utility::HashSet<Id> distinctC1;
  ad_utility::HashSet<Id> distinctC2;
  for (size_t i = 0; i < data.size(); ++i) {
    if (!functional) {
      distinctC1.insert(data[i][0]);
    }
    distinctC2.insert(data[i][1]);
  }
  double multC1 = functional ? 1.0 : data.size() / double(distinctC1.size());
  double multC2 = data.size() / double(distinctC2.size());
  LOG(TRACE) << "Done calculating multiplicities.\n";
  FullRelationMetaData rmd(relId, currentOffset, data.size(),
                           multC1, multC2,
                           functional,
                           !functional &&
                           data.size() > USE_BLOCKS_INDEX_SIZE_TRESHOLD);

  // Write the full pair index.
  out.write(data.data(), data.size() * 2 * sizeof(Id));
  pair<FullRelationMetaData, BlockBasedRelationMetaData> ret;
  ret.first = rmd;

  if (functional) {
    writeFunctionalRelation(data, ret);
  } else {
    writeNonFunctionalRelation(out, data, ret);
  };
  LOG(TRACE) << "Done writing relation.\n";
  return ret;
}

// _____________________________________________________________________________
void Index::writeFunctionalRelation(
    const vector<array<Id, 2>>& data,
    pair<FullRelationMetaData, BlockBasedRelationMetaData>& rmd) {
  // Only has to do something if there are blocks.
  if (rmd.first.hasBlocks()) {
    LOG(TRACE) << "Writing part for functional relation ...\n";
    // Do not write extra LHS and RHS lists.
    rmd.second._startRhs =
        rmd.first._startFullIndex + rmd.first.getNofBytesForFulltextIndex();
    // Since the relation is functional, there are no lhs lists and thus this
    // is trivial.
    rmd.second._offsetAfter = rmd.second._startRhs;
    // Create the block data for the meta data.
    // Blocks are offsets into the full pair index for functional relations.
    size_t nofDistinctLhs = 0;
    Id lastLhs = std::numeric_limits<Id>::max();
    for (size_t i = 0; i < data.size(); ++i) {
      if (data[i][0] != lastLhs) {
        if (nofDistinctLhs % DISTINCT_LHS_PER_BLOCK == 0) {
          rmd.second._blocks.emplace_back(
              BlockMetaData(data[i][0],
                            rmd.first._startFullIndex + i * 2 * sizeof(Id)));
        }
        ++nofDistinctLhs;
      }
    }
  }
}

// _____________________________________________________________________________
void Index::writeNonFunctionalRelation(
    ad_utility::File& out,
    const vector<array<Id, 2>>& data,
    pair<FullRelationMetaData, BlockBasedRelationMetaData>& rmd) {
  // Only has to do something if there are blocks.
  if (rmd.first.hasBlocks()) {
    LOG(TRACE) << "Writing part for non-functional relation ...\n";
    // Make a pass over the data and extract a RHS list for each LHS.
    // Prepare both in buffers.
    // TODO: add compression - at least to RHS.
    pair<Id, off_t>* bufLhs = new pair<Id, off_t>[data.size()];
    Id* bufRhs = new Id[data.size()];
    size_t nofDistinctLhs = 0;
    Id lastLhs = std::numeric_limits<Id>::max();
    size_t nofRhsDone = 0;
    for (; nofRhsDone < data.size(); ++nofRhsDone) {
      if (data[nofRhsDone][0] != lastLhs) {
        bufLhs[nofDistinctLhs++] =
            pair<Id, off_t>(data[nofRhsDone][0], nofRhsDone * sizeof(Id));
        lastLhs = data[nofRhsDone][0];
      }
      bufRhs[nofRhsDone] = data[nofRhsDone][1];
    }

    // Go over the Lhs data once more and adjust the offsets.
    off_t startRhs = rmd.first.getStartOfLhs()
                     + nofDistinctLhs * (sizeof(Id) + sizeof(off_t));

    for (size_t i = 0; i < nofDistinctLhs; ++i) {
      bufLhs[i].second += startRhs;
    }

    // Write to file.
    out.write(bufLhs, nofDistinctLhs * (sizeof(Id) + sizeof(off_t)));
    out.write(bufRhs, data.size() * sizeof(Id));


    // Update meta data.
    rmd.second._startRhs = startRhs;
    rmd.second._offsetAfter =
        startRhs + rmd.first.getNofElements() * sizeof(Id);

    // Create the block data for the FullRelationMetaData.
    // Block are offsets into the LHS list for non-functional relations.
    for (size_t i = 0; i < nofDistinctLhs; ++i) {
      if (i % DISTINCT_LHS_PER_BLOCK == 0) {
        rmd.second._blocks.emplace_back(BlockMetaData(bufLhs[i].first,
                                                      rmd.first.getStartOfLhs() +
                                                      i *
                                                      (sizeof(Id) +
                                                       sizeof(off_t))));
      }
    }
    delete[] bufLhs;
    delete[] bufRhs;
  }
}

// _____________________________________________________________________________
void
Index::createFromOnDiskIndex(const string& onDiskBase, bool allPermutations,
                             bool onDiskLiterals) {
  _onDiskBase = onDiskBase;
  _vocab.readFromFile(onDiskBase + ".vocabulary",
                      onDiskLiterals ? onDiskBase + ".literals-index" : "");
  _psoFile.open(string(_onDiskBase + ".index.pso").c_str(), "r");
  _posFile.open(string(_onDiskBase + ".index.pos").c_str(), "r");
  AD_CHECK(_psoFile.isOpen() && _posFile.isOpen());
  // PSO
  off_t metaFrom;
  off_t metaTo = _psoFile.getLastOffset(&metaFrom);
  unsigned char* buf = new unsigned char[metaTo - metaFrom];
  _psoFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
  _psoMeta.createFromByteBuffer(buf);
  delete[] buf;
  LOG(INFO) << "Registered PSO permutation: " << _psoMeta.statistics()
            << std::endl;
  // POS
  metaTo = _posFile.getLastOffset(&metaFrom);
  buf = new unsigned char[metaTo - metaFrom];
  _posFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
  _posMeta.createFromByteBuffer(buf);
  delete[] buf;
  LOG(INFO) << "Registered POS permutation: " << _posMeta.statistics()
            << std::endl;
  if (allPermutations) {
    _spoFile.open(string(_onDiskBase + ".index.spo").c_str(), "r");
    _sopFile.open(string(_onDiskBase + ".index.sop").c_str(), "r");
    _ospFile.open(string(_onDiskBase + ".index.osp").c_str(), "r");
    _opsFile.open(string(_onDiskBase + ".index.ops").c_str(), "r");
    AD_CHECK(_spoFile.isOpen() && _sopFile.isOpen() && _ospFile.isOpen() &&
             _opsFile.isOpen());
    // SPO
    metaTo = _spoFile.getLastOffset(&metaFrom);
    buf = new unsigned char[metaTo - metaFrom];
    _spoFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
    _spoMeta.createFromByteBuffer(buf);
    delete[] buf;
    LOG(INFO) << "Registered SPO permutation: " << _spoMeta.statistics()
              << std::endl;
    // SOP
    metaTo = _sopFile.getLastOffset(&metaFrom);
    buf = new unsigned char[metaTo - metaFrom];
    _sopFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
    _sopMeta.createFromByteBuffer(buf);
    delete[] buf;
    LOG(INFO) << "Registered SOP permutation: " << _sopMeta.statistics()
              << std::endl;
    // OSP
    metaTo = _ospFile.getLastOffset(&metaFrom);
    buf = new unsigned char[metaTo - metaFrom];
    _ospFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
    _ospMeta.createFromByteBuffer(buf);
    delete[] buf;
    LOG(INFO) << "Registered OSP permutation: " << _ospMeta.statistics()
              << std::endl;
    // OPS
    metaTo = _opsFile.getLastOffset(&metaFrom);
    buf = new unsigned char[metaTo - metaFrom];
    _opsFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
    _opsMeta.createFromByteBuffer(buf);
    delete[] buf;
    LOG(INFO) << "Registered OPS permutation: " << _opsMeta.statistics()
              << std::endl;
  }
}

// _____________________________________________________________________________
bool Index::ready() const {
  return _psoFile.isOpen() && _posFile.isOpen();
}

// _____________________________________________________________________________
void Index::openFileHandles() {
  AD_CHECK(_onDiskBase.size() > 0);
  _psoFile.open((_onDiskBase + ".index.pso").c_str(), "r");
  _posFile.open((_onDiskBase + ".index.pos").c_str(), "r");
  if (ad_utility::File::exists(_onDiskBase + ".index.spo")) {
    _spoFile.open((_onDiskBase + ".index.spo").c_str(), "r");
  }
  if (ad_utility::File::exists(_onDiskBase + ".index.sop")) {
    _sopFile.open((_onDiskBase + ".index.sop").c_str(), "r");
  }
  if (ad_utility::File::exists(_onDiskBase + ".index.osp")) {
    _ospFile.open((_onDiskBase + ".index.osp").c_str(), "r");
  }
  if (ad_utility::File::exists(_onDiskBase + ".index.ops")) {
    _opsFile.open((_onDiskBase + ".index.ops").c_str(), "r");
  }
  AD_CHECK(_psoFile.isOpen());
  AD_CHECK(_posFile.isOpen());
}

// _____________________________________________________________________________
void Index::scanPSO(const string& predicate, WidthTwoList* result) const {
  LOG(DEBUG) << "Performing PSO scan for full relation: " << predicate << "\n";
  Id relId;
  if (_vocab.getId(predicate, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanPSO(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPSO(const string& predicate, const string& subject,
                    WidthOneList* result) const {
  LOG(DEBUG) << "Performing PSO scan of relation " << predicate
             << " with fixed subject: " << subject << "...\n";
  Id relId;
  Id subjId;
  if (_vocab.getId(predicate, &relId) && _vocab.getId(subject, &subjId)) {
    if (_psoMeta.relationExists(relId)) {
      auto rmd = _psoMeta.getRmd(relId);
      if (rmd.hasBlocks()) {
        pair<off_t, size_t> blockOff = rmd._rmdBlocks->getBlockStartAndNofBytesForLhs(
            subjId);
        // Functional relations have blocks point into the pair index,
        // non-functional relations have them point into lhs lists
        if (rmd.isFunctional()) {
          scanFunctionalRelation(blockOff, subjId, _psoFile, result);
        } else {
          pair<off_t, size_t> block2 = rmd._rmdBlocks->getFollowBlockForLhs(
              subjId);
          scanNonFunctionalRelation(blockOff, block2, subjId, _psoFile,
                                    rmd._rmdBlocks->_offsetAfter, result);
        }
      } else {
        // If we don't have blocks, scan the whole relation and filter / restrict.
        WidthTwoList fullRelation;
        fullRelation.resize(rmd.getNofElements());
        _psoFile.read(fullRelation.data(),
                      rmd.getNofElements() * 2 * sizeof(Id),
                      rmd._rmdPairs._startFullIndex);
        getRhsForSingleLhs(fullRelation, subjId, result);
      }
    } else {
      LOG(DEBUG) << "No such relation.\n";
    }
  } else {
    LOG(DEBUG) << "No such subject.\n";
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPOS(const string& predicate, WidthTwoList* result) const {
  LOG(DEBUG) << "Performing POS scan for full relation: " << predicate << "\n";
  Id relId;
  if (_vocab.getId(predicate, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanPOS(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPOS(const string& predicate, const string& object,
                    WidthOneList* result) const {
  LOG(DEBUG) << "Performing POS scan of relation " << predicate
             << " with fixed object: " << object << "...\n";
  Id relId;
  Id objId;
  if (_vocab.getId(predicate, &relId) && _vocab.getId(object, &objId)) {
    if (_posMeta.relationExists(relId)) {
      auto rmd = _posMeta.getRmd(relId);
      if (rmd.hasBlocks()) {
        pair<off_t, size_t> blockOff = rmd._rmdBlocks->getBlockStartAndNofBytesForLhs(
            objId);
        // Functional relations have blocks point into the pair index,
        // non-functional relations have them point into lhs lists
        if (rmd.isFunctional()) {
          scanFunctionalRelation(blockOff, objId, _posFile, result);
        } else {
          pair<off_t, size_t> block2 = rmd._rmdBlocks->getFollowBlockForLhs(
              objId);
          scanNonFunctionalRelation(blockOff, block2, objId, _posFile,
                                    rmd._rmdBlocks->_offsetAfter, result);
        }
      } else {
        // If we don't have blocks, scan the whole relation and filter / restrict.
        WidthTwoList fullRelation;
        fullRelation.resize(rmd.getNofElements());
        _posFile.read(fullRelation.data(),
                      rmd.getNofElements() * 2 * sizeof(Id),
                      rmd._rmdPairs._startFullIndex);
        getRhsForSingleLhs(fullRelation, objId, result);
      }
    } else {
      LOG(DEBUG) << "No such relation.\n";
    }
  } else {
    LOG(DEBUG) << "No such object.\n";
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanSOP(const string& subject, const string& object, WidthOneList*
result) const {
  if (!_sopFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
                 "index permutations. Build an index with option -a "
                 "to use this feature.");
  }
  LOG(DEBUG) << "Performing SOP scan of list for " << subject
             << " with fixed object: " << object << "...\n";
  Id relId;
  Id objId;
  if (_vocab.getId(subject, &relId) && _vocab.getId(object, &objId)) {
    if (_sopMeta.relationExists(relId)) {
      auto rmd = _sopMeta.getRmd(relId);
      if (rmd.hasBlocks()) {
        pair<off_t, size_t> blockOff = rmd._rmdBlocks->getBlockStartAndNofBytesForLhs(
            objId);
        // Functional relations have blocks point into the pair index,
        // non-functional relations have them point into lhs lists
        if (rmd.isFunctional()) {
          scanFunctionalRelation(blockOff, objId, _sopFile, result);
        } else {
          pair<off_t, size_t> block2 = rmd._rmdBlocks->getFollowBlockForLhs(
              objId);
          scanNonFunctionalRelation(blockOff, block2, objId, _sopFile,
                                    rmd._rmdBlocks->_offsetAfter, result);
        }
      } else {
        // If we don't have blocks, scan the whole relation and filter / restrict.
        WidthTwoList fullRelation;
        fullRelation.resize(rmd.getNofElements());
        _sopFile.read(fullRelation.data(),
                      rmd.getNofElements() * 2 * sizeof(Id),
                      rmd._rmdPairs._startFullIndex);
        getRhsForSingleLhs(fullRelation, objId, result);
      }
    } else {
      LOG(DEBUG) << "No such relation.\n";
    }
  } else {
    LOG(DEBUG) << "No such object.\n";
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanSPO(const string& subject, WidthTwoList* result) const {
  if (!_spoFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
                 "index permutations. Build an index with option -a "
                 "to use this feature.");
  }
  LOG(DEBUG) << "Performing SPO scan for full list for: " << subject << "\n";
  Id relId;
  if (_vocab.getId(subject, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanSPO(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanSOP(const string& subject, WidthTwoList* result) const {
  if (!_sopFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
                 "index permutations. Build an index with option -a "
                 "to use this feature.");
  }
  LOG(DEBUG) << "Performing SOP scan for full list for: " << subject << "\n";
  Id relId;
  if (_vocab.getId(subject, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanSOP(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanOPS(const string& object, WidthTwoList* result) const {
  if (!_opsFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
                 "index permutations. Build an index with option -a "
                 "to use this feature.");
  }
  LOG(DEBUG) << "Performing OPS scan for full list for: " << object << "\n";
  Id relId;
  if (_vocab.getId(object, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanOPS(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanOSP(const string& object, WidthTwoList* result) const {
  if (!_ospFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
                 "index permutations. Build an index with option -a "
                 "to use this feature.");
  }
  LOG(DEBUG) << "Performing OSP scan for full list for: " << object << "\n";
  Id relId;
  if (_vocab.getId(object, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanOSP(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPSO(Id predicate, Index::WidthTwoList* result) const {
  if (_psoMeta.relationExists(predicate)) {
    const FullRelationMetaData& rmd = _psoMeta.getRmd(predicate)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _psoFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanPOS(Id predicate, Index::WidthTwoList* result) const {
  if (_posMeta.relationExists(predicate)) {
    const FullRelationMetaData& rmd = _posMeta.getRmd(predicate)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _posFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanSPO(Id subject, Index::WidthTwoList* result) const {
  if (_spoMeta.relationExists(subject)) {
    const FullRelationMetaData& rmd = _spoMeta.getRmd(subject)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _spoFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanSOP(Id subject, Index::WidthTwoList* result) const {
  if (_sopMeta.relationExists(subject)) {
    const FullRelationMetaData& rmd = _sopMeta.getRmd(subject)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _sopFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanOSP(Id object, Index::WidthTwoList* result) const {
  if (_ospMeta.relationExists(object)) {
    const FullRelationMetaData& rmd = _ospMeta.getRmd(object)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _ospFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanOPS(Id object, Index::WidthTwoList* result) const {
  if (_opsMeta.relationExists(object)) {
    const FullRelationMetaData& rmd = _opsMeta.getRmd(object)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _opsFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
string Index::idToString(Id id) const {
  if (id < _vocab.size()) {
    return _vocab[id];
  } else if (id == ID_NO_VALUE) {
    return "";
  } else {
    id -= _vocab.size();
    AD_CHECK(id < _vocab.getExternalVocab().size())
    {
      return _vocab.getExternalVocab()[id];
    }
  }
}

// _____________________________________________________________________________
void Index::scanFunctionalRelation(const pair<off_t, size_t>& blockOff,
                                   Id lhsId, ad_utility::File& indexFile,
                                   WidthOneList* result) const {
  LOG(TRACE) << "Scanning functional relation ...\n";
  WidthTwoList block;
  block.resize(blockOff.second / (2 * sizeof(Id)));
  indexFile.read(block.data(), blockOff.second, blockOff.first);
  auto it = std::lower_bound(block.begin(), block.end(), lhsId,
                             [](const array<Id, 2>& elem, Id key) {
                               return elem[0] < key;
                             });
  if ((*it)[0] == lhsId) {
    result->push_back(array<Id, 1>{(*it)[1]});
  }
  LOG(TRACE) << "Read " << result->size() << " RHS.\n";
}

// _____________________________________________________________________________
void Index::scanNonFunctionalRelation(const pair<off_t, size_t>& blockOff,
                                      const pair<off_t, size_t>& followBlock,
                                      Id lhsId, ad_utility::File& indexFile,
                                      off_t upperBound,
                                      Index::WidthOneList* result) const {
  LOG(TRACE) << "Scanning non-functional relation ...\n";
  vector<pair<Id, off_t>> block;
  block.resize(blockOff.second / (sizeof(Id) + sizeof(off_t)));
  indexFile.read(block.data(), blockOff.second, blockOff.first);
  auto it = std::lower_bound(block.begin(), block.end(), lhsId,
                             [](const pair<Id, off_t>& elem, Id key) {
                               return elem.first < key;
                             });
  if (it->first == lhsId) {
    size_t nofBytes = 0;
    if ((it + 1) != block.end()) {
      LOG(TRACE) << "Obtained upper bound from same block!\n";
      nofBytes = static_cast<size_t>((it + 1)->second - it->second);
    } else {
      //Look at the follow block to determine the upper bound / nofBytes.
      if (followBlock.first == blockOff.first) {
        LOG(TRACE) << "Last block of relation, using rel upper bound!\n";
        nofBytes = static_cast<size_t>(upperBound - it->second);
      } else {
        LOG(TRACE) << "Special case: extra scan of follow block!\n";
        pair<Id, off_t> follower;
        indexFile.read(&follower, sizeof(follower), followBlock.first);
        nofBytes = static_cast<size_t>(follower.second - it->second);
      }
    }
    result->reserve((nofBytes / sizeof(Id)) + 2);
    result->resize(nofBytes / sizeof(Id));
    indexFile.read(result->data(), nofBytes, it->second);
  } else {
    LOG(TRACE) << "Could not find LHS in block. Result will be empty.\n";
  }
}

// _____________________________________________________________________________
size_t Index::relationCardinality(const string& relationName) const {
  if (relationName == INTERNAL_TEXT_MATCH_PREDICATE) {
    return TEXT_PREDICATE_CARDINALITY_ESTIMATE;
  }
  Id relId;
  if (_vocab.getId(relationName, &relId)) {
    if (this->_psoMeta.relationExists(relId)) {
      return this->_psoMeta.getRmd(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::subjectCardinality(const string& sub) const {
  Id relId;
  if (_vocab.getId(sub, &relId)) {
    if (this->_spoMeta.relationExists(relId)) {
      return this->_spoMeta.getRmd(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::objectCardinality(const string& obj) const {
  Id relId;
  if (_vocab.getId(obj, &relId)) {
    if (this->_ospMeta.relationExists(relId)) {
      return this->_ospMeta.getRmd(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::sizeEstimate(const string& sub, const string& pred,
                           const string& obj) const {
  // One or two of the parameters have to be empty strings.
  // This determines the permutations to use.

  // With only one nonempty string, we can get the exact count.
  // With two, we can check if the relation is functional (return 1) or not
  // where we approximate the result size by the block size.
  if (sub.size() > 0 && pred.size() == 0 && obj.size() == 0) {
    return subjectCardinality(sub);
  }
  if (sub.size() == 0 && pred.size() > 0 && obj.size() == 0) {
    return relationCardinality(pred);
  }
  if (sub.size() == 0 && pred.size() == 0 && obj.size() > 0) {
    return objectCardinality(obj);
  }
  if (sub.size() == 0 && pred.size() == 0 && obj.size() == 0) {
    return getNofTriples();
  }
  AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
           "Index::sizeEsimate called with more then one of S/P/O given. "
               "This should never be the case anymore, "
               " since for such SCANs we compute the result "
               "directly and don't need an estimate anymore!");
}

// _____________________________________________________________________________
template<class T>
void Index::writeAsciiListFile(const string& filename, const T& ids) const {
  std::ofstream f(filename.c_str());
  for (size_t i = 0; i < ids.size(); ++i) {
    f << ids[i] << ' ';
  }
  f.close();
}

template void Index::writeAsciiListFile<vector<Id>>(
    const string& filename, const vector<Id>& ids) const;

template void Index::writeAsciiListFile<vector<Score>>(
    const string& filename, const vector<Score>& ids) const;

// _____________________________________________________________________________
bool Index::isLiteral(const string& object) {
  return object.size() > 0 && object[0] == '\"';
}

// _____________________________________________________________________________
bool Index::shouldBeExternalized(const string& object) {
  return Vocabulary::shouldBeExternalized(object);
}

// _____________________________________________________________________________
vector<float> Index::getPSOMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _psoMeta.relationExists(keyId)) {
    auto rmd = _psoMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getPOSMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _posMeta.relationExists(keyId)) {
    auto rmd = _posMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getSPOMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _spoMeta.relationExists(keyId)) {
    auto rmd = _spoMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getSOPMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _sopMeta.relationExists(keyId)) {
    auto rmd = _sopMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getOSPMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _ospMeta.relationExists(keyId)) {
    auto rmd = _ospMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getOPSMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _opsMeta.relationExists(keyId)) {
    auto rmd = _opsMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getSPOMultiplicities() const {
  return vector<float>{
      {
          static_cast<float>(getNofTriples() / getNofSubjects()),
          static_cast<float>(getNofTriples() / getNofPredicates()),
          static_cast<float>(getNofTriples() / getNofObjects())
      }
  };
}

// _____________________________________________________________________________
vector<float> Index::getSOPMultiplicities() const {
  return vector<float>{
      {
          static_cast<float>(getNofTriples() / getNofSubjects()),
          static_cast<float>(getNofTriples() / getNofObjects()),
          static_cast<float>(getNofTriples() / getNofPredicates())
      }
  };
}

// _____________________________________________________________________________
vector<float> Index::getPSOMultiplicities() const {
  return vector<float>{
      {
          static_cast<float>(getNofTriples() / getNofPredicates()),
          static_cast<float>(getNofTriples() / getNofSubjects()),
          static_cast<float>(getNofTriples() / getNofObjects())
      }
  };
}

// _____________________________________________________________________________
vector<float> Index::getPOSMultiplicities() const {
  return vector<float>{
      {
          static_cast<float>(getNofTriples() / getNofPredicates()),
          static_cast<float>(getNofTriples() / getNofObjects()),
          static_cast<float>(getNofTriples() / getNofSubjects())
      }
  };
}

// _____________________________________________________________________________
vector<float> Index::getOSPMultiplicities() const {
  return vector<float>{
      {
          static_cast<float>(getNofTriples() / getNofObjects()),
          static_cast<float>(getNofTriples() / getNofSubjects()),
          static_cast<float>(getNofTriples() / getNofPredicates())
      }
  };
}

// _____________________________________________________________________________
vector<float> Index::getOPSMultiplicities() const {
  return vector<float>{
      {
          static_cast<float>(getNofTriples() / getNofObjects()),
          static_cast<float>(getNofTriples() / getNofPredicates()),
          static_cast<float>(getNofTriples() / getNofSubjects())
      }
  };
}

// _____________________________________________________________________________
void Index::setKbName(const string& name) {
  _psoMeta.setName(name);
  _posMeta.setName(name);
  _spoMeta.setName(name);
  _sopMeta.setName(name);
  _ospMeta.setName(name);
  _opsMeta.setName(name);
}
