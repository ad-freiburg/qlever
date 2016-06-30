// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <algorithm>
#include <unordered_set>
#include <stxxl/algorithm>
#include "../parser/TsvParser.h"
#include "./Index.h"
#include "../parser/NTriplesParser.h"

using std::array;

// _____________________________________________________________________________
void Index::createFromTsvFile(const string& tsvFile, const string& onDiskBase,
                              bool allPermutations) {
  _onDiskBase = onDiskBase;
  string indexFilename = _onDiskBase + ".index";
  size_t nofLines = passTsvFileForVocabulary(tsvFile);
  _vocab.writeToFile(onDiskBase + ".vocabulary");
  ExtVec v(nofLines);
  passTsvFileIntoIdVector(tsvFile, v);
  // PSO permutation
  LOG(INFO) << "Sorting for PSO permutation..." << std::endl;
  stxxl::sort(begin(v), end(v), SortByPSO(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
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
    createPermutation(indexFilename + ".sop", v, _ospMeta, 2, 0, 1);
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
                                   bool allPermutations) {
  _onDiskBase = onDiskBase;
  string indexFilename = _onDiskBase + ".index";
  size_t nofLines = passNTriplesFileForVocabulary(ntFile);
  _vocab.writeToFile(onDiskBase + ".vocabulary");
  ExtVec v(nofLines);
  passNTriplesFileIntoIdVector(ntFile, v);
  LOG(INFO) << "Sorting for PSO permutation..." << std::endl;
  stxxl::sort(begin(v), end(v), SortByPSO(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
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
    createPermutation(indexFilename + ".sop", v, _ospMeta, 2, 0, 1);
    // OPS permutation
    LOG(INFO) << "Sorting for OPS permutation..." << std::endl;
    stxxl::sort(begin(v), end(v), SortByOPS(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
    createPermutation(indexFilename + ".ops", v, _opsMeta, 2, 1, 0);
  }
  openFileHandles();
}

// _____________________________________________________________________________
size_t Index::passTsvFileForVocabulary(const string& tsvFile) {
  LOG(INFO) << "Making pass over TsvFile " << tsvFile << " for vocabulary."
            << std::endl;
  array<string, 3> spo;
  TsvParser p(tsvFile);
  std::unordered_set<string> items;
  size_t i = 0;
  while (p.getLine(spo)) {
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
void Index::passTsvFileIntoIdVector(const string& tsvFile, ExtVec& data) {
  LOG(INFO) << "Making pass over TsvFile " << tsvFile
            << " and creating stxxl vector.\n";
  array<string, 3> spo;
  TsvParser p(tsvFile);
  std::unordered_map<string, Id> vocabMap = _vocab.asMap();
  size_t i = 0;
  // write using vector_bufwriter
  ExtVec::bufwriter_type writer(data);
  while (p.getLine(spo)) {
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
size_t Index::passNTriplesFileForVocabulary(const string& ntFile) {
  LOG(INFO) << "Making pass over NTriples " << ntFile << " for vocabulary."
            << std::endl;
  array<string, 3> spo;
  NTriplesParser p(ntFile);
  std::unordered_set<string> items;
  size_t i = 0;
  while (p.getLine(spo)) {
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
void Index::passNTriplesFileIntoIdVector(const string& ntFile, ExtVec& data) {
  LOG(INFO) << "Making pass over NTriples " << ntFile
            << " and creating stxxl vector.\n";
  array<string, 3> spo;
  NTriplesParser p(ntFile);
  std::unordered_map<string, Id> vocabMap = _vocab.asMap();
  size_t i = 0;
  // write using vector_bufwriter
  ExtVec::bufwriter_type writer(data);
  while (p.getLine(spo)) {
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
      metaData.add(writeRel(out, lastOffset, currentRel, buffer, functional));
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
    metaData.add(writeRel(out, lastOffset, currentRel, buffer, functional));
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
RelationMetaData Index::writeRel(ad_utility::File& out, off_t currentOffset,
                                 Id relId, const vector<array<Id, 2>>& data,
                                 bool functional) {
  LOG(TRACE) << "Writing a relation ...\n";
  AD_CHECK_GT(data.size(), 0);
  RelationMetaData rmd;
  rmd._relId = relId;
  rmd._startFullIndex = currentOffset;
  rmd._nofElements = data.size();

  // Write the full pair index.
  out.write(data.data(), data.size() * 2 * sizeof(Id));

  if (functional) {
    rmd = writeFunctionalRelation(data, rmd);
  } else {
    rmd = writeNonFunctionalRelation(out, data, rmd);
  };
  rmd._nofBlocks = rmd._blocks.size();
  LOG(TRACE) << "Done writing relation.\n";
  return rmd;
}

// _____________________________________________________________________________
RelationMetaData& Index::writeFunctionalRelation(
    const vector<array<Id, 2>>& data, RelationMetaData& rmd) {
  LOG(TRACE) << "Writing part for functional relation ...\n";
  // Do not write extra LHS and RHS lists.
  rmd._startRhs = rmd._startFullIndex + data.size() * 2 * sizeof(Id);
  rmd._offsetAfter = rmd._startRhs;
  // Create the block data for the RelationMetaData.
  // Blocks are offsets into the full pair index for functional relations.
  size_t nofDistinctLhs = 0;
  Id lastLhs = std::numeric_limits<Id>::max();
  for (size_t i = 0; i < data.size(); ++i) {
    if (data[i][0] != lastLhs) {
      if (nofDistinctLhs % DISTINCT_LHS_PER_BLOCK == 0) {
        rmd._blocks.emplace_back(
            BlockMetaData(data[i][0],
                          rmd._startFullIndex + i * 2 * sizeof(Id)));
      }
      ++nofDistinctLhs;
    }
  }
  return rmd;
}

// _____________________________________________________________________________
RelationMetaData& Index::writeNonFunctionalRelation(ad_utility::File& out,
                                                    const vector<array<Id, 2>>& data,
                                                    RelationMetaData& rmd) {
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
  off_t startRhs = rmd.getStartOfLhs()
                   + nofDistinctLhs * (sizeof(Id) + sizeof(off_t));

  for (size_t i = 0; i < nofDistinctLhs; ++i) {
    bufLhs[i].second += startRhs;
  }

  // Write to file.
  out.write(bufLhs, nofDistinctLhs * (sizeof(Id) + sizeof(off_t)));
  out.write(bufRhs, data.size() * sizeof(Id));


  // Update meta data.
  rmd._startRhs = startRhs;
  rmd._offsetAfter = startRhs + rmd._nofElements * sizeof(Id);

  // Create the block data for the RelationMetaData.
  // Block are offsets into the LHS list for non-functional relations.
  for (size_t i = 0; i < nofDistinctLhs; ++i) {
    if (i % DISTINCT_LHS_PER_BLOCK == 0) {
      rmd._blocks.emplace_back(BlockMetaData(bufLhs[i].first,
                                             rmd.getStartOfLhs() +
                                             i * (sizeof(Id) + sizeof(off_t))));
    }
  }

  delete[] bufLhs;
  delete[] bufRhs;
  return rmd;
}

// _____________________________________________________________________________
void Index::createFromOnDiskIndex(const string& onDiskBase) {
  _onDiskBase = onDiskBase;
  _vocab.readFromFile(onDiskBase + ".vocabulary");
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
  if (ad_utility::File::exists(_onDiskBase + ".index.spo")) {
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
    LOG(INFO) << "Registered SOP permutation: " << _psoMeta.statistics()
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
    LOG(TRACE) << "Successfully got relation ID.\n";
    if (_psoMeta.relationExists(relId)) {
      LOG(TRACE) << "Relation exists.\n";
      const RelationMetaData& rmd = _psoMeta.getRmd(relId);
      result->reserve(rmd._nofElements + 2);
      result->resize(rmd._nofElements);
      _psoFile.read(result->data(), rmd._nofElements * 2 * sizeof(Id),
                    rmd._startFullIndex);
    }
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPSO(const string& predicate, const string& subject,
                    WidthOneList* result) const {
  LOG(DEBUG) << "Performing PSO scan of relation" << predicate
             << "with fixed subject: " << subject << "...\n";
  Id relId;
  Id subjId;
  if (_vocab.getId(predicate, &relId) && _vocab.getId(subject, &subjId)) {
    if (_psoMeta.relationExists(relId)) {
      const RelationMetaData& rmd = _psoMeta.getRmd(relId);
      pair<off_t, size_t> blockOff = rmd.getBlockStartAndNofBytesForLhs(subjId);
      // Functional relations have blocks point into the pair index,
      // non-functional relations have them point into lhs lists
      if (rmd.isFunctional()) {
        scanFunctionalRelation(blockOff, subjId, _psoFile, result);
      } else {
        pair<off_t, size_t> block2 = rmd.getFollowBlockForLhs(subjId);
        scanNonFunctionalRelation(blockOff, block2, subjId, _psoFile,
                                  rmd._offsetAfter, result);
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
    LOG(TRACE) << "Successfully got relation ID.\n";
    if (_posMeta.relationExists(relId)) {
      LOG(TRACE) << "Relation exists.\n";
      const RelationMetaData& rmd = _posMeta.getRmd(relId);
      result->reserve(rmd._nofElements + 2);
      result->resize(rmd._nofElements);
      _posFile.read(result->data(), rmd._nofElements * 2 * sizeof(Id),
                    rmd._startFullIndex);
    }
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPOS(const string& predicate, const string& object,
                    WidthOneList* result) const {
  LOG(DEBUG) << "Performing POS scan of relation" << predicate
             << "with fixed object: " << object << "...\n";
  Id relId;
  Id objId;
  if (_vocab.getId(predicate, &relId) && _vocab.getId(object, &objId)) {
    if (_posMeta.relationExists(relId)) {
      const RelationMetaData& rmd = _posMeta.getRmd(relId);
      pair<off_t, size_t> blockOff = rmd.getBlockStartAndNofBytesForLhs(objId);
      // Functional relations have blocks point into the pair index,
      // non-functional relations have them point into lhs lists
      if (rmd.isFunctional()) {
        scanFunctionalRelation(blockOff, objId, _posFile, result);
      } else {
        pair<off_t, size_t> block2 = rmd.getFollowBlockForLhs(objId);
        scanNonFunctionalRelation(blockOff, block2, objId, _posFile,
                                  rmd._offsetAfter, result);
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
const string& Index::idToString(Id id) const {
  assert(id < _vocab.size());
  return _vocab[id];
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
        _psoFile.read(&follower, sizeof(follower), followBlock.first);
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
  if (relationName == IN_CONTEXT_RELATION) {
    return IN_CONTEXT_CARDINALITY_ESTIMATE;
  }
  Id relId;
  if (_vocab.getId(relationName, &relId)) {
    if (this->_psoMeta.relationExists(relId)) {
      return this->_psoMeta.getRmd(relId)._nofElements;
    }
  }
  return 0;
}

// _____________________________________________________________________________
void Index::writeAsciiListFile(string filename, const vector<Id>& ids) const {
  std::ofstream f(filename.c_str());
  for (size_t i = 0; i < ids.size(); ++i) {
    f << ids[i] << ' ';
  }
  f.close();
}
