// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "./VocabularyGenerator.h"
#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <parallel/algorithm>
#include "../util/Conversions.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./ConstantsIndexCreation.h"
#include "./Vocabulary.h"

// ___________________________________________________________________
VocabularyMerger::VocMergeRes VocabularyMerger::mergeVocabulary(
    const std::string& basename, size_t numFiles, StringSortComparator comp) {
  // we sort alphabetically by the token
  class QueueCompare {
   public:
    QueueCompare(StringSortComparator comp) : _comp(comp) {}
    bool operator()(const QueueWord& p1, const QueueWord& p2) {
      // if p1 is smaller (alphabetically)
      // _comp will return false if called like this
      // and the priority queue will thus emit p1 first
      return _comp(p2._value, p1._value);
    }

   private:
    StringSortComparator _comp;
  };
  std::vector<std::ifstream> infiles;

  _outfile.open(basename + ".vocabulary");
  AD_CHECK(_outfile.is_open());
  _outfileExternal.open(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
  AD_CHECK(_outfileExternal.is_open());
  std::vector<bool> endOfFile(numFiles, false);

  // Priority queue for the k-way merge
  std::priority_queue<QueueWord, std::vector<QueueWord>, QueueCompare> queue(
      comp);

  // open and prepare all infiles and mmap output vectors
  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME +
                         std::to_string(i));
    _idVecs.emplace_back(0, basename + PARTIAL_MMAP_IDS + std::to_string(i));
    AD_CHECK(infiles.back().is_open());

    // read the first entry of the vocabulary and add it to the queue
    endOfFile[i] = true;

    uint32_t len;
    if (infiles[i].read((char*)&len, sizeof(len))) {
      std::string word(len, '\0');
      infiles[i].read(&(word[0]), len);
      Id id;
      infiles[i].read((char*)&id, sizeof(id));
      queue.push(QueueWord(word, i, id));
      endOfFile[i] = false;
    }
  }

  std::vector<QueueWord> sortedBuffer;
  sortedBuffer.reserve(_bufferSize + 2);

  std::future<void> writeFuture;

  // start k-way merge
  while (!queue.empty()) {
    sortedBuffer.push_back(std::move(queue.top()));
    queue.pop();
    auto i = sortedBuffer.back()._partialFileId;

    if (sortedBuffer.size() >= _bufferSize) {
      auto task = [this, buf = std::move(sortedBuffer)]() {
        this->writeQueueWordsToIdVec(buf);
      };
      sortedBuffer.clear();
      sortedBuffer.reserve(_bufferSize + 2);
      // wait for the last batch

      if (writeFuture.valid()) {
        LOG(INFO) << "Waiting for the asynchronous write to finish\n";
        writeFuture.get();
      }
      writeFuture = std::async(task);
      // we have moved away our buffer, start over
    }

    // add next word from the same infile to the priority queue
    if (endOfFile[i]) {
      continue;
    }  // file is exhausted, nothing to add

    endOfFile[i] = true;
    uint32_t len;
    if (infiles[i].read((char*)&len, sizeof(len))) {
      std::string word(len, '\0');
      infiles[i].read(&(word[0]), len);
      Id id;
      infiles[i].read((char*)&id, sizeof(id));
      queue.push(QueueWord(word, i, id));
      endOfFile[i] = false;
    }
  }
  if (writeFuture.valid()) {
    writeFuture.get();
  }
  if (!sortedBuffer.empty()) {
    writeQueueWordsToIdVec(sortedBuffer);
  }
  VocMergeRes result;
  result._numWordsTotal = _totalWritten;
  result._langPredLowerBound = _langPredLowerBound;
  result._langPredUpperBound = _langPredUpperBound;
  return result;
}

// ________________________________________________________________________________
void VocabularyMerger::writeQueueWordsToIdVec(
    const std::vector<QueueWord>& buffer) {
  LOG(INFO) << "Start writing a batch of merged words\n";

  auto bufSize = _bufferSize / 5;
  std::future<void> writeFut;
  std::vector<std::pair<size_t, std::pair<size_t, size_t>>> writeBuf;
  writeBuf.reserve(bufSize);
  // avoid duplicates
  for (const auto& top : buffer) {
    if (top._value != _lastWritten) {
      _lastWritten = top._value;

      // write the new word to the vocabulary
      if (top._value < string({EXTERNALIZED_LITERALS_PREFIX})) {
        _outfile << top._value << '\n';
      } else {
        // we have to strip the externalization character again
        _outfileExternal << top._value.substr(1) << '\n';
      }

      // write id to corresponding vec
      writeBuf.emplace_back(top._partialFileId,
                            std::make_pair(top._partialWordId, _totalWritten));

      if (top._value.size() > 0 && top._value[0] == '@') {
        if (!_firstLangPredSeen) {
          // inclusive
          _langPredLowerBound = _totalWritten;
          _firstLangPredSeen = true;
        }
        // exclusive
        _langPredUpperBound = _totalWritten + 1;
      }
      _totalWritten++;
      if (_totalWritten % _bufferSize == 0) {
        LOG(INFO) << "Merged " << _totalWritten << "Words" << std::endl;
      }
    } else {
      // this is a duplicate which already occured in another partial vocabulary
      // in the last step.
      // we already have increased total written, so for the duplicate
      // we have to subtract one again
      size_t minusOne = _totalWritten - 1;
      writeBuf.emplace_back(top._partialFileId,
                            std::make_pair(top._partialWordId, minusOne));
    }

    if (writeBuf.size() >= bufSize) {
      auto task = [this, buf = std::move(writeBuf)]() {
        this->doActualWrite(buf);
      };
      if (writeFut.valid()) {
        writeFut.get();
      }
      writeFut = std::async(task);
      writeBuf.clear();
      writeBuf.reserve(bufSize + 2);
    }
  }

  if (writeFut.valid()) {
    writeFut.get();
  }

  if (!writeBuf.empty()) {
    doActualWrite(writeBuf);
  }

  LOG(INFO) << "Finished writing batch of merged words\n";
}

// ____________________________________________________________________________________________________________
void VocabularyMerger::doActualWrite(
    const std::vector<std::pair<size_t, std::pair<size_t, size_t>>>& buffer) {
  for (const auto& [id, value] : buffer) {
    _idVecs[id].push_back(value);
  }
}

// ______________________________________________________________________________________________
void writePartialIdMapToBinaryFileForMerging(
    std::shared_ptr<const ad_utility::HashMap<string, Id>> map,
    const string& fileName, StringSortComparator comp, bool doParallelSort) {
  LOG(INFO) << "Creating partial vocabulary from set ...\n";
  std::vector<std::pair<string, Id>> els;
  els.reserve(map->size());
  els.insert(begin(els), begin(*map), end(*map));
  LOG(INFO) << "... sorting ...\n";

  auto pred = [comp](const auto& p1, const auto& p2) {
    return comp(p1.first, p2.first);
  };
  if (USE_PARALLEL_SORT && doParallelSort) {
    __gnu_parallel::sort(begin(els), end(els), pred,
                         __gnu_parallel::parallel_tag(NUM_SORT_THREADS));
  } else {
    std::sort(begin(els), end(els), pred);
  }
  LOG(INFO) << "Done creating vocabulary.\n";

  LOG(INFO) << "Writing vocabulary to binary file " << fileName << "\n";
  std::ofstream out(fileName.c_str(),
                    std::ios_base::out | std::ios_base::binary);
  AD_CHECK(out.is_open());
  for (size_t i = 0; i < els.size(); ++i) {
    // 32 bits should be enough for len of string
    std::string_view word = els[i].first;
    uint32_t len = word.size();
    out.write((char*)&len, sizeof(len));
    out.write(word.data(), len);
    Id id = els[i].second;
    out.write((char*)&id, sizeof(id));
  }
  out.close();
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename) {
  ad_utility::HashMap<Id, Id> res;
  IdPairMMapVecView vec(mmapFilename);
  for (const auto [partialId, globalId] : vec) {
    res[partialId] = globalId;
  }
  return res;
}
