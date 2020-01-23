// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#pragma once

#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include "./VocabularyGenerator.h"

#include <parallel/algorithm>
#include "../util/Conversions.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./ConstantsIndexCreation.h"
#include "./Vocabulary.h"

// ___________________________________________________________________
template <class Comp>
VocabularyMerger::VocMergeRes VocabularyMerger::mergeVocabulary(const std::string& basename,
                                                                size_t numFiles, Comp comp) {
  // we sort alphabetically by the token according to the comparator that was
  // given to us

  auto queueCompare = [&comp](const QueueWord& p1, const QueueWord& p2) {
    // if p1 is smaller (alphabetically)
    // _comp will return false if called like this
    // and the priority queue will thus emit p1 first
    return comp(p2._value, p1._value);
  };

  std::vector<std::ifstream> infiles;

  _outfile.open(basename + ".vocabulary");
  AD_CHECK(_outfile.is_open());
  _outfileExternal.open(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
  AD_CHECK(_outfileExternal.is_open());
  std::vector<bool> endOfFile(numFiles, false);

  // Priority queue for the k-way merge
  std::priority_queue<QueueWord, std::vector<QueueWord>, decltype(queueCompare)> queue(
      queueCompare);

  // open and prepare all infiles and mmap output vectors
  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME + std::to_string(i));
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
      queue.push(QueueWord(std::move(word), i, id));
      endOfFile[i] = false;
    }
  }

  std::vector<QueueWord> sortedBuffer;
  sortedBuffer.reserve(_bufferSize);

  std::future<void> writeFuture;

  // start k-way merge
  while (!queue.empty()) {
    // accumulated the globally ordered queue words in a buffer.
    sortedBuffer.push_back(std::move(queue.top()));
    queue.pop();
    auto i = sortedBuffer.back()._partialFileId;

    if (sortedBuffer.size() >= _bufferSize) {
      // asynchronously write the next batch of sorted
      // queue words
      auto writeTask = [this, buf = std::move(sortedBuffer)]() {
        this->writeQueueWordsToIdVec(buf);
      };
      sortedBuffer.clear();
      sortedBuffer.reserve(_bufferSize);
      // wait for the last batch

      if (writeFuture.valid()) {
        LOG(TRACE) << "Waiting for the asynchronous write to finish\n";
        writeFuture.get();
      }
      writeFuture = std::async(writeTask);
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
      queue.push(QueueWord(std::move(word), i, id));
      endOfFile[i] = false;
    }
  }

  // wait for the active write tasks to finish
  if (writeFuture.valid()) {
    writeFuture.get();
  }

  // Handle remaining words in the buffer
  if (!sortedBuffer.empty()) {
    writeQueueWordsToIdVec(sortedBuffer);
  }
  VocMergeRes result;
  result._numWordsTotal = _totalWritten;
  result._langPredLowerBound = _langPredLowerBound;
  result._langPredUpperBound = _langPredUpperBound;

  // completely reset all the inner state
  clear();
  return result;
}

// ________________________________________________________________________________
void VocabularyMerger::writeQueueWordsToIdVec(const std::vector<QueueWord>& buffer) {
  LOG(TRACE) << "Start writing a batch of merged words\n";

  // smaller grained buffer for the actual inner write
  auto bufSize = _bufferSize / 5;
  std::future<void> writeFut;
  std::vector<std::pair<size_t, std::pair<size_t, size_t>>> writeBuf;
  writeBuf.reserve(bufSize);
  // avoid duplicates
  for (auto& top : buffer) {
    if (top._value != _lastWritten) {
      _lastWritten = top._value;

      // TODO<optimization> If we aim to further speed this up, we could
      // order all the write requests to _outfile _externalOutfile and all the
      // idVecs to have a more useful external access pattern.

      // write the new word to the vocabulary
      if (_lastWritten < EXTERNALIZED_LITERALS_PREFIX) {
        _outfile << _lastWritten << '\n';
      } else {
        // we have to strip the externalization character again
        auto& c = _lastWritten[0];
        switch (c) {
          case EXTERNALIZED_LITERALS_PREFIX_CHAR:
            c = '"';
            break;
          case EXTERNALIZED_ENTITIES_PREFIX_CHAR:
            c = '<';
            break;
          default:
            LOG(ERROR) << "Illegal Externalization character met in vocabulary merging. This "
                          "should never happen\n";
            AD_CHECK(false)
        }
        _outfileExternal << _lastWritten << '\n';
      }

      // write id to corresponding vec
      writeBuf.emplace_back(top._partialFileId, std::make_pair(top._partialWordId, _totalWritten));

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
      writeBuf.emplace_back(top._partialFileId, std::make_pair(top._partialWordId, minusOne));
    }

    if (writeBuf.size() >= bufSize) {
      auto task = [this, buf = std::move(writeBuf)]() { this->doActualWrite(buf); };
      if (writeFut.valid()) {
        writeFut.get();
      }
      writeFut = std::async(task);
      writeBuf.clear();
      writeBuf.reserve(bufSize);
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
template <class Pred>
void writePartialIdMapToBinaryFileForMerging(std::shared_ptr<const Index::ItemMap> map,
                                             const string& fileName, Pred pred) {
  LOG(INFO) << "Creating partial vocabulary from set ...\n";
  std::vector<std::pair<string, Id>> els;
  els.reserve(map->size());
  els.insert(begin(els), begin(*map), end(*map));
  LOG(INFO) << "... sorting ...\n";

  auto sort = [&els](const auto& pred) {
    if constexpr (USE_PARALLEL_SORT) {
      __gnu_parallel::sort(begin(els), end(els), pred,
                           __gnu_parallel::parallel_tag(NUM_SORT_THREADS));
    } else {
      std::sort(begin(els), end(els), pred);
    }
  };

  const auto comp = [&pred](const auto& a, const auto& b) { return pred(a.first, b.first); };

  sort(comp);

  LOG(INFO) << "Done creating vocabulary.\n";
  LOG(INFO) << "Writing vocabulary to binary file " << fileName << "\n";
  std::ofstream out(fileName.c_str(), std::ios_base::out | std::ios_base::binary);
  AD_CHECK(out.is_open());
  for (const auto& el : els) {
    // 32 bits should be enough for len of string
    std::string_view word = el.first;
    uint32_t len = word.size();
    out.write((char*)&len, sizeof(len));
    out.write(word.data(), len);
    Id id = el.second;
    out.write((char*)&id, sizeof(id));
  }
  out.close();
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(const string& mmapFilename) {
  ad_utility::HashMap<Id, Id> res;
  IdPairMMapVecView vec(mmapFilename);
  for (const auto [partialId, globalId] : vec) {
    res[partialId] = globalId;
  }
  return res;
}
