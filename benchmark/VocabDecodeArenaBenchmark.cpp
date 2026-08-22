// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>
//
// Isolate FSST decode-into-arena vs copy-into-arena. No Turtle, no HTTP.
// Layer 0 decodes RAM-resident compressed words. Layer 1 times
// CompressedVocabulary::lookupBatch after a warmup pass.

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "backports/memory_resource.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"

using Vocab = CompressedVocabulary<VocabularyInternalExternal>;

namespace {

struct Options {
  std::string vocab;
  std::string arm{"copy"};
  int layer = 0;
  size_t batchSize = 1024;
  size_t warmupBatches = 100;
  size_t timedBatches = 10000;
  size_t startIndex = 0;
};

void usage(const char* argv0) {
  std::cerr
      << "Usage: " << argv0
      << " --vocab <prefix> --arm copy|into --layer 0|1"
         " [--batch-size 1024] [--warmup-batches 100] [--timed-batches 10000]"
         " [--start-index 0]\n"
         "  prefix is the path passed to CompressedVocabulary::open\n"
         "  (Wikidata: .../wikidata.vocabulary).\n";
}

bool parseArgs(int argc, char** argv, Options& o) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto need = [&](const char* name) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << name << "\n";
        return {};
      }
      return argv[++i];
    };
    if (a == "--vocab") {
      o.vocab = need("--vocab");
    } else if (a == "--arm") {
      o.arm = need("--arm");
    } else if (a == "--layer") {
      o.layer = std::stoi(need("--layer"));
    } else if (a == "--batch-size") {
      o.batchSize = static_cast<size_t>(std::stoull(need("--batch-size")));
    } else if (a == "--warmup-batches") {
      o.warmupBatches =
          static_cast<size_t>(std::stoull(need("--warmup-batches")));
    } else if (a == "--timed-batches") {
      o.timedBatches =
          static_cast<size_t>(std::stoull(need("--timed-batches")));
    } else if (a == "--start-index") {
      o.startIndex = static_cast<size_t>(std::stoull(need("--start-index")));
    } else if (a == "-h" || a == "--help") {
      usage(argv[0]);
      return false;
    } else {
      std::cerr << "unknown argument: " << a << "\n";
      usage(argv[0]);
      return false;
    }
  }
  if (o.vocab.empty() || (o.arm != "copy" && o.arm != "into") ||
      (o.layer != 0 && o.layer != 1) || o.batchSize == 0 ||
      o.timedBatches == 0) {
    usage(argv[0]);
    return false;
  }
  return true;
}

struct FixtureWord {
  std::string compressed;
  size_t decoderIdx;
};

uint64_t mixView(uint64_t checksum, std::string_view v) {
  checksum = checksum * 1315423911ull + v.size();
  if (!v.empty()) {
    checksum += static_cast<unsigned char>(v.front());
    checksum += static_cast<unsigned char>(v.back()) << 8;
  }
  return checksum;
}

struct RusageSnap {
  rusage ru;
};

RusageSnap snapUsage() {
  RusageSnap s{};
  AD_CONTRACT_CHECK(getrusage(RUSAGE_SELF, &s.ru) == 0);
  return s;
}

double sec(const timeval& t) {
  return static_cast<double>(t.tv_sec) +
         static_cast<double>(t.tv_usec) / 1'000'000.0;
}

void adviseWillNeed(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return;
  }
  struct stat st {};
  if (fstat(fd, &st) == 0 && st.st_size > 0) {
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_WILLNEED);
  }
  ::close(fd);
}

struct DecodeStats {
  uint64_t checksum = 0;
  uint64_t bytesUsed = 0;
  uint64_t bytesBound = 0;
};

DecodeStats runCopyBatch(const Vocab& vocab,
                         ql::span<const FixtureWord> words) {
  DecodeStats s;
  auto buffer = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  std::vector<std::string_view> views;
  views.reserve(words.size());
  const auto& wrapper = vocab.compressionWrapper();
  for (const auto& w : words) {
    std::string decompressed = wrapper.decompress(w.compressed, w.decoderIdx);
    const size_t n = decompressed.size();
    const size_t bound =
        wrapper.maxDecompressedSize(w.compressed, w.decoderIdx);
    s.bytesUsed += n;
    s.bytesBound += bound;
    if (n == 0) {
      views.emplace_back();
      continue;
    }
    auto* mem = static_cast<char*>(buffer->allocate(n));
    std::memcpy(mem, decompressed.data(), n);
    views.emplace_back(mem, n);
    s.checksum = mixView(s.checksum, views.back());
  }
  volatile uint64_t keep = s.checksum;
  (void)keep;
  (void)buffer;
  return s;
}

DecodeStats runIntoBatch(const Vocab& vocab,
                         ql::span<const FixtureWord> words) {
  DecodeStats s;
  auto buffer = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  std::vector<std::string_view> views;
  views.reserve(words.size());
  std::string scratch;
  const auto& wrapper = vocab.compressionWrapper();
  for (const auto& w : words) {
    const size_t bound =
        wrapper.maxDecompressedSize(w.compressed, w.decoderIdx);
    s.bytesBound += bound;
    if (bound == 0) {
      views.emplace_back();
      continue;
    }
    auto* mem = static_cast<char*>(buffer->allocate(bound));
    const size_t n = wrapper.decompressInto(
        w.compressed, w.decoderIdx, ql::span<char>{mem, bound}, scratch);
    AD_CORRECTNESS_CHECK(n <= bound);
    s.bytesUsed += n;
    views.emplace_back(mem, n);
    s.checksum = mixView(s.checksum, views.back());
  }
  volatile uint64_t keep = s.checksum;
  (void)keep;
  return s;
}

DecodeStats runLookupBatch(const Vocab& vocab, ql::span<const size_t> indices) {
  DecodeStats s;
  auto result = vocab.lookupBatch(indices);
  for (std::string_view v : *result) {
    s.bytesUsed += v.size();
    s.checksum = mixView(s.checksum, v);
  }
  volatile uint64_t keep = s.checksum;
  (void)keep;
  return s;
}

std::vector<size_t> makeIndices(size_t start, size_t batch, size_t vocabSize) {
  std::vector<size_t> idx;
  idx.reserve(batch);
  for (size_t i = 0; i < batch; ++i) {
    idx.push_back((start + i) % vocabSize);
  }
  return idx;
}

}  // namespace

int main(int argc, char** argv) {
  Options opt;
  if (!parseArgs(argc, argv, opt)) {
    return 2;
  }

  Vocab vocab;
  vocab.open(opt.vocab);
  const size_t vocabSize = vocab.size();
  AD_CONTRACT_CHECK(vocabSize >= opt.batchSize);

  const size_t totalBatches = opt.warmupBatches + opt.timedBatches;
  adviseWillNeed(opt.vocab + ".words");

  std::vector<std::vector<FixtureWord>> fixture;
  std::vector<std::vector<size_t>> indexBatches;
  indexBatches.reserve(totalBatches);
  for (size_t b = 0; b < totalBatches; ++b) {
    const size_t start = (opt.startIndex + b * opt.batchSize) % vocabSize;
    indexBatches.push_back(makeIndices(start, opt.batchSize, vocabSize));
  }

  if (opt.layer == 0) {
    fixture.resize(totalBatches);
    for (size_t b = 0; b < totalBatches; ++b) {
      auto compressed = vocab.lookupCompressedBatch(indexBatches[b]);
      AD_CORRECTNESS_CHECK(compressed->size() == opt.batchSize);
      fixture[b].reserve(opt.batchSize);
      for (size_t i = 0; i < opt.batchSize; ++i) {
        FixtureWord w;
        w.compressed = std::string{(*compressed)[i]};
        w.decoderIdx = vocab.decoderIndex(indexBatches[b][i]);
        fixture[b].push_back(std::move(w));
      }
    }
  } else {
    for (size_t b = 0; b < opt.warmupBatches; ++b) {
      (void)runLookupBatch(vocab, indexBatches[b]);
    }
  }

  if (opt.layer == 0) {
    for (size_t b = 0; b < opt.warmupBatches; ++b) {
      if (opt.arm == "copy") {
        (void)runCopyBatch(vocab, fixture[b]);
      } else {
        (void)runIntoBatch(vocab, fixture[b]);
      }
    }
  }

  const auto ru0 = snapUsage();
  const auto t0 = std::chrono::steady_clock::now();
  DecodeStats agg;
  if (opt.layer == 0) {
    for (size_t b = 0; b < opt.timedBatches; ++b) {
      const size_t i = opt.warmupBatches + b;
      DecodeStats one = (opt.arm == "copy") ? runCopyBatch(vocab, fixture[i])
                                            : runIntoBatch(vocab, fixture[i]);
      agg.checksum ^= one.checksum;
      agg.bytesUsed += one.bytesUsed;
      agg.bytesBound += one.bytesBound;
    }
  } else {
    for (size_t b = 0; b < opt.timedBatches; ++b) {
      const size_t i = opt.warmupBatches + b;
      DecodeStats one = runLookupBatch(vocab, indexBatches[i]);
      agg.checksum ^= one.checksum;
      agg.bytesUsed += one.bytesUsed;
    }
  }
  const auto t1 = std::chrono::steady_clock::now();
  const auto ru1 = snapUsage();

  const double wall = std::chrono::duration<double>(t1 - t0).count();
  const double utime = sec(ru1.ru.ru_utime) - sec(ru0.ru.ru_utime);
  const double stime = sec(ru1.ru.ru_stime) - sec(ru0.ru.ru_stime);
  const long minflt = ru1.ru.ru_minflt - ru0.ru.ru_minflt;
  const long majflt = ru1.ru.ru_majflt - ru0.ru.ru_majflt;
  const uint64_t nWords = opt.timedBatches * opt.batchSize;
  const double nsPerWord = wall * 1e9 / static_cast<double>(nWords);
  const double usedBound = agg.bytesBound == 0
                               ? 0.0
                               : static_cast<double>(agg.bytesUsed) /
                                     static_cast<double>(agg.bytesBound);

  std::cout << "arm\tlayer\twall_s\tutime_s\tstime_s\tminflt\tmajflt\t"
               "ns_per_word\tbytes_used\tbytes_bound\tused_over_bound\t"
               "checksum\twords\n";
  const char* armOut = (opt.layer == 1) ? "lookupBatch" : opt.arm.c_str();
  std::cout << armOut << '\t' << opt.layer << '\t' << wall << '\t' << utime
            << '\t' << stime << '\t' << minflt << '\t' << majflt << '\t'
            << nsPerWord << '\t' << agg.bytesUsed << '\t' << agg.bytesBound
            << '\t' << usedBound << '\t' << agg.checksum << '\t' << nWords
            << '\n';
  return 0;
}
