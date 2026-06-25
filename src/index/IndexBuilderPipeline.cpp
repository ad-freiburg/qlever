// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/IndexBuilderPipeline.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/system/error_code.hpp>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <variant>

#include "index/ConstantsIndexBuilding.h"
#include "index/IndexImpl.h"
#include "index/VocabularyMerger.h"
#include "util/CachingMemoryResource.h"
#include "util/Log.h"
#include "util/Timer.h"

namespace qlever::indexBuilder {

namespace net = boost::asio;
using ec_t = boost::system::error_code;
using TripleVec = IndexBuilderPipeline::TripleVec;
using IdTripleArray = IndexBuilderPipeline::IdTripleArray;

namespace {

// Helper for `std::visit`.
template <class... Ts>
struct Overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

// One chunk of work sent from the dispatcher to a worker.
struct Chunk {
  size_t batchIdx_;
  std::vector<TurtleTriple> triples_;
};

// End-of-batch sentinel sent from the dispatcher to a worker.
struct Sentinel {
  size_t batchIdx_;
};

using WorkMsg = std::variant<Chunk, Sentinel>;

using ParserCh = net::experimental::concurrent_channel<void(
    ec_t, std::vector<TurtleTriple>)>;
using WorkCh = net::experimental::concurrent_channel<void(ec_t, WorkMsg)>;

}  // namespace

struct IndexBuilderPipeline::Impl {
  // Resolve the number of worker threads: the value requested by the caller,
  // or the hardware concurrency if unspecified, clamped to at least 1.
  static size_t resolveNumWorkers(std::optional<size_t> numThreads) {
    return std::max<size_t>(
        1, numThreads.value_or(std::thread::hardware_concurrency()));
  }

  // The dedicated thread pool that runs every callback in this pipeline. It is
  // deliberately the FIRST member so that it has the longest lifetime and is
  // destroyed LAST: all the other members below (the channels and strands, as
  // well as the `parser_` whose own channels and strands are bound to this
  // pool's executor) reference this pool's execution context, so they must be
  // destroyed while it is still alive.
  net::thread_pool pool_;

  // Number of workers (one `ItemMapManager` each); also the number of slots
  // that the gatherer needs to collect per partial-vocab batch.
  size_t numWorkers_;

  // Constants and references injected from the caller.
  IndexImpl* index_;
  std::shared_ptr<qlever::parser::AsyncMultifileParser> parser_;
  size_t linesPerPartial_;
  std::string onDiskBase_;
  ad_utility::Synchronized<std::unique_ptr<TripleVec>>* idTriples_;
  std::atomic<size_t>* numHasWordTriples_;
  ad_utility::ProgressBar* progressBar_;
  size_t* numTriplesParsedRef_;

  // Memory pool shared by all per-worker hash maps; matches the original
  // single-pool-per-build-pass usage.
  ad_utility::CachingMemoryResource cachingMemoryResource_;

  // Channels.
  ParserCh parserCh_;
  std::vector<std::unique_ptr<WorkCh>> workCh_;

  // Strands.
  net::strand<net::any_io_executor> parserStrand_;
  net::strand<net::any_io_executor> dispatcherStrand_;
  std::vector<net::strand<net::any_io_executor>> workerStrand_;
  net::strand<net::any_io_executor> gathererStrand_;

  // Per-worker state. Only touched by the worker `w` (via `workerStrand_[w]`).
  struct WorkerState {
    // The map for the current partial-vocab batch (lazily created on first
    // chunk; reset to `nullopt` after the corresponding `Sentinel` ships it
    // to the gatherer).
    std::optional<ItemMapManager> map_;
  };
  std::vector<std::unique_ptr<WorkerState>> workerState_;

  // Per-batch state, owned by the gatherer (touched only on
  // `gathererStrand_`).
  struct BatchState {
    // ID-mapped triples accumulated for this batch.
    std::vector<IdTripleArray> localWriter_;
    // ItemMapAndBuffers shipped by workers for this batch.
    std::vector<ItemMapAndBuffer> maps_;
    // Number of workers that have signaled "no more chunks for this batch".
    size_t workersDone_ = 0;
    // Number of workers that have shipped their map for this batch.
    size_t mapsReceived_ = 0;
    // Output count, used as `numTriplesPerPartialVocab[batchIdx]`.
    size_t actualSize_ = 0;
  };
  std::map<size_t, BatchState> batches_;

  // Next batch index that the gatherer will dispatch to finalize. Finalize
  // dispatch is strictly in batchIdx order so the global `TripleVec` writes
  // (gated by `Synchronized::withWriteLockAndOrdered`) make forward progress
  // without blocking each other.
  size_t nextFinalizeBatch_ = 0;
  size_t inflightFinalizes_ = 0;
  // Set by the dispatcher once it has decided how many partial-vocab batches
  // there are in total. Until then, the gatherer cannot tell whether to
  // signal completion.
  std::optional<size_t> totalBatches_;
  // Count of partial-vocab batches whose finalize has completed.
  size_t finalizedBatches_ = 0;

  // Final per-batch output counts. Filled by finalize callbacks; read after
  // `run()` returns.
  std::map<size_t, size_t> numTriplesPerBatch_;

  // Exception propagation across stages: only the first one is kept.
  std::mutex exceptionMutex_;
  std::exception_ptr firstException_;

  // Set when the pipeline finishes (success or after exception capture).
  std::promise<void> donePromise_;
  bool donePromiseSet_ = false;

  // Cached timer-startup flag (the original code only starts the parsing
  // timer after the very first triple has been delivered).
  bool timerStarted_ = false;

  // Round-robin worker selection. The dispatcher is single-threaded so we
  // don't need atomics here.
  size_t nextWorker_ = 0;

  Impl(IndexImpl* index,
       std::shared_ptr<qlever::parser::AsyncMultifileParser> parser,
       size_t linesPerPartial, std::string onDiskBase,
       ad_utility::Synchronized<std::unique_ptr<TripleVec>>* idTriples,
       std::atomic<size_t>* numHasWordTriples,
       ad_utility::ProgressBar* progressBar, size_t* numTriplesParsedRef,
       std::optional<size_t> numThreads)
      : pool_{resolveNumWorkers(numThreads)},
        numWorkers_{resolveNumWorkers(numThreads)},
        index_{index},
        parser_{std::move(parser)},
        linesPerPartial_{linesPerPartial},
        onDiskBase_{std::move(onDiskBase)},
        idTriples_{idTriples},
        numHasWordTriples_{numHasWordTriples},
        progressBar_{progressBar},
        numTriplesParsedRef_{numTriplesParsedRef},
        parserCh_{pool_.get_executor(), /*max_buffer_size=*/4},
        parserStrand_{net::make_strand(pool_.get_executor())},
        dispatcherStrand_{net::make_strand(pool_.get_executor())},
        gathererStrand_{net::make_strand(pool_.get_executor())} {
    workCh_.reserve(numWorkers_);
    workerStrand_.reserve(numWorkers_);
    workerState_.reserve(numWorkers_);
    for (size_t w = 0; w < numWorkers_; ++w) {
      workCh_.push_back(std::make_unique<WorkCh>(pool_.get_executor(),
                                                 /*max_buffer_size=*/2));
      workerStrand_.push_back(net::make_strand(pool_.get_executor()));
      workerState_.push_back(std::make_unique<WorkerState>());
    }
  }

  // Capture an exception and arrange for `run()` to rethrow it. Idempotent.
  void captureException(std::exception_ptr ep) {
    std::lock_guard l{exceptionMutex_};
    if (!firstException_) {
      firstException_ = std::move(ep);
    }
  }

  // Set the completion promise exactly once.
  void signalDone() {
    if (!donePromiseSet_) {
      donePromiseSet_ = true;
      donePromise_.set_value();
    }
  }

  // ============================================================
  // Parser-feeder: pulls batches from `parser_->getBatch()` and pushes them
  // into `parserCh_`. The actual `getBatch()` call may block, so it runs on
  // a pool thread.
  // ============================================================
  void startParserFeeder() {
    net::post(parserStrand_, [this] { parserFeederStep(); });
  }

  void parserFeederStep() {
    // Ask the async parser for the next batch directly. The parser itself
    // dispatches its work on the pool executor; on completion the handler
    // returns to our parser strand, sends to `parserCh_`, and re-arms.
    parser_->asyncGetNextBatch(
        pool_.get_executor(),
        [this](std::exception_ptr ep,
               std::optional<std::vector<TurtleTriple>> opt) {
          if (ep) {
            captureException(ep);
            parserCh_.close();
            return;
          }
          if (!opt) {
            parserCh_.close();
            return;
          }
          parserCh_.async_send(
              ec_t{}, std::move(*opt),
              net::bind_executor(parserStrand_, [this](ec_t ec) {
                if (ec) return;
                parserFeederStep();
              }));
        });
  }

  // ============================================================
  // Dispatcher: pulls a parser batch from `parserCh_`, routes it to a
  // worker as a `Chunk`, tracks the per-partial-vocab triple count, and
  // broadcasts `Sentinel`s at batch boundaries. On EOF, broadcasts a final
  // sentinel (if there is a non-empty tail) and closes all `workCh_`.
  // ============================================================
  size_t batchIdx_ = 0;
  size_t currentBatchInputCount_ = 0;

  void startDispatcher() {
    net::post(dispatcherStrand_, [this] { dispatcherRecv(); });
  }

  void dispatcherRecv() {
    parserCh_.async_receive(net::bind_executor(
        dispatcherStrand_, [this](ec_t ec, std::vector<TurtleTriple> batch) {
          if (ec) {
            // Parser exhausted: finalize the in-progress batch (if any) and
            // close worker channels.
            dispatcherEof();
            return;
          }
          dispatchOneBatch(std::move(batch));
        }));
  }

  void dispatchOneBatch(std::vector<TurtleTriple> batch) {
    if (!timerStarted_) {
      progressBar_->getTimer().cont();
      timerStarted_ = true;
    }
    size_t size = batch.size();
    *numTriplesParsedRef_ += size;
    if (progressBar_->update()) {
      AD_LOG_INFO << progressBar_->getProgressString() << std::flush;
    }

    size_t w = nextWorker_;
    nextWorker_ = (nextWorker_ + 1) % numWorkers_;
    currentBatchInputCount_ += size;
    size_t myBatchIdx = batchIdx_;
    bool boundaryReached = currentBatchInputCount_ >= linesPerPartial_;

    workCh_[w]->async_send(
        ec_t{}, Chunk{myBatchIdx, std::move(batch)},
        net::bind_executor(dispatcherStrand_, [this, boundaryReached](ec_t ec) {
          if (ec) {
            captureException(std::make_exception_ptr(
                std::runtime_error{"Failed to send chunk to worker."}));
            return;
          }
          if (boundaryReached) {
            broadcastSentinel(batchIdx_, [this] {
              ++batchIdx_;
              currentBatchInputCount_ = 0;
              dispatcherRecv();
            });
          } else {
            dispatcherRecv();
          }
        }));
  }

  // Broadcast a `Sentinel` to every worker. `onAllSent` is invoked on the
  // dispatcher strand once all sends have completed.
  template <typename F>
  void broadcastSentinel(size_t myBatchIdx, F onAllSent) {
    auto remaining = std::make_shared<std::atomic<size_t>>(numWorkers_);
    auto sharedOnDone =
        std::make_shared<std::function<void()>>(std::move(onAllSent));
    for (size_t w = 0; w < numWorkers_; ++w) {
      workCh_[w]->async_send(
          ec_t{}, Sentinel{myBatchIdx},
          net::bind_executor(
              dispatcherStrand_, [this, remaining, sharedOnDone](ec_t ec) {
                if (ec) {
                  captureException(std::make_exception_ptr(std::runtime_error{
                      "Failed to send sentinel to worker."}));
                }
                if (remaining->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                  (*sharedOnDone)();
                }
              }));
    }
  }

  void dispatcherEof() {
    // Finalize: if the in-progress batch has any triples, broadcast a final
    // sentinel for it; otherwise, the total number of batches is simply
    // `batchIdx_`.
    auto finishWorkChannels = [this](size_t totalBatches) {
      // Inform the gatherer that no more batches will arrive.
      net::dispatch(gathererStrand_, [this, totalBatches] {
        totalBatches_ = totalBatches;
        tryComplete();
        tryDispatchFinalize();
      });
      for (auto& ch : workCh_) {
        ch->close();
      }
    };
    if (currentBatchInputCount_ > 0) {
      broadcastSentinel(batchIdx_, [this, finishWorkChannels] {
        size_t total = batchIdx_ + 1;
        ++batchIdx_;
        currentBatchInputCount_ = 0;
        finishWorkChannels(total);
      });
    } else {
      finishWorkChannels(batchIdx_);
    }
  }

  // ============================================================
  // Worker: pulls `WorkMsg`s from its private channel, transforms triples
  // through its dedicated `ItemMapManager` (no locks), and ships results
  // and the map itself to the gatherer.
  // ============================================================
  void startWorker(size_t w) {
    net::post(workerStrand_[w], [this, w] { workerRecv(w); });
  }

  void workerRecv(size_t w) {
    workCh_[w]->async_receive(
        net::bind_executor(workerStrand_[w], [this, w](ec_t ec, WorkMsg msg) {
          if (ec) {
            // Channel closed (e.g., parser exhausted). Worker exits.
            return;
          }
          try {
            std::visit(
                Overloaded{[&](Chunk& c) {
                             processChunk(w, c.batchIdx_,
                                          std::move(c.triples_));
                           },
                           [&](Sentinel& s) { shipMap(w, s.batchIdx_); }},
                msg);
          } catch (...) {
            captureException(std::current_exception());
          }
          // Re-arm on the worker strand to keep handler stack shallow.
          net::post(workerStrand_[w], [this, w] { workerRecv(w); });
        }));
  }

  // Lazily construct `workerState_[w]->map_` for the given `batchIdx`.
  ItemMapManager& ensureMap(size_t w, size_t /*batchIdx*/) {
    auto& state = *workerState_[w];
    if (!state.map_.has_value()) {
      // Each worker reserves a private id range. The minId only needs to be
      // unique across workers within one partial-vocab batch (the partial
      // ids are immediately compressed by `createInternalMapping`), so it
      // does not depend on `batchIdx`.
      const TripleComponentComparator* cmp =
          &index_->getVocab().getCaseComparator();
      ItemAlloc alloc{&cachingMemoryResource_};
      state.map_.emplace(w * 100 * linesPerPartial_, cmp, alloc);
      // Reserve a generous size, matching the original code (which divided
      // `5 * linesPerPartial` across `NUM_PARALLEL_ITEM_MAPS` slots).
      state.map_->map_.map_.reserve(5 * linesPerPartial_ / numWorkers_);
    }
    return *state.map_;
  }

  void processChunk(size_t w, size_t batchIdx,
                    std::vector<TurtleTriple> triples) {
    auto& map = ensureMap(w, batchIdx);
    auto convert = makeTripleToIdsConverter(
        map, index_,
        index_->addHasWordTriples() ? numHasWordTriples_ : nullptr);
    std::vector<IdTripleArray> idTriples;
    idTriples.reserve(triples.size());
    for (auto& triple : triples) {
      auto result = convert(std::move(triple));
      for (auto& t : result) {
        idTriples.push_back(t);
      }
    }
    size_t outSize = idTriples.size();
    net::dispatch(gathererStrand_, [this, batchIdx, ids = std::move(idTriples),
                                    outSize]() mutable {
      auto& s = batches_[batchIdx];
      s.actualSize_ += outSize;
      s.localWriter_.insert(s.localWriter_.end(),
                            std::make_move_iterator(ids.begin()),
                            std::make_move_iterator(ids.end()));
    });
  }

  void shipMap(size_t w, size_t batchIdx) {
    // Lazily create an empty map if this worker received no chunks for this
    // batch (only a `Sentinel`). The merge step in the finalize stage will
    // dedupe the special-id entries that every `ItemMapManager` adds in its
    // constructor.
    ensureMap(w, batchIdx);
    auto& state = *workerState_[w];
    ItemMapAndBuffer mapAndBuffer = std::move(*state.map_).moveMap();
    state.map_.reset();
    net::dispatch(gathererStrand_, [this, batchIdx,
                                    m = std::make_shared<ItemMapAndBuffer>(
                                        std::move(mapAndBuffer))]() mutable {
      auto& s = batches_[batchIdx];
      s.maps_.push_back(std::move(*m));
      ++s.mapsReceived_;
      ++s.workersDone_;
      tryDispatchFinalize();
    });
  }

  // ============================================================
  // Gatherer / finalize coordinator: dispatched on `gathererStrand_`.
  // ============================================================

  // Called after any state-change in `batches_` that may have made the
  // next batch ready.
  void tryDispatchFinalize() {
    while (inflightFinalizes_ < NUM_INFLIGHT_PARTIAL_VOCAB_WRITES) {
      auto it = batches_.find(nextFinalizeBatch_);
      if (it == batches_.end()) return;
      auto& s = it->second;
      if (s.mapsReceived_ < numWorkers_ || s.workersDone_ < numWorkers_) {
        return;
      }
      // This batch is ready. Move its state out and start the finalize.
      size_t batchIdx = nextFinalizeBatch_;
      ++nextFinalizeBatch_;
      ++inflightFinalizes_;
      // Snapshot output triple count for the result vector.
      numTriplesPerBatch_[batchIdx] = s.actualSize_;
      auto maps =
          std::make_shared<std::vector<ItemMapAndBuffer>>(std::move(s.maps_));
      auto localWriter = std::make_shared<std::vector<IdTripleArray>>(
          std::move(s.localWriter_));
      batches_.erase(it);
      net::post(pool_, [this, batchIdx, maps = std::move(maps),
                        localWriter = std::move(localWriter)]() mutable {
        runFinalize(batchIdx, std::move(*maps), std::move(*localWriter));
      });
    }
  }

  void runFinalize(size_t batchIdx, std::vector<ItemMapAndBuffer> maps,
                   std::vector<IdTripleArray> localIds) {
    using namespace ad_utility::vocabulary_merger;
    try {
      AD_LOG_DEBUG << "Finalizing partial vocab batch " << batchIdx
                   << std::endl;
      ItemVec vec = [&]() {
        ad_utility::TimeBlockAndLog l{"vocab maps to vector"};
        return vocabMapsToVector(maps);
      }();
      {
        ad_utility::TimeBlockAndLog l{"sorting by unicode order"};
        sortVocabVector(
            &vec,
            [&c = index_->getVocab().getCaseComparator()](const auto& a,
                                                          const auto& b) {
              return c.isLessInTotalWithExternalFlag(
                  a.first, a.second.isExternal(), b.first,
                  b.second.isExternal());
            },
            true);
      }
      auto mapping = [&]() {
        ad_utility::TimeBlockAndLog l{"creating internal mapping"};
        return createInternalMapping(vec);
      }();
      // Adjacent duplicates now have the same Id, so it suffices to compare
      // those.
      {
        ad_utility::TimeBlockAndLog l{"removing duplicates from the input"};
        vec.erase(std::unique(vec.begin(), vec.end(),
                              [](const auto& a, const auto& b) {
                                return a.second.id() == b.second.id();
                              }),
                  vec.end());
      }
      // Write the id-mapped local triples to the shared global vector.
      // `withWriteLockAndOrdered` enforces order across batches.
      auto writeTriplesFuture =
          std::async(std::launch::async, [this, batchIdx, &localIds, &mapping] {
            idTriples_->withWriteLockAndOrdered(
                [&](auto& writerPtr) {
                  writeMappedIdsToExtVec(localIds, mapping, &writerPtr);
                },
                batchIdx);
          });
      {
        ad_utility::TimeBlockAndLog l{"write partial vocabulary"};
        std::string partialFilename =
            absl::StrCat(onDiskBase_, PARTIAL_VOCAB_WORDS_INFIX, batchIdx);
        writePartialVocabularyToFile(vec, partialFilename);
      }
      AD_LOG_TRACE << "Finished writing the partial vocabulary" << std::endl;
      vec.clear();
      {
        ad_utility::TimeBlockAndLog l{"writing to global file"};
        writeTriplesFuture.get();
      }
    } catch (...) {
      captureException(std::current_exception());
    }
    // Notify the gatherer that this finalize is done.
    net::dispatch(gathererStrand_, [this] {
      --inflightFinalizes_;
      ++finalizedBatches_;
      tryDispatchFinalize();
      tryComplete();
    });
  }

  // Called whenever finalize completion or dispatcher-EOF state changes
  // might mean the whole pipeline is done.
  void tryComplete() {
    if (totalBatches_.has_value() && finalizedBatches_ == *totalBatches_ &&
        inflightFinalizes_ == 0) {
      signalDone();
    }
  }

  // ============================================================
  // Top-level driver.
  // ============================================================
  std::vector<size_t> run() {
    startParserFeeder();
    startDispatcher();
    for (size_t w = 0; w < numWorkers_; ++w) {
      startWorker(w);
    }
    donePromise_.get_future().wait();

    // The pipeline has now consumed the parser to EOF, so all of the parser's
    // work is done. Tear the parser down *here*, while the pool is still alive:
    // its channels and strands are bound to `pool_`'s executor, so they must
    // not outlive the pool. `cancel()` closes the (already drained) channels;
    // `reset()` then destroys the parser (the pipeline is its sole owner) so
    // that its asio state is released against a still-valid execution context.
    if (parser_) {
      parser_->cancel();
      parser_.reset();
    }

    pool_.stop();
    pool_.join();

    if (firstException_) {
      std::rethrow_exception(firstException_);
    }

    // Convert the per-batch map into a dense vector in batchIdx order.
    std::vector<size_t> result;
    result.reserve(numTriplesPerBatch_.size());
    for (auto& [k, v] : numTriplesPerBatch_) {
      result.push_back(v);
    }
    return result;
  }
};

// ____________________________________________________________________________
IndexBuilderPipeline::IndexBuilderPipeline(
    IndexImpl* index,
    std::shared_ptr<qlever::parser::AsyncMultifileParser> parser,
    size_t linesPerPartial, std::string onDiskBase,
    ad_utility::Synchronized<std::unique_ptr<TripleVec>>* idTriples,
    std::atomic<size_t>* numHasWordTriples,
    ad_utility::ProgressBar* progressBar, size_t* numTriplesParsedRef,
    std::optional<size_t> numThreads)
    : impl_{std::make_unique<Impl>(index, std::move(parser), linesPerPartial,
                                   std::move(onDiskBase), idTriples,
                                   numHasWordTriples, progressBar,
                                   numTriplesParsedRef, numThreads)} {}

// ____________________________________________________________________________
IndexBuilderPipeline::~IndexBuilderPipeline() = default;

// ____________________________________________________________________________
std::vector<size_t> IndexBuilderPipeline::run() { return impl_->run(); }

}  // namespace qlever::indexBuilder
