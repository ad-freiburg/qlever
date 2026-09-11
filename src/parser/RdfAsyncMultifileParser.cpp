// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RdfAsyncMultifileParser.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <memory>
#include <optional>
#include <utility>

#include "backports/algorithm.h"
#include "engine/CallFixedSize.h"
#include "parser/AsyncSerialParserAdapter.h"
#include "parser/RdfAsyncParallelParser.h"
#include "parser/Tokenizer.h"
#include "parser/TokenizerCtre.h"

namespace net = boost::asio;

// _____________________________________________________________________________
RdfAsyncMultifileParser::RdfAsyncMultifileParser(
    const ql::any_io_executor& executor,
    ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files,
    const EncodedIriManager* encodedIriManager,
    ad_utility::MemorySize bufferSize, bool useRelaxedParsing)
    : AsyncRdfParserBase{executor},
      encodedIriManager_{encodedIriManager},
      bufferSize_{bufferSize},
      useRelaxedParsing_{useRelaxedParsing},
      fileState_{FileState{std::move(files)}} {}

namespace {
// Create an asynchronous parser for the file in `spec`, which is parsed by the
// given `InnerParser` (for example `TurtleParser<Tokenizer>`).
//
// NOTE: This is a free function template and not simply a part of the lambda in
// `makeFileParser` below, because GCC rejects a nested lambda that uses a type
// which depends on a parameter of the enclosing lambda (see the type alias
// `InnerParser` there).
template <typename InnerParser>
std::unique_ptr<AsyncRdfParserBase> makeFileParserForInnerParser(
    const ql::any_io_executor& executor,
    const qlever::InputFileSpecification& spec,
    ad_utility::MemorySize bufferSize,
    const EncodedIriManager* encodedIriManager, TripleComponent graph) {
  if (spec.parseInParallel_) {
    return std::make_unique<RdfAsyncParallelParser<InnerParser>>(
        executor, spec, bufferSize, encodedIriManager, graph);
  }
  // NOTE: The inner parser is created lazily (see `AsyncSerialParserAdapter`),
  // so that this function stays cheap.
  return std::make_unique<AsyncSerialParserAdapter>(
      executor,
      [spec, bufferSize, encodedIriManager,
       graph = std::move(graph)]() -> std::unique_ptr<RdfParserBase> {
        return std::make_unique<RdfStreamParser<InnerParser>>(
            spec, bufferSize, encodedIriManager, graph);
      });
}
}  // namespace

// _____________________________________________________________________________
std::unique_ptr<AsyncRdfParserBase> RdfAsyncMultifileParser::makeFileParser(
    const qlever::InputFileSpecification& spec) const {
  auto graph = defaultGraphFromSpec(spec);
  auto makeParserImpl = ad_utility::ApplyAsValueIdentity{
      [this, &spec, &graph](
          auto useRelaxed,
          auto isTurtleInput) -> std::unique_ptr<AsyncRdfParserBase> {
        using TokenizerT =
            std::conditional_t<useRelaxed == 1, TokenizerCtre, Tokenizer>;
        using InnerParser =
            std::conditional_t<isTurtleInput == 1, TurtleParser<TokenizerT>,
                               NQuadParser<TokenizerT>>;
        return makeFileParserForInnerParser<InnerParser>(
            executor(), spec, bufferSize_, encodedIriManager_, graph);
      }};
  // The call to `callFixedSize` lifts the runtime booleans to compile-time
  // integers, exactly like `makeSingleRdfParser` in `RdfParser.cpp` (which
  // this function mirrors for the asynchronous parsers).
  return ad_utility::callFixedSize(
      std::array{useRelaxedParsing_ ? 1 : 0,
                 spec.filetype_ == qlever::Filetype::Turtle ? 1 : 0},
      makeParserImpl);
}

// _____________________________________________________________________________
std::shared_ptr<RdfAsyncMultifileParser::OpenFile>
RdfAsyncMultifileParser::pickFile() {
  // NOTE: The whole scheduling decision, including the opening of a new file,
  // has to happen inside a single critical section. Were a file opened outside
  // of the lock, then a concurrent call could see neither an unopened nor an
  // open file and wrongly conclude that all inputs are exhausted, although it
  // should simply have waited for that file. This is only affordable because
  // `makeFileParser` is cheap, see there.
  return fileState_.withWriteLock([this](FileState& state)
                                      -> std::shared_ptr<OpenFile> {
    // Step 1: prefer an open file that currently has no call in flight at all.
    auto idle = ql::ranges::find_if(state.openFiles_, [](const auto& f) {
      return f->numCallsInFlight_ == 0;
    });
    if (idle != state.openFiles_.end()) {
      ++(*idle)->numCallsInFlight_;
      return *idle;
    }
    // Step 2: no open file is idle; open the next unopened file, if there is
    // one.
    if (auto spec = state.files_.get(); spec.has_value()) {
      auto file = std::make_shared<OpenFile>(OpenFile{
          makeFileParser(spec.value()), spec.value().parseInParallel_, 1});
      state.openFiles_.push_back(file);
      return file;
    }
    // Step 3: no unopened files are left either, so if there is no open file,
    // every input is exhausted.
    if (state.openFiles_.empty()) {
      return nullptr;
    }
    // Step 4: every open file is busy; pick the one with the fewest calls in
    // flight, preferring parallel files. For a serial file, the call is then
    // queued by that file's own `AsyncSerialParserAdapter`, so no thread
    // blocks either way.
    auto it = ql::ranges::min_element(state.openFiles_, {}, [](const auto& f) {
      return std::pair{!f->supportsConcurrentCalls_, f->numCallsInFlight_};
    });
    ++(*it)->numCallsInFlight_;
    return *it;
  });
}

// _____________________________________________________________________________
void RdfAsyncMultifileParser::releaseFile(const std::shared_ptr<OpenFile>& file,
                                          bool wasExhausted) {
  fileState_.withWriteLock([&file, wasExhausted](FileState& state) {
    --file->numCallsInFlight_;
    if (!wasExhausted) {
      return;
    }
    // This file is exhausted; remove it from the open list, unless another
    // concurrent call for the very same file already did so.
    auto it = ql::ranges::find(state.openFiles_, file);
    if (it != state.openFiles_.end()) {
      state.openFiles_.erase(it);
    }
  });
}

// _____________________________________________________________________________
net::awaitable<RdfAsyncMultifileParser::OptionalTriples>
RdfAsyncMultifileParser::getBatchCoroutine() {
  while (true) {
    // Another call has failed in the meantime, so there is nothing left to
    // parse.
    if (errorWasEncountered_.load()) {
      co_return std::nullopt;
    }
    // NOTE: `pickFile()` is called inside the `try` because it may throw (the
    // constructor of `RdfAsyncParallelParser` opens its input file), and such
    // an error has to be reported with the same semantics as a parse error.
    std::shared_ptr<OpenFile> file;
    try {
      file = pickFile();
      if (file == nullptr) {
        co_return std::nullopt;
      }
      auto batch = co_await file->parser_->asyncGetBatch(net::use_awaitable);
      releaseFile(file, !batch.has_value());
      if (batch.has_value()) {
        co_return batch;
      }
      // The picked file turned out to be already exhausted; try again with
      // (possibly) another file.
      continue;
    } catch (...) {
      if (file != nullptr) {
        releaseFile(file, false);
      }
      // Only the first error is propagated to its caller, all subsequent
      // calls get a clean end of the input instead, see the class comment.
      if (!errorWasEncountered_.exchange(true)) {
        throw;
      }
      co_return std::nullopt;
    }
  }
}

// _____________________________________________________________________________
void RdfAsyncMultifileParser::asyncGetBatchImpl(Handler handler) {
  net::co_spawn(executor(), getBatchCoroutine(), std::move(handler));
}
