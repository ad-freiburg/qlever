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
        if (spec.parseInParallel_) {
          return std::make_unique<RdfAsyncParallelParser<InnerParser>>(
              executor(), spec, bufferSize_, encodedIriManager_, graph);
        }
        return std::make_unique<AsyncSerialParserAdapter>(
            executor(), std::make_unique<RdfStreamParser<InnerParser>>(
                            spec, bufferSize_, encodedIriManager_, graph));
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
  auto fileOrSpec = fileState_.withWriteLock([](FileState& state) {
    // Prefer the earliest-opened open file that can take another call.
    // `openFiles_` is in the order in which the files were opened.
    for (auto& file : state.openFiles_) {
      if (file->supportsConcurrentCalls_ || file->numCallsInFlight_ == 0) {
        ++file->numCallsInFlight_;
        return FileOrSpec{file, std::nullopt};
      }
    }
    // No open file has spare capacity; open the next unopened file, if there
    // is one. The actual opening happens outside of the lock, see below.
    if (auto spec = state.files_.get(); spec.has_value()) {
      return FileOrSpec{nullptr, std::move(spec)};
    }
    // No unopened files are left either. If there is no open file, every input
    // is exhausted.
    if (state.openFiles_.empty()) {
      return FileOrSpec{nullptr, std::nullopt};
    }
    // Otherwise every open file is serial and busy (a parallel file would have
    // been picked by the loop above); pick the one with the fewest calls in
    // flight. The call is then queued by that file's own
    // `AsyncSerialParserAdapter`, so no thread blocks.
    auto it = ql::ranges::min_element(state.openFiles_, {}, [](const auto& f) {
      return f->numCallsInFlight_;
    });
    ++(*it)->numCallsInFlight_;
    return FileOrSpec{*it, std::nullopt};
  });
  if (!fileOrSpec.specToOpen_.has_value()) {
    return std::move(fileOrSpec.file_);
  }
  // Construct the per-file parser (which opens the input file, and hence may
  // block) *without* holding the lock, and only then publish it. Note that
  // this means that concurrent calls may publish their files in a different
  // order than the one in which they took the specifications; the scheduling
  // policy only treats `openFiles_` as a preference order, so this is
  // harmless.
  const auto& spec = fileOrSpec.specToOpen_.value();
  auto file = std::make_shared<OpenFile>(
      OpenFile{makeFileParser(spec), spec.parseInParallel_, 1});
  fileState_.withWriteLock(
      [&file](FileState& state) { state.openFiles_.push_back(file); });
  return file;
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
    auto file = pickFile();
    if (file == nullptr) {
      co_return std::nullopt;
    }
    try {
      auto batch = co_await file->parser_->asyncGetBatch(net::use_awaitable);
      releaseFile(file, !batch.has_value());
      if (batch.has_value()) {
        co_return batch;
      }
      // The picked file turned out to be already exhausted; try again with
      // (possibly) another file.
      continue;
    } catch (...) {
      releaseFile(file, false);
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
