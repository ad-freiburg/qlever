// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RdfAsyncParallelParser.h"

#include <boost/asio/use_future.hpp>
#include <string>

#include "parser/Tokenizer.h"
#include "parser/TokenizerCtre.h"

// ____________________________________________________________________________
template <typename Parser>
RdfAsyncParallelParser<Parser>::RdfAsyncParallelParser(
    const boost::asio::any_io_executor& executor,
    const qlever::InputFileSpecification& spec,
    ad_utility::MemorySize blocksize,
    const EncodedIriManager* encodedIriManager,
    const TripleComponent& defaultGraphIri)
    : executor_{executor},
      state_{encodedIriManager, defaultGraphIri},
      blockSource_{executor, spec.makeAsyncBlockSource(executor, blocksize),
                   detail::findEndOfLastStatement,
                   std::string{detail::statementBoundaryDescription}},
      blockFetchPermit_{executor, 1} {
  // NOTE: The header is parsed eagerly and synchronously, so the `executor`
  // has to be running already (which it is for a `boost::asio::thread_pool`,
  // the intended use of this class).
  state_.parseHeader([this]() {
    return blockSource_.asyncGetNextBlock(boost::asio::use_future).get();
  });
}

template class RdfAsyncParallelParser<TurtleParser<Tokenizer>>;
template class RdfAsyncParallelParser<TurtleParser<TokenizerCtre>>;
template class RdfAsyncParallelParser<NQuadParser<Tokenizer>>;
template class RdfAsyncParallelParser<NQuadParser<TokenizerCtre>>;
