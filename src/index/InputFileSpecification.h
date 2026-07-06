// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H
#define QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H

#include <boost/asio/any_io_executor.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include "parser/AsyncBlockSource.h"
#include "util/MemorySize/MemorySize.h"
#include "util/http/MediaTypes.h"

namespace qlever {

// An enum to distinguish between `Turtle` and `NQuad` files.
enum class Filetype { Turtle, NQuad };

// Convert a `MediaType` (e.g. as parsed from an HTTP `Content-Type` header,
// see `InputFileServer::filetypeFromContentType`) to the `Filetype` used for
// selecting an RDF parser. Returns `nullopt` for any `MediaType` that isn't a
// supported RDF input format (e.g. `json`, `csv`).
std::optional<Filetype> filetypeFromMediaType(ad_utility::MediaType mediaType);

// Specify a single input file or stream for the index builder. The source of
// bytes is either a filename or a factory that produces an `AsyncBlockSource`.
struct InputFileSpecification {
  // A factory that, when called, creates a ready-to-use `AsyncBlockSource`.
  // The `any_io_executor` is the executor on which I/O will be dispatched, the
  // `MemorySize` is the preferred blocksize, and the `string_view` is a
  // descriptor of the resource used for logging and debugging.
  using AsyncBlockSourceFactory =
      std::function<std::unique_ptr<qlever::parser::AsyncBlockSource>(
          const boost::asio::any_io_executor&, ad_utility::MemorySize,
          std::string_view)>;

  struct BufferFactoryAndDescription {
    AsyncBlockSourceFactory bufferFactory_;
    std::string description_;
  };
  // The byte source: either a filename or a factory.
  std::variant<std::string, BufferFactoryAndDescription> source_;

  Filetype filetype_;
  // All triples that don't have a dedicated graph (either because the input
  // format is N-Triples or Turtle, or because the corresponding line in the
  // N-Quads format has no explicit graph) will be stored in this graph. The
  // graph has to be specified without angle brackets. If set to `nullopt`, the
  // global default graph will be used.
  std::optional<std::string> defaultGraph_;

  // If set to `true`, then the parallel RDF parser will be used for this file.
  // This always works for N-Triples and N-Quads files, and for well-behaved
  // Turtle files with all prefixes at the beginning and no multiline literals.
  bool parseInParallel_ = false;

  // Remember if the value for parallel parsing was set explicitly (via the
  // command line).
  bool parseInParallelSetExplicitly_ = false;

  // Return the filename/description for the `source_`.
  const std::string& filename() const {
    return std::visit(
        [](const auto& arg) -> const auto& {
          if constexpr (ad_utility::isSimilar<std::string, decltype(arg)>) {
            return arg;
          } else {
            return arg.description_;
          }
        },
        source_);
  }

  // Create and return an `AsyncBlockSource` for this spec. For filename-based
  // specs, an `AsyncFileBlockSource` with the given `exec` and `blocksize` is
  // returned. For factory-based specs, the factory is called.
  std::unique_ptr<qlever::parser::AsyncBlockSource> makeAsyncBlockSource(
      const boost::asio::any_io_executor& exec,
      ad_utility::MemorySize blocksize) const {
    if (std::holds_alternative<std::string>(source_)) {
      return std::make_unique<qlever::parser::AsyncFileBlockSource>(
          exec, blocksize, std::get<std::string>(source_));
    }
    auto& [factory, description] =
        std::get<BufferFactoryAndDescription>(source_);
    return factory(exec, blocksize, description);
  }
};
}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H
