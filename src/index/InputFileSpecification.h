// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <kalmbach@cs.uni-freiburg.de>
//
#pragma once

#include <optional>
#include <string>
namespace qlever {

// An enum to distinguish between `Turtle` and `NQuad` files.
enum class Filetype { Turtle, NQuad };

// Specify a single input file or stream for the index builder.
struct InputFileSpecification {
  std::string filename_;
  Filetype filetype_;
  // All triples that don't have a dedicated graph (either because the input
  // format is NTriples or Turtle, or because the corresponding line in the
  // NQuad format had no explicit graph) will be stored in this graph. If set to
  // `nullopt`, the global default graph will be used.
  std::optional<std::string> defaultGraph_;

  // If set to true, then the parallel RDF parser will be used for this file.
  // This always works for NTriples and NQuad files, and for well-behaved Turtle
  // files with all prefixes at the beginning and no multiline literals.
  bool parseInParallel_ = false;

  bool operator==(const InputFileSpecification&) const = default;
};
}  // namespace qlever
