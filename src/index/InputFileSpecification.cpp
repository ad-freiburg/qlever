// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/InputFileSpecification.h"

namespace qlever {

// _____________________________________________________________________________
std::optional<Filetype> filetypeFromMediaType(ad_utility::MediaType mediaType) {
  using enum ad_utility::MediaType;
  switch (mediaType) {
    case turtle:
    case ntriples:
      return Filetype::Turtle;
    case nquads:
      return Filetype::NQuad;
    default:
      return std::nullopt;
  }
}

}  // namespace qlever
