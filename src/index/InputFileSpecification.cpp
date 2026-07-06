// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/InputFileSpecification.h"

namespace qlever {

// _____________________________________________________________________________
std::optional<Filetype> filetypeFromMediaType(ad_utility::MediaType mediaType) {
  switch (mediaType) {
    case ad_utility::MediaType::turtle:
    case ad_utility::MediaType::ntriples:
      return Filetype::Turtle;
    case ad_utility::MediaType::nquads:
      return Filetype::NQuad;
    default:
      return std::nullopt;
  }
}

}  // namespace qlever
