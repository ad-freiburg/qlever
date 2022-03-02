// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string_view>

namespace ad_utility::streams {

class StringSupplier {
 public:
  virtual std::string_view next() = 0;
  [[nodiscard]] virtual bool hasNext() const = 0;

  virtual ~StringSupplier() = default;
};

}  // namespace ad_utility::streams
