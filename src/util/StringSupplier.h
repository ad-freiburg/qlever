// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <boost/beast/http.hpp>
#include <string_view>

namespace ad_utility::streams {

class StringSupplier {
 public:
  virtual std::string_view next() = 0;
  [[nodiscard]] virtual bool hasNext() const = 0;

  constexpr virtual void prepareHttpHeaders(
      [[maybe_unused]] boost::beast::http::header<
          false, boost::beast::http::fields>& header) const {}
};

}  // namespace ad_utility::streams