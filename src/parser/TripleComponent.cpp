// Copyright 2018 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "parser/TripleComponent.h"

#include "absl/strings/str_cat.h"

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& stream, const TripleComponent& obj) {
  std::visit(
      [&stream]<typename T>(const T& value) -> void {
        if constexpr (std::is_same_v<T, Variable>) {
          stream << value.name();
        } else if constexpr (std::is_same_v<T, TripleComponent::UNDEF>) {
          stream << "UNDEF";
        } else {
          stream << value;
        }
      },
      obj._variant);
  return stream;
}

// ____________________________________________________________________________
[[nodiscard]] std::string TripleComponent::toString() const {
  std::stringstream stream;
  stream << *this;
  return std::move(stream).str();
}
