// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <optional>
#include <string>
#include <utility>

// Forward declaration because of cyclic dependencies
// TODO<joka921> The coupling of the `Variable` with its `evaluate` methods
// is not very clean and should be refactored.
struct Context;
enum ContextRole : int;

class Variable {
 public:
  std::string _name;

  explicit Variable(std::string name);

  // TODO<joka921> There are several similar variants of this function across
  // the codebase. Unify them!
  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      const Context& context, [[maybe_unused]] ContextRole role) const;

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _name; }

  // ___________________________________________________________________________
  [[nodiscard]] const std::string& name() const { return _name; }

  // Needed for consistency with the `Alias` class.
  [[nodiscard]] const std::string& targetVariable() const { return _name; }

  bool operator==(const Variable&) const = default;

  // Make the type hashable for absl, see
  // https://abseil.io/docs/cpp/guides/hash.
  template <typename H>
  friend H AbslHashValue(H h, const Variable& v) {
    return H::combine(std::move(h), v._name);
  }
};
