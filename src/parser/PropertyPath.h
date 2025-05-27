//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_PROPERTYPATH_H
#define QLEVER_SRC_PARSER_PROPERTYPATH_H

#include <cstdint>
#include <variant>
#include <vector>

#include "backports/concepts.h"
#include "parser/Iri.h"
#include "util/OverloadCallOperator.h"
#include "util/TypeTraits.h"

// Class representing complex graph patterns. This includes basic patterns as a
// baseline, alternative patterns, sequence patterns, inverse patterns, negated
// patterns, and property paths with minimum and maximum lengths.
class PropertyPath {
 private:
  // Represent a basic path with just a single IRI.
  struct BasicPath {
    ad_utility::triple_component::Iri iri_;

    bool operator==(const BasicPath&) const = default;
  };

 public:
  // Enum representing the different modifiers that can be applied to a graph
  // pattern.
  enum class Modifier : std::uint8_t {
    SEQUENCE,
    ALTERNATIVE,
    INVERSE,
    NEGATED
  };

 private:
  // Represent a modified path that can have multiple children and a modifier.
  // This is used to represent sequence patterns, alternative patterns, inverse
  // patterns, and negated patterns.
  struct ModifiedPath {
    std::vector<PropertyPath> children_;
    Modifier modifier_;

    bool operator==(const ModifiedPath&) const = default;

    void writeToStream(std::ostream& out) const;
  };

  // Represent a property path with a minimum and maximum length. The underlying
  // path is the only child of this path.
  class MinMaxPath {
   public:
    size_t min_;
    size_t max_;
    // We can't use `ad_utility::CopyableUniquePtr` here, because the type is
    // not complete at this point, so the assertion that checks for copy
    // semantics fails.
    std::unique_ptr<PropertyPath> child_;

    // Construct an instance of MinMaxPath with the given minimum and maximum
    // length and the child path.
    MinMaxPath(size_t min, size_t max, std::unique_ptr<PropertyPath> child)
        : min_{min}, max_{max}, child_{std::move(child)} {}

    // Default move and copy constructors and assignment operators.
    MinMaxPath(MinMaxPath&&) = default;
    MinMaxPath(const MinMaxPath& other)
        : min_{other.min_},
          max_{other.max_},
          child_{std::make_unique<PropertyPath>(*other.child_)} {}
    MinMaxPath& operator=(MinMaxPath&&) = default;
    MinMaxPath& operator=(const MinMaxPath& other) {
      min_ = other.min_;
      max_ = other.max_;
      child_ = std::make_unique<PropertyPath>(*other.child_);
      return *this;
    }

    // Make sure `child_` is compared by value, not by pointer.
    bool operator==(const MinMaxPath&) const;
  };

  // The main content of this object.
  std::variant<BasicPath, ModifiedPath, MinMaxPath> path_;

  // Private constructor that initializes the path with a variant type.
  explicit PropertyPath(std::variant<BasicPath, ModifiedPath, MinMaxPath> path);

 public:
  // Default copy and move constructors and assignment operators.
  PropertyPath(PropertyPath&&) = default;
  PropertyPath(const PropertyPath&) = default;
  PropertyPath& operator=(PropertyPath&&) = default;
  PropertyPath& operator=(const PropertyPath&) = default;

  // Create a basic PropertyPath from a basic IRI.
  static PropertyPath fromIri(ad_utility::triple_component::Iri iri);

  // Create a PropertyPath with a minimum and maximum length.
  static PropertyPath makeWithLength(PropertyPath child, size_t min,
                                     size_t max);

  // Create an alternative pattern with the given children.
  static PropertyPath makeAlternative(std::vector<PropertyPath> children);

  // Create a sequence pattern with the given children.
  static PropertyPath makeSequence(std::vector<PropertyPath> children);

  // Create an inverse pattern with the given child.
  static PropertyPath makeInverse(PropertyPath child);

  // Create a negated property path with the given children.
  static PropertyPath makeNegated(std::vector<PropertyPath> children);

  bool operator==(const PropertyPath& other) const = default;

  // Serialize this object into an output stream.
  void writeToStream(std::ostream& out) const;

  // Serialize this object into a string representation.
  [[nodiscard]] std::string asString() const;

  // Acquire the IRI of the path if it is a basic path. If the path is not a
  // basic path, this function will throw an assertion error.
  const ad_utility::triple_component::Iri& getIri() const;

  // Check if the path is a basic path with an IRI. Return true if it is, false
  // otherwise.
  bool isIri() const;

  // If the path is a modified path with an inverse modifier, return the pointer
  // to its only child. Otherwise, return nullptr.
  const PropertyPath* getInvertedChild() const;

  // Process the path with the given functions. The functions are called
  // depending on which internal representation this instance has.
  CPP_template(typename T, typename Func1, typename Func2, typename Func3)(
      requires ad_utility::InvocableWithConvertibleReturnType<
          Func1, T, const ad_utility::triple_component::Iri&>
          CPP_and ad_utility::InvocableWithConvertibleReturnType<
              Func2, T, const std::vector<PropertyPath>&, Modifier>
              CPP_and ad_utility::InvocableWithConvertibleReturnType<
                  Func3, T, const PropertyPath&, size_t, size_t>) T
      handlePath(const Func1& func1, const Func2& func2,
                 const Func3& func3) const {
    return std::visit(
        ad_utility::OverloadCallOperator{
            [&func1](const BasicPath& path) { return func1(path.iri_); },
            [&func2](const ModifiedPath& path) {
              return func2(path.children_, path.modifier_);
            },
            [&func3](const MinMaxPath& path) {
              return func3(*path.child_, path.min_, path.max_);
            }},
        path_);
  }
};

// Allow the `PropertyPath` to be printed to an output stream.
std::ostream& operator<<(std::ostream& out, const PropertyPath& p);

#endif  // QLEVER_SRC_PARSER_PROPERTYPATH_H
