//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Florian Kramer <florian.kramer@mail.uni-freiburg.de>
//           Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_PROPERTYPATH_H
#define QLEVER_SRC_PARSER_PROPERTYPATH_H

#include <cstdint>
#include <variant>
#include <vector>

#include "backports/concepts.h"
#include "backports/three_way_comparison.h"
#include "rdfTypes/Iri.h"
#include "util/Exception.h"
#include "util/OverloadCallOperator.h"
#include "util/TypeTraits.h"

// Class representing property paths. This includes simple IRIs as a baseline,
// alternative paths, sequence paths, inverse paths, negated paths, and paths
// with minimum and maximum lengths.
class PropertyPath {
 public:
  // Enum representing the different modifiers that can be applied to a property
  // path.
  enum class Modifier : std::uint8_t {
    SEQUENCE,
    ALTERNATIVE,
    INVERSE,
    NEGATED
  };

 private:
  // Represent a modified path that can have multiple children and a modifier.
  // This is used to represent sequence paths, alternative paths, inverse paths,
  // and negated paths.
  struct ModifiedPath {
    std::vector<PropertyPath> children_;
    Modifier modifier_;

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(ModifiedPath, children_,
                                                modifier_)

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
        : min_{min}, max_{max}, child_{std::move(child)} {
      AD_CONTRACT_CHECK(
          min_ <= max_,
          "The minimum length must not be greater than the maximum "
          "length in property paths.");
    }

    // Default move and copy constructors and assignment operators.
    MinMaxPath(MinMaxPath&&) = default;
    MinMaxPath(const MinMaxPath& other)
        : min_{other.min_},
          max_{other.max_},
          child_{std::make_unique<PropertyPath>(*other.child_)} {}
    MinMaxPath& operator=(MinMaxPath&&) = default;
    // This cannot be defaulted, because `std::unique_ptr` doesn't have a copy
    // assignment operator.
    MinMaxPath& operator=(const MinMaxPath& other);

    // Make sure `child_` is compared by value, not by pointer.
    bool operator==(const MinMaxPath&) const;
  };

  // The main content of this object.
  std::variant<ad_utility::triple_component::Iri, ModifiedPath, MinMaxPath>
      path_;

  // Private constructor that initializes the path with a variant type.
  explicit PropertyPath(
      std::variant<ad_utility::triple_component::Iri, ModifiedPath, MinMaxPath>
          path);

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

  // Create an alternative property path with the given children.
  static PropertyPath makeAlternative(std::vector<PropertyPath> children);

  // Create a sequence property path with the given children.
  static PropertyPath makeSequence(std::vector<PropertyPath> children);

  // Create an inverse property path with the given child.
  static PropertyPath makeInverse(PropertyPath child);

  // Create a negated property path with the given children, for multiple
  // children the semantics are equivalent to `!(<a> | <b>)`, applying the union
  // before the negation.
  static PropertyPath makeNegated(std::vector<PropertyPath> children);

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(PropertyPath, path_)

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
  std::optional<std::reference_wrapper<const PropertyPath>>
  getChildOfInvertedPath() const;

  // Process the path with the given functions. The functions are called
  // depending on which internal representation this instance has.
  CPP_template(typename T, typename IriFunc, typename ModifiedPathFunc,
               typename MinMaxPathFunc)(
      requires ad_utility::InvocableWithConvertibleReturnType<
          IriFunc, T, const ad_utility::triple_component::Iri&>
          CPP_and ad_utility::InvocableWithConvertibleReturnType<
              ModifiedPathFunc, T, const std::vector<PropertyPath>&, Modifier>
              CPP_and ad_utility::InvocableWithConvertibleReturnType<
                  MinMaxPathFunc, T, const PropertyPath&, size_t, size_t>) T
      handlePath(const IriFunc& iriFunc,
                 const ModifiedPathFunc& modifiedPathFunc,
                 const MinMaxPathFunc& minMaxPathFunc) const {
    return std::visit(
        ad_utility::OverloadCallOperator{
            iriFunc,
            [&modifiedPathFunc](const ModifiedPath& path) {
              return modifiedPathFunc(path.children_, path.modifier_);
            },
            [&minMaxPathFunc](const MinMaxPath& path) {
              return minMaxPathFunc(*path.child_, path.min_, path.max_);
            }},
        path_);
  }
};

// Allow the `PropertyPath` to be printed to an output stream.
std::ostream& operator<<(std::ostream& out, const PropertyPath& p);

#endif  // QLEVER_SRC_PARSER_PROPERTYPATH_H
