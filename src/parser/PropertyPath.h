//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_PROPERTYPATH_H
#define QLEVER_SRC_PARSER_PROPERTYPATH_H

#include <cstdint>
#include <functional>
#include <variant>
#include <vector>

#include "backports/concepts.h"
#include "parser/Iri.h"
#include "util/TypeTraits.h"

class PropertyPath {
 private:
  struct BasicPath {
    ad_utility::triple_component::Iri iri_;

    bool operator==(const BasicPath&) const = default;
  };

 public:
  enum class Modifier : std::uint8_t {
    SEQUENCE,
    ALTERNATIVE,
    INVERSE,
    NEGATED
  };

 private:
  struct ModifiedPath {
    std::vector<PropertyPath> children_;
    Modifier modifier_;

    bool operator==(const ModifiedPath&) const = default;
  };

  class MinMaxPath {
   public:
    size_t min_;
    size_t max_;
    std::unique_ptr<PropertyPath> child_;

    MinMaxPath(size_t min, size_t max, std::unique_ptr<PropertyPath> child)
        : min_{min}, max_{max}, child_{std::move(child)} {}

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

    bool operator==(const MinMaxPath&) const;
  };

  std::variant<BasicPath, ModifiedPath, MinMaxPath> path_;

  explicit PropertyPath(std::variant<BasicPath, ModifiedPath, MinMaxPath> path);

 public:
  PropertyPath(PropertyPath&&) = default;
  PropertyPath(const PropertyPath&) = default;
  PropertyPath& operator=(PropertyPath&&) = default;
  PropertyPath& operator=(const PropertyPath&) = default;

  static PropertyPath fromIri(ad_utility::triple_component::Iri iri);

  static PropertyPath makeWithLength(PropertyPath child, size_t min,
                                     size_t max);

  static PropertyPath makeAlternative(std::vector<PropertyPath> children);

  static PropertyPath makeSequence(std::vector<PropertyPath> children);

  static PropertyPath makeInverse(PropertyPath child);

  static PropertyPath makeNegated(std::vector<PropertyPath> children);

  bool operator==(const PropertyPath& other) const = default;

  void writeToStream(std::ostream& out) const;

  [[nodiscard]] std::string asString() const;

  const ad_utility::triple_component::Iri& getIri() const;

  bool isIri() const;

  const PropertyPath* getInvertedChild() const;

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

#endif  // QLEVER_SRC_PARSER_PROPERTYPATH_H
