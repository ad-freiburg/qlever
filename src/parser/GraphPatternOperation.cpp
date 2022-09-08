//  Copyright 2022, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "parser/GraphPatternOperation.h"

#include "parser/ParsedQuery.h"
namespace parsedQuery {

void BasicGraphPattern::appendTriples(BasicGraphPattern pattern) {
  ad_utility::appendVector(_triples, std::move(pattern._triples));
}

// _____________________________________________________________________________
void GraphPatternOperation::toString(std::ostringstream& os,
                                     int indentation) const {
  for (int j = 1; j < indentation; ++j) os << "  ";

  visit([&os, indentation](auto&& arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, Optional>) {
      os << "OPTIONAL ";
      arg._child.toString(os, indentation);
    } else if constexpr (std::is_same_v<T, GroupGraphPattern>) {
      os << "GROUP ";
      arg._child.toString(os, indentation);
    } else if constexpr (std::is_same_v<T, Union>) {
      arg._child1.toString(os, indentation);
      os << " UNION ";
      arg._child2.toString(os, indentation);
    } else if constexpr (std::is_same_v<T, Subquery>) {
      // TODO<joka921> make the subquery a value-semantics type.
      os << arg.get().asString();
    } else if constexpr (std::is_same_v<T, Values>) {
      os << "VALUES (";
      for (const auto& v : arg._inlineValues._variables) {
        os << v << ' ';
      }
      os << ") ";

      for (const auto& v : arg._inlineValues._values) {
        os << "(";
        for (const auto& val : v) {
          os << val << ' ';
        }
        os << ')';
      }
    } else if constexpr (std::is_same_v<T, BasicGraphPattern>) {
      for (size_t i = 0; i + 1 < arg._triples.size(); ++i) {
        os << "\n";
        for (int j = 0; j < indentation; ++j) os << "  ";
        os << arg._triples[i].asString() << ',';
      }
      if (arg._triples.size() > 0) {
        os << "\n";
        for (int j = 0; j < indentation; ++j) os << "  ";
        os << arg._triples.back().asString();
      }

    } else if constexpr (std::is_same_v<T, Bind>) {
      os << "Some kind of BIND\n";
      // TODO<joka921> proper ToString (are they used for something?)
    } else if constexpr (std::is_same_v<T, Minus>) {
      os << "MINUS ";
      arg._child.toString(os, indentation);
    } else {
      static_assert(std::is_same_v<T, TransPath>);
      os << "TRANS PATH from " << arg._left << " to " << arg._right
         << " with at least " << arg._min << " and at most " << arg._max
         << " steps of ";
      arg._childGraphPattern.toString(os, indentation);
    }
  });
}
}  // namespace parsedQuery
