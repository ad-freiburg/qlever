// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "ParsedQuery.h"

#include <absl/strings/str_split.h>

#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "./RdfEscaping.h"
#include "absl/strings/str_join.h"

using std::string;
using std::vector;

// _____________________________________________________________________________
string ParsedQuery::asString() const {
  std::ostringstream os;

  // PREFIX
  os << "PREFIX: {";
  for (size_t i = 0; i < _prefixes.size(); ++i) {
    os << "\n\t" << _prefixes[i].asString();
    if (i + 1 < _prefixes.size()) {
      os << ',';
    }
  }
  os << "\n}";

  bool usesSelect = hasSelectClause();
  bool usesAsterisk = usesSelect && selectClause().isAsterisk();

  if (usesSelect) {
    const auto& selectClause = this->selectClause();
    // SELECT
    os << "\nSELECT: {\n\t";
    // TODO<joka921> is this needed?
    /*
    os <<
    absl::StrJoin(selectClause._varsAndAliasesOrAsterisk.getSelectedVariables(),
                        ", ");
                        */
    os << "\n}";

    // ALIASES
    os << "\nALIASES: {\n\t";
    if (!usesAsterisk) {
      for (const auto& alias : selectClause.getAliases()) {
        os << alias._expression.getDescriptor() << "\n\t";
      }
      os << "{";
    }
  } else if (hasConstructClause()) {
    const auto& constructClause = this->constructClause();
    os << "\n CONSTRUCT {\n\t";
    for (const auto& triple : constructClause) {
      os << triple[0].toSparql();
      os << ' ';
      os << triple[1].toSparql();
      os << ' ';
      os << triple[2].toSparql();
      os << " .\n";
    }
    os << "}";
  }

  // WHERE
  os << "\nWHERE: \n";
  _rootGraphPattern.toString(os, 1);

  os << "\nLIMIT: " << (_limitOffset._limit);
  os << "\nTEXTLIMIT: " << (_limitOffset._textLimit);
  os << "\nOFFSET: " << (_limitOffset._offset);
  if (usesSelect) {
    const auto& selectClause = this->selectClause();
    os << "\nDISTINCT modifier is " << (selectClause._distinct ? "" : "not ")
       << "present.";
    os << "\nREDUCED modifier is " << (selectClause._reduced ? "" : "not ")
       << "present.";
  }
  os << "\nORDER BY: ";
  if (_orderBy.empty()) {
    os << "not specified";
  } else {
    for (auto& key : _orderBy) {
      os << key.variable_ << (key.isDescending_ ? " (DESC)" : " (ASC)") << "\t";
    }
  }
  os << "\n";
  return std::move(os).str();
}

// _____________________________________________________________________________
string SparqlPrefix::asString() const {
  std::ostringstream os;
  os << "{" << _prefix << ": " << _uri << "}";
  return std::move(os).str();
}

// _____________________________________________________________________________
string SparqlTriple::asString() const {
  std::ostringstream os;
  os << "{s: " << _s << ", p: " << _p << ", o: " << _o << "}";
  return std::move(os).str();
}

// _____________________________________________________________________________
string SparqlFilter::asString() const {
  std::ostringstream os;
  os << "FILTER(" << _lhs;
  switch (_type) {
    case EQ:
      os << " < ";
      break;
    case NE:
      os << " != ";
      break;
    case LT:
      os << " < ";
      break;
    case LE:
      os << " <= ";
      break;
    case GT:
      os << " > ";
      break;
    case GE:
      os << " >= ";
      break;
    case LANG_MATCHES:
      os << " LANG_MATCHES ";
      break;
    case PREFIX:
      os << " PREFIX ";
      break;
    case SparqlFilter::REGEX:
      os << " REGEX ";
      if (_regexIgnoreCase) {
        os << "ignoring case ";
      }
      break;
  }
  os << _rhs << ")";
  return std::move(os).str();
}

// _____________________________________________________________________________
void ParsedQuery::expandPrefixes() {
  ad_utility::HashMap<string, string> prefixMap;
  prefixMap["ql"] = "<QLever-internal-function/>";
  for (const auto& p : _prefixes) {
    prefixMap[p._prefix] = p._uri;
  }

  for (auto& f : _rootGraphPattern._filters) {
    expandPrefix(f._lhs, prefixMap);
    // TODO<joka921>: proper type system for variable/regex/iri/etc
    if (f._type != SparqlFilter::REGEX) {
      expandPrefix(f._rhs, prefixMap);
    }
  }

  vector<GraphPattern*> graphPatterns;
  graphPatterns.push_back(&_rootGraphPattern);
  // Traverse the graph pattern tree using dfs expanding the prefixes in every
  // pattern.
  while (!graphPatterns.empty()) {
    GraphPattern* pattern = graphPatterns.back();
    graphPatterns.pop_back();
    for (GraphPatternOperation& p : pattern->_children) {
      p.visit([&graphPatterns, &prefixMap = std::as_const(prefixMap),
               this](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, GraphPatternOperation::Subquery>) {
          arg._subquery._prefixes = _prefixes;
          arg._subquery.expandPrefixes();
        } else if constexpr (std::is_same_v<T,
                                            GraphPatternOperation::TransPath>) {
          AD_FAIL();
          // we may never be in an transitive path here or a
          // subquery?TODO<joka921, verify by florian> at least this
          // shoudn't have worked before
        } else if constexpr (std::is_same_v<T,
                                            GraphPatternOperation::Optional> ||
                             std::is_same_v<
                                 T, GraphPatternOperation::GroupGraphPattern> ||
                             std::is_same_v<T, GraphPatternOperation::Minus>) {
          graphPatterns.push_back(&arg._child);
        } else if constexpr (std::is_same_v<T, GraphPatternOperation::Union>) {
          graphPatterns.push_back(&arg._child1);
          graphPatterns.push_back(&arg._child2);
        } else if constexpr (std::is_same_v<T, GraphPatternOperation::Values>) {
          for (auto& row : arg._inlineValues._values) {
            for (std::string& value : row) {
              expandPrefix(value, prefixMap);
            }
          }

        } else if constexpr (std::is_same_v<T, GraphPatternOperation::Bind>) {
          for (auto ptr : arg.strings()) {
            expandPrefix(*ptr, prefixMap);
          }
        } else {
          static_assert(
              std::is_same_v<T, GraphPatternOperation::BasicGraphPattern>);
          for (auto& trip : arg._whereClauseTriples) {
            if (trip._s.isString()) {
              expandPrefix(trip._s.getString(), prefixMap);
            }
            expandPrefix(trip._p, prefixMap);
            if (trip._p._operation == PropertyPath::Operation::IRI &&
                trip._p._iri.find("in-context") != string::npos) {
              if (trip._o.isString()) {
                std::string& str = trip._o.getString();
                std::vector<std::string> tokens = absl::StrSplit(str, ' ');
                str = "";
                for (size_t i = 0; i < tokens.size(); ++i) {
                  expandPrefix(tokens[i], prefixMap);
                  str += tokens[i];
                  if (i + 1 < tokens.size()) {
                    str += " ";
                  }
                }
                trip._o = str;
              }
            } else {
              if (trip._o.isString()) {
                std::string& str = trip._o.getString();
                expandPrefix(str, prefixMap);
              }
            }
          }
        }
      });
    }
  }
}

// _____________________________________________________________________________
void ParsedQuery::expandPrefix(
    PropertyPath& item, const ad_utility::HashMap<string, string>& prefixMap) {
  // Use dfs to process all leaves of the proprety path tree.
  std::vector<PropertyPath*> to_process;
  to_process.push_back(&item);
  while (!to_process.empty()) {
    PropertyPath* p = to_process.back();
    to_process.pop_back();
    if (p->_operation == PropertyPath::Operation::IRI) {
      expandPrefix(p->_iri, prefixMap);
    } else {
      for (PropertyPath& c : p->_children) {
        to_process.push_back(&c);
      }
    }
  }
}

// _____________________________________________________________________________
void ParsedQuery::expandPrefix(
    string& item, const ad_utility::HashMap<string, string>& prefixMap) {
  if (!item.starts_with("?") && !item.starts_with("<")) {
    std::optional<string> langtag = std::nullopt;
    if (item.starts_with("@")) {
      auto secondPos = item.find('@', 1);
      if (secondPos == string::npos) {
        throw ParseException(
            "langtaged predicates must have form @lang@ActualPredicate. Second "
            "@ is missing in " +
            item);
      }
      langtag = item.substr(1, secondPos - 1);
      item = item.substr(secondPos + 1);
    }

    size_t i = item.rfind(':');
    size_t from = item.find("^^");
    if (from == string::npos) {
      from = 0;
    } else {
      from += 2;
    }
    if (i != string::npos && i >= from &&
        prefixMap.contains(item.substr(from, i - from))) {
      string prefixUri = prefixMap.at(item.substr(from, i - from));
      // Note that substr(0, 0) yields the empty string.
      item = item.substr(0, from) + prefixUri.substr(0, prefixUri.size() - 1) +
             item.substr(i + 1) + '>';
      item = RdfEscaping::unescapePrefixedIri(item);
    }
    if (langtag) {
      item =
          ad_utility::convertToLanguageTaggedPredicate(item, langtag.value());
    }
  }
}

void ParsedQuery::merge(const ParsedQuery& p) {
  _prefixes.insert(_prefixes.begin(), p._prefixes.begin(), p._prefixes.end());

  auto& children = _rootGraphPattern._children;
  auto& otherChildren = p._rootGraphPattern._children;
  children.insert(children.end(), otherChildren.begin(), otherChildren.end());

  // update the ids
  _numGraphPatterns = 0;
  _rootGraphPattern.recomputeIds(&_numGraphPatterns);
}

// _____________________________________________________________________________
void ParsedQuery::GraphPattern::toString(std::ostringstream& os,
                                         int indentation) const {
  for (int j = 1; j < indentation; ++j) os << "  ";
  os << "{";
  for (size_t i = 0; i + 1 < _filters.size(); ++i) {
    os << "\n";
    for (int j = 0; j < indentation; ++j) os << "  ";
    os << _filters[i].asString() << ',';
  }
  if (_filters.size() > 0) {
    os << "\n";
    for (int j = 0; j < indentation; ++j) os << "  ";
    os << _filters.back().asString();
  }
  for (const GraphPatternOperation& child : _children) {
    os << "\n";
    child.toString(os, indentation + 1);
  }
  os << "\n";
  for (int j = 1; j < indentation; ++j) os << "  ";
  os << "}";
}

// _____________________________________________________________________________
void ParsedQuery::GraphPattern::recomputeIds(size_t* id_count) {
  bool allocatedIdCounter = false;
  if (id_count == nullptr) {
    id_count = new size_t(0);
    allocatedIdCounter = true;
  }
  _id = *id_count;
  (*id_count)++;
  for (auto& op : _children) {
    op.visit([&id_count](auto&& arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, GraphPatternOperation::Union>) {
        arg._child1.recomputeIds(id_count);
        arg._child2.recomputeIds(id_count);
      } else if constexpr (std::is_same_v<T, GraphPatternOperation::Optional> ||
                           std::is_same_v<
                               T, GraphPatternOperation::GroupGraphPattern> ||
                           std::is_same_v<T, GraphPatternOperation::Minus>) {
        arg._child.recomputeIds(id_count);
      } else if constexpr (std::is_same_v<T,
                                          GraphPatternOperation::TransPath>) {
        arg._childGraphPattern.recomputeIds(id_count);
      } else if constexpr (std::is_same_v<T, GraphPatternOperation::Values>) {
        arg._id = (*id_count)++;
      } else {
        static_assert(
            std::is_same_v<T, GraphPatternOperation::Subquery> ||
            std::is_same_v<T, GraphPatternOperation::BasicGraphPattern> ||
            std::is_same_v<T, GraphPatternOperation::Bind>);
        // subquery children have their own id space
        // TODO:joka921 look at the optimizer if it is ok, that
        // BasicGraphPatterns and Values have no ids at all. at the same time
        // assert that the above else-if is exhaustive.
      }
    });
  }

  if (allocatedIdCounter) {
    delete id_count;
  }
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
      os << arg._subquery.asString();
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
      for (size_t i = 0; i + 1 < arg._whereClauseTriples.size(); ++i) {
        os << "\n";
        for (int j = 0; j < indentation; ++j) os << "  ";
        os << arg._whereClauseTriples[i].asString() << ',';
      }
      if (arg._whereClauseTriples.size() > 0) {
        os << "\n";
        for (int j = 0; j < indentation; ++j) os << "  ";
        os << arg._whereClauseTriples.back().asString();
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

// __________________________________________________________________________
ParsedQuery::GraphPattern::GraphPattern() : _optional(false) {}

void ParsedQuery::GraphPattern::addLanguageFilter(const std::string& lhs,
                                                  const std::string& rhs) {
  auto langTag = rhs.substr(1, rhs.size() - 2);
  // First find a suitable triple for the given variable. It
  // must use a predicate that is neither a variable nor a complex
  // predicate path.
  if (_children.empty() ||
      !_children.back().is<GraphPatternOperation::BasicGraphPattern>()) {
    _children.emplace_back(GraphPatternOperation::BasicGraphPattern{});
  }
  auto& t = _children.back()
                .get<GraphPatternOperation::BasicGraphPattern>()
                ._whereClauseTriples;
  auto it = std::find_if(t.begin(), t.end(), [&lhs](const auto& tr) {
    // TODO<joka921> Also apply the efficient language filter for
    // property paths like "rdfs:label|skos:altLabel".
    return tr._o == lhs && (tr._p._operation == PropertyPath::Operation::IRI &&
                            !isVariable(tr._p));
  });
  if (it == t.end()) {
    LOG(DEBUG) << "language filter variable " + lhs +
                      " did not appear as object in any suitable "
                      "triple. "
                      "Using literal-to-language predicate instead.\n";
    auto langEntity = ad_utility::convertLangtagToEntityUri(langTag);
    PropertyPath taggedPredicate(PropertyPath::Operation::IRI);
    taggedPredicate._iri = LANGUAGE_PREDICATE;
    SparqlTriple triple(lhs, taggedPredicate, langEntity);
    // This `find` is linear in the number of triples (per language filter).
    // This shouldn't be a problem here, though. If needed, we could use a
    // (hash)-set instead of a vector.
    if (std::find(t.begin(), t.end(), triple) != t.end()) {
      LOG(DEBUG) << "Ignoring duplicate triple: lang(" << lhs << ") = " << rhs
                 << std::endl;
    } else {
      t.push_back(triple);
    }
  } else {
    // replace the triple
    PropertyPath taggedPredicate(PropertyPath::Operation::IRI);
    taggedPredicate._iri = '@' + langTag + '@' + it->_p._iri;
    SparqlTriple taggedTriple(it->_s, taggedPredicate, it->_o);
    LOG(DEBUG) << "replacing predicate " << it->_p.asString() << " with "
               << taggedTriple._p.asString() << std::endl;
    *it = taggedTriple;
  }
}
