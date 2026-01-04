#include "ConstructQueryCache.h"
#include "rdfTypes/Iri.h"

// using statements ____________________________________________________________
using string = std::string;
template<typename T>
using opt = std::optional<T>;
// _____________________________________________________________________________

// cache implementations _______________________________________________________
//TODO<ms2144>: I dont quite understand what happens in the method body here.
size_t ConstructQueryCache::VariableKeyHash::operator()(const ConstructQueryCache::VariableKey& key) const {
  return std::hash<std::string>{}(key.variable_.name()) ^
         (std::hash<uint64_t>{}(key.rowIndex_) << 1);
}


size_t ConstructQueryCache::IriKeyHash::operator()(const IriKey& key) const {
  return std::hash<std::string>{}(key.iri_.iri()) ^
         (std::hash<const ConstructQueryExportContext*>{}(&key.context_.get()) << 1);
}

size_t ConstructQueryCache::LiteralKeyHash::operator()(const LiteralKey& key) const {
  return std::hash<std::string>{}(key.literal_.literal()) ^
  (std::hash<const ConstructQueryExportContext*>{}(&key.context_.get()) << 1);
}

//______________________________________________________________________________

// methods for interacting with the cache ______________________________________
template<>
opt<string> ConstructQueryCache::evaluateWithCacheImpl<Variable>(
    const Variable& term,
    const ConstructQueryExportContext& context,
    PositionInTriple posInTriple){

  VariableKey key{term, context._row};

  auto it = variableCache_.find(key);
  if (it != variableCache_.end()) {
    ++stats_.variableHits_;
    return it->second;
  }

  ++stats_.variableMisses_;
  opt<string> result = term.evaluate(context, posInTriple);
  variableCache_[key] = result;

  return result;
}

//TODO<ms2144> the ad_utility::triple_component::Iri class does not have an evaluate method,
// why? Can I add the one from "src/parser/data/Iri.h" to ad_utility::triple_component::Iri?
// I would otherwise use the ad_utility_::triple_component::Iri class here instead of the
// "src/parser/data/Iri.h".
template<>
opt<string> ConstructQueryCache::evaluateWithCacheImpl<Iri>(
    const Iri& term,
    const ConstructQueryExportContext& context,
    PositionInTriple posInTriple){

  IriKey key{term, std::cref(context)};

  auto it = iriCache_.find(key);
  if (it != iriCache_.end()) {
    ++stats_.iriHits_;
    return it->second;
  }

  ++stats_.iriMisses_;
  opt<string> result = term.evaluate(context, posInTriple);
  iriCache_[key] = result;
  return result;
}

template<>
opt<string> ConstructQueryCache::evaluateWithCacheImpl<Literal>(
    const Literal& term,
    const ConstructQueryExportContext& context,
    PositionInTriple posInTriple)
{
  const Literal& literal = term;

  LiteralKey key{literal, std::cref(context)};

  auto it = literalCache_.find(key);
  // cache hit
  if (it != literalCache_.end()) {
    ++stats_.literalHits_;
    return it->second;
  }

  // cache miss
  ++stats_.literalMisses_;
  opt<string> result = literal.evaluate(context, posInTriple);
  literalCache_[key] = result;
  return result;
}
//______________________________________________________________________________

void ConstructQueryCache::startNewRow(uint64_t row) {
  if (currentRow_ != row) {
    variableCache_.clear();
    currentRow_ = row;
  }
}

opt<string> ConstructQueryCache::evaluateWithCache(
    const GraphTerm& term,
    const ConstructQueryExportContext& context,
    PositionInTriple posInTriple) {

  return std::visit(
      [this, &context, posInTriple](const auto& concreteTerm) -> opt<string> {
        return evaluateWithCacheImpl(concreteTerm, context, posInTriple);
      },
      term);
}

void ConstructQueryCache::clearAll() {
  variableCache_.clear();
  iriCache_.clear();
  literalCache_.clear();
  currentRow_ = 0;
  resetStats();
}
