#ifndef QLEVER_SRC_ENGINE_CONSTRUCTQUERYCACHE_H
#define QLEVER_SRC_ENGINE_CONSTRUCTQUERYCACHE_H

#include <cassert>
#include <limits>
#include <memory>
#include <utility>

#include "rdfTypes/Variable.h"
#include "parser/data/Iri.h"
#include "parser/data/Literal.h"
#include "parser/data/BlankNode.h"

// Class for caching the bindings to Graptherms of rows of triples in the CONSTRUCT-clause
// of a CONSTRUCT query.

class ConstructQueryCache{
  template<typename T>
  using opt = std::optional<T>;
  using string = std::string;
 public:
  ConstructQueryCache() = default;

  // clear cache for a new row.
  void clearRowCache() {
  }

  // cache statistics (for debugging/optimization)
  class CacheStats {
   public:
    size_t variableHits_{0};
    size_t variableMisses_{0};
    size_t iriHits_{0};
    size_t iriMisses_{0};
    size_t literalHits_{0};
    size_t literalMisses_{0};
    size_t blankNodeHits_{0};
    size_t blankNodeMisses_{0};

   public:
    [[nodiscard]] double variableHitRate() const {
      return double(variableHits_) / (double(variableMisses_) + double(variableHits_));
    }

    [[nodiscard]] size_t variableHits() const {
      return variableHits_;
    }

    [[nodiscard]] size_t variableMisses() const {
      return variableHits_;
    }

    [[nodiscard]] size_t iriHits() const {
      return iriHits_;
    }

    [[nodiscard]] size_t iriMisses() const {
      return iriMisses_;
    }

    [[nodiscard]] size_t literalHits() const {
      return literalHits_;
    }

    [[nodiscard]] size_t literalMisses() const {
      return literalMisses_;
    }

    [[nodiscard]] size_t blankNodeHits() const {
      return blankNodeHits_;
    }

    [[nodiscard]] size_t blankNodeMisses() const {
      return blankNodeMisses_;
    }

  };

  CacheStats getStats() const { return stats_; }
  void resetStats() {stats_ = CacheStats{};}
  void startNewRow(uint64_t row);


  template<typename T>
  std::optional<std::string> evaluateWithCacheImpl(
      const T& term,
      const ConstructQueryExportContext& context,
      PositionInTriple posInTriple
  );


  opt<string> evaluateWithCache(
      const GraphTerm& term,
      const ConstructQueryExportContext& context,
      PositionInTriple posInTriple
  );
  void clearAll();

 private:
  // cache for row-specific variables
  struct VariableKey {
    Variable variable_;
    uint64_t rowIndex_{};

    bool operator==(const VariableKey& other) const {
      return variable_ == other.variable_ &&
             rowIndex_ == other.rowIndex_;
    }
  };

  struct VariableKeyHash {
    size_t operator()(const VariableKey& key) const;
  };

  // maps (variableKey, variablename) -> VariableKeyHash
  std::unordered_map<VariableKey, opt<std::string>, VariableKeyHash> variableCache_;

  // Cache for IRIs (which are global for a CONSTRUCT-query clause, i.e. the same for each row in the CONSTRUCT-clause)
  struct IriKey {
    Iri iri_;
    // We might also need context-specific info like base IRI (TODO<ms2144> what is base IRI?)
    std::reference_wrapper<const ConstructQueryExportContext> context_;

    bool operator==(const IriKey& other) const {
      return iri_ == other.iri_ && &context_.get() == &other.context_.get();
    }
  };

  struct IriKeyHash {
    size_t operator()(const IriKey& key) const;
  };

  // maps (irikey, iriname) -> IriKeyHash
  std::unordered_map<IriKey, std::optional<std::string>, IriKeyHash> iriCache_;

  // Cache for literals (global for CONSTRUCT-clause )
  struct LiteralKey {
    Literal literal_;
    std::reference_wrapper<const ConstructQueryExportContext> context_;

    bool operator==(const LiteralKey& other) const {
      return literal_ == other.literal_ &&
             &context_.get() == &other.context_.get();
    }
  };

  struct LiteralKeyHash {
    size_t operator()(const LiteralKey& key) const;
  };

  // maps (LiteralKey, LiteralName) -> LiteralKeyHash
  std::unordered_map<LiteralKey, std::optional<std::string>, LiteralKeyHash> literalCache_;

  // Cache for blank nodes (global for CONSTRUCT-clause )
  struct BlankNodeKey{
    BlankNode blankNode_;
    std::reference_wrapper<const ConstructQueryExportContext> context_;

    bool operator==(const BlankNodeKey& other) const {
      return blankNode_ == other.blankNode_&&
             &context_.get() == &other.context_.get();
    }
  };

  struct BlankNodeKeyHash {
    size_t operator()(const BlankNodeKey& key) const;
  };

  // maps (BlankNodeKey, BlankNodeName) -> BlankNodeKeyHash
  std::unordered_map<BlankNodeKey, std::optional<std::string>, BlankNodeKeyHash> blankNodeCache_;

  CacheStats stats_;
  // current row (for clearing variable cache)
  uint64_t currentRow_{0};

};
template <typename T>
std::optional<std::string> ConstructQueryCache::evaluateWithCacheImpl(
    const T& term,
    const ConstructQueryExportContext& context,
    PositionInTriple posInTriple) {
  return {};
}

#endif
