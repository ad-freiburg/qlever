#pragma once

#include <memory>
#include <string>
#include "../global/Id.h"
#include "../util/HashMap.h"
#include "IndexMetaData.h"
#include "PatternContainer.h"
#include "Vocabulary.h"
#include "VocabularyData.h"

class PatternIndex {
  static const uint32_t PATTERNS_FILE_VERSION;

 public:
  PatternIndex();

  std::shared_ptr<PatternContainer> getPatternData();
  std::shared_ptr<const PatternContainer> getPatternData() const;

  const std::vector<Id> getPredicateGlobalIds() const;

  /**
   * @return The multiplicity of the Entites column (0) of the full has-relation
   *         relation after unrolling the patterns.
   */
  double getHasPredicateMultiplicityEntities() const;

  /**
   * @return The multiplicity of the Predicates column (0) of the full
   * has-relation relation after unrolling the patterns.
   */
  double getHasPredicateMultiplicityPredicates() const;

  /**
   * @return The size of the full has-relation relation after unrolling the
   *         patterns.
   */
  size_t getHasPredicateFullSize() const;

  /**
   * @brief Generate a has-relation predicate encoded using patterns. the
   * idTriples vector has to be in SPO order.
   */
  void createPatterns(VocabularyData* vocabData,
                      const std::string& filename_base);

  void createPatternsFromExistingIndex(Id langPredLowerBound,
                                       Id langPredUpperBound,
                                       IndexMetaDataMmapView& meta_data,
                                       ad_utility::File& file,
                                       const std::string& filename_base);

  /**
   * @brief Takes the triples sorted by PSO or POS and generates a new namespace
   * that only contains predicates. This namespace is then used for storing
   * patterns and explicit has-predicate entries. This namespace needs to be
   * created exactly once when generating the patterns using the createPatterns
   * method, and will be loaded from the patterns file in subsequent runs.
   */
  void generatePredicateLocalNamespace(VocabularyData* vocabData);

  void generatePredicateLocalNamespaceFromExistingIndex(
      Id langPredLowerBound, Id langPredUpperBound,
      IndexMetaDataHmap& meta_data);

  void loadPatternIndex(const std::string& filename_base);

  friend class CreatePatternsFixture_createPatterns_Test;

 private:
  /**
   * @brief Creates the data required for the "pattern-trick" used for fast
   *        ql:has-relation evaluation when selection relation counts.
   * @param fileName The name of the file in which the data should be stored
   * @param args The arguments that need to be passed to the constructor of
   *             VecReaderType. VecReaderType should allow for iterating over
   *             the tuples of the spo permutation after having been constructed
   *             using args.
   */
  template <typename PatternId, typename VecReaderType, typename... Args>
  void createPatternsImpl(
      const string& fileName,
      std::shared_ptr<PatternContainerImpl<PatternId>> pattern_data,
      const std::vector<Id>& predicate_global_id,
      const ad_utility::HashMap<Id, size_t>& predicate_local_id,
      double& fullHasPredicateMultiplicityEntities,
      double& fullHasPredicateMultiplicityPredicates,
      size_t& fullHasPredicateSize, const size_t maxNumPatterns,
      const Id langPredLowerBound, const Id langPredUpperBound,
      Args&... vecReaderArgs);

  void throwExceptionIfNotInitialized() const;

  /**
   * @brief Load the pattern_data stored in a patterns file.
   */
  template <typename PatternId>
  std::shared_ptr<PatternContainerImpl<PatternId>> loadPatternData(
      ad_utility::File* file);

  // Pattern trick data
  size_t _maxNumPatterns;
  double _fullHasPredicateMultiplicityEntities;
  double _fullHasPredicateMultiplicityPredicates;
  size_t _fullHasPredicateSize;

  /**
   * @brief Maps predicate ids from the global namespace to the predicate local
   * namespace (which only contains predicates). This map is only used
   * during index creation and is otherwise not initialized.
   */
  ad_utility::HashMap<Id, size_t> _predicate_global_to_local_ids;

  /**
   * @brief Maps from the predicate local namespace (containing only predicates)
   * to the global namespace (also containing subjects and objects).
   */
  std::vector<Id> _predicate_local_to_global_ids;

  /**
   * @brief Contains the has-predicate predicate encoded using patterns.
   * Predicate ids are in the predicate local namespace.
   */
  std::shared_ptr<PatternContainer> _pattern_container;

  bool _initialized;
};
