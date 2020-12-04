#pragma once

#include <memory>
#include <string>
#include "../global/Id.h"
#include "../util/HashMap.h"
#include "IndexMetaData.h"
#include "PatternContainer.h"
#include "Vocabulary.h"
#include "VocabularyData.h"

enum class HasRelationType { SUBJECT, OBJECT };

// This struct holds compile time information about the data that differs
// when building the pattern trick data for subjects or objects.
template <HasRelationType TYPE>
struct HasRelationTypeData {};

template <>
struct HasRelationTypeData<HasRelationType::SUBJECT> {
  static constexpr size_t SUBJECT_COL = 0;
};

template <>
struct HasRelationTypeData<HasRelationType::OBJECT> {
  static constexpr size_t SUBJECT_COL = 2;
};

class PatternIndex {
  static const uint32_t PATTERNS_FILE_VERSION;
  static const uint32_t NAMESPACE_FILE_VERSION;

  static const std::string PATTERNS_NAMESPACE_SUFFIX;
  static const std::string PATTERNS_SUBJECTS_SUFFIX;
  static const std::string PATTERNS_OBJECTS_SUFFIX;

 public:
  struct PatternMetaData {
    /**
     * @brief The multiplicity of the Entites column (0) of the full
     * has-relation relation after unrolling the patterns.
     */
    double fullHasPredicateMultiplicityEntities;

    /**
     * @brief The multiplicity of the Predicates column (0) of the full
     * has-relation relation after unrolling the patterns.
     */
    double fullHasPredicateMultiplicityPredicates;

    /**
     * @brief The size of the full has-relation relation after unrolling the
     *         patterns.
     */
    size_t fullHasPredicateSize;
  };

  PatternIndex();

  std::shared_ptr<PatternContainer> getSubjectPatternData();
  std::shared_ptr<const PatternContainer> getSubjectPatternData() const;

  const PatternMetaData& getSubjectMetaData() const;

  std::shared_ptr<PatternContainer> getObjectPatternData();
  std::shared_ptr<const PatternContainer> getObjectPatternData() const;

  const PatternMetaData& getObjectMetaData() const;

  const std::vector<Id> getPredicateGlobalIds() const;

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
   * @brief Generate a has-relation predicate mapping objects to predicates
   * encoded using patterns. The idTriples vector has to be in OPS order.
   */
  void createPatternsForObjects(VocabularyData* vocabData,
                                const std::string& filename_base);

  void createPatternsForObjectsFromExistingIndex(
      Id langPredLowerBound, Id langPredUpperBound,
      IndexMetaDataMmapView& meta_data, ad_utility::File& file,
      const std::string& filename_base);

  /**
   * @brief Takes the triples sorted by PSO or POS and generates a new namespace
   * that only contains predicates. This namespace is then used for storing
   * patterns and explicit has-predicate entries. This namespace needs to be
   * created exactly once when generating the patterns using the createPatterns
   * method, and will be loaded from the patterns file in subsequent runs.
   */
  void generatePredicateLocalNamespace(VocabularyData* vocabData,
                                       const std::string& filename_base);

  void generatePredicateLocalNamespaceFromExistingIndex(
      Id langPredLowerBound, Id langPredUpperBound,
      IndexMetaDataHmap& meta_data, const std::string& filename_base);

  void loadPatternIndex(const std::string& filename_base);

  friend class CreatePatternsFixture_createPatterns_Test;

 private:
  template <HasRelationType TYPE, typename VecReaderType, typename... Args>
  static void callCreatePatternsImpl(
      size_t num_bytes_per_predicate_id, const string& fileName,
      std::shared_ptr<PatternContainer>* pattern_data,
      const ad_utility::HashMap<Id, size_t>& predicate_local_id,
      PatternMetaData* metadata, const size_t maxNumPatterns,
      const Id langPredLowerBound, const Id langPredUpperBound,
      Args&... vecReaderArgs);

  /**
   * @brief Creates the data required for the "pattern-trick" used for fast
   *        ql:has-relation evaluation when selection relation counts.
   * @param fileName The name of the file in which the data should be stored
   * @param args The arguments that need to be passed to the constructor of
   *             VecReaderType. VecReaderType should allow for iterating over
   *             the tuples of the spo permutation after having been constructed
   *             using args.
   */
  template <typename PatternId, HasRelationType TYPE, typename VecReaderType,
            typename... Args>
  static void createPatternsImpl(
      const string& fileName,
      std::shared_ptr<PatternContainerImpl<PatternId>> pattern_data,
      const ad_utility::HashMap<Id, size_t>& predicate_local_id,
      PatternMetaData* metadata, const size_t maxNumPatterns,
      const Id langPredLowerBound, const Id langPredUpperBound,
      Args&... vecReaderArgs);

  void throwExceptionIfNotInitialized() const;

  static void loadPatternFile(const std::string& path,
                              size_t num_bytes_per_predicate,
                              PatternMetaData* meta_data_storage,
                              std::shared_ptr<PatternContainer>* storage);

  static void loadNamespaceFile(const std::string& path,
                                std::vector<Id>* namespace_storage);

  static void writeNamespaceFile(const std::string& path, const vector<Id>& ns);

  /**
   * @brief Load the pattern_data stored in a patterns file.
   */
  template <typename PatternId>
  static std::shared_ptr<PatternContainerImpl<PatternId>> loadPatternData(
      ad_utility::File* file);

  // Pattern trick data
  size_t _maxNumPatterns;

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

  size_t _num_bytes_per_predicate;

  /**
   * @brief Contains the has-predicate predicate encoded using patterns.
   * Predicate ids are in the predicate local namespace.
   */
  std::shared_ptr<PatternContainer> _subjects_pattern_data;
  PatternMetaData _subject_meta_data;

  std::shared_ptr<PatternContainer> _objects_pattern_data;
  PatternMetaData _object_meta_data;

  bool _initialized;
};
