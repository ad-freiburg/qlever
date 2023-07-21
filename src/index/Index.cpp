// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./Index.h"

#include "./IndexImpl.h"

// ____________________________________________________________________________
Index::Index(ad_utility::AllocatorWithLimit<Id> allocator)
    : pimpl_{std::make_unique<IndexImpl>(std::move(allocator))} {}
Index::Index(Index&&) noexcept = default;

// Needs to be in the .cpp file because of the unique_ptr to a forwarded class.
// See
// https://stackoverflow.com/questions/13414652/forward-declaration-with-unique-ptr
Index::~Index() = default;

// ____________________________________________________________________________
void Index::createFromFile(const std::string& filename) {
  pimpl_->createFromFile(filename);
}

// ____________________________________________________________________________
void Index::addPatternsToExistingIndex() {
  pimpl_->addPatternsToExistingIndex();
}

// ____________________________________________________________________________
void Index::createFromOnDiskIndex(const std::string& onDiskBase) {
  pimpl_->createFromOnDiskIndex(onDiskBase);
}

// ____________________________________________________________________________
void Index::addTextFromContextFile(const std::string& contextFile,
                                   bool addWordsFromLiterals) {
  pimpl_->addTextFromContextFile(contextFile, addWordsFromLiterals);
}

// ____________________________________________________________________________
void Index::buildDocsDB(const std::string& docsFile) {
  pimpl_->buildDocsDB(docsFile);
}

// ____________________________________________________________________________
void Index::addTextFromOnDiskIndex() { pimpl_->addTextFromOnDiskIndex(); }

// ____________________________________________________________________________
auto Index::getVocab() const -> const Vocab& { return pimpl_->getVocab(); }

// ____________________________________________________________________________
auto Index::getNonConstVocabForTesting() -> Vocab& {
  return pimpl_->getNonConstVocabForTesting();
}

// ____________________________________________________________________________
auto Index::getTextVocab() const -> const TextVocab& {
  return pimpl_->getTextVocab();
}

// ____________________________________________________________________________
size_t Index::getCardinality(const TripleComponent& comp,
                             Permutation::Enum p) const {
  return pimpl_->getCardinality(comp, p);
}

// ____________________________________________________________________________
size_t Index::getCardinality(Id id, Permutation::Enum p) const {
  return pimpl_->getCardinality(id, p);
}

// ____________________________________________________________________________
std::optional<std::string> Index::idToOptionalString(VocabIndex id) const {
  return pimpl_->idToOptionalString(id);
}

// ____________________________________________________________________________
bool Index::getId(const std::string& element, Id* id) const {
  return pimpl_->getId(element, id);
}

// ____________________________________________________________________________
std::pair<Id, Id> Index::prefix_range(const std::string& prefix) const {
  return pimpl_->prefix_range(prefix);
}

// ____________________________________________________________________________
const vector<PatternID>& Index::getHasPattern() const {
  return pimpl_->getHasPattern();
}

// ____________________________________________________________________________
const CompactVectorOfStrings<Id>& Index::getHasPredicate() const {
  return pimpl_->getHasPredicate();
}

// ____________________________________________________________________________
const CompactVectorOfStrings<Id>& Index::getPatterns() const {
  return pimpl_->getPatterns();
}

// ____________________________________________________________________________
double Index::getAvgNumDistinctPredicatesPerSubject() const {
  return pimpl_->getAvgNumDistinctPredicatesPerSubject();
}

// ____________________________________________________________________________
double Index::getAvgNumDistinctSubjectsPerPredicate() const {
  return pimpl_->getAvgNumDistinctSubjectsPerPredicate();
}

// ____________________________________________________________________________
size_t Index::getNumDistinctSubjectPredicatePairs() const {
  return pimpl_->getNumDistinctSubjectPredicatePairs();
}

// ____________________________________________________________________________
std::string_view Index::wordIdToString(WordIndex wordIndex) const {
  return pimpl_->wordIdToString(wordIndex);
}

// ____________________________________________________________________________
size_t Index::getSizeEstimate(const std::string& words) const {
  return pimpl_->getSizeEstimate(words);
}

// ____________________________________________________________________________
void Index::getContextListForWords(const std::string& words,
                                   IdTable* result) const {
  return pimpl_->getContextListForWords(words, result);
}

// ____________________________________________________________________________
void Index::getECListForWordsOneVar(const std::string& words, size_t limit,
                                    IdTable* result) const {
  return pimpl_->getECListForWordsOneVar(words, limit, result);
}

// ____________________________________________________________________________
void Index::getECListForWords(const std::string& words, size_t nofVars,
                              size_t limit, IdTable* result) const {
  return pimpl_->getECListForWords(words, nofVars, limit, result);
}

// ____________________________________________________________________________
void Index::getFilteredECListForWords(const std::string& words,
                                      const IdTable& filter,
                                      size_t filterColumn, size_t nofVars,
                                      size_t limit, IdTable* result) const {
  return pimpl_->getFilteredECListForWords(words, filter, filterColumn, nofVars,
                                           limit, result);
}

// ____________________________________________________________________________
void Index::getFilteredECListForWordsWidthOne(const std::string& words,
                                              const IdTable& filter,
                                              size_t nofVars, size_t limit,
                                              IdTable* result) const {
  return pimpl_->getFilteredECListForWordsWidthOne(words, filter, nofVars,
                                                   limit, result);
}

// ____________________________________________________________________________
Index::WordEntityPostings Index::getContextEntityScoreListsForWords(
    const std::string& words) const {
  return pimpl_->getContextEntityScoreListsForWords(words);
}

// ____________________________________________________________________________
template <size_t I>
void Index::getECListForWordsAndSingleSub(
    const std::string& words, const vector<std::array<Id, I>>& subres,
    size_t subResMainCol, size_t limit,
    vector<std::array<Id, 3 + I>>& res) const {
  return pimpl_->getECListForWordsAndSingleSub(words, subres, subResMainCol,
                                               limit, res);
}

// ____________________________________________________________________________
void Index::getECListForWordsAndTwoW1Subs(
    const std::string& words, const vector<std::array<Id, 1>> subres1,
    const vector<std::array<Id, 1>> subres2, size_t limit,
    vector<std::array<Id, 5>>& res) const {
  return pimpl_->getECListForWordsAndTwoW1Subs(words, subres1, subres2, limit,
                                               res);
}

// ____________________________________________________________________________
void Index::getECListForWordsAndSubtrees(
    const std::string& words,
    const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResVecs,
    size_t limit, vector<vector<Id>>& res) const {
  return pimpl_->getECListForWordsAndSubtrees(words, subResVecs, limit, res);
}

// ____________________________________________________________________________
Index::WordEntityPostings Index::getWordPostingsForTerm(
    const std::string& term) const {
  return pimpl_->getWordPostingsForTerm(term);
}

// ____________________________________________________________________________
Index::WordEntityPostings Index::getEntityPostingsForTerm(
    const std::string& term) const {
  return pimpl_->getEntityPostingsForTerm(term);
}

// ____________________________________________________________________________
std::string Index::getTextExcerpt(TextRecordIndex cid) const {
  return pimpl_->getTextExcerpt(cid);
}

// ____________________________________________________________________________
float Index::getAverageNofEntityContexts() const {
  return pimpl_->getAverageNofEntityContexts();
}

// ____________________________________________________________________________
void Index::setKbName(const std::string& name) {
  return pimpl_->setKbName(name);
}

// ____________________________________________________________________________
void Index::setTextName(const std::string& name) {
  return pimpl_->setTextName(name);
}

// ____________________________________________________________________________
void Index::setUsePatterns(bool usePatterns) {
  return pimpl_->setUsePatterns(usePatterns);
}

// ____________________________________________________________________________
void Index::setLoadAllPermutations(bool loadAllPermutations) {
  return pimpl_->setLoadAllPermutations(loadAllPermutations);
}

// ____________________________________________________________________________
void Index::setKeepTempFiles(bool keepTempFiles) {
  return pimpl_->setKeepTempFiles(keepTempFiles);
}

// ____________________________________________________________________________
uint64_t& Index::stxxlMemoryInBytes() { return pimpl_->stxxlMemoryInBytes(); }

// ____________________________________________________________________________
uint64_t& Index::blocksizePermutationsInBytes() {
  return pimpl_->blocksizePermutationInBytes();
}

// ____________________________________________________________________________
const uint64_t& Index::stxxlMemoryInBytes() const {
  return pimpl_->stxxlMemoryInBytes();
}

// ____________________________________________________________________________
void Index::setOnDiskBase(const std::string& onDiskBase) {
  return pimpl_->setOnDiskBase(onDiskBase);
}

// ____________________________________________________________________________
void Index::setSettingsFile(const std::string& filename) {
  return pimpl_->setSettingsFile(filename);
}

// ____________________________________________________________________________
void Index::setPrefixCompression(bool compressed) {
  return pimpl_->setPrefixCompression(compressed);
}

// ____________________________________________________________________________
void Index::setNumTriplesPerBatch(uint64_t numTriplesPerBatch) {
  return pimpl_->setNumTriplesPerBatch(numTriplesPerBatch);
}

// ____________________________________________________________________________
const std::string& Index::getTextName() const { return pimpl_->getTextName(); }

// ____________________________________________________________________________
const std::string& Index::getKbName() const { return pimpl_->getKbName(); }

// ____________________________________________________________________________
Index::NumNormalAndInternal Index::numTriples() const {
  return pimpl_->numTriples();
}

// ____________________________________________________________________________
size_t Index::getNofTextRecords() const { return pimpl_->getNofTextRecords(); }

// ____________________________________________________________________________
size_t Index::getNofWordPostings() const {
  return pimpl_->getNofWordPostings();
}

// ____________________________________________________________________________
size_t Index::getNofEntityPostings() const {
  return pimpl_->getNofEntityPostings();
}

// ____________________________________________________________________________
Index::NumNormalAndInternal Index::numDistinctSubjects() const {
  return pimpl_->numDistinctSubjects();
}

// ____________________________________________________________________________
Index::NumNormalAndInternal Index::numDistinctObjects() const {
  return pimpl_->numDistinctObjects();
}

// ____________________________________________________________________________
Index::NumNormalAndInternal Index::numDistinctPredicates() const {
  return pimpl_->numDistinctPredicates();
}

// ____________________________________________________________________________
bool Index::hasAllPermutations() const { return pimpl_->hasAllPermutations(); }

// ____________________________________________________________________________
vector<float> Index::getMultiplicities(Permutation::Enum p) const {
  return pimpl_->getMultiplicities(p);
}

// ____________________________________________________________________________
vector<float> Index::getMultiplicities(const TripleComponent& key,
                                       Permutation::Enum p) const {
  return pimpl_->getMultiplicities(key, p);
}

// ____________________________________________________________________________
IdTable Index::scan(
    const TripleComponent& col0String,
    std::optional<std::reference_wrapper<const TripleComponent>> col1String,
    Permutation::Enum p, ad_utility::SharedConcurrentTimeoutTimer timer) const {
  return pimpl_->scan(col0String, col1String, p, std::move(timer));
}

// ____________________________________________________________________________
IdTable Index::scan(Id col0Id, std::optional<Id> col1Id, Permutation::Enum p,
                    ad_utility::SharedConcurrentTimeoutTimer timer) const {
  return pimpl_->scan(col0Id, col1Id, p, std::move(timer));
}

// ____________________________________________________________________________
size_t Index::getResultSizeOfScan(const TripleComponent& col0String,
                                  const TripleComponent& col1String,
                                  const Permutation::Enum& permutation) const {
  return pimpl_->getResultSizeOfScan(col0String, col1String, permutation);
}
