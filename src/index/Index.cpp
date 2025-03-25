// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
// Björn Buchhold (2014-2017, buchhold@informatik.uni-freiburg.de)
// Johannes Kalmbach (2018- , kalmbach@informatik.uni-freiburg.de)

#include "index/Index.h"

#include "index/IndexImpl.h"
#include "util/MemorySize/MemorySize.h"

// ____________________________________________________________________________
Index::Index(ad_utility::AllocatorWithLimit<Id> allocator)
    : pimpl_{std::make_unique<IndexImpl>(std::move(allocator))} {}
Index::Index(Index&&) noexcept = default;

// Needs to be in the .cpp file because of the unique_ptr to a forwarded class.
// See
// https://stackoverflow.com/questions/13414652/forward-declaration-with-unique-ptr
Index::~Index() = default;

// ____________________________________________________________________________
void Index::createFromOnDiskIndex(const std::string& onDiskBase) {
  pimpl_->createFromOnDiskIndex(onDiskBase);
}

// ____________________________________________________________________________
void Index::buildTextIndexFile(
    std::optional<std::pair<std::string, std::string>> wordsAndDocsFile,
    bool addWordsFromLiterals) {
  pimpl_->buildTextIndexFile(wordsAndDocsFile, addWordsFromLiterals);
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
ad_utility::BlankNodeManager* Index::getBlankNodeManager() const {
  return pimpl_->getBlankNodeManager();
}

// ____________________________________________________________________________
size_t Index::getCardinality(
    const TripleComponent& comp, Permutation::Enum p,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  return pimpl_->getCardinality(comp, p, locatedTriplesSnapshot);
}

// ____________________________________________________________________________
size_t Index::getCardinality(
    Id id, Permutation::Enum p,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  return pimpl_->getCardinality(id, p, locatedTriplesSnapshot);
}

// ____________________________________________________________________________
std::string Index::indexToString(VocabIndex id) const {
  return pimpl_->indexToString(id);
}

// ____________________________________________________________________________
std::string_view Index::indexToString(WordVocabIndex id) const {
  return pimpl_->indexToString(id);
}

// ____________________________________________________________________________
Index::Vocab::PrefixRanges Index::prefixRanges(std::string_view prefix) const {
  return pimpl_->prefixRanges(prefix);
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
size_t Index::getSizeOfTextBlockForWord(const std::string& word) const {
  return pimpl_->getSizeOfTextBlockForWord(word);
}

// ____________________________________________________________________________
size_t Index::getSizeOfTextBlockForEntities(const std::string& word) const {
  return pimpl_->getSizeOfTextBlockForEntities(word);
}

// ____________________________________________________________________________
size_t Index::getSizeEstimate(const std::string& words) const {
  return pimpl_->getSizeEstimate(words);
}

// ____________________________________________________________________________
IdTable Index::getWordPostingsForTerm(
    const std::string& term,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  return pimpl_->getWordPostingsForTerm(term, allocator);
}

// ____________________________________________________________________________
IdTable Index::getEntityMentionsForWord(
    const string& term,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  return pimpl_->getEntityMentionsForWord(term, allocator);
}

// ____________________________________________________________________________
size_t Index::getIndexOfBestSuitedElTerm(const vector<string>& terms) const {
  return pimpl_->getIndexOfBestSuitedElTerm(terms);
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
bool& Index::usePatterns() { return pimpl_->usePatterns(); }

// ____________________________________________________________________________
bool& Index::loadAllPermutations() { return pimpl_->loadAllPermutations(); }

// ____________________________________________________________________________
void Index::setKeepTempFiles(bool keepTempFiles) {
  return pimpl_->setKeepTempFiles(keepTempFiles);
}

// ____________________________________________________________________________
ad_utility::MemorySize& Index::memoryLimitIndexBuilding() {
  return pimpl_->memoryLimitIndexBuilding();
}

// ____________________________________________________________________________
const ad_utility::MemorySize& Index::memoryLimitIndexBuilding() const {
  return std::as_const(*pimpl_).memoryLimitIndexBuilding();
}

// ____________________________________________________________________________
ad_utility::MemorySize& Index::parserBufferSize() {
  return pimpl_->parserBufferSize();
}

// ____________________________________________________________________________
const ad_utility::MemorySize& Index::parserBufferSize() const {
  return std::as_const(*pimpl_).parserBufferSize();
}

// ____________________________________________________________________________
ad_utility::MemorySize& Index::blocksizePermutationsPerColumn() {
  return pimpl_->blocksizePermutationPerColumn();
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
void Index::setNumTriplesPerBatch(uint64_t numTriplesPerBatch) {
  return pimpl_->setNumTriplesPerBatch(numTriplesPerBatch);
}

// ____________________________________________________________________________
void Index::storeTextScoringParamsInConfiguration(
    TextScoringMetric scoringMetric, float b, float k) {
  return pimpl_->storeTextScoringParamsInConfiguration(scoringMetric, b, k);
}

// ____________________________________________________________________________
const std::string& Index::getTextName() const { return pimpl_->getTextName(); }

// ____________________________________________________________________________
const std::string& Index::getKbName() const { return pimpl_->getKbName(); }

// ____________________________________________________________________________
const std::string& Index::getIndexId() const { return pimpl_->getIndexId(); }

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
size_t Index::getNofNonLiteralsInTextIndex() const {
  return pimpl_->getNofNonLiteralsInTextIndex();
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
vector<float> Index::getMultiplicities(
    const TripleComponent& key, Permutation::Enum p,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  return pimpl_->getMultiplicities(key, p, locatedTriplesSnapshot);
}

// ____________________________________________________________________________
IdTable Index::scan(
    const ScanSpecificationAsTripleComponent& scanSpecification,
    Permutation::Enum p, Permutation::ColumnIndicesRef additionalColumns,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot,
    const LimitOffsetClause& limitOffset) const {
  return pimpl_->scan(scanSpecification, p, additionalColumns,
                      cancellationHandle, locatedTriplesSnapshot, limitOffset);
}

// ____________________________________________________________________________
IdTable Index::scan(
    const ScanSpecification& scanSpecification, Permutation::Enum p,
    Permutation::ColumnIndicesRef additionalColumns,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot,
    const LimitOffsetClause& limitOffset) const {
  return pimpl_->scan(scanSpecification, p, additionalColumns,
                      cancellationHandle, locatedTriplesSnapshot, limitOffset);
}

// ____________________________________________________________________________
size_t Index::getResultSizeOfScan(
    const ScanSpecification& scanSpecification,
    const Permutation::Enum& permutation,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  return pimpl_->getResultSizeOfScan(scanSpecification, permutation,
                                     locatedTriplesSnapshot);
}

// ____________________________________________________________________________
void Index::createFromFiles(const std::vector<InputFileSpecification>& files) {
  return pimpl_->createFromFiles(files);
}

// ____________________________________________________________________________
const DeltaTriplesManager& Index::deltaTriplesManager() const {
  return pimpl_->deltaTriplesManager();
}

// ____________________________________________________________________________
DeltaTriplesManager& Index::deltaTriplesManager() {
  return pimpl_->deltaTriplesManager();
}
