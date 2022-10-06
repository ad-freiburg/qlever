// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./Index.h"

#include "./IndexImpl.h"

// ____________________________________________________________
Index::Index() : pimpl_{std::make_unique<IndexImpl>()} {}
Index::Index(Index&&) noexcept = default;

// Needs to be in the .cpp file because of the unique_ptr to a forwarded class.
// See
// https://stackoverflow.com/questions/13414652/forward-declaration-with-unique-ptr
Index::~Index() = default;

// ___________________________________________________________
template <class Parser>
void Index::createFromFile(const std::string& filename) {
  pimpl_->template createFromFile<Parser>(filename);
}

// Explicit instantiations.
template void Index::createFromFile<TurtleStreamParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleMmapParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleParserAuto>(const string& filename);

// __________________________________________________________
void Index::addPatternsToExistingIndex() {
  pimpl_->addPatternsToExistingIndex();
}

// _________________________________________________________
void Index::createFromOnDiskIndex(const std::string& onDiskBase) {
  pimpl_->createFromOnDiskIndex(onDiskBase);
}

// ________________________________________________________
void Index::addTextFromContextFile(const std::string& contextFile,
                                   bool addWordsFromLiterals) {
  pimpl_->addTextFromContextFile(contextFile, addWordsFromLiterals);
}

// _______________________________________________________
void Index::buildDocsDB(const std::string& docsFile) {
  pimpl_->buildDocsDB(docsFile);
}

// ______________________________________________________
void Index::addTextFromOnDiskIndex() { pimpl_->addTextFromOnDiskIndex(); }

// _____________________________________________________
auto Index::getVocab() const -> const Vocab& { return pimpl_->getVocab(); }

// ____________________________________________________
auto Index::getNonConstVocabForTesting() -> Vocab& {
  return pimpl_->getNonConstVocabForTesting();
}

// ___________________________________________________
auto Index::getTextVocab() const -> const TextVocab& {
  return pimpl_->getTextVocab();
}

// __________________________________________________
size_t Index::relationCardinality(const std::string& relationName) const {
  return pimpl_->relationCardinality(relationName);
}

// _________________________________________________
size_t Index::subjectCardinality(const TripleComponent& sub) const {
  return pimpl_->subjectCardinality(sub);
}

// ________________________________________________
size_t Index::objectCardinality(const TripleComponent& obj) const {
  return pimpl_->objectCardinality(obj);
}

// _______________________________________________
std::optional<std::string> Index::idToOptionalString(Id id) const {
  return pimpl_->idToOptionalString(id);
}

// ______________________________________________
bool Index::getId(const std::string& element, Id* id) const {
  return pimpl_->getId(element, id);
}

// _____________________________________________
std::pair<Id, Id> Index::prefix_range(const std::string& prefix) const {
  return pimpl_->prefix_range(prefix);
}

// ____________________________________________
const vector<PatternID>& Index::getHasPattern() const {
  return pimpl_->getHasPattern();
}

// ___________________________________________
const CompactVectorOfStrings<Id>& Index::getHasPredicate() const {
  return pimpl_->getHasPredicate();
}

// __________________________________________
const CompactVectorOfStrings<Id>& Index::getPatterns() const {
  return pimpl_->getPatterns();
}

// _________________________________________
double Index::getAvgNumDistinctPredicatesPerSubject() const {
  return pimpl_->getAvgNumDistinctPredicatesPerSubject();
}

// ________________________________________
double Index::getAvgNumDistinctSubjectsPerPredicate() const {
  return pimpl_->getAvgNumDistinctSubjectsPerPredicate();
}

// _______________________________________
size_t Index::getNumDistinctSubjectPredicatePairs() const {
  return pimpl_->getNumDistinctSubjectPredicatePairs();
}

// ______________________________________
std::string_view Index::wordIdToString(WordIndex wordIndex) const {
  return pimpl_->wordIdToString(wordIndex);
}

// _____________________________________
size_t Index::getSizeEstimate(const std::string& words) const {
  return pimpl_->getSizeEstimate(words);
}

// ____________________________________
void Index::getContextListForWords(const std::string& words,
                                   IdTable* result) const {
  return pimpl_->getContextListForWords(words, result);
}

// ___________________________________
void Index::getECListForWordsOneVar(const std::string& words, size_t limit,
                                    IdTable* result) const {
  return pimpl_->getECListForWordsOneVar(words, limit, result);
}

// __________________________________
void Index::getECListForWords(const std::string& words, size_t nofVars,
                              size_t limit, IdTable* result) const {
  return pimpl_->getECListForWords(words, nofVars, limit, result);
}

// _________________________________
void Index::getFilteredECListForWords(const std::string& words,
                                      const IdTable& filter,
                                      size_t filterColumn, size_t nofVars,
                                      size_t limit, IdTable* result) const {
  return pimpl_->getFilteredECListForWords(words, filter, filterColumn, nofVars,
                                           limit, result);
}

// ________________________________
void Index::getFilteredECListForWordsWidthOne(const std::string& words,
                                              const IdTable& filter,
                                              size_t nofVars, size_t limit,
                                              IdTable* result) const {
  return pimpl_->getFilteredECListForWordsWidthOne(words, filter, nofVars,
                                                   limit, result);
}

// _______________________________
void Index::getContextEntityScoreListsForWords(const std::string& words,
                                               vector<TextRecordIndex>& cids,
                                               vector<Id>& eids,
                                               vector<Score>& scores) const {
  return pimpl_->getContextEntityScoreListsForWords(words, cids, eids, scores);
}

// ______________________________
template <size_t I>
void Index::getECListForWordsAndSingleSub(
    const std::string& words, const vector<std::array<Id, I>>& subres,
    size_t subResMainCol, size_t limit,
    vector<std::array<Id, 3 + I>>& res) const {
  return pimpl_->getECListForWordsAndSingleSub(words, subres, subResMainCol,
                                               limit, res);
}

// _____________________________
void Index::getECListForWordsAndTwoW1Subs(
    const std::string& words, const vector<std::array<Id, 1>> subres1,
    const vector<std::array<Id, 1>> subres2, size_t limit,
    vector<std::array<Id, 5>>& res) const {
  return pimpl_->getECListForWordsAndTwoW1Subs(words, subres1, subres2, limit,
                                               res);
}

// ____________________________
void Index::getECListForWordsAndSubtrees(
    const std::string& words,
    const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResVecs,
    size_t limit, vector<vector<Id>>& res) const {
  return pimpl_->getECListForWordsAndSubtrees(words, subResVecs, limit, res);
}

// ___________________________
void Index::getWordPostingsForTerm(const std::string& term,
                                   vector<TextRecordIndex>& cids,
                                   vector<Score>& scores) const {
  return pimpl_->getWordPostingsForTerm(term, cids, scores);
}

// __________________________
void Index::getEntityPostingsForTerm(const std::string& term,
                                     vector<TextRecordIndex>& cids,
                                     vector<Id>& eids,
                                     vector<Score>& scores) const {
  return pimpl_->getEntityPostingsForTerm(term, cids, eids, scores);
}

// _________________________
std::string Index::getTextExcerpt(TextRecordIndex cid) const {
  return pimpl_->getTextExcerpt(cid);
}

/*
// ________________________
void Index::dumpAsciiLists(const vector<std::string>& lists,
                           bool decodeGapsFreq) const {
  return pimpl_->dumpAsciiLists(lists, decodeGapsFreq);
}
 */

// _______________________
/*
void Index::dumpAsciiLists(const TextBlockMetaData& tbmd) const {
  return pimpl_->dumpAsciiLists(tbmd);
}
 */

// ______________________
float Index::getAverageNofEntityContexts() const {
  return pimpl_->getAverageNofEntityContexts();
}

// _______________________________________________________________________
void Index::setKbName(const std::string& name) {
  return pimpl_->setKbName(name);
}

// ______________________________________________________________________
void Index::setTextName(const std::string& name) {
  return pimpl_->setTextName(name);
}

// _____________________________________________________________________
void Index::setUsePatterns(bool usePatterns) {
  return pimpl_->setUsePatterns(usePatterns);
}

// ____________________________________________________________________
void Index::setLoadAllPermutations(bool loadAllPermutations) {
  return pimpl_->setLoadAllPermutations(loadAllPermutations);
}

// ___________________________________________________________________
void Index::setKeepTempFiles(bool keepTempFiles) {
  return pimpl_->setKeepTempFiles(keepTempFiles);
}

// __________________________________________________________________
uint64_t& Index::stxxlMemoryInBytes() { return pimpl_->stxxlMemoryInBytes(); }

// _________________________________________________________________
const uint64_t& Index::stxxlMemoryInBytes() const {
  return pimpl_->stxxlMemoryInBytes();
}

// ________________________________________________________________
void Index::setOnDiskBase(const std::string& onDiskBase) {
  return pimpl_->setOnDiskBase(onDiskBase);
}

// _______________________________________________________________
void Index::setSettingsFile(const std::string& filename) {
  return pimpl_->setSettingsFile(filename);
}

// ______________________________________________________________
void Index::setPrefixCompression(bool compressed) {
  return pimpl_->setPrefixCompression(compressed);
}

// _____________________________________________________________
void Index::setNumTriplesPerBatch(uint64_t numTriplesPerBatch) {
  return pimpl_->setNumTriplesPerBatch(numTriplesPerBatch);
}

// ____________________________________________________________
const std::string& Index::getTextName() const { return pimpl_->getTextName(); }

// ___________________________________________________________
const std::string& Index::getKbName() const { return pimpl_->getKbName(); }

// __________________________________________________________
size_t Index::getNofTriples() const { return pimpl_->getNofTriples(); }

// _________________________________________________________
size_t Index::getNofTextRecords() const { return pimpl_->getNofTextRecords(); }

// ________________________________________________________
size_t Index::getNofWordPostings() const {
  return pimpl_->getNofWordPostings();
}

// _______________________________________________________
size_t Index::getNofEntityPostings() const {
  return pimpl_->getNofEntityPostings();
}

// ______________________________________________________
size_t Index::getNofSubjects() const { return pimpl_->getNofSubjects(); }

// _____________________________________________________
size_t Index::getNofObjects() const { return pimpl_->getNofObjects(); }

// _____________________________________________________
size_t Index::getNofPredicates() const { return pimpl_->getNofPredicates(); }

// _____________________________________________________
bool Index::hasAllPermutations() const { return pimpl_->hasAllPermutations(); }

// _____________________________________________________
vector<float> Index::getMultiplicities(Permutation p) const {
  return pimpl_->applyToPermutation(p, [this](const auto& permutation) {
    return pimpl_->getMultiplicities(permutation);
  });
}

// _____________________________________________________
vector<float> Index::getMultiplicities(const TripleComponent& key,
                                       Permutation permutation) const {
  return pimpl_->applyToPermutation(permutation, [this, key](const auto& p) {
    return pimpl_->getMultiplicities(key, p);
  });
}

// _____________________________________________________
void Index::scan(Id key, IdTable* result, Permutation p,
                 ad_utility::SharedConcurrentTimeoutTimer timer) const {
  return pimpl_->applyToPermutation(p, [&](const auto& perm) {
    return pimpl_->scan(key, result, perm, std::move(timer));
  });
}

// _____________________________________________________
void Index::scan(const TripleComponent& key, IdTable* result,
                 const Permutation& p,
                 ad_utility::SharedConcurrentTimeoutTimer timer) const {
  return pimpl_->applyToPermutation(p, [&](const auto& perm) {
    return pimpl_->scan(key, result, perm, std::move(timer));
  });
}

// _____________________________________________________
void Index::scan(const TripleComponent& col0String,
                 const TripleComponent& col1String, IdTable* result,
                 Permutation p,
                 ad_utility::SharedConcurrentTimeoutTimer timer) const {
  return pimpl_->applyToPermutation(p, [&](const auto& perm) {
    return pimpl_->scan(col0String, col1String, result, perm, std::move(timer));
  });
}

// _____________________________________________________
std::pair<size_t, size_t> Index::getNumTriplesActuallyAndAdded() const {
  return pimpl_->getNumTriplesActuallyAndAdded();
}
