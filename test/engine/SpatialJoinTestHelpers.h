#ifndef QLEVER_TEST_ENGINE_SPATIALJOINTESTHELPERS_H
#define QLEVER_TEST_ENGINE_SPATIALJOINTESTHELPERS_H

#include <cstdlib>

#include "../util/IndexTestHelpers.h"
#include "./../../src/util/GeoSparqlHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/SpatialJoinAlgorithms.h"
#include "parser/data/Variable.h"

namespace SpatialJoinTestHelpers {

auto makePointLiteral = [](std::string_view c1, std::string_view c2) {
  return absl::StrCat(" \"POINT(", c1, " ", c2, ")\"^^<", GEO_WKT_LITERAL, ">");
};

auto makeAreaLiteral = [](std::string_view coordinateList) {
  return absl::StrCat("\"POLYGON((", coordinateList, "))\"^^<", GEO_WKT_LITERAL,
                      ">");
};

const std::string pointUniFreiburg = makePointLiteral("7.83505", "48.01267");
const std::string pointMinster = makePointLiteral("7.85298", "47.99557");
const std::string pointLondonEye = makePointLiteral("-0.11957", "51.50333");
const std::string pointStatueOfLiberty =
    makePointLiteral("-74.04454", "40.68925");
const std::string pointEiffelTower = makePointLiteral("2.29451", "48.85825");

const std::string areaUniFreiburg = makeAreaLiteral(
    "7.8346338 48.0126612,7.8348921 48.0123905,7.8349457 "
    "48.0124216,7.8349855 48.0124448,7.8353244 48.0126418,7.8354091 "
    "48.0126911,7.8352246 48.0129047,7.8351668 48.0128798,7.8349471 "
    "48.0127886,7.8347248 48.0126986,7.8346338 48.0126612");

const std::string areaMuenster = makeAreaLiteral(
    "7.8520522 47.9956071,7.8520528 47.9955872,7.8521103 "
    "47.995588,7.8521117 47.9955419,7.852113 47.9954975,7.8520523 "
    "47.9954968,7.8520527 47.995477,7.8521152 47.9954775,7.8521154 "
    "47.9954688,7.8521299 47.995469,7.8521311 47.9954303,7.8521611 "
    "47.9954307,7.8521587 47.9954718,7.8522674 47.9954741,7.8522681 "
    "47.9954676,7.8522746 47.9954643,7.8522832 47.9954599,7.8522976 "
    "47.99546,7.8523031 47.995455,7.8523048 47.9954217,7.8522781 "
    "47.9954213,7.8522786 47.9954058,7.8523123 47.9954065,7.852314 "
    "47.9953744,7.8523383 47.9953748,7.8523373 47.9954062,7.8524164 "
    "47.995408,7.8524176 47.9953858,7.852441 47.9953865,7.8524398 "
    "47.9954085,7.8525077 47.9954101,7.8525088 47.9953886,7.8525316 "
    "47.9953892,7.8525305 47.9954106,7.8526031 47.9954123,7.8526042 "
    "47.9953915,7.8526276 47.9953922,7.8526265 47.9954128,7.8526944 "
    "47.9954144,7.8526954 47.9953943,7.8527183 47.9953949,7.8527173 "
    "47.9954149,7.8527892 47.9954165,7.8527903 47.9953974,7.8528131 "
    "47.9953979,7.8528122 47.9954171,7.852871 47.9954182,7.8528712 "
    "47.995416,7.8528791 47.9954112,7.85289 47.9954113,7.8528971 "
    "47.9954158,7.8528974 47.9954052,7.8528925 47.9954052,7.8528928 "
    "47.9953971,7.8529015 47.9953972,7.8529024 47.9953702,7.852897 "
    "47.9953701,7.8528972 47.9953645,7.8529037 47.9953645,7.8529038 "
    "47.9953613,7.8529069 47.9953614,7.8529071 47.9953541,7.8529151 "
    "47.9953542,7.8529149 47.9953581,7.8529218 47.9953582,7.8529217 "
    "47.9953631,7.8529621 47.9953637,7.8529623 47.9953572,7.8529719 "
    "47.9953573,7.8529716 47.9953642,7.8530114 47.9953648,7.8530116 "
    "47.9953587,7.8530192 47.9953589,7.853019 47.995365,7.8530635 "
    "47.9953657,7.8530637 47.9953607,7.8530716 47.9953608,7.8530715 "
    "47.9953657,7.8530758 47.9953657,7.8530757 47.9953688,7.8530817 "
    "47.9953689,7.8530815 47.9953742,7.8530747 47.9953741,7.8530737 "
    "47.9954052,7.8530794 47.9954053,7.8530792 47.995413,7.8530717 "
    "47.9954129,7.8530708 47.9954199,7.8531165 47.9954207,7.8531229 "
    "47.9954131,7.8531292 47.9954209,7.8531444 47.9954211,7.8531444 "
    "47.9954238,7.8531569 47.995424,7.8531661 47.9954152,7.853171 "
    "47.9954201,7.853183 47.9954203,7.8531829 47.9954234,7.8531973 "
    "47.9954236,7.8531977 47.9954138,7.8532142 47.9954141,7.8532141 "
    "47.9954253,7.8532425 47.9954355,7.8532514 47.9954298,7.8532593 "
    "47.9954353,7.8532915 47.9954255,7.8532923 47.9954155,7.8533067 "
    "47.995416,7.8533055 47.9954261,7.8533304 47.9954368,7.8533399 "
    "47.995431,7.85335 47.9954372,7.8533758 47.9954288,7.853377 "
    "47.9954188,7.8533932 47.9954192,7.8533924 47.9954298,7.8534151 "
    "47.9954395,7.8534278 47.9954345,7.8534373 47.995441,7.8534664 "
    "47.995432,7.8534672 47.9954209,7.8534832 47.9954211,7.8534828 "
    "47.9954322,7.8535077 47.9954449,7.8535224 47.9954375,7.8535325 "
    "47.995448,7.8535644 47.9954403,7.8535717 47.9954305,7.8535866 "
    "47.9954356,7.8535796 47.9954443,7.8536079 47.9954674,7.8536221 "
    "47.9954629,7.8536221 47.9954735,7.8536573 47.9954801,7.8536707 "
    "47.9954728,7.8536813 47.9954812,7.8536686 47.9954876,7.8536776 "
    "47.9955168,7.8536958 47.9955192,7.8536876 47.9955286,7.8537133 "
    "47.9955444,7.85373 47.9955428,7.8537318 47.9955528,7.8537154 "
    "47.9955545,7.8537069 47.9955819,7.8537168 47.995588,7.8537044 "
    "47.9955948,7.8537086 47.9956193,7.8537263 47.9956245,7.8537206 "
    "47.9956347,7.8537069 47.9956317,7.8536802 47.9956473,7.8536819 "
    "47.9956577,7.8536667 47.9956604,7.8536506 47.9956817,7.8536639 "
    "47.9956902,7.8536543 47.9956981,7.8536394 47.9956887,7.8536331 "
    "47.9956931,7.853609 47.9956954,7.8536024 47.9957048,7.8535868 "
    "47.9957028,7.8535591 47.9957206,7.8535642 47.9957285,7.8535487 "
    "47.9957327,7.8535423 47.9957215,7.853508 47.9957131,7.8534942 "
    "47.9957215,7.8534818 47.9957186,7.8534587 47.9957284,7.853458 "
    "47.9957389,7.8534421 47.9957388,7.8534424 47.9957273,7.853418 "
    "47.995714,7.8534099 47.9957194,7.8534021 47.995713,7.8533721 "
    "47.9957242,7.8533712 47.9957359,7.8533558 47.9957351,7.8533565 "
    "47.9957247,7.8533269 47.9957094,7.8533171 47.9957165,7.8533073 "
    "47.9957088,7.8532874 47.9957186,7.8532866 47.9957296,7.8532698 "
    "47.9957295,7.8532698 47.9957189,7.8532466 47.9957048,7.8532372 "
    "47.9957131,7.8532277 47.995705,7.8532014 47.9957171,7.8532009 "
    "47.9957284,7.8531844 47.9957281,7.8531847 47.9957174,7.8531778 "
    "47.9957102,7.853163 47.9957245,7.8530549 47.9957225,7.8530552 "
    "47.9957161,7.8529541 47.9957138,7.8529535 47.9957236,7.8529578 "
    "47.9957237,7.8529577 47.9957269,7.852953 47.9957268,7.8529529 "
    "47.9957308,7.8529477 47.9957307,7.8529478 47.9957271,7.8528964 "
    "47.9957256,7.8528963 47.9957288,7.8528915 47.9957287,7.8528916 "
    "47.9957256,7.8528876 47.9957255,7.8528875 47.9957223,7.8528912 "
    "47.9957224,7.8528908 47.9957195,7.8528811 47.9957194,7.8527983 "
    "47.9957162,7.8527981 47.9957192,7.8527723 47.9957185,7.8527732 "
    "47.9957016,7.852703 47.9957003,7.8527021 47.9957175,7.8526791 "
    "47.9957171,7.8526788 47.9957225,7.8526097 47.9957225,7.8526099 "
    "47.995718,7.8525863 47.9957183,7.8525874 47.9956981,7.8525155 "
    "47.9956967,7.8525144 47.995718,7.8524916 47.9957174,7.8524927 "
    "47.9956963,7.8524241 47.995695,7.852423 47.9957153,7.8523996 "
    "47.9957148,7.8524007 47.9956946,7.8523226 47.9956931,7.8523217 "
    "47.9957212,7.8522948 47.9957208,7.8522957 47.9956927,7.8522663 "
    "47.9956923,7.8522667 47.9956784,7.8522926 47.9956787,7.8522937 "
    "47.9956433,7.8522882 47.995635,7.8522723 47.9956351,7.8522611 "
    "47.9956281,7.8522613 47.9956189,7.8521543 47.9956174,7.852153 "
    "47.9956591,7.8521196 47.9956587,7.8521209 47.995617,7.8521109 "
    "47.9956168,7.8521111 47.9956079,7.8520522 47.9956071");

const std::string areaLondonEye = makeAreaLiteral(
    "-0.1198608 51.5027451,-0.1197395 51.5027354,-0.1194922 "
    "51.5039381,-0.1196135 51.5039478,-0.1198608 51.5027451");

const std::string areaStatueOfLiberty = makeAreaLiteral(
    "-74.0451069 40.6893455,-74.045004 40.6892215,-74.0451023 "
    "40.6891073,-74.0449107 40.6890721,-74.0449537 "
    "40.6889343,-74.0447746 40.6889506,-74.0446495 "
    "40.6888049,-74.0445067 40.6889076,-74.0442008 "
    "40.6888563,-74.0441463 40.6890663,-74.0441411 "
    "40.6890854,-74.0441339 40.6890874,-74.0441198 "
    "40.6890912,-74.0439637 40.6891376,-74.0440941 "
    "40.6892849,-74.0440057 40.6894071,-74.0441949 "
    "40.6894309,-74.0441638 40.6895702,-74.0443261 "
    "40.6895495,-74.0443498 40.6895782,-74.0443989 "
    "40.6896372,-74.0444277 40.6896741,-74.0445955 "
    "40.6895939,-74.0447392 40.6896561,-74.0447498 "
    "40.6896615,-74.0447718 40.6895577,-74.0447983 "
    "40.6895442,-74.0448287 40.6895279,-74.0449638 "
    "40.6895497,-74.0449628 40.6895443,-74.044961 40.6895356,-74.0449576 "
    "40.6895192,-74.044935 40.689421,-74.0451069 40.6893455");

const std::string areaEiffelTower = makeAreaLiteral(
    "2.2933119 48.858248,2.2935432 48.8581003,2.2935574 "
    "48.8581099,2.2935712 48.8581004,2.2936112 48.8581232,2.2936086 "
    "48.8581249,2.293611 48.8581262,2.2936415 48.8581385,2.293672 "
    "48.8581477,2.2937035 48.8581504,2.293734 48.858149,2.2937827 "
    "48.8581439,2.2938856 48.8581182,2.2939778 48.8580882,2.2940648 "
    "48.8580483,2.2941435 48.8579991,2.2941937 48.8579588,2.2942364 "
    "48.8579197,2.2942775 48.8578753,2.2943096 48.8578312,2.2943307 "
    "48.8577908,2.2943447 48.857745,2.2943478 48.8577118,2.2943394 "
    "48.8576885,2.2943306 48.8576773,2.2943205 48.8576677,2.2943158 "
    "48.8576707,2.2942802 48.8576465,2.2942977 48.8576355,2.2942817 "
    "48.8576248,2.2942926 48.8576181,2.2944653 48.8575069,2.2945144 "
    "48.8574753,2.2947414 48.8576291,2.294725 48.8576392,2.2947426 "
    "48.857651,2.294706 48.8576751,2.294698 48.8576696,2.2946846 "
    "48.8576782,2.2946744 48.8576865,2.2946881 48.8576957,2.2946548 "
    "48.857717,2.2946554 48.8577213,2.2946713 48.8577905,2.2946982 "
    "48.8578393,2.2947088 48.8578585,2.2947529 48.8579196,2.2948133 "
    "48.8579803,2.2948836 48.85803,2.2949462 48.8580637,2.2950051 "
    "48.8580923,2.2950719 48.85812,2.2951347 48.8581406,2.2951996 "
    "48.8581564,2.2952689 48.8581663,2.295334 48.8581699,2.2953613 "
    "48.8581518,2.2953739 48.8581604,2.2953965 48.8581497,2.2954016 "
    "48.8581464,2.2953933 48.8581409,2.2954304 48.8581172,2.2954473 "
    "48.8581285,2.2954631 48.8581182,2.2956897 48.8582718,2.295653 "
    "48.8582954,2.2955837 48.85834,2.2954575 48.8584212,2.2954416 "
    "48.858411,2.2954238 48.8584227,2.2953878 48.8583981,2.2953925 "
    "48.858395,2.2953701 48.8583857,2.2953419 48.8583779,2.2953057 "
    "48.8583737,2.2952111 48.8583776,2.2951081 48.858403,2.2950157 "
    "48.8584326,2.2949284 48.8584723,2.2948889 48.8584961,2.2947988 "
    "48.8585613,2.2947558 48.8586003,2.2947144 48.8586446,2.294682 "
    "48.8586886,2.2946605 48.8587289,2.2946462 48.8587747,2.294644 "
    "48.8587962,2.2946462 48.8588051,2.2946486 48.8588068,2.2946938 "
    "48.8588377,2.2946607 48.8588587,2.294663 48.8588603,2.294681 "
    "48.858849,2.2947169 48.8588737,2.2946988 48.858885,2.2947154 "
    "48.8588961,2.2944834 48.8590453,2.2943809 48.8589771,2.2943708 "
    "48.8589703,2.2942571 48.8588932,2.2942741 48.8588824,2.2942567 "
    "48.8588708,2.2942893 48.8588493,2.294306 48.8588605,2.2943103 "
    "48.8588577,2.2942883 48.8588426,2.2943122 48.8588275,2.2943227 "
    "48.8588209,2.2943283 48.8588173,2.2943315 48.8588125,2.2943333 "
    "48.8588018,2.2943166 48.8587327,2.294301 48.8586978,2.2942783 "
    "48.8586648,2.2942406 48.8586191,2.2942064 48.858577,2.2941734 "
    "48.8585464,2.2941015 48.8584943,2.2940384 48.8584609,2.2939792 "
    "48.8584325,2.293912 48.8584052,2.2938415 48.8583828,2.293784 "
    "48.8583695,2.2937145 48.8583599,2.2936514 48.8583593,2.2936122 "
    "48.8583846,2.293606 48.8583807,2.2935688 48.8584044,2.2935515 "
    "48.8583929,2.293536 48.8584028,2.2933119 48.858248");

// compared to the other areas, this one is not real, because it would be way
// too large. Here the borders of germany get approximated by just a few points
// to not make this file too crowded. As this geometry is only needed because
// the distance from the midpoint to the borders can't be ignored, it's not
// necessary to insert the complete geometry
const std::string approximatedAreaGermany = makeAreaLiteral(
    "7.20369317867016 53.62121249029073, "
    "9.335040870259194 54.77156944262062, 13.97127141588071 53.7058383745324, "
    "14.77327338230339 51.01654754091759, 11.916828022441791 "
    "50.36932046223437, "
    "13.674640551587391 48.68663848319227, 12.773761630400273 "
    "47.74969625921073, "
    "7.720050609106677 47.64617710434852, 8.313312337693318 "
    "48.997548751390326, "
    "6.50056816701192 49.535220384133375, 6.0391423781112 51.804566644690524, "
    "7.20369317867016 53.62121249029073");

// helper function to create a vector of strings from a result table
inline std::vector<std::string> printTable(const QueryExecutionContext* qec,
                                           const Result* table) {
  std::vector<std::string> output;
  for (size_t i = 0; i < table->idTable().numRows(); i++) {
    std::string line = "";
    for (size_t k = 0; k < table->idTable().numColumns(); k++) {
      auto test = ExportQueryExecutionTrees::idToStringAndType(
          qec->getIndex(), table->idTable().at(i, k), {});
      line += test.value().first;
      line += " ";
    }
    output.push_back(line.substr(0, line.size() - 1));  // ignore last " "
  }
  return output;
}

// this helper function reorders an input vector according to the variable to
// column map to make the string array match the order of the result, which
// should be tested (it uses a vector of vectors (the first vector is containing
// each column of the result, each column consist of a vector, where each entry
// is a row of this column))
inline std::vector<std::vector<std::string>> orderColAccordingToVarColMap(
    VariableToColumnMap varColMaps,
    std::vector<std::vector<std::string>> columns,
    std::vector<std::string> columnNames) {
  std::vector<std::vector<std::string>> result;
  auto indVariableMap = copySortedByColumnIndex(varColMaps);
  for (size_t i = 0; i < indVariableMap.size(); i++) {
    for (size_t k = 0; k < columnNames.size(); k++) {
      if (indVariableMap.at(i).first.name() == columnNames.at(k)) {
        result.push_back(columns.at(k));
        break;
      }
    }
  }
  return result;
}

// helper function to create a vector of strings representing rows, from a
// vector of strings representing columns. Please make sure, that the order of
// the columns is already matching the order of the result. If this is not the
// case call the function orderColAccordingToVarColMap
inline std::vector<std::string> createRowVectorFromColumnVector(
    std::vector<std::vector<std::string>> column_vector) {
  std::vector<std::string> result;
  if (column_vector.size() > 0) {
    for (size_t i = 0; i < column_vector.at(0).size(); i++) {
      std::string str = "";
      for (size_t k = 0; k < column_vector.size(); k++) {
        str += column_vector.at(k).at(i);
        str += " ";
      }
      result.push_back(str.substr(0, str.size() - 1));
    }
  }
  return result;
}

inline void addPoint(std::string& kg, std::string number, std::string name,
                     std::string point) {
  kg += absl::StrCat("<node_", number, "> <name> ", name, " .<node_", number,
                     "> <hasGeometry> <geometry", number, "> .<geometry",
                     number, "> <asWKT> ", point, " .");
}

inline void addArea(std::string& kg, std::string number, std::string name,
                    std::string area) {
  kg += absl::StrCat("<nodeArea_", number, "> <name> ", name, " . \n",
                     "<nodeArea_", number, "> <hasGeometry> <geometryArea",
                     number, "> .\n", "<geometryArea", number, "> <asWKT> ",
                     area, " .\n");
}

// create a small test dataset, which focuses on points or polygons as geometry
// objects. Note, that some of these objects have a polygon representation, but
// when choosing points, they get represented a single point. I took those
// points, such that it is obvious, which pair of objects should be included,
// when the maximum distance is x meters. Please note, that these datapoints
// are only partially copied from a real input file. Copying the query will
// therefore likely not result in the same results as here (the names,
// coordinates, etc. might be different in the real datasets). If usePolygons is
// set to false, all objects are represented by a point, otherwise all objects
// are represented by their area.
inline std::string createSmallDataset(bool usePolygons = false) {
  std::string kg;
  if (usePolygons) {
    addArea(kg, "1", "\"Uni Freiburg TF Area\"", areaUniFreiburg);
    addArea(kg, "2", "\"Minster Freiburg Area\"", areaMuenster);
    addArea(kg, "3", "\"London Eye Area\"", areaLondonEye);
    addArea(kg, "4", "\"Statue of liberty Area\"", areaStatueOfLiberty);
    addArea(kg, "5", "\"eiffel tower Area\"", areaEiffelTower);
  } else {
    addPoint(kg, "1", "\"Uni Freiburg TF\"", pointUniFreiburg);
    addPoint(kg, "2", "\"Minster Freiburg\"", pointMinster);
    addPoint(kg, "3", "\"London Eye\"", pointLondonEye);
    addPoint(kg, "4", "\"Statue of liberty\"", pointStatueOfLiberty);
    addPoint(kg, "5", "\"eiffel tower\"", pointEiffelTower);
  }
  return kg;
}

inline std::string createMixedDataset() {
  std::string kg;
  addArea(kg, "1", "\"Uni Freiburg TF Area\"", areaUniFreiburg);
  addPoint(kg, "2", "\"Minster Freiburg\"", pointMinster);
  addArea(kg, "3", "\"London Eye Area\"", areaLondonEye);
  addPoint(kg, "4", "\"Statue of liberty\"", pointStatueOfLiberty);
  addArea(kg, "5", "\"eiffel tower Area\"", areaEiffelTower);
  return kg;
}

// a mixed dataset, which contains points and areas. One of them is the geometry
// of germany, where the distance from the midpoint to the borders can not be
// ignored or approximated as zero
inline std::string createTrueDistanceDataset() {
  std::string kg;
  addPoint(kg, "1", "\"Uni Freiburg TF\"", pointUniFreiburg);
  addArea(kg, "2", "\"Minster Freiburg Area\"", areaMuenster);
  addPoint(kg, "3", "\"London Eye\"", pointLondonEye);
  addArea(kg, "4", "\"Statue of liberty Area\"", areaStatueOfLiberty);
  addPoint(kg, "5", "\"eiffel tower\"", pointEiffelTower);
  addArea(kg, "6", "\"Germany\"", approximatedAreaGermany);
  return kg;
}

// Build a `QueryExecutionContext` from the given turtle, but set some memory
// defaults to higher values to make it possible to test large geometric
// literals.
inline auto buildQec(std::string turtleKg) {
  ad_utility::testing::TestIndexConfig config{turtleKg};
  config.blocksizePermutations = 16_MB;
  config.parserBufferSize = 10_kB;
  return ad_utility::testing::getQec(std::move(config));
}

inline QueryExecutionContext* buildTestQEC(bool useAreas = false) {
  return buildQec(createSmallDataset(useAreas));
}

inline QueryExecutionContext* buildMixedAreaPointQEC(
    bool useTrueDistanceDataset = false) {
  std::string kg = useTrueDistanceDataset ? createTrueDistanceDataset()
                                          : createMixedDataset();
  return buildQec(kg);
}

// Create `QueryExecutionContext` with a dataset that contains an additional
// area without `<name>` predicate (so that our `libspatialjoin` test has two
// sides of different size), as well as an object with an invalid geometry.
inline QueryExecutionContext* buildNonSelfJoinDataset() {
  std::string kg = createTrueDistanceDataset();
  kg += absl::StrCat(
      "<nodeAreaAdded> <hasGeometry> <geometryAreaAdded> .\n",
      "<geometryAreaAdded> <asWKT> ", approximatedAreaGermany, " .\n",
      "<invalidObjectAdded> <hasGeometry> <geometryInvalidAdded> .\n",
      "<geometryInvalidAdded> <asWKT> 42 .\n");
  return buildQec(kg);
}

inline std::shared_ptr<QueryExecutionTree> buildIndexScan(
    QueryExecutionContext* qec, std::array<std::string, 3> triple) {
  TripleComponent subject{Variable{triple.at(0)}};
  TripleComponent object{Variable{triple.at(2)}};
  return ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{
          subject, TripleComponent::Iri::fromIriref(triple.at(1)), object});
}

inline std::shared_ptr<QueryExecutionTree> buildJoin(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> tree1,
    std::shared_ptr<QueryExecutionTree> tree2, Variable joinVariable) {
  auto varCol1 = tree1->getVariableColumns();
  auto varCol2 = tree2->getVariableColumns();
  size_t col1 = varCol1[joinVariable].columnIndex_;
  size_t col2 = varCol2[joinVariable].columnIndex_;
  return ad_utility::makeExecutionTree<Join>(qec, tree1, tree2, col1, col2);
}

inline std::shared_ptr<QueryExecutionTree> buildMediumChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple1,
    std::array<std::string, 3> triple2, std::array<std::string, 3> triple3,
    std::string joinVariable1_, std::string joinVariable2_) {
  Variable joinVariable1{joinVariable1_};
  Variable joinVariable2{joinVariable2_};
  auto scan1 = buildIndexScan(qec, triple1);
  auto scan2 = buildIndexScan(qec, triple2);
  auto scan3 = buildIndexScan(qec, triple3);
  auto join = buildJoin(qec, scan1, scan2, joinVariable1);
  return buildJoin(qec, join, scan3, joinVariable2);
}

inline std::shared_ptr<QueryExecutionTree> buildSmallChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple1,
    std::array<std::string, 3> triple2, std::string joinVariable_) {
  Variable joinVariable{joinVariable_};
  auto scan1 = buildIndexScan(qec, triple1);
  auto scan2 = buildIndexScan(qec, triple2);
  return buildJoin(qec, scan1, scan2, joinVariable);
}

// this function creates a minimum viable SpatialJoinAlgorithms class, which
// gets used in testing to access the wrapper methods. Note that not all
// functions of this class work properly, as many necessary parameters are
// defaulted as nullpointer or std::nullopt. The maxDist is necessary, because
// one of the wrapper classes needs a proper maxDistance, otherwise the wrapper
// can't be used to test the function
inline SpatialJoinAlgorithms getDummySpatialJoinAlgsForWrapperTesting(
    size_t maxDist = 1000,
    std::optional<QueryExecutionContext*> qec = std::nullopt) {
  if (!qec) {
    qec = buildTestQEC();
  }
  MaxDistanceConfig task{static_cast<double>(maxDist)};
  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec.value(),
          SpatialJoinConfiguration{task, Variable{"?point1"},
                                   Variable{"?point2"}},
          std::nullopt, std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  PreparedSpatialJoinParams params{nullptr,
                                   nullptr,
                                   nullptr,
                                   nullptr,
                                   0,
                                   0,
                                   std::vector<ColumnIndex>{},
                                   1,
                                   spatialJoin->getMaxDist(),
                                   std::nullopt,
                                   std::nullopt};

  return {qec.value(), params, spatialJoin->onlyForTestingGetConfig()};
}

}  // namespace SpatialJoinTestHelpers

#endif  // QLEVER_TEST_ENGINE_SPATIALJOINTESTHELPERS_H
