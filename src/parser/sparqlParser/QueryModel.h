//
// Created by johannes on 08.05.21.
//

#ifndef QLEVER_QUERYMODEL_H
#define QLEVER_QUERYMODEL_H

#include <variant>

namespace QueryModel {
using numericLiteral = std::variant<int64_t, double>;
}

#endif  // QLEVER_QUERYMODEL_H
