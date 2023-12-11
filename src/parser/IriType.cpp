//
// Created by beckermann on 12/11/23.
//

#include "IriType.h"

IriType::IriType(std::string iri) { this->iri = iri; }

std::string_view IriType::getIri() const { return this->iri; }
