---
name: sparql-rdf
description: Understand SPARQL queries, RDF data models, SPARQL functions, and semantic web concepts. Use when working with SPARQL queries, RDF triples, or semantic data.
---

# SPARQL & RDF Skill for QLever

Provides guidance on SPARQL query semantics, RDF data models, and semantic web standards for the QLever database.

## RDF Basics

### Core Concepts

**RDF Triple**: The fundamental unit of RDF data
```
Subject  Predicate  Object
  ↓         ↓        ↓
<Alice>  <knows>  <Bob>
```

**Components**:
- **IRI (Internationalized Resource Identifier)**: Global identifier like `<http://example.org/Alice>`
- **Literal**: Data values like `"Alice"@en` or `"42"^^xsd:integer`
- **Blank Node**: Anonymous resource like `_:b1`

### RDF Data Model

```
// Triple store contains millions of triples
<http://example.org/Alice> <http://example.org/knows> <http://example.org/Bob>
<http://example.org/Alice> <http://xmlns.com/foaf/0.1/name> "Alice Smith"@en
<http://example.org/Alice> <http://xmlns.com/foaf/0.1/age> "30"^^xsd:integer
```

## SPARQL Query Basics

### Query Structure

```sparql
PREFIX ex: <http://example.org/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name ?age
WHERE {
  ?person foaf:name ?name ;
          foaf:age ?age ;
          ex:knows ex:Bob .
}
LIMIT 10
```

**Components**:
- **PREFIX**: Define namespace shortcuts
- **SELECT**: Choose variables to return
- **WHERE**: Define graph patterns to match
- **LIMIT**: Restrict result count

### Graph Patterns

**Basic Pattern** (triple matching):
```sparql
SELECT ?name WHERE {
  ?person foaf:name ?name .
}
```

**Multiple Triples** (join):
```sparql
SELECT ?name ?knows WHERE {
  ?person foaf:name ?name ;
          ex:knows ?knows .
}
```

**Optional Patterns** (left join):
```sparql
SELECT ?name ?email WHERE {
  ?person foaf:name ?name .
  OPTIONAL { ?person foaf:mbox ?email . }
}
```

## Common SPARQL Patterns

### Filtering Results

```sparql
SELECT ?name ?age WHERE {
  ?person foaf:name ?name ;
          foaf:age ?age .
  FILTER (?age > 18)
}
```

### Aggregation

```sparql
SELECT ?department (COUNT(?employee) AS ?count)
WHERE {
  ?employee ex:worksDepartment ?department .
}
GROUP BY ?department
```

### Sorting & Limits

```sparql
SELECT ?name ?age WHERE {
  ?person foaf:name ?name ;
          foaf:age ?age .
}
ORDER BY DESC(?age)
LIMIT 10
```

### DISTINCT Results

```sparql
SELECT DISTINCT ?type WHERE {
  ?resource rdf:type ?type .
}
```

## SPARQL Functions

### String Functions

```sparql
FILTER (STRLEN(?name) > 5)              // String length
FILTER (CONTAINS(?name, "John"))        // Contains substring
FILTER (STRSTARTS(?name, "J"))          // Starts with
FILTER (STRENDS(?name, "n"))            // Ends with
FILTER (LCASE(?name) = "john")          // Lowercase
FILTER (UCASE(?name) = "JOHN")          // Uppercase
BIND (SUBSTR(?name, 1, 3) AS ?prefix)   // Substring
BIND (CONCAT(?first, " ", ?last) AS ?full)  // Concatenation
```

### Numeric Functions

```sparql
FILTER (?age > 18 && ?age < 65)         // Arithmetic operators
BIND (ABS(?value) AS ?absolute)         // Absolute value
BIND (ROUND(?price) AS ?rounded)        // Rounding
BIND (FLOOR(?value) AS ?floored)        // Floor function
BIND (CEIL(?value) AS ?ceiled)          // Ceiling function
```

### Date/Time Functions

```sparql
BIND (NOW() AS ?current)                // Current timestamp
BIND (YEAR(?date) AS ?year)             // Extract year
BIND (MONTH(?date) AS ?month)           // Extract month
BIND (DAY(?date) AS ?day)               // Extract day
```

### Type Functions

```sparql
BIND (DATATYPE(?literal) AS ?type)      // Get datatype
BIND (LANG(?text) AS ?language)         // Get language tag
FILTER (isIRI(?subject))                // Check if IRI
FILTER (isLiteral(?object))             // Check if literal
FILTER (isBLANK(?node))                 // Check if blank node
```

## QLever-Specific Features

### Full-Text Search

```sparql
PREFIX bif: <http://www.openlinksw.com/schemas/bif#>

SELECT ?document WHERE {
  ?document rdfs:label ?label .
  ?label bif:contains "searchterm" .
}
```

### Spatial Queries (S2 Geometry)

```sparql
PREFIX geo: <http://www.opengis.net/ont/sf#>
PREFIX spatial: <http://www.openlinksw.com/schemas/spatial#>

SELECT ?place WHERE {
  ?place geo:hasGeometry ?geom ;
         spatial:near (52.5, 13.4) 10 .
}
```

## Property Paths

### Path Expression Examples

```sparql
// Zero or more edges
SELECT ?descendant WHERE {
  ?person foaf:knows+ ?descendant .
}

// One or more edges
SELECT ?contact WHERE {
  ?person foaf:knows* ?contact .
}

// Specific path
SELECT ?grandchild WHERE {
  ?person foaf:knows/foaf:knows ?grandchild .
}

// Alternative paths
SELECT ?related WHERE {
  ?person (foaf:knows | foaf:worksWith) ?related .
}
```

## Named Graphs

```sparql
SELECT ?fact FROM <http://example.org/dataset1> WHERE {
  ?s ?p ?o .
}
```

## Common Performance Issues

### Query Optimization Tips

1. **Bind early**: Filter/BIND before expensive operations
   ```sparql
   SELECT ?name WHERE {
     ?person foaf:name ?name .
     BIND (LCASE(?name) AS ?lower)
     FILTER (?lower = "alice")
   }
   ```

2. **Use OPTIONAL carefully**: May slow queries if not needed

3. **Avoid large result sets**: Use LIMIT and OFFSET

4. **Index-friendly patterns**:
   - Start with specific subjects/objects when possible
   - Avoid overly broad patterns

## RDF Vocabularies in QLever

Common vocabularies used:

| Prefix | Namespace | Use |
|--------|-----------|-----|
| `rdf` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` | RDF core |
| `rdfs` | `http://www.w3.org/2000/01/rdf-schema#` | RDF schema |
| `xsd` | `http://www.w3.org/2001/XMLSchema#` | XML Schema datatypes |
| `foaf` | `http://xmlns.com/foaf/0.1/` | Friend of a Friend |
| `dcat` | `http://www.w3.org/ns/dcat#` | Data Catalog |
| `dbo` | `http://dbpedia.org/ontology/` | DBpedia Ontology |

## Debugging SPARQL Queries

### Step-by-step approach

1. **Test basic pattern**:
   ```sparql
   SELECT * WHERE { ?s ?p ?o . } LIMIT 1
   ```

2. **Add filters incrementally**:
   ```sparql
   SELECT * WHERE { ?s ?p ?o . FILTER (?p = rdf:type) . }
   ```

3. **Check data exists**:
   ```sparql
   SELECT DISTINCT ?p WHERE { ?s ?p ?o . }
   ```

4. **Verify joins**:
   ```sparql
   SELECT ?x ?y WHERE {
     ?x ?p1 ?v .
     ?v ?p2 ?y .
  }
   ```

## SPARQL Standards

- **W3C SPARQL 1.1 Query Language**: https://www.w3.org/TR/sparql11-query/
- **RDF 1.1 Concepts**: https://www.w3.org/TR/rdf11-concepts/
- **SPARQL 1.1 Federated Query**: https://www.w3.org/TR/sparql11-federated-query/

## Testing SPARQL Queries in QLever

### Using QLever Server

```bash
# Start server
./build/ServerMain

# Execute query (curl example)
curl -X POST http://localhost:7023 \
  -d "query=SELECT%20*%20WHERE%20%7B%20%3Fs%20%3Fp%20%3Fo%20%7D%20LIMIT%2010"
```

### Building test datasets

```bash
# Use IndexBuilderMain to load RDF files
./build/IndexBuilderMain --file data.nt --index-dir index/
```
