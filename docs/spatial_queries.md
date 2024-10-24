# Spatial Queries in QLever

QLever has limited support for features from the [OGC GeoSPARQL standard](https://docs.ogc.org/is/22-047r1/22-047r1.html). Additionally, QLever supports some custom spatial querying features.

## GeoSPARQL geometric relations

QLever itself currently does not support ad-hoc calculation of geometric relations (e.g. `geof:sfContains`, `geof:sfIntersects`, ...).

However, for OpenStreetMap data, geometric relations can be precomputed as part of the dataset (e.g. `ogc:sfContains`, `ogc:sfIntersects`, ... triples) using [`osm2rdf`](https://github.com/ad-freiburg/osm2rdf). After precomputation, geometric queries are much faster than ad-hoc methods.

Geometries from `osm2rdf` are represented as `geo:wktLiteral`, which can be addressed by `geo:hasGeometry/geo:asWKT`. `osm2rdf` also provides centroids of objects via `geo:hasCentroid/geo:asWKT`. Please note that the geometric relations are given as triples between the OpenStreetMap entities, not the geometries.

### Example: All buildings in the city of Freiburg

```sparql
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:>
PREFIX ogc: <http://www.opengis.net/rdf#>
PREFIX osmrel: <https://www.openstreetmap.org/relation/>

SELECT ?osm_id ?hasgeometry WHERE {
  osmrel:62768 ogc:sfContains ?osm_id .
  ?osm_id geo:hasGeometry/geo:asWKT ?hasgeometry .
  ?osm_id osmkey:building [] .
}
```

Try it: <https://qlever.cs.uni-freiburg.de/osm-planet/7cxklb>

## GeoSPARQL functions

Currently QLever implements these functions from GeoSPARQL:

### `geof:distance`

The function `geof:distance(?x, ?y)` currently expects two values with `geo:wktLiteral` datatype, each containing points. It returns the distance in kilometers. Currently, non-point geometries and different units of measurement are unsupported. 

### `geof:latitude`, `geof:longitude`

The functions `geof:latitude(?x)` and `geof:longitude(?x)` extract the latitude or longitude coordinate from a valid coordinate point with `geo:wktLiteral` datatype.

## Nearest neighbors search

QLever supports a custom fast spatial search operation. It can be invoked using two special predicates.

### `<nearest-neighbors:k>` or `<nearest-neighbors:k:m>`

A spatial search for nearest neighbors can be realized using `?left <nearest-neighbors:k:m> ?right`. The variables `?left` and `?right` must refer to points with `geo:wktLiteral` datatype. Additionally, please replace `k` and `m` with integers as follows:

- For each point `?left` QLever will output the `k` nearest points from `?right`. Of course, the sets `?left` and `?right` can each be limited using further statements.
- Using the optional integer value `m` a maximum distance in meters can be given that restricts the search radius.

### `<max-distance-in-meters:m>`

The predicate `<max-distance-in-meters:m>` behaves almost like the `<nearest-neighbors:k:m>` predicate. The parameter `m` also refers to the maximum search radius in meters. The only difference is that any number of result points within the search radius are emitted as output.

### Example: Railway stations and supermarkets

This example query calculates the three closest supermarkets to every railway station. QLever can answer this hard query for the entire planet's OpenStreetMap data within 8 - 10 seconds on a good consumer PC.

```sparql
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:>
PREFIX ogc: <http://www.opengis.net/rdf#>
PREFIX osmrel: <https://www.openstreetmap.org/relation/>
SELECT * WHERE {
  {
    ?station osmkey:railway "station" .
    ?station osmkey:name ?name .
    ?station geo:hasCentroid/geo:asWKT ?station_geometry .
  }
  ?station_geometry <nearest-neighbors:3> ?supermarket_geometry .
  {
    ?supermarket osmkey:shop "supermarket" .
    ?supermarket geo:hasCentroid/geo:asWKT ?supermarket_geometry .
  }
}
```

Try it: <https://qlever.cs.uni-freiburg.de/osm-planet/swauai>
