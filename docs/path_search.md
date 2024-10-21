# Path Search Feature Documentation for SPARQL Engine

## Overview

The Path Search feature in this SPARQL engine allows users to perform advanced queries
to find paths between sources and targets in a graph. It supports a variety of configurations,
including single or multiple source and target nodes, optional edge properties, and
custom algorithms for path discovery. This feature is accessed using the `SERVICE` keyword
and the service IRI `<https://qlever.cs.uni-freiburg.de/pathSearch/>`.

## Basic Syntax

The general structure of a Path Search query is as follows:

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>

SELECT ?start ?end ?path ?edge WHERE {
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;  # Specify the algorithm
           pathSearch:source <sourceNode> ;            # Specify the source node(s)
           pathSearch:target <targetNode> ;            # Specify the target node(s)
           pathSearch:pathColumn ?path ;               # Bind the path variable
           pathSearch:edgeColumn ?edge ;               # Bind the edge variable
           pathSearch:start ?start ;                   # Bind the edge start variable
           pathSearch:end ?end ;                       # Bind the edge end variable
    {SELECT * WHERE {
        ?start <predicate> ?end.                       # Define the edge pattern
    }}
  }
}
```

### Parameters

- **pathSearch:algorithm**: Defines the algorithm used to search paths. Currently, only `pathSearch:allPaths` is supported.
- **pathSearch:source**: Defines the source node(s) of the search.
- **pathSearch:target** (optional): Defines the target node(s) of the search.
- **pathSearch:pathColumn**: Defines the variable for the path.
- **pathSearch:edgeColumn**: Defines the variable for the edge.
- **pathSearch:start**: Defines the variable for the start of the edges.
- **pathSearch:end**: Defines the variable for the end of the edges.
- **pathSearch:edgeProperty** (optional): Specifies properties for the edges in the path.
- **pathSearch:cartesian** (optional): Controls the behaviour of path searches between
 source and target nodes. Expects a boolean. The default is `true`.
  - If set to `true`, the search will compute the paths from each source to **all targets**
  - If set to `false`, the search will compute the paths from each source to exactly
  **one target**. Sources and targets are paired based on their index (i.e. the paths
  from the first source to the first target are searched, then the second source and
  target, and so on).


### Example 1: Single Source and Target

The simplest case is searching for paths between a single source and a single target:

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>

SELECT ?start ?end ?path ?edge WHERE {
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;
           pathSearch:source <source> ;
           pathSearch:target <target> ;
           pathSearch:pathColumn ?path ;
           pathSearch:edgeColumn ?edge ;
           pathSearch:start ?start ;
           pathSearch:end ?end ;
    {
      SELECT * WHERE {
        ?start <predicate> ?end.
      }
    }
  }
}
```

### Example 2: Multiple Sources or Targets

It is possible to specify a set of sources or targets for the path search.

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>

SELECT ?start ?end ?path ?edge WHERE {
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;
           pathSearch:source <source1> ;
           pathSearch:source <source2> ;
           pathSearch:target <target1> ;
           pathSearch:target <target2> ;
           pathSearch:pathColumn ?path ;
           pathSearch:edgeColumn ?edge ;
           pathSearch:start ?start ;
           pathSearch:end ?end ;
    {
      SELECT * WHERE {
        ?start <predicate> ?end.
      }
    }
  }
}
```

This query will search forall between all sources and all targets, i.e.
- (`<source1>`, `<target1>`)
- (`<source1>`, `<target2>`)
- (`<source2>`, `<target1>`)
- (`<source2>`, `<target2>`)

It is possible to specify, whether the sources and targets should be combined according
to the cartesian product (as seen above) or if they should be matched up pairwise, i.e.
- (`<source1>`, `<target1>`)
- (`<source2>`, `<target2>`)

This can be done with the parameter `pathSearch:cartesian`. This parameter expects a
boolean. If set to `true`, then the cartesian product is used to match the sources with
the targets.
If set to `false`, then the sources and targets are matched pairwise. If left 
unspecified, then the default `true` is used.

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>

SELECT ?start ?end ?path ?edge WHERE {
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;
           pathSearch:source <source1> ;
           pathSearch:source <source2> ;
           pathSearch:target <target1> ;
           pathSearch:target <target2> ;
           pathSearch:pathColumn ?path ;
           pathSearch:edgeColumn ?edge ;
           pathSearch:start ?start ;
           pathSearch:end ?end ;
           pathSearch:cartesian false;
    {
      SELECT * WHERE {
        ?start <predicate> ?end.
      }
    }
  }
}
```

### Example 3: Edge Properties

You can also include edge properties in the path search to further refine the results:

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>

SELECT ?start ?end ?path ?edge WHERE {
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;
           pathSearch:source <source> ;
           pathSearch:target <target> ;
           pathSearch:pathColumn ?path ;
           pathSearch:edgeColumn ?edge ;
           pathSearch:edgeProperty ?middle ;
           pathSearch:start ?start ;
           pathSearch:end ?end ;
    {
      SELECT * WHERE {
        ?start <predicate1> ?middle.
        ?middle <predicate2> ?end.
      }
    }
  }
}
```

This is esecially useful for [N-ary relations](https://www.w3.org/TR/swbp-n-aryRelations/). 
Considering the example above, it is possible to query additional relations of `?middle`:

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>

SELECT ?start ?end ?path ?edge WHERE {
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;
           pathSearch:source <source> ;
           pathSearch:target <target> ;
           pathSearch:pathColumn ?path ;
           pathSearch:edgeColumn ?edge ;
           pathSearch:edgeProperty ?middle ;
           pathSearch:edgeProperty ?edgeInfo ;
           pathSearch:start ?start ;
           pathSearch:end ?end ;
    {
      SELECT * WHERE {
        ?start <predicate1> ?middle.
        ?middle <predicate2> ?end.
        ?middle <predicate3> ?edgeInfo.
      }
    }
  }
}
```

This makes it possible to query additional properties of the edge between `?start` and `?end` (such as `?edgeInfo` in the example above).


### Example 4: Source or Target as Variables

You can also bind the source and/or target dynamically using variables. The examples
below use `VALUES` clauses, which can be convenient to specify sources and targets.
However, the source/target variables can also be bound using any regular SPARQL construct.

#### Source Variable

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>

SELECT ?start ?end ?path ?edge WHERE {
  VALUES ?source {<source>}
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;
           pathSearch:source ?source ;
           pathSearch:target <target> ;
           pathSearch:pathColumn ?path ;
           pathSearch:edgeColumn ?edge ;
           pathSearch:start ?start ;
           pathSearch:end ?end ;
    {
      SELECT * WHERE {
        ?start <p> ?end.
      }
    }
  }
}
```

#### Target Variable

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>

SELECT ?start ?end ?path ?edge WHERE {
  VALUES ?target {<target>}
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;
           pathSearch:source <source> ;
           pathSearch:target ?target ;
           pathSearch:pathColumn ?path ;
           pathSearch:edgeColumn ?edge ;
           pathSearch:start ?start ;
           pathSearch:end ?end ;
    {
      SELECT * WHERE {
        ?start <p> ?end.
      }
    }
  }
}
```

## Error Handling

The Path Search feature will throw errors in the following scenarios:

- **Missing Start Parameter**: If the `start` parameter is not specified, an error will be raised.
- **Multiple Start or End Variables**: If multiple `start` or `end` variables are defined, an error is raised.
- **Invalid Non-Variable Start/End**: If the `start` or `end` parameter is not bound to a variable, the query will fail.
- **Unsupported Argument**: Arguments other than those listed (like custom user arguments) will cause an error.
- **Non-IRI Predicate**: Predicates must be IRIs. If not, an error will occur.

### Example: Missing Start Parameter

```sparql
PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>
SELECT ?start ?end ?path ?edge WHERE {
  SERVICE pathSearch: {
    _:path pathSearch:algorithm pathSearch:allPaths ;
           pathSearch:source <x> ;
           pathSearch:target <z> ;
           pathSearch:pathColumn ?path ;
           pathSearch:edgeColumn ?edge ;
           pathSearch:end ?end ;    # Missing start
    {
      SELECT * WHERE {
        ?start <p> ?end.
      }
    }
  }
}
```

This query would fail with a "Missing parameter 'start'" error.

