#Query Planning


##Strategy:

* Create a graph from the query.
Each triple corresponds to a node, there is an edge between two nodes iff they share a variable.
Text operations always form cliques (all triples are connected via the context variable).
Turn them into a single nodes with the word-part stored as payload.
This node naturally has an edge to each connected variable.

* Build a QueryExecutionTree object.
The important part is its root operation.
Such operations can have a no children (<span style="font-variant: small-caps">scan, text_no_filter</span>), one child (<span style="font-variant: small-caps">sort, text_with_filter, distinct, filter, order_by</span>) or two children (<span style="font-variant: small-caps">join, multi_column_join</span>). 
Children are always QueryExecutionTree objects on their own and their results can be cached and/or reused within a query (e.g. a <span style="font-variant: small-caps">scan</span> for type/object/name).
In general, each node of the graph will correspond to an operation with no children.

* Modifiers like <span style="font-variant: small-caps">order_by</span> or <span style="font-variant: small-caps">distinct</span> are applied in the end (topmost in the tree). 
<span style="font-variant: small-caps">limit</span> and <span style="font-variant: small-caps">offset</span> aren't applied at all and only considered when creating readable (<span style="font-variant: small-caps">json</span>, std::cout, etc) results.

##Building the execution tree from the graph:

We use a dynamic programming approach. _TODO: read more related work and cite something, it's not new and I read it for RDF3X where it isn't new either._

Let `n` be the number of nodes in the graph (i.e. the number of triples in the <span style="font-variant: small-caps">sparql</span> query for queries without text and possible less for queries with text part). 
We then create DP table with `n` rows where the `k`'th row contains all possible QueryExecutionsTrees for sub-queries that contain `k` triples (`k in [1 .. n]`).
We seed the first row with the `n` <span style="font-variant: small-caps">scan</span>s (or <span style="font-variant: small-caps">text_without_filter</span>) operations pertaining to the nodes. 
Then we create row after row by trying all possible merges.
The `k`'th row is created by trying to merge all combinations of rows `i` and `j`, s.th. `i + j = k`.
It is possible to merge two sub-trees iff: 1) They do not already overlap in terms of included nodes/scans and 2) there is an edge between one of their contained nodes in the query graph.
To merge two subtrees, a join is created. 
Any subtree that is not yet sorted on the join column, is sorted by an extra SORT operation.
There are two special cases: 
1) If at least one subtree pertains to a <span style="font-variant: small-caps">text_without_filter</span> operation, we create both possible plans: <span style="font-variant: small-caps">join</span> normally (plus <span style="font-variant: small-caps">sort</span> before) and a <span style="font-variant: small-caps">text_with_filter</span> operation. <sup>[1](#textwfilter)</sup>
2) If they are connected by more than one edge, we have something we called a "cyclic query" already, and create a special <span style="font-variant: small-caps">two_column_join</span> operation.

Before we return a row, we prune away unneeded execution trees. 
For each combination of: included nodes + included filters + sort column, we only keep the one with the lowest cost estimate.

After each row, we apply all possible <span style="font-variant: small-caps">filter</span> operations<sup>.[2](#filterfn)</sup>
When a <span style="font-variant: small-caps">filter</span> is applied we *add* another tree to that row. The exception is the very last (`n`'th) row where we replace the trees.
This allows <span style="font-variant: small-caps">filter</span> operations to be taken at any time of the query. Usually it is better to take them earlier because they can only reduce the number of elements and are usually fast to evaluate, but sometimes it is better for them to be delayed because only then, a <span style="font-variant: small-caps">text_with_filter</span> operation can be created (it's only possible if one of the children is a <span style="font-variant: small-caps">text_without_filter</span> operation and not if a <span style="font-variant: small-caps">filter</span> is applied to it already).
The exception is the last row where all <span style="font-variant: small-caps">filter</span>s have to be taken.
Finally, the tree with the lowest cost estimate is used.

&nbsp;

<span style="font-size: small">
<a name="textwfilter">1</a>: When a <span style="font-variant: small-caps">text_with_filter</span> operation is created, one subtree is kept as a child and the <span style="font-variant: small-caps">text_without_filter</span> operation is removed / included in the operation.

<a name="filterfn">2</a>: A <span style="font-variant: small-caps">filter</span> operation can be applied as soon as all it's variables are covered somewhere in the query. This is possible because the number of distinct elements for each variable becomes lower while the query is executed. It is the highest after an initial <span style="font-variant: small-caps">scan</span> and always corresponds to the intersection after each join. That said, multiplicity and total number fo rows may become larger throughout the query - possibly by a lot. 
</span>