# Performance Comparison Scripts
**Note**: *This information is incomplete and may be wrong as it has been added
after the fact using guesses and the original code*

[compare_performance_only_own.py](compare_performance_only_own.py): This
script is used to performance test different QLever `SparqlEngineMain` binaries
(e.g. after a performance relevant change)

An example using the FreebaseEasy index and the
[query-file-notext.txt](query-sets/query-file-notext.txt) query set with nicer
TSV output can be run as follows:

```
./compare_performance_only_own.py \
   --index <path-to-wikipedia-freebase-easy>/wikipedia-freebase-easy \
   --queryfile query-sets/query-file-notext.txt \
   qlever-version-name ../build/SparqlEngineMain cost_factors.default.tsv \
   | column -t -s $'\t' | less -S
```

Sadly it seems no check for valid execution is performed. The query sets still
use the old `<in-text>` yet no errors are reported.
