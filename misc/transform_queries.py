import argparse
import sys
import compare_performance

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--queryfile',
                    type=str,
                    help='One line per (standard) SPARQL query. TS specific transformations are made by the python script.',
                    required=True)

parser.add_argument('--rdf3x-query-outfile', type=str)

parser.add_argument('--my-query-outfile', type=str)

parser.add_argument('--bifc-query-outfile', type=str)

parser.add_argument('--bifc-inc-query-outfile', type=str)

parser.add_argument('--broccoli-query-outfile', type=str)

parser.add_argument('--names', action='store_true', help='Should I select names for all variables?',
                    default=False)

def main():
    args = vars(parser.parse_args())
    queries = args['queryfile']
    if (args['rdf3x_query_outfile']):
        with open(args['rdf3x_query_outfile'], 'w') as outfile:
            for line in open(queries):
                print(compare_performance.rewrite_for_rdf3x(line), file=outfile)
    if (args['my_query_outfile']):
        with open(args['my_query_outfile'], 'w') as outfile:
            for line in open(queries):
                q = line
                if (args['names']):
                    q = compare_performance.add_getting_names(q)
                print(compare_performance.expanded_to_my_syntax(q), file=outfile)
    if (args['bifc_query_outfile']):
        with open(args['bifc_query_outfile'], 'w') as outfile:
            for line in open(queries):
                print(compare_performance.rewrite_to_bif_contains(line), file=outfile)
    if (args['bifc_inc_query_outfile']):
        with open(args['bifc_inc_query_outfile'], 'w') as outfile:
            for line in open(queries):
                print(compare_performance.rewrite_to_bif_contains_inc(line), file=outfile)
    if (args['broccoli_query_outfile']):
        with open(args['broccoli_query_outfile'], 'w') as outfile:
            for line in open(queries):
                print(compare_performance.rewrite_for_broccoli(line), file=outfile)

if __name__ == '__main__':
    main()
