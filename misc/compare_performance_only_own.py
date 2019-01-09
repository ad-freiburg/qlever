#!/usr/bin/env python3
import argparse
import sys
import os
import subprocess
from subprocess import Popen

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--queryfile',
                    type=str,
                    help='One line per (standard) SPARQL query. TS specific \
                          transformations are made by the python script.',
                    required=True)

parser.add_argument('--index',
                    type=str,
                    help='Index to use.',
                    required=True)

parser.add_argument('binaries', metavar='B', type=str, nargs='+',
                    help='binaries to use where each binary is specified as 3 \
                          string <binary>, <name> and <cost factor>')


def expanded_to_my_syntax(q):
    before_where, after_where = q.split('WHERE')
    no_mod, mod = after_where.strip().split('}')
    clauses = no_mod.strip('{').split('.')
    new_clauses = []
    context_to_words = {}
    for c in clauses:
        if '<word:' in c:
            try:
                s, p, o = c.strip().split(' ')
            except ValueError:
                print("Problem in : " + c)
                exit(1)
            if o not in context_to_words:
                context_to_words[o] = []
            context_to_words[o].append(s[6: -1])
        else:
            new_clauses.append(c)
    for c, ws in context_to_words.items():
        new_clauses.append(' ' + c + ' <in-text> "' + ' '.join(ws) + '" ')
    new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
    return 'WHERE'.join([before_where, new_after_where])


def get_query_times(query_file, name, binary, costFactors, index):
    with open('__tmp.myqueries', 'w') as tmpfile:
        for line in open(query_file):
            try:
                tmpfile.write(
                    expanded_to_my_syntax(line.strip().split('\t')[1]) + '\n')
            except IndexError:
                print("Problem with tabs in : " + line)
                exit(1)
    coutfile = open('__tmp.cout.' + name, 'w')
    myout = subprocess.check_output(
        [binary, '-i', index, '-t', '--queryfile', '__tmp.myqueries', '-c',
         costFactors]).decode('utf-8')
    print(myout, file=coutfile)
    print('\n\n\n', file=coutfile)
    times = []
    nof_matches_no_limit = []
    nof_matches_limit = []
    for line in myout.split('\n'):
        i = line.find('Done. Time: ')
        if i >= 0:
            j = line.find('ms')
            times.append(line[i + 12: j + 2])
        i = line.find('Number of matches (no limit): ')
        if i >= 0:
            nof_matches_no_limit.append(line[i + 30:])
        i = line.find('Number of matches (limit): ')
        if i >= 0:
            nof_matches_limit.append(line[i + 27:])
    # os.remove('__tmp.myqueries')
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(
            nof_matches_no_limit) or len(times) != len(nof_matches_limit):
        print('PROBLEM PROCESSING: ' + name + ' WITH PATH: ' + binary)
    return list(zip(times, nof_matches_no_limit, nof_matches_limit))


def process_queries_and_print_stats(query_file, binaries, index):
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    th_titles = ['id', 'query']
    results = []
    for (name, path, costFactors) in binaries:
        th_titles.append(name + "_times")
        th_titles.append(name + "_nof_matches_no_limit")
        # th_titles.append(name + "_nof_matches_limit")
        r = get_query_times(query_file, name, path, costFactors, index)
        results.append(r)
    print('\t'.join(th_titles))
    print('\t'.join(['----'] * len(th_titles)))
    for i in range(0, len(queries)):
        line_strs = [queries[i]]
        for res in results:
            line_strs.append(res[i][0])
            line_strs.append(res[i][1])
            # line_strs.append(res[i][2])
        print('\t'.join(line_strs))


def main():
    args = vars(parser.parse_args())
    queries = args['queryfile']
    index = args['index']
    arg_bins = args['binaries']
    assert len(arg_bins) % 3 == 0
    binaries = []
    for i in range(0, len(arg_bins) // 3):
        binaries.append(
            (arg_bins[3 * i], arg_bins[3 * i + 1], arg_bins[3 * i + 2]))
    process_queries_and_print_stats(queries, binaries, index)


if __name__ == '__main__':
    main()
