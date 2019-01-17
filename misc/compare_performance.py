#!/usr/bin/env python3
import argparse
import sys
import subprocess
import json, requests


__author__ = 'buchholb'

virtuoso_run_binary = "/home/buchholb/virtuoso/bin/isql"
virtuoso_isql_port = "1111"
bifc_isql_port = "1113"
bifc_inc_isql_port = "1114"
virtuso_isql_user = "dba"
rdf3x_run_binary = "/home/buchholb/rdf3x-0.3.8/bin/rdf3xquery"
rdf3x_db = "/local/raid/ad/buchholb/eval/rdf3x/wikipedia-fbeasy.contains.db"
#my_index = "/local/raid/ad/buchholb/eval/wikipedia-fbeasy.withstopwords"
my_index = "/local/raid/ad/buchholb/wikipedia-fbeasy/wikipedia-fbeasy"
my_binary = "/home/buchholb/SparqlEngine/build/SparqlEngineMain"
broccoli_api = 'http://localhost:6001/'

parser = argparse.ArgumentParser()

parser.add_argument('--queryfile',
                    type=str,
                    help='One line per (standard) SPARQL query. TS specific transformations are made by the python script.',
                    required=True)

parser.add_argument('--virtuoso-pwd',
                    type=str,
                    help='Password for the (already running) virtuoso instance',
                    required=True)

parser.add_argument('--all', action='store_true', help='should all be executed?',
                    default=False)
parser.add_argument('--virt', action='store_true', help='should virt be executed?',
                    default=False)
parser.add_argument('--bifc', action='store_true', help='should bifc be executed?',
                    default=False)
parser.add_argument('--bifc-inc', action='store_true', help='should bifc_inc be executed?',
                    default=False)
parser.add_argument('--mine', action='store_true', help='should mine be executed?',
                    default=False)
parser.add_argument('--rdf3x', action='store_true', help='should rdf3x be executed?',
                    default=False)
parser.add_argument('--broccoli', action='store_true', help='should broccoli be executed?',
                    default=False)


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
                if o not in context_to_words:
                    context_to_words[o] = []
                context_to_words[o].append(s[6: -1])
            except ValueError:
                print("Problem in : " + c, file=sys.stderr)
                exit(1)
        elif '<in-text>' in c: # <subject> <in-text> ?context
            try:
                s, p, o = c.strip().split(' ')
                new_clauses.append(o + ' ql:contains-entity ' + s)
            except ValueError:
                print("Problem in : " + c, file=sys.stderr)
                exit(1)
        else:
            new_clauses.append(c)
    for c, ws in context_to_words.items():
        new_clauses.append(' ' + c + ' ql:contains-word "' + ' '.join(ws) + '" ')
    new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
    return 'WHERE'.join([before_where, new_after_where])


def rewrite_for_rdf3x(q):
    if '*>' in q:
        print('Inexpressible prefix search in rdf3x: ' + q.strip(), file=sys.stderr)
        return 'NOT POSSIBLE: ' + q.strip()
    if 'FILTER' in q and 'FILTER' in q[q.find('FILTER') + 5:]:
        print('More than one FILTER inexpressible in rdf3x: ' + q.strip(), file=sys.stderr)
        return 'NOT POSSIBLE: ' + q.strip()
    before_where, after_where = q.split('WHERE')
    if '<in-text>' in q and 'DISTINCT' not in q:
        before_where = before_where[:before_where.find('SELECT')] \
                       + ' SELECT DISTINCT' \
                       + before_where[before_where.find('SELECT') + 6:]
    no_mod, mod = after_where.strip().split('}')
    clauses = no_mod.strip('{').split('.')
    new_clauses = []
    for c in clauses:
        if 'FILTER' in c and '!=' in c:
            e1 = c[c.find('?'): c.find('!=')].strip()
            e2 = c[c.find('!=') + 2: c.find(')')].strip()
            new_clauses.append(' FILTER(!(' + e1 + '=' + e2 + ')) ')
        else:
            if "<in-text>" in c:
                s,p,o = c.strip().split(' ');
                if "<word:" in s:
                    p = "<contains-word>"
                else:
                    p = "<contains-entity>"
                new_clauses.append(' '.join(['', o, p, s, '']))
            else:
                new_clauses.append(c)
    if 'OFFSET' in mod:
        print('Inexpressible in OFFSET clause in rdf3x: ' + mod, file=sys.stderr)
        return 'NOT POSSIBLE: ' + q.strip()
    new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
    return 'WHERE'.join([before_where, new_after_where])


def rewrite_to_bif_contains_inc(q):
    before_where, after_where = q.split('WHERE')
    if '<in-text>' in q and 'DISTINCT' not in q:
        before_where = before_where[:before_where.find('SELECT')] \
                       + ' SELECT DISTINCT' \
                       + before_where[before_where.find('SELECT') + 6:]
    no_mod, mod = after_where.strip().split('}')
    clauses = no_mod.strip('{').split('.')
    new_clauses = []
    context_to_words = {}
    for c in clauses:
        if '<word:' in c:
            try:
                s, p, o = c.strip().split(' ')
                if o not in context_to_words:
                    context_to_words[o] = []
                word = s[6: -1]
                if word[-1] == '*' or word.isdigit():
                    word = "'" + word + "'"
                context_to_words[o].append(word)
            except ValueError:
                print("Problem in : " + c, file=sys.stderr)
                exit(1)
        else:
            new_clauses.append(c)
    for c, ws in context_to_words.items():
        new_clauses.append(' ' + c + ' <text> ?text' + c[1:] + ' ')
        new_clauses.append(' ?text' + c[1:] + ' bif:contains "'
                           + ' and '.join(ws) + '" ')
    new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
    return 'WHERE'.join([before_where, new_after_where])


def rewrite_to_bif_contains(q):
    before_where, after_where = q.split('WHERE')
    if '<in-text>' in q and 'DISTINCT' not in q:
        before_where = before_where[:before_where.find('SELECT')] \
                       + ' SELECT DISTINCT' \
                       + before_where[before_where.find('SELECT') + 6:]
    no_mod, mod = after_where.strip().split('}')
    clauses = no_mod.strip('{').split('.')
    new_clauses = []
    context_to_words = {}
    entities_to_contexts = {}
    for c in clauses:
        if '<word:' in c:
            try:
                s, p, o = c.strip().split(' ')
                if o not in context_to_words:
                    context_to_words[o] = []
                word = s[6: -1]
                if word[-1] == '*' or word.isdigit():
                    word = "'" + word + "'"
                context_to_words[o].append(word)
            except ValueError:
                print("Problem in : " + c, file=sys.stderr)
                exit(1)
        else:
            if '<in-text>' not in c:
                new_clauses.append(c)
            else:
                try:
                    s, p, o = c.strip().split(' ')
                    if s[0] != '?':
                        print('Inexpressible in bif:c without in-c: ' + c, file=sys.stderr)
                        return 'NOT POSSIBLE: ' + q.strip()
                    if s not in entities_to_contexts:
                        entities_to_contexts[s] = []
                    entities_to_contexts[s].append(o)
                    times_on_rhs = 0
                    for e, cs in entities_to_contexts.items():
                        if o in cs:
                            times_on_rhs += 1
                    if times_on_rhs > 1:
                        print('Inexpressible in bif:c without in-c: ' + q.strip(), file=sys.stderr)
                        return 'NOT POSSIBLE: ' + q.strip()
                except ValueError:
                    print("Problem in : " + c, file=sys.stderr)
                    exit(1)
    for e, cs in entities_to_contexts.items():
        for i, c in enumerate(cs):
            new_clauses.append(' ' + e + ' <text> ?text' + str(i) + ' ')
            new_clauses.append(' ?text' + str(i) + ' bif:contains "' + ' and '.join(context_to_words[c]) + '" ')
    new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
    return 'WHERE'.join([before_where, new_after_where])


def rewrite_for_broccoli(q):
    before_where, after_where = q.split('WHERE')
    if before_where.count('?') > 1:
        print("Inexpressible in Broccoli: " + q.strip(), file=sys.stderr)
        return "NOT POSSIBLE: " + q.strip()
    i = before_where.find('?')
    j = before_where.find(' ', i)
    var_to_var = dict()
    select_var = before_where[i:j]
    var_to_var[select_var] = '$1'
    after_where = after_where.replace(select_var, '$1')
    j = 0
    while '?' in after_where:
        i = after_where.find('?', j)
        j = after_where.find(' ', i)
        sparql_var = after_where[i:j]
        bro_var = '$' + str(len(var_to_var) + 1)
        var_to_var[sparql_var] = bro_var
        after_where = after_where.replace(sparql_var, bro_var)
    no_mod, mod = after_where.strip().split('}')
    if 'FILTER' in after_where or 'ORDER' in mod:
        print("Inexpressible in Broccoli: " + q.strip(), file=sys.stderr)
        return "NOT POSSIBLE: " + q.strip()
    clauses = no_mod.strip('{').split('.')
    new_clauses = []
    equals_vars = []
    context_to_words = {}
    context_to_entities = {}
    for c in clauses:
        try:
            s, p, o = c.strip().split(' ')
            if p == '<in-text>':
                if '<word' in s:
                    if o not in context_to_words:
                        context_to_words[o] = []
                    context_to_words[o].append(s[6: -1])
                elif s[0] == '<':
                    bro_var = '$' + str(len(var_to_var) + 1)
                    var_to_var[s] = bro_var
                    entity = s[1:-1]
                    new_clauses.append(bro_var + " :r:equals :e:" + entity)
                    equals_vars.append(bro_var)
                    if o not in context_to_entities:
                        context_to_entities[o] = []
                    context_to_entities[o].append(bro_var)
                else:
                    if o not in context_to_entities:
                        context_to_entities[o] = []
                    context_to_entities[o].append(s)
            else:
                if p[0] != '<':
                    print("Inexpressible in Broccoli: " + c, file=sys.stderr)
                    return "NOT POSSIBLE: " + q.strip()
                p = ':r:' + p[1:-1]
                if s[0] == '<':
                    var_to_var[s] = '$' + str(len(var_to_var) + 1)
                    entity = s[1:-1]
                    s = var_to_var[s]
                    new_clauses.append(s + " :r:equals :e:" + entity)
                    equals_vars.append(s)
                if o[0] == '<':
                    if p == ':r:is-a':
                        o = ':e:' + o[1:-1]
                    else:
                        var_to_var[o] = '$' + str(len(var_to_var) + 1)
                        entity = o[1:-1]
                        o = var_to_var[o]
                        new_clauses.append(o + " :r:equals :e:" + entity)
                        equals_vars.append(o)
                new_clauses.append(' '.join([s, p, o]))
        except ValueError:
            print("Problem in : " + c, file=sys.stderr)
            exit(1)
    if len(context_to_entities) > 0 and len(context_to_words) == 0:
        print("Inexpressible in Broccoli: " + c, file=sys.stderr)
        return "NOT POSSIBLE: " + q.strip()
    for c, es in context_to_entities.items():
        if es[0] in equals_vars:
            new_clauses.append('$1 :r:has-occurrence-of '
                               + ' '.join(context_to_words[c]) + ' '
                               + ' '.join(es))
        else:
            new_clauses.append(es[0] + ' :r:occurs-with '
                           + ' '.join(context_to_words[c]) + ' '
                           + ' '.join(es[1:]))
    for c, ws in context_to_words.items():
        if c not in context_to_entities:
            new_clauses.append('$1 :r:has-occurrence-of ' + ' '.join(ws))

    new_after_where = ';'.join(new_clauses)
    broccoli_mod = '&nofrelations=0&nofhitgroups=0&nofclasses=0&nofwords=0'
    if 'LIMIT' in mod:
        i = mod.find('LIMIT')
        limit = mod[i+6:mod.find(' ', i + 6)]
        broccoli_mod += '&nofinstances=' + limit
    else:
        broccoli_mod += '&nofinstances=999999'
    if 'OFFSET' in mod:
        i = mod.find('OFFSET')
        offset = mod[i+6:mod.find(' ', i + 7)]
        broccoli_mod += '&firstinstance=' + offset
    return '?s=' + new_after_where.strip() + '&query=$1' + broccoli_mod




def get_virtuoso_query_times(query_file, pwd):
    print('get_virtuoso_query_times', file=sys.stderr)
    with open('__tmp.virtqueries', 'w') as tmpfile:
        for line in open(query_file):
            query = 'SPARQL ' + line.strip().split('\t')[1] + ';'
            if '<in-text>' in query and 'DISTINCT' not in query:
                query = query.replace('SELECT', 'SELECT DISTINCT')
            print(query, file=tmpfile)
    virtout = subprocess.check_output(
        [virtuoso_run_binary, virtuoso_isql_port, virtuso_isql_user, pwd,
         '__tmp.virtqueries']).decode('utf-8')
    coutfile = open('__tmp.cout.virtuoso', 'w')
    coutfile.write(virtout)
    coutfile.write('\n\n\n\n')
    times = []
    nof_matches = []
    for line in virtout.split('\n'):
        i = line.find('Rows. --')
        if i >= 0:
            j = line.find('msec')
            times.append(line[i + 9: j + 2])
            nof_matches.append(int(line[:i]))
            # os.remove('__tmp.rdf3xqueries')
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches):
        print('PROBLEM PROCESSING VIRTUOSO: q:' + str(len(queries)) + ' t:'
              + str(len(times)) + ' #:' + str(len(nof_matches)),
              file=sys.stderr)
    return times, nof_matches


def get_virtuoso_bifc_query_times(query_file, pwd):
    print('get_virtuoso_bifc_query_times', file=sys.stderr)
    impossibles = {}
    with open('__tmp.bifc_queries', 'w') as tmpfile:
        i = 0
        for line in open(query_file):
            bifc_query = rewrite_to_bif_contains(line.strip().split('\t')[1])
            if 'NOT POSSIBLE:' in bifc_query:
                impossibles[i] = True
            else:
                tmpfile.write('SPARQL ' + bifc_query + ';\n')
            i += 1
    virtout = subprocess.check_output(
        [virtuoso_run_binary, bifc_isql_port, virtuso_isql_user, pwd,
         '__tmp.bifc_queries']).decode('utf-8')
    coutfile = open('__tmp.cout.bifc', 'w')
    coutfile.write(virtout)
    coutfile.write('\n\n\n\n')
    times = []
    nof_matches = []
    for line in virtout.split('\n'):
        i = line.find('Rows. --')
        if i >= 0:
            while len(times) in impossibles:
                times.append('-')
                nof_matches.append(0)
            j = line.find('msec')
            times.append(line[i + 9: j + 2])
            nof_matches.append(int(line[:i]))
            # os.remove('__tmp.bifc_queries')
    queries = []
    while len(times) in impossibles:
        times.append('-')
        nof_matches.append(0)
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches):
        print('PROBLEM PROCESSING VIRTUOSO: q:' + str(len(queries)) + ' t:' +
              str(len(times)) + ' #:' + str(len(nof_matches)), file=sys.stderr)
    return times, nof_matches


def get_virtuoso_bifc_inc_query_times(query_file, pwd):
    print('get_virtuoso_bifc_inc_query_times', file=sys.stderr)
    with open('__tmp.bifc_inc_queries', 'w') as tmpfile:
        for line in open(query_file):
            bifc_query = rewrite_to_bif_contains_inc(line.strip().split('\t')[1])
            tmpfile.write('SPARQL ' + bifc_query + ';\n')
    virtout = subprocess.check_output(
        [virtuoso_run_binary, bifc_inc_isql_port, virtuso_isql_user, pwd,
         '__tmp.bifc_inc_queries']).decode('utf-8')
    coutfile = open('__tmp.cout.bifc_inc', 'w')
    coutfile.write(virtout)
    coutfile.write('\n\n\n\n')
    times = []
    nof_matches = []
    for line in virtout.split('\n'):
        i = line.find('Rows. --')
        if i >= 0:
            j = line.find('msec')
            times.append(line[i + 9: j + 2])
            nof_matches.append(int(line[:i]))
            # os.remove('__tmp.bifc_inc_queries')
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches):
        print('PROBLEM PROCESSING VIRTUOSO: q:' + str(len(queries)) + ' t:' +
              str(len(times)) + ' #:' + str(len(nof_matches)), file=sys.stderr)
    return times, nof_matches


def get_rdf3X_query_times(query_file):
    print('get_rdf3X_query_times', file=sys.stderr)
    impossibles = {}
    with open('__tmp.rdf3x_queries', 'w') as tmpfile:
        i = 0
        for line in open(query_file):
            rdf3x_query = rewrite_for_rdf3x(line.strip().split('\t')[1])
            if 'NOT POSSIBLE:' in rdf3x_query:
                impossibles[i] = True
            else:
                tmpfile.write(rdf3x_query + '\n')
            i += 1
    rdf3xout = subprocess.check_output(
        [rdf3x_run_binary, rdf3x_db, '__tmp.rdf3x_queries']).decode('utf-8')
    coutfile = open('__tmp.cout.rdf3x', 'w')
    coutfile.write(rdf3xout)
    coutfile.write('\n\n\n\n')
    times = []
    nof_matches = []
    last_ln = 0
    for ln, line in enumerate(rdf3xout.split('\n')):
        i = line.find('Done. Time: ')
        if i >= 0:
            while len(times) in impossibles:
                times.append('-')
                nof_matches.append(0)
            j = line.find('ms')
            times.append(line[i + 12: j + 2])
            nof_matches.append(ln - last_ln - 1)
            last_ln = ln + 1
    while len(times) in impossibles:
        times.append('-')
        nof_matches.append(0)
    # os.remove('__tmp.rdf3x_queries')
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches):
        print('PROBLEM PROCESSING RDF3X: q:' + str(len(queries)) + ' t:' +
                str(len(times)) + ' #:' + str(len(nof_matches)), file=sys.stderr)
    return times, nof_matches


def get_my_query_times(query_file):
    print('get_my_query_times', file=sys.stderr)
    with open('__tmp.my_queries', 'w') as tmpfile:
        for line in open(query_file):
            try:
                tmpfile.write(
                    expanded_to_my_syntax(line.strip().split('\t')[1]) + '\n')
            except IndexError:
                print("Problem with tabs in : " + line)
                exit(1)
    coutfile = open('__tmp.cout.mine', 'w')
    myout = subprocess.check_output(
        [my_binary, '-i', my_index, '-t', '--queryfile', '__tmp.my_queries']).decode('utf-8')
    coutfile.write(myout)
    coutfile.write('\n\n\n\n')
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
            nof_matches_no_limit.append(int(line[i + 30:]))
        i = line.find('Number of matches (limit): ')
        if i >= 0:
            nof_matches_limit.append(int(line[i + 27:]))
    # os.remove('__tmp.my_queries')
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches_limit):
        print('PROBLEM PROCESSING MINE: q:' + str(len(queries)) + ' t:' +
                str(len(times)) + ' #:' + str(len(nof_matches_limit)), file=sys.stderr)
    return times, nof_matches_limit


def get_broccoli_query_times(query_file):
    print('get_broccoli_query_times', file=sys.stderr)
    impossibles = {}
    with open('__tmp.broccoli_queries', 'w') as tmpfile:
        i = 0
        for line in open(query_file):
            broccoli_query = rewrite_for_broccoli(line.strip().split('\t')[1])
            if 'NOT POSSIBLE:' in broccoli_query:
                impossibles[i] = True
            else:
                tmpfile.write(broccoli_query + '\n')
            i += 1
    times = []
    nof_matches = []
    while len(times) in impossibles:
        times.append('-')
        nof_matches.append(0)
    first = True
    with open('__tmp.cout.broccoli', 'w') as coutfile:
        for line in open('__tmp.broccoli_queries'):
            if first:
                out = requests.get(broccoli_api + line.strip()
                                   + '&format=json&cmd=clearcache').json()
                first = False
            else:
                out = requests.get(broccoli_api + line.strip()
                               + '&format=json').json()
            print(str(out), file=coutfile)
            times.append(out['result']['time']['total'])
            nof_matches.append(int(out['result']['res']['instances']['sent']))
            while len(times) in impossibles:
                times.append('-')
                nof_matches.append(0)
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches):
        print('PROBLEM PROCESSING BROCCOLI: q:' + str(len(queries)) + ' t:' +
              str(len(times)) + ' #:' + str(len(nof_matches)), file=sys.stderr)
    return times, nof_matches


def print_result_table(queries, headers, time_and_counts):
    print('\t'.join(headers))
    print('\t'.join(['---'] * len(headers)))
    data = []
    for i in range(2, len(headers)):
        data.append([])
    for i in range(0, len(queries)):
        row = [queries[i]]
        approach_num = 0
        for (times, counts) in time_and_counts:
            row.append(str(times[i]))
            row.append(str(counts[i]))
            if times[i] != '-':
                data[approach_num * 2].append(float(times[i].strip().strip('ms').strip()))
                data[approach_num * 2 + 1].append(float(counts[i]))
            approach_num += 1
        print('\t'.join(row))
    print('\t'.join(['average', ' '] + ['{0:.2f}'.format((sum(l) / len(l))) for l in data]))


def main():
    args = vars(parser.parse_args())
    query_file = args['queryfile']
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    all = args['all']
    virt = args['virt']
    bifc = args['bifc']
    bifc_inc = args['bifc_inc']
    rdf3x = args['rdf3x']
    mine = args['mine']
    broccoli = args['broccoli']
    headers = []
    headers.append('id')
    headers.append('query')
    time_and_counts = []
    if all or virt:
        headers.append('virt')
        headers.append('#m_virt')
        times, matches = get_virtuoso_query_times(query_file, args['virtuoso_pwd'])
        time_and_counts.append((times, matches))
    if all or bifc:
        headers.append('bifc')
        headers.append('#m_bifc')
        times, matches = get_virtuoso_bifc_query_times(query_file, args['virtuoso_pwd'])
        time_and_counts.append((times, matches))
    if all or bifc_inc:
        headers.append('bifc_inc')
        headers.append('#m_bifc_inc')
        times, matches = get_virtuoso_bifc_inc_query_times(query_file, args['virtuoso_pwd'])
        time_and_counts.append((times, matches))
    if all or rdf3x:
        headers.append('rdf3x')
        headers.append('#m_rdf3x')
        times, matches = get_rdf3X_query_times(query_file)
        time_and_counts.append((times, matches))
    if all or mine:
        headers.append('mine')
        headers.append('#m_mine')
        times, matches = get_my_query_times(query_file)
        time_and_counts.append((times, matches))
    if all or broccoli:
        headers.append('broccoli')
        headers.append('#m_broccoli')
        times, matches = get_broccoli_query_times(query_file)
        time_and_counts.append((times, matches))
    print_result_table(queries, headers, time_and_counts)


if __name__ == '__main__':
    main()
