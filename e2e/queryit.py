#!/usr/bin/env python3
"""
QLever Query Tool for End2End Testing
"""

import sys
import urllib.parse
import urllib.request
import json
import icu
import yaml
from typing import Dict, Any, List
from datatypes import datatypes


class Color:
    """
    Enum-like class for storing ANSI Color Codes
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def eprint(*args, color=Color.FAIL, **kwargs):
    """
    Prints a custom coloured exception
    """
    sys.stdout.write(color)
    print(*args, **kwargs)
    print(Color.ENDC)


def wprint(*args, color=Color.FAIL, **kwargs):
    """
    Prints a custom coloured warning
    """
    sys.stdout.write(color)
    print(*args, **kwargs)
    print(Color.WARNING)


def exec_query(endpoint_url: str, sparql: str, action,
               max_send: int = 4096) -> [None, Dict[str, Any]]:
    """
    Execute a single SPARQL query against the given endpoint
    """
    params = urllib.parse.urlencode({'query': sparql, 'send': max_send, 'action': action})
    url_suffix = '/?' + params
    request = urllib.request.Request(endpoint_url + url_suffix)
    conn = urllib.request.urlopen(request)
    if conn.status != 200:
        eprint("Error executing SPARQL Query: ", sparql)
        return None
    return json.load(conn)


def check_keys_in_result(result: Dict[str, Any], required_keys: List[str]) -> bool:
    """
    Tests if the required keys are presented in the JSON structure
    """
    for key in required_keys:
        if key not in result:
            eprint('QLever Result is missing "%s" field' % key)
            return False
    return True


def check_structure_qlever_json(result: Dict[str, Any]) -> bool:
    """
    Checks a QLever Result object for sanity
    """
    return check_keys_in_result(result, ['query', 'status', 'resultsize', 'selected', 'res'])



def check_structure_sparql_json(result: Dict[str, Any]) -> bool:
    """
    Checks a sparql-results+json object for sanity
    """
    if not check_keys_in_result(result, ['head', 'results']):
        return False
    if not check_keys_in_result(result['head'], ['vars']):
        return False
    if not check_keys_in_result(result['results'], ['bindings']):
        return False
    return True


def quotes_inner(quoted: str) -> str:
    """
    For a string containing a quoted part returns the inner part
    """
    left_quote = quoted.find('"')
    right_quote = quoted.rfind('"')
    if right_quote < 0:
        right_quote = len(quoted)
    return quoted[left_quote + 1:right_quote]


def check_row_sparql_json(variables: List[str], gold_row: List[Any], actual_row: Dict[str, any],
                          row_datatype: List[str]) -> bool:
    """
    Test if the input content (gold_row) and its variables are a match of a given row of the SparqlJSON result (actual_row)
    along with the input datatype provided by the yaml file
    """
    import datatypes

    for i, gold in enumerate(gold_row):
        if gold is None:
            continue
        var = variables[i]
        if var not in actual_row and var.startswith("TEXT("):
            var = var[5:-1]
        if var not in actual_row:
            eprint("{} not contained in row {}".format(var, actual_row))
            return False
        # retrieves the value from the result-variable
        actual = actual_row[var]
        target_type = "literal"
        # checks if variable type is 'iri'
        if isinstance(gold, str) and gold.startswith('<'):
            target_type = "iri"
            if not (gold[0] == '<' and gold[-1] == '>'):
                eprint("\tbad iri: " + gold + "\n")
                return False
            gold = gold[1:-1]
        if not actual["type"] == target_type:
            return False
        matches = None
        # retrieve all the possible datatypes of the input variable column
        multiple_datatype = row_datatype[i].split("|")
        for multitype_value in multiple_datatype:
            value_parsed = ex_actual_parsed = None
            try:
                value_parsed = datatypes.cast_datatype(gold, multitype_value, True)
            except Exception as e:
                ex_actual_parsed = e
            finally:
                if value_parsed:
                    actual_type = None
                    if "datatype" in actual:
                        # retrieves the datatype of the result-variable
                        actual_type = actual["datatype"][actual["datatype"].rfind('#') + 1:]
                        if not multitype_value == actual_type:
                            continue  # jump incorrect type
                    # test if the input value, result row and the lexical form are a match
                    if multitype_value != "string" and actual_type is not None:
                        actual_value = actual["value"]
                        parsed_actual = datatypes.cast_datatype(actual_value, actual_type, True)
                        # check same value and lexical form (0.1 != 00.1)
                        # https://www.w3.org/TR/rdf11-concepts/#section-rdf-graph
                        matches = value_parsed == parsed_actual and \
                                  multitype_value == actual_type and \
                                  str(gold) == actual_value
                    # test the match when the expected-variable and/or result-variable are null or raw-string type
                    else:
                        wprint("\tWarning_row_sparql_json = " + str(ex_actual_parsed))
                        matches = value_parsed == actual["value"]  # empty datatype in result-values or empty values
                # test the match when the expected-variable cannot be cast to the desired result due to possible multi-datatype
                # (example: string to decimal exception) or when the expected-variable value is null
                else:
                    matches = gold == actual["value"]  # empty values
                if matches:
                    break
        if not matches:
            return False
    return True


def check_row_qlever_json(gold_row: List[Any], actual_row: List[Any],
                          row_datatype: List[str]) -> bool:
    """
    Test if the input content (gold_row) is a match of a given row of the QLeverJSON result (actual_row)
    along with the input datatype provided by the yaml file
    Returns true if they match
    """
    import datatypes
    # tests if the input-row size matches the result-row
    if len(gold_row) != len(actual_row):
        return False
    for i, gold in enumerate(gold_row):
        if gold is None:
            continue
        actual = actual_row[i]
        actual_type = None
        # retrieve all the possible datatypes of the input variable column
        multiple_datatype = row_datatype[i].split("|")
        if actual and actual[0] == '"':
            # retrieves the value from the result-variable
            actual_value = actual[1:actual.rfind('"')]
            # retrieves the datatype from the result-variable
            actual_type = actual[actual.rfind('#') + 1:-1]
        else:
            actual_value = actual
        matches = None
        for multitype_value in multiple_datatype:
            if actual_type is None:
                actual_type = multitype_value
            if not multitype_value == actual_type:
                continue  # jump incorrect type
            value_parsed = actual_parsed = None
            ex_value_parsed = ex_actual_parsed = None
            try:
                value_parsed = datatypes.cast_datatype(gold, multitype_value, True)
            except Exception as e:
                ex_value_parsed = e
            finally:
                try:
                    actual_parsed = datatypes.cast_datatype(actual_value, actual_type, True)
                except Exception as e:
                    ex_actual_parsed = e
                finally:
                    # test if the input value, result row and the lexical form are a match
                    if value_parsed and actual_parsed:
                        # check same value and lexical form (0.1 != 00.1)
                        # https://www.w3.org/TR/rdf11-concepts/#section-rdf-graph
                        matches = value_parsed == actual_parsed and str(gold) == actual_value
                    elif value_parsed and not actual_parsed:
                        wprint("\tWarning_row_qlever_json = 1_ " + str(ex_actual_parsed))
                        matches = False
                    # test the match when the expected-variable cannot be cast to the desired result due to possible multi-datatype
                    # (example: string to decimal exception) or when the expected-variable value is null
                    elif not value_parsed and not actual_parsed:
                        # actual_parsed
                        wprint("\tWarning_row_qlever_json (actual) = " + str(ex_actual_parsed))
                        # value_parsed
                        wprint("\tWarning_row_qlever_json (gold) = " + str(ex_value_parsed))
                        matches = gold == actual_value  # empty/null values
                    else:  # not value_parsed and actual_parsed
                        wprint("\tWarning_row_qlever_json (actual) = " + str(ex_actual_parsed))
                        matches = False  # empty/null values
        if not matches:
            return False
    return True


def query_checks(query: Dict[str, Any],
                 result: Dict[str, Any], test_check_method) -> bool:
    """
    Tests the checks specified in the query
    """
    if 'checks' not in query:
        return True
    passed = True
    checks = query['checks']
    row_data_type = []
    for check in checks:
        res = test_check_method(check, result, row_data_type)
        if not res[0]:
            passed = False
        elif len(row_data_type) == 0:
            row_data_type = res[1]  # retrieve row_data_type for row_check
    return passed


def test_check_qlever_json(check_dict: Dict[str, Any], result: Dict[str, Any], row_data_type: List[str]) -> (bool, str):
    """
    Test if the named result check holds. Returns True if it does
    """

    collator = icu.Collator.createInstance(icu.Locale('de_DE.UTF-8'))
    res = result['res']
    for check, value in check_dict.items():
        if check == 'num_rows':
            if len(res) != int(value):
                eprint("num_rows_qlever_json check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, len(res)))
                return False, row_data_type
        elif check == 'num_cols':
            for row in res:
                if len(row) != int(value):
                    eprint("num_cols_qlever_json check failed:\n" +
                           "\texpected %r, got %r, row: %s" %
                           (value, len(row), json.dumps(row)))
                    return False, row_data_type
        elif check == 'selected':
            if value != result['selected']:
                eprint("selected_qlever_json check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, result['selected']))
                return False, row_data_type
        elif check == 'row_data_types':
            for column_data_type in value:
                multiple_datatype = column_data_type.split("|")
                # tests if the datatype(s) is/are correct
                for mul_datatype in multiple_datatype:
                    if mul_datatype not in datatypes:
                        eprint("row_data_types_qlever_json check failed:\n" +
                               "\tunexpected data type \'" + mul_datatype + "\'")
                        return False, row_data_type
                # add the datatype(s) to the list to be used when matching rows
                row_data_type.append(column_data_type)
        elif check == 'res':
            gold_res = value
            if len(gold_res) != len(res):
                eprint("res_qlever_json check failed:\n" +
                       "\texpected number of rows: %r" % len(gold_res) +
                       "\tdoes not match actual: %r" % len(value))
                return False, row_data_type
            for i, gold_row in enumerate(gold_res):
                actual_row = res[i]
                if not check_row_qlever_json(gold_row, actual_row, row_data_type):
                    eprint("res_qlever_json check failed:\n" +
                           "\tat row %r" % i +
                           "\n\t\texpected: %r \n\t\tgot: %r" %
                           (gold_row, actual_row))
                    return False, row_data_type
        elif check == 'contains_row':
            found = False
            gold_row = value
            for actual_row in res:
                if check_row_qlever_json(gold_row, actual_row, row_data_type):
                    found = True
                    break
            if not found:
                eprint("contains_row_qlever_json check failed:\n" +
                       "\tdid not find %r" % gold_row)
                return False, row_data_type
        elif check == 'contains_warning':
            for requested_warning in value:
                found = False
                for actual_warning in result["warnings"]:
                    if actual_warning.find(requested_warning):
                        found = True
                        break
                if not found:
                    eprint("contains_warning_qlever_json check failed:\n" +
                           "\tdid not find %r" % requested_warning)
                    eprint("actual warnings:", result["warnings"])
                    return False, row_data_type
        elif check.startswith('order_'):
            try:
                direction, var = value['dir'], value['var']
                col_type = check.split('_')[1]
                col_idx = result['selected'].index(var)
                for row_idx in range(1, len(res)):
                    previous = res[row_idx - 1][col_idx]
                    current = res[row_idx][col_idx]
                    if col_type == 'numeric':
                        previous_value = float(quotes_inner(previous))
                        current_value = float(quotes_inner(current))
                    elif col_type == 'string':
                        previous_value = collator.getSortKey(previous)
                        current_value = collator.getSortKey(current)
                    else:
                        eprint('Unknown/Unexpected Order Type: ' + col_type)
                        return False, row_data_type
                    if direction.lower() == 'asc' and previous_value > current_value:
                        eprint('order_numeric_qlever_json check failed:\n\tnot ascending for {} and {}'.format(
                            previous_value,
                            current_value))
                        return False, row_data_type
                    if direction.lower() == 'desc' and previous_value < current_value:
                        eprint('order_numeric_qlever_json check failed:\n\tnot ascending for {} and {}'.format(
                            previous_value,
                            current_value))
                        return False, row_data_type
            except ValueError as ex:
                eprint('order_numeric_qlever_json check failed:\n\t' + str(ex))
                return False, row_data_type
        else:
            eprint("Unexpected check (qlever_json) : " + check)
            return False, row_data_type

    return True, row_data_type


def test_check_sparql_json(check_dict: Dict[str, Any], result: Dict[str, Any], row_data_type: List[str]) -> (bool, str):
    """
    Test if the named result check holds. Returns True if it does
    """

    collator = icu.Collator.createInstance(icu.Locale('de_DE.UTF-8'))
    res = result['results']['bindings']
    for check, value in check_dict.items():
        if check == 'num_rows':
            if len(res) != int(value):
                eprint("num_rows_sparql_json check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, len(res)))
                return False, row_data_type
        elif check == 'num_cols':
            # undefined variables don't create bindings,
            # so this check is not really possible for the
            # sparql json ( only maximum columns check is possible )
            for row in res:
                if len(row) > int(value):
                    eprint("num_cols_sparql_json check failed:\n" +
                           "\texpected %r, got %r, row: %s" %
                           (value, len(row), json.dumps(row)))
                    return False, row_data_type
        elif check == 'selected':
            if value != result['head']['vars']:
                eprint("selected_sparql_json check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, result['selected']))
                return False, row_data_type
        elif check == 'row_data_types':
            for column_data_type in value:
                multiple_datatype = column_data_type.split("|")
                # tests if the datatype(s) is/are correct
                for mul_datatype in multiple_datatype:
                    if mul_datatype not in datatypes:
                        eprint("row_data_types_sparql_json check failed:\n" +
                               "\tunexpected data type \'" + mul_datatype + "\'")
                        return False, row_data_type
                # add the datatype(s) to the list to be used when matching rows
                row_data_type.append(column_data_type)
        elif check == 'res':
            gold_res = value
            if len(gold_res) != len(res):
                eprint("res_sparql_json check failed:\n" +
                       "\texpected number of rows: %r" % len(gold_res) +
                       "\tdoes not match actual: %r" % len(value))
                return False, row_data_type
            for i, gold_row in enumerate(gold_res):
                actual_row = res[i]
                if not check_row_sparql_json(result["head"]["vars"], gold_row, actual_row, row_data_type):
                    eprint("res_sparql_json check failed:\n" +
                           "\tat row %r" % i +
                           "\n\t\texpected: %r \n\t\tgot: %r" %
                           (gold_row, actual_row))
                    return False, row_data_type
        elif check == 'contains_row':
            found = False
            gold_row = value
            for actual_row in res:
                if check_row_sparql_json(result["head"]["vars"], gold_row, actual_row, row_data_type):
                    found = True
                    break
            if not found:
                eprint("contains_row_sparql_json check failed:\n" +
                       "\tdid not find %r" % gold_row)
                return False, row_data_type
        elif check == 'contains_warning':
            # currently the sparql_json contains no warnings
            # TODO<joka921> is there any harm in adding them?
            pass
        elif check.startswith('order_'):
            try:
                direction, var = value['dir'], value['var']
                col_type = check.split('_')[1]
                for row_idx in range(1, len(res)):
                    previous = res[row_idx - 1][var]
                    current = res[row_idx][var]
                    if col_type == 'numeric':
                        previous_value = float(previous['value'])
                        current_value = float(current['value'])
                    elif col_type == 'string':
                        previous_value = collator.getSortKey(previous['value'])
                        current_value = collator.getSortKey(current['value'])
                    else:
                        eprint('Unknown/Unexpected Order Type: ' + col_type)
                        return False, row_data_type
                    if direction.lower() == 'asc' and previous_value > current_value:
                        eprint('order_numeric_sparql_json check failed:\n\tnot ascending for {} and {}'.format(
                            previous_value['value'],
                            current_value['value']))
                        return False, row_data_type
                    if direction.lower() == 'desc' and previous_value < current_value:
                        eprint('order_numeric_sparql_json check failed:\n\tnot ascending for {} and {}'.format(
                            previous_value['value'],
                            current_value['value']))
                        return False, row_data_type
            except ValueError as ex:
                eprint('order_numeric_sparql_json check failed:\n\t' + str(ex))
                return False, row_data_type
        else:
            eprint("Unexpected check : " + check)
            return False, row_data_type

    return True, row_data_type


def print_qlever_result(result: Dict[str, Any]) -> None:
    """
    Prints a QLever Result to stdout
    """
    eprint(json.dumps(result))


def main() -> None:
    """
    Run QLever queries stored in a YAML file against a QLever instance
    """
    if len(sys.argv) != 3:
        eprint("Usage: ", sys.argv[0], "<yaml_in> <qlever_endpoint_url>")
        sys.exit(1)

    inpath = sys.argv[1]
    endpoint_url = sys.argv[2]
    error_detected = False
    with open(inpath, 'rb') if inpath != '-' else sys.stdin as infile:
        yaml_tree = yaml.safe_load(infile)
        queries = yaml_tree['queries']
        for query in queries:
            query_name = query['query']
            query_type = query['type']
            query_sparql = query['sparql']
            print(Color.HEADER + 'Trying: ', query_name,
                  '(%s)' % query_type + Color.ENDC)
            print('SPARQL:')
            print(query_sparql)
            sys.stdout.flush()
            result = exec_query(endpoint_url, query_sparql, action="qlever_json_export")
            if not result:
                # A print was already done in exec_query()
                error_detected = True
                print_qlever_result(result)
                continue

            if not check_structure_qlever_json(result):
                error_detected = True
                print_qlever_result(result)
                continue

            if result['status'] != 'OK':
                eprint('QLever Result "status" is not "OK"')
                error_detected = True
                print_qlever_result(result)
                continue

            if not query_checks(query, result, test_check_qlever_json):
                error_detected = True
                continue

            result = exec_query(endpoint_url, query_sparql, action="sparql_json_export")
            if not result:
                # A print was already done in exec_query()
                error_detected = True
                print_qlever_result(result)
                continue

            if not check_structure_sparql_json(result):
                error_detected = True
                print_qlever_result(result)
                continue

            if not query_checks(query, result, test_check_sparql_json):
                error_detected = True
                continue

    if error_detected:
        print(Color.FAIL + 'Query tool found errors!' + Color.ENDC)
        sys.exit(2)

    print(Color.OKGREEN + 'Query tool did not find errors, search harder!' + Color.ENDC)


if __name__ == '__main__':
    print("Python {:s} on {:s}\n".format(sys.version, sys.platform))
    main()
