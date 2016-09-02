import argparse
import sys

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--ontologytxt',
                    type=str,
                    help='Broccoli ontology.txt file.',
                    required=True)


def handleSubject(element):
    if not element:
        return element
    new = '<'
    for c in element:
        if c == '<':
            new += '&lt'
        elif c == '>':
            new += '&gt'
        elif c == ' ':
            new += '_'
        elif c == '"':
            new += '%22'
        else:
            new += c
    new += '>'
    return new


def handleObject(element):
    if element and (element[0] != '"' or element[-1] != '>'):
        new = '<'
        for c in element:
            if c == '<':
                new += '&lt'
            elif c == '>':
                new += '&gt'
            elif c == ' ':
                new += '_'
            elif c == '"':
                new += '%22'
            else:
                new += c
        new += '>'
        return new
    else:
        return element


def writeNtFileToStdout(ontologytxt):
    for line in open(ontologytxt):
        cols = line.strip('\n').split('\t')
        if len(cols) != 4 or cols[3] != '.':
            print('Ignoring malformed line: ' + line, file=sys.stderr)
        else:
            print('\t'.join([handleSubject(cols[0]), handleSubject(cols[1]),
                             handleObject(cols[2]), '.']))


def main():
    args = vars(parser.parse_args())
    ontologytxt = args['ontologytxt']
    writeNtFileToStdout(ontologytxt)


if __name__ == '__main__':
    main()
