import argparse
import sys

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--nt',
                    type=str,
                    help='n-triple file.',
                    required=True)


def writeNtFileToStdout(nt):
    for line in open(nt):
        cols = line.strip('\n').split('\t')
        if len(cols) != 4 or cols[3] != '.':
            print('Ignoring malformed line: ' + line, file=sys.stderr)
        else:
            lang_start = cols[2].rfind('"@');
            if lang_start > 0 and cols[2].rfind('"', lang_start + 1) == -1:
                lang = cols[2][lang_start + 2:]
                if cols[1][-1] == '>':
                    cols[1] = cols[1][:-1] + '.' + lang + '>'
                else:
                    cols[1] += ('.' + lang)
                cols[2] = cols[2][:lang_start + 1]
            print('\t'.join([cols[0], cols[1], cols[2], '.']))


def main():
    args = vars(parser.parse_args())
    nt = args['nt']
    writeNtFileToStdout(nt)


if __name__ == '__main__':
    main()
