import argparse
import sys

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--wordsfile',
                    type=str,
                    help='Broccoli Wordsfile.',
                    required=True)

def writeContextFileToStdout(wordsfile):
		for line in open(wordsfile):
			cols = line.strip('\n').split('\t')
			if cols[0].startswith(':e:'):
				cols[0] = '<' + cols[0][3:] + '>'
				entityFlag = '1'
			else:
				entityFlag = '0'
			print '\t'.join([entityFlag, cols[0], cols[1], cols[3]])


def main():
    args = vars(parser.parse_args())
    wordsfile = args['wordsfile']
    writeContextFileToStdout(wordsfile)


if __name__ == '__main__':
    main()
