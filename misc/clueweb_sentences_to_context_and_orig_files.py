import argparse
import sys

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--sentences',
                    type=str,
                    help='Clueweb sentences file with recognized entities.',
                    required=True)

parser.add_argument('--w',
                    type=str,
                    help='Output filename for the contexts file ("wordsfile").',
                    required=True)

parser.add_argument('--d',
                    type=str,
                    help='Output filename for the orig text file ("docsfile").',
                    required=True)

parser.add_argument('--first-context-id',
                    type=int,
                    help='Start assigning context ids from this number here.',
                    default=0)

parser.add_argument('--stop-tokens-file',
                    type=str,
                    help='A list of tokens to omit in the context file. '
                         'Usually punctuation and stopwords, '
                         'one item a line and lower case.',
                    required=True)

parser.add_argument('--length-one-non-alnum-is-not-stop',
                    help='Usually, non-alpha-numeric tokens of length 1 '
                         'are ignored. This option disables that so that '
                         'everything can be used in the words-file.',
                    default=False,
                    action='store_true')


def tokenize_sentence(s):
    tokens = []
    inside_entity = False
    current_token_from = 0
    for i in range(0, len(s)):
        if inside_entity:
            if s[i] == ']' and (i + 1 == len(s) or s[i + 1] == ' '):
                i += 1
                tokens.append(s[current_token_from:i])
                current_token_from = i + 1
                inside_entity = False
            elif i + 1 < len(s) and s[i + 1] == '[':
                if i > current_token_from:
                    tok = s[current_token_from:i]
                    if ' ' not in tok:
                        tokens.append(tok)
                inside_entity = False
                current_token_from = i + 1
        else:
            if s[i] == ' ' and i > current_token_from:
                tokens.append(s[current_token_from:i])
                current_token_from = i + 1
            elif s[i] == '[' and i > 0 and s[i - 1] == ' ':
                inside_entity = True
    if current_token_from < len(s):
        tokens.append(s[current_token_from:])
    return tokens


def is_marked_entity(token):
    return token[0] == '[' and token[-1] == ']' and token.find('|') != -1


def token_to_docsfile(token):
    if not is_marked_entity(token):
        return token
    else:
        return token.split('|')[1][:-1]


def entity_id_to_full_entity(entity_id):
    return '<http://rdf.freebase.com/ns/' + entity_id + '>'


def should_be_written_to_wordsfile(word, stop_tokens, write_len_1_non_alnum):
    return (write_len_1_non_alnum or len(
        word) > 1 or word.isalnum()) and word not in stop_tokens


def write_context_to_wordsfile(context_id, tokens, wordsfile, stop_tokens,
                               write_len_1_non_alnum):
    for t in tokens:
        try:
            is_entity = is_marked_entity(t)
            lower = t.lower()
            if not is_entity:
                if should_be_written_to_wordsfile(lower, stop_tokens,
                                                  write_len_1_non_alnum):
                    print('\t'.join([lower, '0', context_id, '1']),
                          file=wordsfile)
            else:
                spl = t.split('|')
                words = spl[1][:-1].lower()
                for word in words.split(' '):
                    if should_be_written_to_wordsfile(word, stop_tokens,
                                                      write_len_1_non_alnum):
                        print('\t'.join([word, '0', context_id, '1']),
                              file=wordsfile)
                    entity = entity_id_to_full_entity(spl[0][1:])
                    print('\t'.join([entity, '1', context_id, '1']),
                          file=wordsfile)
        except IndexError:
            print('Problem on token: ' + t, file=sys.stderr)


def process(sentences, context_file_name, orig_file_name, first_context_id,
            stop_tokens, write_len_1_non_alnum):
    context_id = first_context_id
    with open(context_file_name, 'w') as wordsfile:
        with open(orig_file_name, 'w') as docsfile:
            for line in open(sentences, 'r'):
                cols = line.strip().split('\t')
                if len(cols) != 2:
                    print("Ignoring line without exactly one tab, "
                          "line number (starting from 0): "
                          + str(context_id - first_context_id), file=sys.stderr)
                    continue
                tokens = tokenize_sentence(cols[0])
                docsfile_tokens = [token_to_docsfile(t) for t in tokens]
                print('\t'.join([str(context_id), ' '.join(docsfile_tokens)]),
                      file=docsfile)
                write_context_to_wordsfile(str(context_id), tokens, wordsfile,
                                           stop_tokens, write_len_1_non_alnum)
                context_id += 1


def read_stop_tokens(file_name):
    stop_tokens = set()
    for line in open(file_name, 'r'):
        stop_tokens.add(line.strip())
    return stop_tokens


def main():
    args = vars(parser.parse_args())
    stop_tokens = read_stop_tokens(args['stop_tokens_file'])
    process(args['sentences'], args['w'], args['d'], args['first_context_id'],
            stop_tokens, args['length_one_non_alnum_is_not_stop'])


if __name__ == '__main__':
    main()
