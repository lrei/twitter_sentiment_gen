#!/env/python
# Author: Luis Rei <me@luisrei.com>
# License: MIT License
'''
Reads from a file and outputs to files:
    argv[1]     - lines of utf8 encoded text (input)
    pos.txt     - lines that contain happy smileys (with smiley removed)
    neg.txt     - lines that contain negative smileys (with smiley removed)
    other.txt   - the remaining tweets

    lower-cases output.
    removes short tweets
'''

from __future__ import print_function
import sys
import re
import codecs
import itertools
import random
import multiprocessing

# matches only the smileys ":)" and ":("
pos_smileys = [u':)', u':D', u':-)', u':-))', u':]', u'=)', u'(:', u':o)']
neg_smileys = [u':(', u';(', u':-(', u':-[', u":'(", u":[", u":{", u">:("]
POS = True
NEG = False
chunk_size = 10000000
M = chunk_size / 1000000
random.seed()


def process_line(unicode_line, remove_smiley=True, lower=True, min_tokens=5):
    '''
    Identifies smileys or lack of them.
    If they exist they are removed.
    text is lowered (converted to lower case)
    < min_tokens => discarded
    '''
    if lower:
        unicode_line = unicode_line.strip().lower()

    tokens = unicode_line.split()

    tokens2 = [x for x in tokens if not x.startswith(u'@')]
    if len(tokens2) < min_tokens:
        return None

    has_pos = False
    has_neg = False

    for sm in pos_smileys:
        if sm in tokens:
            has_pos = True
            break

    for sm in neg_smileys:
        if sm in tokens:
            has_neg = True
            break


    if not has_neg and not has_pos:
        return (None, unicode_line)  # No smileys
    
    if has_pos and has_neg:
        return None  # Ambiguous

    coin_flip = random.random() > 0.5

    if has_pos:
        if remove_smiley and coin_flip:
            tokens = [x for x in tokens if x not in pos_smileys]

        unicode_line = u' '.join(tokens)
        return (POS, unicode_line)

    if has_neg:
        if remove_smiley and coin_flip:
            tokens = [x for x in tokens if x not in neg_smileys]

        unicode_line = u' '.join(tokens)
        return (NEG, unicode_line)

    # should be unreachable
    return None


def main():
    ''' Main '''
    # Stats Vars
    n_pos = 0
    n_neg = 0
    n = 0
    # open output files
    f_pos = codecs.open('pos.txt', 'w', encoding='utf-8')
    f_neg = codecs.open('neg.txt', 'w', encoding='utf-8')
    f_other = codecs.open('other.txt', 'w', encoding='utf-8')

    # Multiprocessing pool
    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    # chunked reading
    print('Opening %s' % (sys.argv[1],))
    with codecs.open(sys.argv[1], 'r', encoding='utf-8') as f:
        while True:
            lines = list(itertools.islice(f, chunk_size))
            if not lines:
                break  # EOF

            # process chunk
            output = pool.map(process_line, lines)
            n += 1
            # output chunk
            for res in output:
                if res is None:
                    continue
                if res[0] is None:
                    f_other.write(res[1] + u'\n')
                if res[0] == POS:
                    n_pos += 1
                    f_pos.write(res[1] + u'\n')
                if res[0] == NEG:
                    f_neg.write(res[1] + u'\n')
                    n_neg += 1
            print('Chunk %d (%dM/chunk): %d positive, %d negative' % (n,
                  chunk_size/M, n_pos, n_neg))

    # explicitly close files
    f_pos.close()
    f_neg.close()
    f_other.close()


if __name__ == '__main__':
    main()
