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
import os
import re
import gzip
import json
import codecs
import itertools
import random
import argparse
import multiprocessing
from functools import partial


pos_smileys = [u':)', u':D', u':-)', u':-))', u':]', u'=)', u'(:', u':o)']
neg_smileys = [u':(', u';(', u':-(', u':-[', u":'(", u":[", u":{", u">:("]

# add unicode emoticons
pos_smileys += [unichr(x) 
                for x in range(int('1F600', 16), int('1F600', 16) + 16)]
neg_smileys += [unichr(x) 
                for x in range(int('1F620', 16), int('1F620', 16) + 16)]

POS = True
NEG = False
random.seed()


def process_line(prob_smiley, json_line):
    '''
    Identifies smileys or lack of them.
    If they exist they are removed.
    text is lowered (converted to lower case)
    < min_tokens => discarded
    '''

    unicode_line = json.loads(json_line)['text'].strip()

    tokens = unicode_line.split()

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
        return (None, unicode_line)  # Ambiguous

    # "Flip a coin to see if smiley will be removed
    remove_smiley = random.random() > prob_smiley

    if has_pos:
        if remove_smiley:
            tokens = [x for x in tokens if x not in pos_smileys]

        unicode_line = u' '.join(tokens)
        return (POS, unicode_line)

    if has_neg:
        if remove_smiley:
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

    # arguments
    parser = argparse.ArgumentParser()

    # parser.add_argument('-n', '--num_jobs', action="store", 
    # dest="num_jobs", type=int, default=0)

    parser.add_argument('-p', '--prob_smiley', action="store", 
                        dest="prob_smiley", default=0.4)

    parser.add_argument('input_tweet_file')
    parser.add_argument('output_directory')
    args = parser.parse_args()

    # open output files
    pos_path = os.path.join(args.output_directory, 'pos.txt')
    f_pos = codecs.open(pos_path, 'w', encoding='utf-8')

    neg_path = os.path.join(args.output_directory, 'neg.txt')
    f_neg = codecs.open(neg_path, 'w', encoding='utf-8')

    other_path = os.path.join(args.output_directory, 'other.txt')
    f_other = codecs.open(other_path, 'w', encoding='utf-8')


    # Read and Process
    with gzip.open(args.input_tweet_file, 'r') as f:
        for line in f:
            res = process_line(args.prob_smiley, line)
            n += 1

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

    # explicitly close files
    f_pos.close()
    f_neg.close()
    f_other.close()


if __name__ == '__main__':
    main()
