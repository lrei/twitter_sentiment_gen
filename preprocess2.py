# -*- coding: utf-8 -*-
"""
This module implements some extra preprocessing steps used in some cases:

    - (optional) remove hash signs, #HASHTAG -> ['HASHTAG']
    - (optional) replaces numbers with special token
    - (optional) lowercases tweets' text (replacements not included).

Reads tweet's from infile and writes to outfile; 
Files are Line Delimited JSON (LDJSON) files.
"""

from __future__ import print_function
from __future__ import division
import sys
import re
import json
import argparse
from functools import partial

from MultiprocessFiles import MultiprocessFiles


# number
# numbers can include .,/ e.g. 12,399.05 or 12.2.2005 or 12/2/2005
base_number = r'\d+((\,\d+)*(\.\d+)*(\/\d+)*)*'
number = r'\b' + base_number + r'\b'
num_re = re.compile(number, re.UNICODE)

# hastag
hash_re = re.compile(r'(\A|\s)#(\w+)', re.UNICODE)


def preprocess2(lowercase, break_hashtags, replacements, line):
    """ returns lowered line """
    try:
        tweet = json.loads(line)
        text = tweet['text']
    except:
        tweet = dict()
        text = line.decode('utf8')

    # Break hash sign: #hashtag -> # hashtag
    if break_hashtags:
        text = hash_re.sub(r' # \2', text)

    # Replace numbers
    if replacements['number']:
        text = num_re.sub(replacements['number'], text)

    # lowercase
    if lowercase:
        text = text.lower()

    tweet['text'] = text
    return tweet



def main():
    """ main """
    parser = argparse.ArgumentParser()

    parser.add_argument('-n', '--num_jobs', action="store", 
                        dest="num_procs", type=int, default=0)

    parser.add_argument('-b', '--break_hashtags', action="store_true", 
                        dest="break_hashtags", default=False)

    parser.add_argument('-l', '--lowercase', action="store_true", 
                        dest="lowercase", default=False)
 
    
    parser.add_argument('input_tweet_file')
    parser.add_argument('output_file')
    args = parser.parse_args()

    infile = args.input_tweet_file
    outfile = args.output_file

    num_procs = args.num_procs

    replacements = json.load(open('replacements.json'))

    if (not args.lowercase) and (not args.break_hashtags) and (not numbers):
        print('Nothing to do')
        sys.exit(0)

    pp2 = partial(preprocess2, args.lowercase, args.break_hashtags, 
                  replacements)


    multiprocess = MultiprocessFiles(infile, outfile, pp2, 
                                     queue_size=2000, num_procs=num_procs)
    multiprocess.run()


if __name__ == '__main__':
    main()
