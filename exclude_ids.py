# -*- coding: utf-8 -*-
# Authors: Luis Rei <me@luisrei.com>
# License: MIT License

"""
Removes tweets by id
"""

from __future__ import print_function
from __future__ import division
import sys
import json
import gzip
import argparse
from functools import partial


def exclude_ids(ids, tweet_line):
    """Returns tweet if it tweet id is not in ids
    Returns: A dictionary
    """
    try:
        tweet = json.loads(tweet_line)
    except:
        return None

    tweet_id = 0
    # extract useful properties
    if 'id' in tweet:
        tweet_id = int(tweet['id'])
    elif 'id_str' in tweet:
        tweet_id = int(tweet['id_str'])

    if tweet_id in ids:
        return None

    return tweet


def main():
    """ main """

    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('ids_file')
    parser.add_argument('output_file')
    args = parser.parse_args()

    infile = args.input_file
    outfile = args.output_file

    if not args.ids_file:
        print('no ids file provided')
        sys.exit(0)

    idlist = open(args.ids_file, 'r').readlines()
    idlist = set([int(x.strip()) for x in idlist])
    f = partial(exclude_ids, idlist)

    with gzip.open(infile) as fin, gzip.open(outfile, 'w') as fout:
        for line in fin:
            tweet = f(line)
            if tweet is not None:
                fout.write(line)


if __name__ == '__main__':
    main()
