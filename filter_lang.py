# -*- coding: utf-8 -*-
# Authors: Luis Rei <me@luisrei.com>
# License: MIT License

"""Filters tweets by field lang.

Reads a Line Delimited JSON file containing tweets.
Outputs only those with the selected language.
"""

from __future__ import print_function
from __future__ import division
import sys
import json
import argparse
from functools import partial

from MultiprocessFiles import MultiprocessFiles


def filter_line(lang, tweet_line):
    """Returns tweet if it is in 'lang' language.

    Tweet includes text, lang, entities and tweet id.

    Args:
        lang: string
            BCP 47 language identifier
        tweet_line: JSON object
            represents one tweet

    Returns: A dictionary
    """
    try:
        tweet = json.loads(tweet_line)
    except:
        return None

    # sanity check
    if 'lang' not in tweet or 'text' not in tweet:
        return None

    # make sure there are no empty tweets
    if not tweet['text']:
        return None

    # filter based on language
    if tweet['lang'] != lang:
        return None

    # extract useful properties
    ntweet = {'text': tweet['text'], 'lang': tweet['lang']}
    if 'entities' in tweet:
        ntweet['entities'] = tweet['entities']
    if 'id' in tweet:
        ntweet['id'] = tweet['id']
    elif 'id_str' in tweet:
        ntweet['id'] = int(tweet['id_str'])
    if 'created_at' in tweet:
        ntweet['created_at'] = tweet['created_at']
    if 'retweeted_status' in tweet:
        ntweet['retweet_id'] = tweet['retweeted_status']['id_str']
    if 'retweet_id' in tweet:
        ntweet['retweet_id'] = tweet['retweet_id']

    return ntweet


def main():
    """ main """
    lang_codes = ['en']

    parser = argparse.ArgumentParser()
    parser.add_argument('input_tweet_file')
    parser.add_argument('output_files',
                        help='one or more file paths comma_seperated')
    parser.add_argument('-l', '--lang_codes',
                        help='language codes comma-seperated')
    parser.add_argument('-n', '--num_jobs', type=int, default=0,
                        help='number of worker processes to use. Default: \
                              number of cores')
    parser.add_argument('-s', '--queue_size', type=int, default=3000)
    args = parser.parse_args()

    infile = args.input_tweet_file
    outfiles = args.output_files.split(',')

    if args.lang_codes:
        lang_codes = args.lang_codes.split(',')

    if not len(outfiles) == len(lang_codes):
        print('Output files and language codes do not match in size')
        sys.exit(0)

    for lang_code, outfile in zip(lang_codes, outfiles):
        func = partial(filter_line, lang_code)
        print('Using %s as language code' % lang_code)
        multiprocess = MultiprocessFiles(infile, outfile, func,
                                         num_procs=args.num_jobs,
                                         queue_size=args.queue_size)
        multiprocess.run()


if __name__ == '__main__':
    main()
