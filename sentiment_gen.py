#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Authors: Luis Rei <luis.rei@ijs.si>
# License: MIT License

"""
Generate a POS/NEG/NEUTRAL sentiment dataset from LD-JSON tweets (e.g. sample)
and collected news tweets also in LD-JSON format.

0 - optionally process newsfeed tweets into LD-JSON lines
1 - Create tweets.nf.lang.txt: LD tweet text for a given lang
2 - Create tweets.lang.txt: Filter through langid.py and ldig
3 - Create tweets.pp.lang.tx: Preprocessing - Tokenization and Lower-Casing
4 - Create pos.lang.txt / neg.lang.txt / back.lang.txt:
    filter emoticons with a probability of keeping them in
5 - Create neutal.lang.txt from news_tweets
"""

from __future__ import print_function
import os
import json
import argparse
from functools import partial

from newsfeed_tweets import convert_tweets
from filter_lang import filter_line
from tweet_pp import preprocess_tweet
from lowercase import lower_line
from classify_langid import filter_classify_lang_line
from MultiprocessFiles import MultiprocessFiles
import twokenize
from tokenize import word_tokenize, tokenize_tweet


def main():
    """ main """
    # default parameters
    min_tokens = 10
    max_num_urls = 1
    max_num_users = 3
    newsfeed_path = None
    langid_min_prob = 0.8
    replacements = json.load(open('replacements.json'))

    parser = argparse.ArgumentParser()
    parser.add_argument('lang_codes', help='lang codes comma-seperated')
    parser.add_argument('prob_smiley', type=float)
    parser.add_argument('min_langid_prob', type=float)
    parser.add_argument('tweets_file')
    parser.add_argument('tweet_news_path')
    parser.add_argument('-p', '--news_feed_path')
    parser.add_argument('-t', '--min_tokens', type=int)
    parser.add_argument('-url', '--max_urls', type=int)
    parser.add_argument('-u', '--max_users', type=int)
    parser.add_argument('-l', '--lowercasing', dest='lowercasing',
                        action='store_true')
    parser.add_argument('-prob', '--langid_min_prob')
    parser.add_argument('-s', '--simple', dest='simple', action='store_true',
                        help='selects simple tokenizer instead of twokenizer')
    parser.add_argument('-tw', '--twokenize', dest='twokenize',
                        action='store_true',
                        help='twokenizer that does not break apostroph words')
    args = parser.parse_args()

    lang_codes = unicode(args.lang_codes).split(',')
    prob_smiley = args.prob_smiley
    min_langid_prob = args.min_langid_prob
    tweets_file = args.tweets_file
    news_tweets_path = args.tweet_news_path
    if args.news_feed_path:
        newsfeed_path = args.news_feed_path
    if args.min_tokens:
        min_tokens = args.min_tokens
    if args.max_urls:
        max_num_urls = args.max_urls
    if args.max_users:
        max_num_users = args.max_users
    lowercasing = args.lowercasing
    if args.langid_min_prob:
        langid_min_prob = args.langid_min_prob
    tokenize_function = twokenize.tokenize2
    if args.simple:
        tokenize_function = word_tokenize
    if args.twokenize:
        tokenize_function = twokenize.tokenize

    # create tmpdir
    # tmpdir = './tmp'
    # if not os.path.isdir(tmpdir):
    #    os.makedirs(tmpdir)

    filename = os.path.basename(tweets_file)
    tweets_path = os.path.dirname(tweets_file)

    # Read newsfeed pickled tweets
    if newsfeed_path is not None:
        # @todo remove if tweets_path exists
        convert_tweets(newsfeed_path, tweets_path, filename)

    # Filter Based on Language
    for lang_code in lang_codes:
        print('Using %s as language code' % lang_code)
        func = partial(filter_line, lang_code)
        outfile = 'tweets.' + lang_code + '.json.gz'
        outfile = os.path.join(tweets_path, outfile)
        multiprocess_filter_lang = MultiprocessFiles(tweets_file, outfile, func,
                                                   num_procs=0,
                                                   queue_size=200000)
        multiprocess_filter_lang.run()

        if lowercasing:
            infile = outfile
            outfile = 'tweets.lowercase.' + lang_code + '.json.gz'
            outfile = os.path.join(tweets_path, outfile)
            lower_case = MultiprocessFiles(infile, outfile, lower_line,
                                          num_procs=0, queue_size=200000)
            lower_case.run()

            # Preprocess Text
            input_file = outfile
            output_file = 'tweets.pp.' + lang_code + '.json.gz'
            output_file = os.path.join(tweets_path, output_file)

            func = partial(preprocess_tweet, min_tokens,
                           max_num_urls, max_num_users, replacements)
            preprocess = MultiprocessFiles(input_file, output_file, func,
                                           num_procs=0, queue_size=200000)
            preprocess.run()

            # Delete lowercase file
            os.remove(outfile)

        else:
            # Preprocess Text
            input_file = outfile
            output_file = 'tweets.pp.' + lang_code + '.json.gz'
            output_file = os.path.join(tweets_path, output_file)
            func = partial(preprocess_tweet, min_tokens,
                           max_num_urls, max_num_users, replacements)
            preprocess = MultiprocessFiles(input_file, output_file, func,
                                           num_procs=0, queue_size=200000)
            preprocess.run()

        tweet_file = output_file
        dest_file = 'tweets.' + lang_code + '.pp.lid.json.gz'
        dest_file = os.path.join(tweets_path, dest_file)

        func = partial(filter_classify_lang_line, lang_code, langid_min_prob,
                       replacements)
        classify = MultiprocessFiles(tweet_file, dest_file, func, num_procs=0,
                                     queue_size=200000)
        classify.run()

        source = dest_file
        dest = 'tweets.' + lang_code + '.tok.json.gz'
        dest = os.path.join(tweets_path, dest)

        func = partial(tokenize_tweet, tokenize_function)
        tokenizer = MultiprocessFiles(source, dest, func, num_procs=0,
                                      queue_size=200000)
        tokenizer.run()

    # @todo remove tmpdir

if __name__ == '__main__':
    main()
