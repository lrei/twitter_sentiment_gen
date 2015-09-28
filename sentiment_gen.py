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
import codecs
import gzip
import sys

from newsfeed_tweets import convert_tweets
from filter_lang import filter_line
from preprocess import preprocess_tweet
from preprocess2 import preprocess2
from classify_langid import filter_classify_lang_line
from MultiprocessFiles import MultiprocessFiles
import twokenize
from tokenize import word_tokenize, tokenize_tweet
from filter_emoticons import process_line
from exclude_ids import exclude_ids
from filter_unique import make_unique


def main():
    """ main """
    replacements = json.load(open('replacements.json'))
    # Stats Vars
    n_pos = 0
    n_neg = 0
    n = 0
    POS = True
    NEG = False

    parser = argparse.ArgumentParser()

    # Basic
    parser.add_argument('tweets_file')
    parser.add_argument('-p', '--news_feed_path', default=None)
    parser.add_argument('lang_codes', help='lang codes comma-seperated')
    parser.add_argument('-j', '--num_jobs', default=0, type=int,
                        help='0 uses all cores available')
    parser.add_argument('-q', '--queue_size', type=int, default=2000)
    parser.add_argument('output_dir')

    # Exclude Tweets
    parser.add_argument('-e', '--exclude_ids', action='store_true',
                        default=False)
    parser.add_argument('-f', '--ids_file')

    # Preprocessing Part I
    parser.add_argument('-t', '--min_tokens', type=int, default=5)
    parser.add_argument('-r', '--max_urls', type=int, default=2)
    parser.add_argument('-u', '--max_users', type=int, default=3)

    # Language Identification
    parser.add_argument('-prob', '--langid_min_prob', type=float, default=0.8)

    # Tokenization
    parser.add_argument('-s', '--simple', dest='simple', action='store_true',
                        help='selects simple tokenizer')
    parser.add_argument('-to', '--twokenize', dest='twokenize',
                        action='store_true',
                        help='Keep contractions')
    parser.add_argument('-tw', '--twokenize2', dest='twokenize2',
                        action='store_true',
                        help='Break apostroph words')
 
    # Preprocessing Part II
    parser.add_argument('-l', '--lowercase', dest='lowercase',
                        action='store_true', default=False)
    parser.add_argument('-b', '--break_hash', dest='break_hash',
                        action='store_true', default=False)

    # Sentiment Dataset Generation
    parser.add_argument('-m', '--prob_smiley', type=float, default=0.4,
                        help='probability to keep smiley')

    args = parser.parse_args()

    lang_codes = unicode(args.lang_codes).split(',')
    prob_smiley = args.prob_smiley
    tweets_file = args.tweets_file
    tweets_path = args.output_dir
    newsfeed_path = args.news_feed_path
    min_tokens = args.min_tokens
    max_num_urls = args.max_urls
    max_num_users = args.max_users
    langid_min_prob = args.langid_min_prob
    num_jobs = args.num_jobs
    qsize = args.queue_size
    tokenize_function = twokenize.tokenize2
    if args.simple:
        tokenize_function = word_tokenize
    if args.twokenize:
        tokenize_function = twokenize.tokenize
    if args.twokenize2:
        tokenize_function = twokenize.tokenize2
    if args.exclude_ids and not args.ids_file:
        print('no ids file provided')
        sys.exit(0)

    filename = os.path.basename(tweets_file)

    # Read newsfeed pickled tweets
    if newsfeed_path is not None:
        convert_tweets(newsfeed_path, tweets_path, filename)
        # added if tweets file argument is not correct, try if it works
        tweets_file = os.path.join(tweets_path, filename)

    for lang_code in lang_codes:
        print('Using %s as language code' % lang_code)

        # new dir for each language
        tmpdir = os.path.join(tweets_path, 'generated_tweets_' +
                              str(lang_code))
        if not os.path.isdir(tmpdir):
            os.makedirs(tmpdir)
        tweets_path = tmpdir

        # Filter Based on Language
        func = partial(filter_line, lang_code)
        outfile = 'tweets.' + lang_code + '.json.gz'
        outfile = os.path.join(tweets_path, outfile)
        multiprocess_filter_lang = MultiprocessFiles(tweets_file, outfile, func,
                                                     num_procs=num_jobs,
                                                     queue_size=qsize)
        multiprocess_filter_lang.run()

        # Exclude ids
        if args.exclude_ids:
            idlist = open(args.ids_file, 'r').readlines()
            idlist = set([int(x.strip()) for x in idlist])
            func = partial(exclude_ids, idlist)

            infile = outfile
            outfile = 'tweets.' + lang_code + '.exc.json.gz'
            outfile = os.path.join(tweets_path, outfile)
            exclude = MultiprocessFiles(infile, outfile, func,
                                        num_procs=num_jobs,
                                        queue_size=qsize)
            exclude.run()

            os.remove(infile)

        # Preprocess Text
        input_file = outfile
        output_file = 'tweets.' + lang_code + '.pp.json.gz'
        output_file = os.path.join(tweets_path, output_file)
        func = partial(preprocess_tweet, min_tokens,
                       max_num_urls, max_num_users, replacements)
        preprocess = MultiprocessFiles(input_file, output_file, func,
                                       num_procs=num_jobs, queue_size=qsize)
        preprocess.run()

        os.remove(input_file)

        # Lang Identification
        infile = output_file
        dest_file = 'tweets.' + lang_code + '.pp.lid.json.gz'
        dest_file = os.path.join(tweets_path, dest_file)

        func = partial(filter_classify_lang_line, lang_code, langid_min_prob,
                       replacements)

        classify = MultiprocessFiles(infile, dest_file, func,
                                     num_procs=num_jobs, queue_size=qsize)
        classify.run()

        os.remove(infile)

        # Preprocess 2
        if args.lowercase or args.break_hash or replacements['number']:
            infile = dest_file
            outfile = 'tweets.lowercase.' + lang_code + '.json.gz'
            outfile = os.path.join(tweets_path, outfile)
            pp2_func = partial(preprocess2, args.lowercase, args.break_hash,
                               replacements)
            pp2 = MultiprocessFiles(infile, outfile, pp2_func,
                                    num_procs=num_jobs, queue_size=qsize)
            pp2.run()

            os.remove(infile)

        # Tokenization
        if args.lowercase or args.break_hash or replacements['number']:
            source = outfile
        else:
            source = dest_file
        dest = 'tweets.' + lang_code + '.pp.lid.tok.json.gz'
        dest = os.path.join(tweets_path, dest)

        func = partial(tokenize_tweet, tokenize_function)
        tokenizer = MultiprocessFiles(source, dest, func, num_procs=num_jobs,
                                      queue_size=qsize)
        tokenizer.run()

        os.remove(source)

        # Filter unique
        infile = dest
        outfile = 'tweets.' + lang_code + '.final.json.gz'
        outfile = os.path.join(tweets_path, outfile)

        tweet_ids = set()
        make_unique(infile, outfile, tweet_ids)

        os.remove(infile)

        # Filter emoticons
        pos_path = os.path.join(tweets_path, 'pos.txt')
        f_pos = codecs.open(pos_path, 'w', encoding='utf-8')

        neg_path = os.path.join(tweets_path, 'neg.txt')
        f_neg = codecs.open(neg_path, 'w', encoding='utf-8')

        other_path = os.path.join(tweets_path, 'other.txt')
        f_other = codecs.open(other_path, 'w', encoding='utf-8')

        # Read and Process
        with gzip.open(outfile, 'r') as f:
            for line in f:
                res = process_line(prob_smiley, line)
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
