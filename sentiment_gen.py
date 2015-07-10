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


import sys
import os
from newsfeed_tweets import convert_tweets
from filter_lang import filter_tweets
from tweet_text import preprocess_tweet_file

def main():
    cmd_name = sys.argv[0]
    min_tokens = 10 # default parameters
    max_num_urls = 1
    max_num_users = 3 
    newsfeed_path = None
    lowercasing = 'yes'

    # usage
    if len(sys.argv) not in [6, 7, 8, 9, 10, 11]:
        usage = 'Usage:\n\t%{cmd} ' \
                'lang_code1[,lang_code2,...] prob_smiley min_langid_prob ' \
                'tweets_file tweet_news_path [news_feed_path] [min_tokens]' \
                '[max_urls] [max_users] [lowercasing(yes/no)]'
        usage.format(cmd=cmd_name)
        print usage
        sys.exit(0)

    lang_codes = sys.argv[1].split(',')
    prob_smiley = float(sys.argv[2])
    min_langid_prob = float(sys.argv[3])
    tweets_file = sys.argv[4]
    news_tweets_path = sys.argv[5]
    if len(sys.argv) >= 7:
        newsfeed_path = sys.argv[6]
    if len(sys.argv) >= 8:
        min_tokens = int(sys.argv[7])
    if len(sys.argv) >= 9:
        max_num_urls = int(sys.argv[8])
    if len(sys.argv) >= 10:
        max_num_users = int(sys.argv[9])
    if len(sys.argv) >= 11:
        lowercasing = sys.argv[10]

    # create tmpdir 
    #tmpdir = './tmp'
    #if not os.path.isdir(tmpdir):
    #    os.makedirs(tmpdir)
        


    filename = os.path.basename(tweets_file)
    tweets_path = os.path.dirname(tweets_file)

    # Read newsfeed pickled tweets
    if newsfeed_path is not None and newsfeed_path != '-':
        # @todo remove if tweets_path exists
        convert_tweets(newsfeed_path, tweets_path, filename)


    # Filter Based on Language
    for lang_code in lang_codes:
        outfile = 'tweets.' + lang_code + '.json.gz' # name of the file with filtered tweets
        outfile = os.path.join(tweets_path, outfile) 
        filter_tweets(tweets_file, outfile, lang=lang_code)
   
        # Preprocess Text  
        input_file = outfile 
        output_file = 'tweets.pp.' + lang_code + '.json.gz' 
        output_file = os.path.join(tweets_path, output_file) 
        preprocess_tweet_file(input_file, output_file, min_tokens, max_num_urls, max_num_users, lowercasing)



    #@todo remove tmpdir

if __name__ == '__main__':
    main()
