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


def main():
    cmd_name = sys.argv[0]
    print len(sys.argv)

    # usage
    if len(sys.argv) not in [6, 7]:
        usage = 'Usage:\n\t%{cmd} ' \
                'lang_code prob_smiley min_langid_prob ' \
                'tweets_path tweet_news_path [news_feed_path]'
        usage.format(cmd=cmd_name)
        print usage
        sys.exit(0)

    lang_code = sys.argv[1]
    prob_smiley = float(sys.argv[2])
    min_langid_prob = float(sys.argv[3])
    tweets_path = sys.argv[4]
    news_tweets_path = sys.argv[5]
    if len(sys.argv) == 7:
        newsfeed_path = sys.argv[6]
    else:
        newsfeed_path = None

    # create tmpdir 
    tmpdir = './tmp'
    if not os.path.isdir(tmpdir):
        os.makedirs(tmpdir)
        


    # Read newsfeed pickled tweets
    if newsfeed_path is not None:
        # @todo remove if tweets_path exists
        convert_tweets(newsfeed_path, tweets_path
        )

    # Filter Based on Language
        
    
    
    # Preprocess Text


    #@todo remove tmpdir

if __name__ == '__main__':
    main()
