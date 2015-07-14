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


import os
from newsfeed_tweets import convert_tweets
from filter_lang import filter_tweets
from tweet_text import preprocess_tweet_file
from lowercase import lowercase

import argparse

def main():
    min_tokens = 10 # default parameters
    max_num_urls = 1
    max_num_users = 3 
    newsfeed_path = None
    lowercasing = False
    replace_hashtags = False
    replace_users = False
    replacements = {'user': 'TUSERUSER', 'url': 'TURLURL', 'hashtag': 'THASHTAG', 
                    'symbol': 'TSYMBOL'}
    
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
    parser.add_argument('-l', '--lowercasing', dest='lowercasing', action='store_true')
    parser.add_argument('-rh', '--replace_hashtags', dest='replace_hashtag', action='store_true')
    parser.add_argument('-ru', '--replace_users', dest='replace_users', action='store_true')
    parser.add_argument('-s', '--hashtag_symbol')
    
    args = parser.parse_args()   
    
    lang_codes = args.lang_codes.split(',')
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
    replace_hashtags = args.replace_hashtags
    replace_users = args.replace_users
    if args.hashtag_symbol:
        #update hashtag symbol
        replacements['hashtag'] = args.hashtag_symbol 
    



    # create tmpdir 
    #tmpdir = './tmp'
    #if not os.path.isdir(tmpdir):
    #    os.makedirs(tmpdir)
    
    
    filename = os.path.basename(tweets_file)
    tweets_path = os.path.dirname(tweets_file)

    # Read newsfeed pickled tweets
    if newsfeed_path is not None:
        # @todo remove if tweets_path exists
        convert_tweets(newsfeed_path, tweets_path, filename)


    # Filter Based on Language
    for lang_code in lang_codes:
        outfile = 'tweets.' + lang_code + '.json.gz' # name of the file with filtered tweets
        outfile = os.path.join(tweets_path, outfile) 
        filter_tweets(tweets_file, outfile, lang=lang_code)
   
        if lowercasing:
            infile = outfile
            outfile = 'tweets.lowercase.' + lang_code + '.json.gz'
            outfile = os.path.join(tweets_path, outfile)
            lowercase(infile, outfile)
            
            # Preprocess Text  
            input_file = outfile 
            output_file = 'tweets.pp.lowercase.' + lang_code + '.json.gz' 
            output_file = os.path.join(tweets_path, output_file) 
                   
            
            preprocess_tweet_file(input_file, output_file, min_tokens, 
                                  max_num_urls, max_num_users, replace_hashtags, 
                                  replace_users, replacements)

        else:      
            # Preprocess Text  
            input_file = outfile
            output_file = 'tweets.pp.' + lang_code + '.json.gz' 
            output_file = os.path.join(tweets_path, output_file) 
            preprocess_tweet_file(input_file, output_file, min_tokens, 
                                  max_num_urls, max_num_users, replace_hashtags, 
                                  replace_users, replacements)

        
    #@todo remove tmpdir
    
if __name__ == '__main__':
    main()
