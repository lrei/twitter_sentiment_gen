"""Reads a Line Delimited JSON file containing tweets.

Tweets in file should be preprocessed.
Outputs only those with the selected language probability > 'langid_min_prob'.
"""

from __future__ import print_function
import json
import argparse
import sys
import re
from functools import partial

from MultiprocessFiles import MultiprocessFiles
import twokenize

re_tok = re.compile(r'(\w+|[^\w\s]+)', re.UNICODE)


def word_tokenize(text):
    """ Simple tokenization function: breaks text on regex word boundaries """
    t = u' '.join(re_tok.split(text))
    return t.split()


def tokenize_tweet(tokenize, tweet):
    """ Tokenizes tweet with tokenize function.

    Args:
        tokenize: function
            tokenize function to apply to tweet
        tweet: JSON object
            object to be tokenized
    Returns:
        tweet: dictionary
            tweet with tokenized text
    """
    try:
        tweet = json.loads(tweet)
    except:
        return None

    tweet['text'] = u" ".join(tokenize(tweet['text']))

    return tweet


def main():
    """ main """
    parser = argparse.ArgumentParser()
    parser.add_argument('tweet_infiles', help='input files comma seperated')
    parser.add_argument('dest_files', help='output files comma seperated')

    parser.add_argument('-s', '--simple', dest='simple', action='store_true',
                        help='selects simple tokenizer instead of twokenizer')

    parser.add_argument('-t', '--twokenize', dest='twokenize',
                        action='store_true',
                        help='twokenizer that does not break apostroph words')

    parser.add_argument('-a', '--twokenize2', dest='twokenize2',
                        action='store_true',
                        help='twokenizer that breaks apostroph words')

    parser.add_argument('-n', '--num_jobs', type=int, default=0,
                        help='number of worker processes to use. Default: \
                              number of cores')
    parser.add_argument('-q', '--queue_size', type=int, default=2000)

    args = parser.parse_args()

    tweet_files = args.tweet_infiles.split(',')
    dest_files = args.dest_files.split(',')

    if not len(tweet_files) == len(dest_files):
        print('tweet_files and dest_files are different sizes')
        sys.exit(0)

    tokenize_function = twokenize.tokenize2
    if args.simple:
        tokenize_function = word_tokenize
        print("Simple Tokenize")
    if args.twokenize:
        tokenize_function = twokenize.tokenize
        print("Tokenize")
    if args.twokenize2:
        tokenize_function = twokenize.tokenize2
        print("Tokenize 2")

    func = partial(tokenize_tweet, tokenize_function)

    for source, dest in zip(tweet_files, dest_files):
        multiprocess = MultiprocessFiles(source, dest, func, 
                                         num_procs=args.num_jobs,
                                         queue_size=args.queue_size)
        multiprocess.run()


if __name__ == '__main__':
    main()
