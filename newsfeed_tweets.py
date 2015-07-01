# -*- coding: utf-8 -*-
# Authors: Luis Rei <luis.rei@ijs.si>
# License: MIT License

"""
Reads a directory of directories (recursive) containing tweets in the
newsfeed pickled/gzipped format.
Outputs Line Delimited JSON containing only 'text' and 'lang'
Also replaces newlines in the tweet text and strips()s them
"""

from __future__ import print_function
import os
import codecs
import gzip
import cPickle as pickle
import json
from multiprocessing import Pool, cpu_count
from functools import partial


def load_tweets(file_path, open_function=open):
    """Reads a newsfeed.ijs.si gzipped pickle tweet archive.
    Args:
        file_path: string
            The file path.
    Returns:
        A list of dictionaries
    """

    print("reading %s" % file_path)
    try:
        with open_function(file_path, 'r') as tweet_f:
            tweets = map(pickle.loads, tweet_f.read().split('\x00')[:-1])

            # conver json string to dict
            tweets = [json.loads(x[u'raw_json'])
                      for x in tweets if u'raw_json' in x]

            # check properties
            tweets = [x for x in tweets if u'lang' in x and u'text' in x]

            # remove other properties for the sake of disk space
            tweets = [{u'text': t[u'text'], u'lang': t[u'lang']}
                      for t in tweets]

            # remove newlines, strip from text
            for t in tweets:
                t[u'text'] = t[u'text'].replace(u'\n', u' ').strip()

            # make sure there are no empty tweets
            tweets = [x for x in tweets if x[u'text']]

            return tweets
    except:
        print('Failed to read %s' % file_path)
        return None

    return None


def convert_tweets(read_dir, file_path):
    """Recursive read a directory opening all pickle.gz files, loading them
    and writting the cleartext to a single file (one line per tweet)
    """

    pool = Pool(cpu_count() - 2)
    func_gz = partial(load_tweets, open_function=gzip.open)
    func_pi = partial(load_tweets, open_function=open)

    with codecs.open(file_path, 'wa', encoding='utf8') as destination:
        for root, _, filenames in os.walk(read_dir):
            # Process Directory
            filenames = [os.path.join(root, x) for x in filenames]

            # Process gzipped files
            files_gz = [x for x in filenames if x.endswith('.pickle.gz')]
            tweets_per_file = pool.map(func_gz, files_gz)
            for tweets in tweets_per_file:
                if tweets is None:
                    continue
                for tweet in tweets:
                    destination.write(json.dumps(tweet) + u'\n')

            # Process non-gzipped files
            files_pi = [x for x in filenames if x.endswith('.pickle')]
            tweets_per_file = pool.map(func_pi, files_pi)
            for tweets in tweets_per_file:
                if tweets is None:
                    continue
                for tweet in tweets:
                    destination.write(json.dumps(tweet) + u'\n')


def main():
    """
    Example usage:
    assumes NF pickled tweets are in the current dir outputs to tw
    """
    convert_tweets('./', './tweets.txt')


if __name__ == '__main__':
    main()
