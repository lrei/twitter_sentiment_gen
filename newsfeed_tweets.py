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
import shutil


def load_tweets(file_path, open_function=open, dest_path=None):
    """Reads a newsfeed.ijs.si gzipped pickle tweet archive.
    Args:
        file_path: string
            The file path.
    Returns:
        A list of dictionaries
    """

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
        
        if not tweets:
            return
                           
    except:
        print('Failed to read %s' % file_path)
        return None

    
    #write new file
    write_file = os.path.basename(file_path)
    write_file = write_file.split('.')[0] + '.json.gzip'
    write_file = os.path.join(dest_path, write_file)
    
    try:
        with gzip.open(write_file, 'wa') as destination:        
            for tweet in tweets:
                tweet_string = json.dumps(tweet) + u'\n'
                tweet_string = tweet_string.encode('utf8')
                destination.write(tweet_string)
    except:
        print('Failed to write %s' % write_file)
    return None




def convert_tweets(read_dir, dest_path, filename):
    """Recursive read a directory opening all pickle.gz files, loading them
    and writting the cleartext to a single file named filename (one line per tweet)
    """
    
    n_processed = 0

    pool = Pool(cpu_count() - 2)
    func_gz = partial(load_tweets, open_function=gzip.open, dest_path=dest_path)
    func_pi = partial(load_tweets, open_function=open, dest_path=dest_path)



    for root, _, filenames in os.walk(read_dir):
        #
        # Process Directory
        #
        
        # Convert filenames to full path names
        filenames = [os.path.join(root, x) for x in filenames]

        # Process gzipped files
        files_gz = [x for x in filenames if x.endswith('.pickle.gz')]
        pool.map(func_gz, files_gz)
        n_processed += len(files_gz)

        # Process non-gzipped files
        files_pi = [x for x in filenames if x.endswith('.pickle')]
        pool.map(func_pi, files_pi)
        n_processed += len(files_pi)
        
        print('Total processed files: %d' %(n_processed,))
    
    # Now put all the files together in one
    list_of_files = os.listdir(dest_path)
    
    list_of_files = [os.path.join(dest_path, x) for x in list_of_files]
    
    dest_file = os.path.join(dest_path, filename) # name of file    
    with open(dest_file, 'wb') as outfile:
        for fname in list_of_files:
            with open(fname, 'rb') as infile:
                 shutil.copyfileobj(infile, outfile)
            os.remove(fname)
    

def main():
    """
    Example usage:
    assumes NF pickled tweets are in the current dir outputs to tweets.json.gz
    """
    convert_tweets('./', './', '.tweets.json.gz')


if __name__ == '__main__':
    main()
