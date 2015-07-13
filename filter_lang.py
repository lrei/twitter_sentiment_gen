# -*- coding: utf-8 -*-
# Authors: Luis Rei <me@luisrei.com>
# License: MIT License

"""
Reads a Line Delimited JSON file containing tweets.
Outputs only those with the selected language.
"""

from __future__ import print_function
from __future__ import division
import sys
import json
import gzip
import multiprocessing
import Queue
import time
import itertools
import argparse



# MULTIPROCESSING
NUM_PROCS = multiprocessing.cpu_count() - 2
QUEUE_MAX_SIZE = NUM_PROCS * 50


def filter_line(tweet_line, lang=u'en'):
    """
    returns tweet if it is in one language (lang)
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
    ntweet = {'text':tweet['text'], 'lang':tweet['lang']}
    if 'entities' in tweet:
        ntweet['entities'] = tweet['entities']
    if 'id' in tweet:
        ntweet['id'] = tweet['id']

    return ntweet


def worker(q, writeq, lang):
    while True:
        try:
            entry = q.get(block=False)
        except Queue.Empty:
            break
            
        tweet = filter_line(entry, lang)
        if tweet is not None:
            tweet_string = json.dumps(tweet)  + u'\n'
            writeq.put(tweet_string)
    writeq.put(-1)


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = list(itertools.islice(file_object, chunk_size))
        if not data:
            break
        yield data
        
        
def writer(q, outfile, n_readers):
    with gzip.open(outfile, 'a') as destination:
        while True:
            tweet = q.get(block=True)
            if type(tweet) == int:
                if tweet == -1:
                    n_readers = n_readers -1
                    if n_readers == 0:
                        break
            else:
               destination.write(tweet)


def filter_tweets(infile, outfile, lang=u'en'):
    """
    todo this later
    """ 
    workq = multiprocessing.Queue()
    writeq = multiprocessing.Queue()
    n_processed = 0
    with gzip.open(infile, 'r') as source:
        for batch in read_in_chunks(source, QUEUE_MAX_SIZE):
            # add to queue      
            [workq.put(entry) for entry in batch]
            
            batch_length = len(batch)
            n_processed += batch_length
            start_time = time.time()
            del batch[:]
        
            # start procs
            procs = []
            for i in xrange(NUM_PROCS):
                proc = multiprocessing.Process(target=worker,
                                        args=(workq, writeq, lang))
                proc.start()
                procs.append(proc)

            proc = multiprocessing.Process(target=writer,
                                        args=(writeq,outfile, NUM_PROCS))
            proc.start()
            procs.append(proc)
            # wait for processes to finish
            [proc.join() for proc in procs]
            end_time = time.time()
            processed_per_second = (batch_length / (end_time - start_time)) / 1000
            print('total processed lines = %fk' % (n_processed / 1000))
            print('processed lines per second = %fk' % processed_per_second)
            


def main():
    '''main'''
    lang_codes = [u'en']
    
    parser = argparse.ArgumentParser()
    parser.add_argument('input_tweet_file')
    parser.add_argument('output_files', help='one or more file_names comma_seperated')
    parser.add_argument('-l', '--lang_codes', help='language codes comma-seperated')
    
    args = parser.parse_args()
    

    infile = args.input_tweet_file
    outfiles = args.output_files.split(',')

    if args.lang_codes:
        lang_codes = unicode(args.lang_codes).split(',')
        
    if not len(outfiles) == len(lang_codes):
        print('Output files and language codes does not match in size')
        sys.exit(0)
                
    
    for lang_code, outfile in zip(lang_codes, outfiles):
        print('Using %s as language code' % lang_code)
        filter_tweets(infile, outfile, lang=lang_code)
    

if __name__ == '__main__':
    main()
