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


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = list(itertools.islice(file_object, chunk_size))
        if not data:
            break
        yield data
        
        
def writer(q, outfile):
    with gzip.open(outfile, 'wa') as destination:
         while True:
            try:
                tweet = q.get(block=False)
            except Queue.Empty:
                break
                
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
            # add to queu      
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

            time.sleep(1)
            proc = multiprocessing.Process(target=writer,
                                        args=(writeq,outfile))
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
    lang_code = u'en'

    if len(sys.argv) not in [3, 4]:
        print(sys.argv[0] + ' input_tweet_file output_tweet_file [lang_code]')
        sys.exit(0)

    infile = sys.argv[1]
    outfile = sys.argv[2]

    if len(sys.argv) == 4:
        lang_code = unicode(sys.argv[3])

    print('Using %s as language code' % lang_code)

    filter_tweets(infile, outfile, lang=lang_code)


if __name__ == '__main__':
    main()
