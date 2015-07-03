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
import langid
import time
import itertools

# MULTIPROCESSING
NUM_PROCS = multiprocessing.cpu_count() - 2
QUEUE_MAX_SIZE = NUM_PROCS * 50


def filter_line(tweet_line, lang=u'en', langid_min_prob=0.75):
    """
    if langid_min_prob < 0, dont use langid
    """
    try:
        tweet = json.loads(tweet_line)
    except:
        return None

    # sanity check
    if 'lang' not in tweet or 'text' not in tweet:
        return None

    # filter based on language
    if tweet['lang'] != lang:
        return None
        
     # extract text
    text = tweet['text']

    # make sure there are no empty tweets
    if not text:
        text = None

    #
    # Filter based on language using langid
    #
    if langid_min_prob >= 0 and text is not None:
        # Check if identified language is the expected language
        lid, prob = langid.classify(text)
        if lid != lang:
            return None

        # Filter based ong langid minimum probability
        if prob < langid_min_prob:
            return None

    return text


def worker(q, writeq, lang, langid_min_prob):
    while True:
        try:
            entry = q.get(block=False)
        except Queue.Empty:
            break
            
        tweet = filter_line(entry, lang, langid_min_prob)
        if tweet is not None:
            tweet_string = tweet + u'\n'
            tweet_string = tweet_string.encode('utf-8')
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
                text = q.get(block=False)
            except Queue.Empty:
                break
                
            destination.write(text)


def filter_tweets(infile, outfile, langid_min_prob=0.8, lang=u'en'):
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
                                        args=(workq, writeq, lang,
                                                langid_min_prob))
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
    langid_min_prob = 0.8

    if len(sys.argv) not in [3, 4]:
        print(sys.argv[0] + ' input_tweet_file output_tweet_text [lang_code] [langid_min_prob]')
        sys.exit(0)

    infile = sys.argv[1]
    outfile = sys.argv[2]

    if len(sys.argv) >= 4:
        lang_code = unicode(sys.argv[3])

    if len(sys.argv) == 5:
        langid_min_prob = unicode(sys.argv[4])
        
    print('Using %s as language code' % lang_code)
    print('Using %f langid min probability' % langid_min_prob)

    filter_tweets(infile, outfile, langid_min_prob, lang=lang_code)


if __name__ == '__main__':
    main()
