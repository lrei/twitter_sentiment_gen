'''
this module lowercases text from infile and writes it to outfile, processes .json.gz files
'''

from __future__ import print_function
from __future__ import division
import json
import gzip
import multiprocessing
import Queue
import itertools
import argparse
from filter_lang import NUM_PROCS, QUEUE_MAX_SIZE


def lower_line(line):
    """
    returns lowered line
    """
    try:
        tweet = json.loads(line)
    except:
        return None
 
    # lower
    tweet['text'] = tweet['text'].lower()

    return tweet


def worker(q, writeq):
    while True:
        try:
            entry = q.get(block=False)
        except Queue.Empty:
            break
            
        tweet = lower_line(entry)
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


def lowercase(infile, outfile):
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
            del batch[:]
        
            # start procs
            procs = []
            for i in xrange(NUM_PROCS):
                proc = multiprocessing.Process(target=worker,
                                        args=(workq, writeq))
                proc.start()
                procs.append(proc)

            proc = multiprocessing.Process(target=writer,
                                        args=(writeq,outfile, NUM_PROCS))
            proc.start()
            procs.append(proc)
            # wait for processes to finish
            [proc.join() for proc in procs]
            print('total lowered lines = %fk' % (n_processed / 1000))
    
    
def main():
    '''main'''
    
    parser = argparse.ArgumentParser()
    parser.add_argument('input_tweet_file')
    parser.add_argument('output_file')
    
    args = parser.parse_args()
    

    infile = args.input_tweet_file
    outfile = args.output_file


    lowercase(infile, outfile)
    

if __name__ == '__main__':
    main()