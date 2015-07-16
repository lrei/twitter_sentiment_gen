'''
this module lowercases tweets' text from infile and writes it to outfile, processes .json.gz files
'''

from __future__ import print_function
from __future__ import division
import json
import gzip
import multiprocessing
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
        entry = q.get(block=True)
        if type(entry) == int:
            if entry < 0:
                break
            
        tweet = lower_line(entry)
        if tweet is not None:
            tweet_string = json.dumps(tweet)  + u'\n'
            writeq.put(tweet_string)
    
    writeq.put(-1)
        
        
def writer(q, outfile, n_readers):
    counter = 0
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
                counter += 1
                if counter % 2*QUEUE_MAX_SIZE == 0:
                   print('total lowered lines = %dk' % (int(counter / 1000)))

            
def reader(q, infile, n_workers):
    with gzip.open(infile, 'r') as source:
        for line in source:
            # add to queue      
            q.put(line)
            
    for ii in range(n_workers):
        q.put(-1)



def lowercase(infile, outfile):
    """
    todo this later
    """ 
    workq = multiprocessing.Queue()
    writeq = multiprocessing.Queue()
    
    
    # start procs
    procs = []
    proc = multiprocessing.Process(target=reader,
                                    args=(workq, infile, NUM_PROCS))
    proc.start()
    procs.append(proc)
    
    
    for i in xrange(NUM_PROCS):
        proc = multiprocessing.Process(target=worker,
                                        args=(workq, writeq))
        proc.start()
        procs.append(proc)


    proc = multiprocessing.Process(target=writer,
                                   args=(writeq, outfile, NUM_PROCS))
    proc.start()
    procs.append(proc)
    # wait for processes to finish
    [proc.join() for proc in procs]
   
    
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