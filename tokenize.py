"""
Reads a Line Delimited JSON file containing tweets. Tweets in file should be preprocessed.
Outputs only those with the selected language probability higher than 'langid_min_prob'.
"""


import gzip
import json
import multiprocessing
import argparse
import sys
import re
import twokenize
import time


QUEUE_MAX_SIZE = 20000
re_tok = re.compile(r'\w+|[^\w\s]+', re.UNICODE)


def word_tokenize(text):
    ''' Simple tokenization function: breaks text on regex word boundaries '''
    return u' '.join(re_tok.findall(text))


def worker(q, writeq, tokenize):
    while True:
        tweet = q.get(block=True)
        if type(tweet) == int:
            if tweet < 0:
                break

        try:
            tweet = json.loads(tweet)
        except:
            continue

        tweet['text'] = u" ".join(tokenize(tweet['text']))
        if tweet['text']:
            tweet_string = json.dumps(tweet) + '\n'
            writeq.put(tweet_string)

    writeq.put(-1)


def writer(q, outfile, n_readers):
    counter = 0
    with gzip.open(outfile, 'a') as destination:
        while True:
            tweet = q.get(block=True)
            if type(tweet) == int:
                if tweet == -1:
                    n_readers = n_readers - 1
                    if n_readers == 0:
                        break
            else:
                destination.write(tweet)
                counter += 1
                if counter % 2 * QUEUE_MAX_SIZE == 0:
                    print('total tokenized lines = %dk' % (int(counter / 1000)))


def reader(q, infile, n_workers):
    with gzip.open(infile, 'r') as source:
        for line in source:
            # add to queue
            q.put(line)

    for ii in range(n_workers):
        q.put(-1)


def tokenize_file(tweet_file, outfile, tokenize_function):
    #
    # Filter based on language using langid
    #
    NUM_PROCS = multiprocessing.cpu_count()
    workq = multiprocessing.Queue(QUEUE_MAX_SIZE)
    writeq = multiprocessing.Queue()

    # start procs
    procs = []
    proc = multiprocessing.Process(target=reader,
                                   args=(workq, tweet_file, NUM_PROCS))
    proc.start()
    procs.append(proc)

    for i in xrange(NUM_PROCS):
        proc = multiprocessing.Process(target=worker, args=(workq, writeq,
                                                            tokenize_function))
        proc.start()
        procs.append(proc)

    proc = multiprocessing.Process(target=writer,
                                   args=(writeq, outfile, NUM_PROCS))
    proc.start()
    procs.append(proc)
    # wait for processes to finish
    print('waiting processes')
    time.sleep(1)
    [proc.join() for proc in procs]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('tweet_infiles', help='input files comma seperated')
    parser.add_argument('dest_files', help='output files comma seperated')
    parser.add_argument('-s', '--simple', type=bool,
                        help='selects simple tokenizer instead of twokenizer')
    parser.add_argument('-t', '--twokenize', type=bool,
                        help='twokenizer that does not break apostroph words')

    args = parser.parse_args()

    tweet_files = args.tweet_infiles.split(',')
    dest_files = args.dest_files.split(',')
    
    if not len(tweet_files) == len(dest_files):
        print('tweet_files and dest_files are different sizes')
        sys.exit(0)

    tokenize_function = twokenize.tokenize2
    if args.simple:
        tokenize_function = word_tokenize
    if args.twokenize:
        tokenize_function = twokenize.tokenize

    for source, dest in zip(tweet_files, dest_files):
        tokenize_file(source, dest, tokenize_function)


if __name__ == '__main__':
    main()
