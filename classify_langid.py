
import gzip
import json
from tweet_text import word_tokenize
import multiprocessing
import Queue
#import time
import itertools
import langid
import argparse
from filter_lang import NUM_PROCS, QUEUE_MAX_SIZE



def filter_classify_lang_line(line, lang, langid_min_prob, replacements):
    try:
        tweet = json.loads(line)
    except:
        return None
            
    if tweet is None:
        return None
    
    tweet = word_tokenize(tweet)        
    
    tokens = tweet['text'].split()
    
    list_replacements = replacements.values()
    tokens = [x for x in tokens if x not in list_replacements
              and x.isalpha() and x.lower() != 'rt']
    
    if not tokens:
        return  None
        
    text = ' '.join(tokens)
    

    # Check if identified language is the expected language
    lid, prob = langid.classify(text) #without properties
    if lid != lang:
        return None

    # Filter based ong langid minimum probability
    if prob < langid_min_prob:
        return None
        
    return tweet


def worker(q, writeq, lang, langid_min_prob, replacements):
    while True:
        try:
            entry = q.get(block=False)
        except Queue.Empty:
            break
        
        tweet = filter_classify_lang_line(entry, lang, langid_min_prob, replacements)
        if tweet is not None:
            tweet_string = json.dumps(tweet)  + u'\n'
            writeq.put(tweet_string)
            
    writeq.put(-1)
    

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


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = list(itertools.islice(file_object, chunk_size))
        if not data:
            break
        yield data


def filter_langid(tweet_file, outfile, replacements, lang=u'en', langid_min_prob=0.80):    
    #
    # Filter based on language using langid
    #
    
    
    workq = multiprocessing.Queue()
    writeq = multiprocessing.Queue()
    n_processed = 0
    
    
    with gzip.open(tweet_file, 'r') as source:
        for batch in read_in_chunks(source, QUEUE_MAX_SIZE):
             # add to queue    
            [workq.put(entry) for entry in batch]
            
            batch_length = len(batch)
            n_processed += batch_length
#            start_time = time.time()
            del batch[:]
        
            # start procs
            procs = []
            for i in xrange(NUM_PROCS):
                proc = multiprocessing.Process(target=worker,
                                        args=(workq, writeq, lang, langid_min_prob, replacements))
                proc.start()
                procs.append(proc)

            proc = multiprocessing.Process(target=writer,
                                        args=(writeq,outfile, NUM_PROCS))
            proc.start()
            procs.append(proc)
            # wait for processes to finish
            [proc.join() for proc in procs]
#            end_time = time.time()
#            processed_per_second = (batch_length / (end_time - start_time)) / 1000
            print('total classified lines = %fk' % (n_processed / 1000))
#            print('classified lines per second = %fk' % processed_per_second)
           
    
def main():
    lang_code = u'en'
    langid_min_prob = 0.8
    replacements = {'user': 'TUSERUSER', 'url': 'TURLURL', 'hashtag': 'THASHTAG', 
                    'symbol': 'TSYMBOL'}
    
    
    parser = argparse.ArgumentParser()
    parser.add_argument('tweet_infile')
    parser.add_argument('dest_file')
    parser.add_argument('-lc', '--lang_code')
    parser.add_argument('-p', '---langid_min_prob', type=float)
    parser.add_argument('-s', '--hashtag_symbol')
 
    args = parser.parse_args()

    
    tweet_file = args.tweet_infile
    dest_file = args.dest_file
    if args.lang_code:
        lang_code = args.lang_code
    
    if args.langid_min_prob:
        langid_min_prob = args.langid_min_prob
       
    #update hashtag symbol 
    if args.hashtag_symbol:
        replacements['hashtag'] = args.hashtag_symbol
        
    filter_langid(tweet_file, dest_file, replacements, lang_code, langid_min_prob)
       
   
if __name__ == '__main__':
    main()