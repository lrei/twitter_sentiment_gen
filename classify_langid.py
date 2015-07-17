"""
Reads a Line Delimited JSON file containing tweets. Tweets in file should be preprocessed so classify works better when removing replacements for entities.
Outputs only those with the selected language probability higher than 'langid_min_prob'.
"""


import gzip
import json
from tweet_text import word_tokenize
import multiprocessing
import langid
import argparse
from filter_lang import NUM_PROCS, QUEUE_MAX_SIZE
import sys



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
        entry = q.get(block=True)
        if type(entry) == int:
            if entry < 0:
                break
        
        tweet = filter_classify_lang_line(entry, lang, langid_min_prob, replacements)
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
                   print('total classified lines = %dk' % (int(counter / 1000)))


def reader(q, infile, n_workers):
    with gzip.open(infile, 'r') as source:
        for line in source:
            # add to queue      
            q.put(line)
            
    for ii in range(n_workers):
        q.put(-1)

def filter_langid(tweet_file, outfile, replacements, lang=u'en', langid_min_prob=0.80):    
    #
    # Filter based on language using langid
    #
    
    
    workq = multiprocessing.Queue(QUEUE_MAX_SIZE)
    writeq = multiprocessing.Queue()
    
    
    # start procs
    procs = []
    proc = multiprocessing.Process(target=reader,
                                    args=(workq, tweet_file, NUM_PROCS))
    proc.start()
    procs.append(proc)

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

           
    
def main():
    lang_codes = [u'en']
    langid_min_prob = 0.8
    replacements = {'user': 'TUSERUSER', 'url': 'TURLURL', 'hashtag': 'THASHTAG', 
                    'symbol': 'TSYMBOL'} #also in tweet_text and sentiment_gen
    
    
    parser = argparse.ArgumentParser()
    parser.add_argument('tweet_infiles', help='input files comma seperated')
    parser.add_argument('dest_files', help='output files comma seperated')
    parser.add_argument('-lc', '--lang_codes')
    parser.add_argument('-p', '---langid_min_prob', type=float, help='outputs only tweets that have langid_min_prob or higher probability')
    parser.add_argument('-s', '--hashtag_symbol', help='symbol with which hashtags are replaced (needed to remove from text when using classify function)')
 
    args = parser.parse_args()

    
    tweet_files = args.tweet_infiles.split(',')
    dest_files = args.dest_files.split(',')
    if args.lang_codes:
        lang_codes = unicode(args.lang_codes).split(',')

    
    if not len(tweet_files) == len(dest_files):
        print('tweet_files and dest_files are different sizes')
        sys.exit(0)
    
    
    if not len(dest_files) == len(lang_codes):
        print('different number of files and language codes')
        sys.exit(0)
    
    if args.langid_min_prob:
        langid_min_prob = args.langid_min_prob
       
    #update hashtag symbol 
    if args.hashtag_symbol:
        replacements['hashtag'] = args.hashtag_symbol
       
        
         
    for tweet_file, dest_file, lang_code in zip(tweet_files, dest_files, lang_codes):         
        filter_langid(tweet_file, dest_file, replacements, lang_code, langid_min_prob)
       
   
if __name__ == '__main__':
    main()