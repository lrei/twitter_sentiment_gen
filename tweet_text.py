'''
Tweet text preprocessing module, processes .json.gz files
'''

from __future__ import print_function
import re
import multiprocessing
import gzip
import json
import argparse
from filter_lang import QUEUE_MAX_SIZE, NUM_PROCS
import sys


#re_user = re.compile(r'(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9]+)')
re_tok = re.compile(r'\w+|[^\w\s]+')



#def simple_replace_user(tweet):
#    ''' Simple regex replacement for twitter @user'''
#    tweet['text'] = re_user.sub(replacements['user'], tweet['text'])
#    return tweet


def update_indices(list_of_indices, delta, start_index):
    if list_of_indices is None:
        return
        
    
    
    for element in list_of_indices:
        if element[0] < start_index:
            continue
        element[0] += delta
        element[1] += delta



def replace_entity(entity, tweet, indices_list, list1, list2, list3, list4, replacements):
    for index_list in indices_list: 
        username_length = index_list[1] - index_list[0] 
        replacement_word_length = len(replacements[entity])
        tweet['text'] = tweet['text'][:index_list[0]] + replacements[entity] + \
        tweet['text'][index_list[1]:]
        
        delta = replacement_word_length - username_length
        update_indices(list1, delta, index_list[0])
        update_indices(list2, delta, index_list[0])
        update_indices(list3, delta, index_list[0])
        update_indices(list4, delta, index_list[0]) 
        update_indices(indices_list, delta, index_list[0]) 
        

def replace_entities(tweet, replace_hashtags, replace_users, replacements):
    ''' Replacement for twitter @user'''
    if 'entities' not in tweet:
        return None
       
    if 'user_mentions' in tweet['entities']:
        list_of_users = tweet['entities']['user_mentions']
    else:
        list_of_users = None
    
    if 'urls' in tweet['entities']:
        list_of_urls = tweet['entities']['urls']
    else:
        list_of_urls = None
    
    if 'hashtags' in tweet['entities']:
        list_of_hashtags = tweet['entities']['hashtags']
    else:
        list_of_hashtags = None
    
    if 'symbols' in tweet['entities']:
        list_of_symbols = tweet['entities']['symbols']
    else:
        list_of_symbols = None
    
    if 'media' in tweet['entities']:
        list_of_media = tweet['entities']['media']
    else:
        list_of_media = None
    
    # update indices when replacing entities    
    # check existance
    if list_of_users is not None:
        list_of_users_indices = [user['indices'] for user in list_of_users] 
    else:
        list_of_users_indices = None
        
    if list_of_urls is not None:
        list_of_urls_indices = [url['indices'] for url in list_of_urls] 
    else:
        list_of_urls_indices = None
        
    if list_of_hashtags is not None:
        list_of_hashtags_indices = [hashtag['indices'] for hashtag in list_of_hashtags] 
    else:
        list_of_hashtags_indices = None
        
    if list_of_symbols is not None:
        list_of_symbols_indices = [symbol['indices'] for symbol in list_of_symbols] 
    else:
        list_of_symbols_indices = None
            
    if list_of_media is not None:
        list_of_media_indices = [media['indices'] for media in list_of_media]
    else:
        list_of_media_indices = None



    if list_of_users_indices is not None and replace_users:
        replace_entity('user', tweet, list_of_users_indices, list_of_urls_indices, 
                        list_of_hashtags_indices, list_of_symbols_indices, 
                        list_of_media_indices, replacements)    

    if list_of_urls_indices is not None:     
        replace_entity('url', tweet, list_of_urls_indices, list_of_users_indices,
                    list_of_hashtags_indices, list_of_symbols_indices, 
                    list_of_media_indices, replacements)
     
    if list_of_hashtags_indices is not None and replace_hashtags:       
        replace_entity('hashtag', tweet, list_of_hashtags_indices, 
                    list_of_users_indices, list_of_urls_indices, 
                    list_of_symbols_indices, list_of_media_indices, replacements)
                   
    if list_of_symbols_indices is not None:        
        replace_entity('symbol', tweet, list_of_symbols_indices, list_of_users_indices, 
                    list_of_urls_indices, list_of_hashtags_indices, 
                    list_of_media_indices, replacements)     
                   
    if list_of_media_indices is not None:
        replace_entity('url', tweet, list_of_media_indices, list_of_symbols_indices, 
                    list_of_users_indices, list_of_urls_indices, 
                    list_of_hashtags_indices, replacements) 
    
    
    # remove property entities, include id
    ntweet = {u'text': tweet[u'text'], u'lang': tweet[u'lang'], u'id': tweet['id']}
    
    return ntweet      
    

def word_tokenize(tweet):
    ''' Simple tokenization function: breaks text on regex word boundaries '''
    tweet['text'] = u' '.join(re_tok.findall(tweet['text']))
    return tweet

def preprocess_tweet(min_tokens, max_num_urls, max_num_users, replace_hashtags, 
                     replace_users, replacements, tweet_line):
    ''' Preprocess a single tweet '''
    try:
        tweet = json.loads(tweet_line)
    except:
        return None   
       

    #filter based on num of urls
    if len(tweet['entities']['urls']) > max_num_urls:
        return None

    #filter based on num of user mentions
    if len(tweet['entities']['user_mentions']) > max_num_users:
        return None

    
    # replace entities
    tweet = replace_entities(tweet, replace_hashtags, replace_users, replacements)
    
    
    # tokenize
    tweet = word_tokenize(tweet)
    
    
    
    #filter based on num of tokens
    tokens = tweet['text'].split()
    list_replacements = replacements.values()
    tokens = [x for x in tokens if x not in list_replacements
              and x.isalpha() and x.lower() != 'rt']
        
        
    if len(tokens) < min_tokens:
        return None
        
    
    return tweet
    


def worker(q, writeq, min_tokens, max_num_urls, max_num_users, replace_hashtags, 
           replace_users, replacements):
           
    while True:
        entry = q.get(block=True)
        if type(entry) == int:
            if entry < 0:
                break
                
        # process tweet
        tweet = preprocess_tweet(min_tokens, max_num_urls, max_num_users, replace_hashtags, 
                                 replace_users, replacements, entry)
        if tweet is not None:
            tweet_string = json.dumps(tweet)  + u'\n'
            writeq.put(tweet_string)
            
    # exit
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
                   print('total processed lines = %dk' % (int(counter / 1000)))
    


def reader(q, infile, n_workers):
    with gzip.open(infile, 'r') as source:
        for line in source:
            # add to queue      
            q.put(line)
            
    for ii in range(n_workers):
        q.put(-1)


def preprocess_tweet_file(input_fname, output_fname, min_tokens, max_num_urls, 
                           max_num_users, replace_hashtags, replace_users, replacements):
    ''' Preprocess an entire file '''
    
    workq = multiprocessing.Queue(QUEUE_MAX_SIZE)
    writeq = multiprocessing.Queue()
    
    
    # start procs
    procs = []
    proc = multiprocessing.Process(target=reader,
                                    args=(workq, input_fname, NUM_PROCS))
    proc.start()
    procs.append(proc)
    
    
    for i in xrange(NUM_PROCS):
        proc = multiprocessing.Process(target=worker,
                                        args=(workq, writeq, min_tokens, 
                                              max_num_urls, max_num_users, replace_hashtags, 
                                              replace_users, replacements))
        proc.start()
        procs.append(proc)

    proc = multiprocessing.Process(target=writer,
                                   args=(writeq, output_fname, NUM_PROCS))
    proc.start()
    procs.append(proc)
    # wait for processes to finish
    [proc.join() for proc in procs]
    


def main():
    min_tokens = 10 # default parameters
    max_num_urls = 1
    max_num_users = 3 
    replace_users = False
    replace_hashtags = False
    replacements = {'user': 'TUSERUSER', 'url': 'TURLURL', 'hashtag': 'THASHTAG', 
                    'symbol': 'TSYMBOL'}

    parser = argparse.ArgumentParser()
    parser.add_argument('input_files', help='input files comma seperated')
    parser.add_argument('output_files', help='output files comma seperated')
    parser.add_argument('-t', '--min_tokens', type=int)
    parser.add_argument('-url', '--max_urls', type=int)
    parser.add_argument('-u', '--max_users', type=int)
    parser.add_argument('-rh', '--replace_hashtags', dest='replace_hashtags', action='store_true')
    parser.add_argument('-ru', '--replace_users', dest='replace_users', action='store_true')
    parser.add_argument('-s', '--hashtag_symbol', help='symbol used to replace hashtags')
    args = parser.parse_args()

    if args.min_tokens:
        min_tokens = args.min_tokens
        
    if args.max_urls:
        max_num_urls = args.max_urls
        
    if args.max_users:
        max_num_users = args.max_users
    replace_hashtags = args.replace_hashtags
    replace_users = args.replace_users
    if args.hashtag_symbol:
        replacements['hashtag'] = args.hashtag_symbol
        replace_hashtags = True
        
        
    infiles = args.input_files.split(',')
    outfiles = args.output_files.split(',')
            
    if not len(infiles) == len(outfiles):
        print('Input files and output_files do not match in size')
        sys.exit(0)
    
    for infile, outfile in zip(infiles, outfiles):
        preprocess_tweet_file(infile, outfile, min_tokens, max_num_urls, 
                            max_num_users, replace_hashtags, replace_users, replacements)


if __name__ == '__main__':
    main()