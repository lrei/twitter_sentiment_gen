'''
Tweet text preprocessing module, processes .json.gz files
'''

from __future__ import print_function
import re
import multiprocessing
import gzip
import json
from functools import partial
import argparse


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
    


def preprocess_tweet_lines(lines, pool, min_tokens, max_num_urls, max_num_users, 
                            replace_hashtags, replace_users, replacements):
    ''' Parallel preprocess multiple tweets (as a list of tweets) '''
    func = partial(preprocess_tweet, min_tokens, max_num_urls, max_num_users, 
                   replace_hashtags, replace_users, replacements)
    lines = pool.map(func, lines)
    return lines


def preprocess_tweet_file(input_fname, output_fname, min_tokens, max_num_urls, 
                           max_num_users, replace_hashtags, replace_users, replacements):
    ''' Preprocess an entire file '''
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    
    # read input
    with gzip.open(input_fname, 'r') as source:
        lines = preprocess_tweet_lines(source.readlines(), pool, min_tokens, 
                                       max_num_urls, max_num_users, 
                                       replace_hashtags, replace_users, replacements)

    # write to output
    with gzip.open(output_fname, 'w') as destination:
        for line in lines:
            if line is not None:
                tweet_string = json.dumps(line)  + u'\n'
                destination.write(tweet_string)




def main():
    min_tokens = 10 # default parameters
    max_num_urls = 1
    max_num_users = 3 
    replace_users = False
    replace_hashtags = False
    replacements = {'user': 'TUSERUSER', 'url': 'TURLURL', 'hashtag': 'THASHTAG', 
                    'symbol': 'TSYMBOL'}

    parser = argparse.ArgumentParser()
    parser.add_argument('infile')
    parser.add_argument('outfile')
    parser.add_argument('-t', '--min_tokens', type=int)
    parser.add_argument('-url', '--max_urls', type=int)
    parser.add_argument('-u', '--max_users', type=int)
    parser.add_argument('-rh', '--replace_hashtags', dest='replace_hashtags', action='store_true')
    parser.add_argument('-ru', '--replace_users', dest='replace_users', action='store_true')
    parser.add_argument('-s', '--hashtag_symbol')
    args = parser.parse_args()

    if args.min_tokens:
        min_tokens = args.min_tokens
        
    if args.max_urls:
        max_num_urls = args.max_urls
        
    if args.max_users:
        max_num_users = args.max_users
    replace_hashtags = args.replace_hashtags
    replace_users = args.replace_users
    if args.hashtag_symbol and replace_hashtags:
        replacements['hashtag'] = args.hashtag_symbol
        
        
        
    preprocess_tweet_file(args.infile, args.outfile, min_tokens, max_num_urls, 
                          max_num_users, replace_hashtags, replace_users, replacements)


if __name__ == '__main__':
    main()