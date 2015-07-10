'''
Simple tweet text preprocessing module, processes .json.gz files
'''

from __future__ import print_function
import sys
import re
import multiprocessing
import gzip
import json
from functools import partial


#re_user = re.compile(r'(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9]+)')
re_tok = re.compile(r'\w+|[^\w\s]+')
replacements = {'user': 'TUSERUSER', 'url': 'TURLURL', 'hashtag': 'THASHTAG', 
                'symbol': 'TSYMBOL'}


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



def replace_entity(entity, tweet, indices_list, list1, list2, list3, list4):
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
        

def replace_entities(tweet):
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



    if list_of_users_indices is not None:
        replace_entity('user', tweet, list_of_users_indices, list_of_urls_indices, 
                        list_of_hashtags_indices, list_of_symbols_indices, 
                        list_of_media_indices)    

    if list_of_urls_indices is not None:     
        replace_entity('url', tweet, list_of_urls_indices, list_of_users_indices,
                    list_of_hashtags_indices, list_of_symbols_indices, 
                    list_of_media_indices)
     
    if list_of_hashtags_indices is not None:       
        replace_entity('hashtag', tweet, list_of_hashtags_indices, 
                    list_of_users_indices, list_of_urls_indices, 
                    list_of_symbols_indices, list_of_media_indices)
                   
    if list_of_symbols_indices is not None:        
        replace_entity('symbol', tweet, list_of_symbols_indices, list_of_users_indices, 
                    list_of_urls_indices, list_of_hashtags_indices, 
                    list_of_media_indices)     
                   
    if list_of_media_indices is not None:
        replace_entity('url', tweet, list_of_media_indices, list_of_symbols_indices, 
                    list_of_users_indices, list_of_urls_indices, 
                    list_of_hashtags_indices) 
    
    
    # remove property entities, include id
    ntweet = {u'text': tweet[u'text'], u'lang': tweet[u'lang']}
    if 'id' in tweet:
        ntweet['id'] = tweet['id']
    
    return ntweet      
    

def word_tokenize(tweet):
    ''' Simple tokenization function: breaks text on regex word boundaries '''
    tweet['text'] = u' '.join(re_tok.findall(tweet['text']))
    return tweet

def preprocess_tweet(min_tokens, max_num_urls, max_num_users, lowercasing, tweet_line):
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

    
    # lower
    if lowercasing == 'yes':
        tweet['text'] = tweet['text'].lower()
    
    # replace entities
    tweet = replace_entities(tweet)
    
    
    # tokenize
    tweet = word_tokenize(tweet)
    
    
    
    #filter based on num of tokens
    tokens = tweet['text'].split()
    tokens = [x for x in tokens
              if x not in replacements['user'] 
              and x not in replacements['url']
              and x not in  replacements['symbol']
              and x not in replacements['hashtag']
              and x.isalpha() and x not in 'rt']
        
        
    if len(tokens) < min_tokens:
        return None
        
    
    return tweet
    


def preprocess_tweet_lines(lines, pool, min_tokens, max_num_urls, max_num_users, lowercasing):
    ''' Parallel preprocess multiple tweets (as a list of tweets) '''
    func = partial(preprocess_tweet, min_tokens, max_num_urls, max_num_users, lowercasing)
    lines = pool.map(func, lines)
    return lines


def preprocess_tweet_file(input_fname, output_fname, min_tokens, max_num_urls, max_num_users, lowercasing):
    ''' Preprocess an entire file '''
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    # read input
    with gzip.open(input_fname, 'r') as source:
        lines = preprocess_tweet_lines(source.readlines(), pool, min_tokens, max_num_urls, max_num_users, lowercasing)

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
    lowercasing = 'no'
    
    cmd_name = sys.argv[0]
    if len(sys.argv) not in [3, 4, 5, 6, 7]:
        usage = 'Usage:\n\t%{cmd} ' \
                'infile outfile [min_tokens] [max_urls] [max_users] [lowercasing(yes/no)]'
        usage.format(cmd=cmd_name)
        print(usage)
        sys.exit(0)
    
    if len(sys.argv) >= 4:
        min_tokens = int(sys.argv[3])
        
    if len(sys.argv) >= 5:
        max_num_urls = int(sys.argv[4])
        
    if len(sys.argv) >= 6:
        max_num_users = int(sys.argv[5])
    
    if len(sys.argv) >= 7:
        lowercasing = sys.argv[6]
        
    preprocess_tweet_file(sys.argv[1], sys.argv[2], min_tokens, max_num_urls, max_num_users, lowercasing)




if __name__ == '__main__':
    main()