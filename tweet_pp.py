"""Tweet text preprocessing module, processes Line Delimited JSON files.

Reads Line Delimited JSON file and and processes tweet's text. In text it
replaces entities specified in replacements.json.
"""

from __future__ import print_function
import json
import argparse
import sys
from functools import partial

from MultiprocessFiles import MultiprocessFiles


def update_indices(list_of_indices, delta, start_index):
    """Updates indices in list by delta"""
    if list_of_indices is None:
        return

    for element in list_of_indices:
        if element[0] < start_index:
            continue
        element[0] += delta
        element[1] += delta


def replace_entity(entity, tweet, indices_list, list1, list2, list3, list4,
                   replacements):
    """Replaces entity in tweet based on indices_list

    Other lists are just updated so they correspond with new processed text.
    """
    for index_list in indices_list:
        if index_list[1] > 140:
            return None
        entity_length = index_list[1] - index_list[0]
        replacement_word_length = len(replacements[entity])
        tweet['text'] = (tweet['text'][:index_list[0]] + replacements[entity] +
                         tweet['text'][index_list[1]:])
        delta = replacement_word_length - entity_length
        update_indices(list1, delta, index_list[0])
        update_indices(list2, delta, index_list[0])
        update_indices(list3, delta, index_list[0])
        update_indices(list4, delta, index_list[0])
        update_indices(indices_list, delta, index_list[0])
    
    return True


def replace_entities(tweet, replacements):
    '''Replaces entities specified in replacements for one tweet's text'''
    if 'entities' not in tweet:
        return tweet

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

    # update indices when replacing entities and check existance
    if list_of_users is not None:
        list_of_users_indices = [user['indices'] for user in list_of_users]
    else:
        list_of_users_indices = None

    if list_of_urls is not None:
        list_of_urls_indices = [url['indices'] for url in list_of_urls]
    else:
        list_of_urls_indices = None

    if list_of_hashtags is not None:
        list_of_hashtags_indices = [hashtag['indices'] for hashtag in
                                    list_of_hashtags]
    else:
        list_of_hashtags_indices = None

    if list_of_symbols is not None:
        list_of_symbols_indices = [symbol['indices'] for symbol in
                                   list_of_symbols]
    else:
        list_of_symbols_indices = None

    if list_of_media is not None:
        list_of_media_indices = [media['indices'] for media in list_of_media]
    else:
        list_of_media_indices = None

    if list_of_users_indices is not None and replacements['user'] is not None:
        ret = replace_entity('user', tweet, list_of_users_indices,
                             list_of_urls_indices, list_of_hashtags_indices,
                             list_of_symbols_indices, list_of_media_indices,
                             replacements)
        if not ret:
            return None

    if list_of_urls_indices is not None and replacements['url'] is not None:
        ret = replace_entity('url', tweet, list_of_urls_indices,
                             list_of_users_indices, list_of_hashtags_indices,
                             list_of_symbols_indices, list_of_media_indices,
                             replacements)
        if not ret:
            return None

    if (list_of_hashtags_indices is not None and
            replacements['hashtag'] is not None):
        ret = replace_entity('hashtag', tweet, list_of_hashtags_indices,
                             list_of_users_indices, list_of_urls_indices,
                             list_of_symbols_indices, list_of_media_indices,
                             replacements)
        if not ret:
            return None

    if (list_of_symbols_indices is not None and
            replacements['symbol'] is not None):
        ret = replace_entity('symbol', tweet, list_of_symbols_indices,
                             list_of_users_indices, list_of_urls_indices,
                             list_of_hashtags_indices, list_of_media_indices,
                             replacements)
        if not ret:
            return None

    if list_of_media_indices is not None and replacements['url'] is not None:
        ret = replace_entity('url', tweet, list_of_media_indices,
                             list_of_symbols_indices, list_of_users_indices,
                             list_of_urls_indices, list_of_hashtags_indices,
                             replacements)
        if not ret:
            return None

    # remove field entities
    ntweet = {u'text': tweet['text'], u'lang': tweet['lang'],
              u'id': tweet['id']}
    if 'created_at' in tweet:
        ntweet['created_at'] = tweet['created_at']

    return ntweet


def preprocess_tweet(min_tokens, max_num_urls, max_num_users, replacements,
                     tweet_line):
    """ Preprocess a single tweet """
    try:
        tweet = json.loads(tweet_line)
    except:
        return None

    # filter based on num of urls
    if len(tweet['entities']['urls']) > max_num_urls:
        return None

    # filter based on num of user mentions
    if len(tweet['entities']['user_mentions']) > max_num_users:
        return None

    # replace entities
    tweet = replace_entities(tweet, replacements)

    # filter based on num of tokens
    tokens = tweet['text'].split()
    list_replacements = replacements.values()
    tokens = [x for x in tokens if x not in list_replacements and
              x.isalpha() and x.lower() != u'rt']

    if len(tokens) < min_tokens:
        return None

    return tweet


def main():
    """ main """
    # default parameters
    min_tokens = 5
    max_num_urls = 2
    max_num_users = 3
    replacements = json.load(open('replacements.json'))

    parser = argparse.ArgumentParser()
    parser.add_argument('input_files', help='input file paths comma seperated')
    parser.add_argument('output_files',
                        help='output file paths comma seperated')
    parser.add_argument('-t', '--min_tokens', type=int)
    parser.add_argument('-url', '--max_urls', type=int)
    parser.add_argument('-u', '--max_users', type=int)
    args = parser.parse_args()

    if args.min_tokens:
        min_tokens = args.min_tokens

    if args.max_urls:
        max_num_urls = args.max_urls

    if args.max_users:
        max_num_users = args.max_users

    infiles = args.input_files.split(',')
    outfiles = args.output_files.split(',')

    if not len(infiles) == len(outfiles):
        print('Input files and output_files do not match in size')
        sys.exit(0)

    func = partial(preprocess_tweet, min_tokens,
                   max_num_urls, max_num_users, replacements)
    for infile, outfile in zip(infiles, outfiles):
        multiprocess = MultiprocessFiles(infile, outfile, func, num_procs=0,
                                         queue_size=200000)
        multiprocess.run()


if __name__ == '__main__':
    main()
