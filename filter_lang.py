# -*- coding: utf-8 -*-
# Authors: Luis Rei <me@luisrei.com>
# License: MIT License

"""
Reads a Line Delimited JSON file containing tweets.
Outputs only those with the selected language.
"""

from __future__ import print_function
import sys
import codecs
import json

import langid


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

    #
    # Filter based on language using langid
    #
    if langid_min_prob >= 0:
        # Check if identified language is the expected language
        lid, prob = langid.classify(tweet['text'])
        if lid != lang:
            return None

        # Filter based ong langid minimum probability
        if prob < langid_min_prob:
            return None

    # extract text
    text = tweet['text']

    # remove newlines, strip
    text.replace(u'\n', u'').strip()

    # make sure there are no empty tweets
    if not text:
        text = None

    return text


def filter_tweets(infile, outfile, lang=u'en', langid_min_prob=0.75):
    """
    todo this later
    """
    with codecs.open(infile, 'r', encoding='utf8') as source:
        with codecs.open(outfile, 'wa', encoding='utf8') as destination:
            for tweet_line in source:
                tweet = filter_line(tweet_line, lang, langid_min_prob)
                if tweet is not None:
                    destination.write(tweet + u'\n')


def main():
    '''main'''
    lang_code = u'en'

    if len(sys.argv) not in [3, 4]:
        print(sys.argv[0] + ' input_tweet_file output_tweet_text [lang_code]')
        sys.exit(0)

    infile = sys.argv[1]
    outfile = sys.argv[2]

    if len(sys.argv) == 4:
        lang_code = unicode(sys.argv[3])

    print('Using %s as language code' % lang_code)

    filter_tweets(infile, outfile, lang=lang_code)


if __name__ == '__main__':
    main()
