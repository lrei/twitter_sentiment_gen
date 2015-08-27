"""Uses langid on tweets to check if languages match.

Reads a Line Delimited JSON file containing tweets.
Tweets in file should be preprocessed.
Outputs only those with the selected language probability higher than
'langid_min_prob'.
"""

from __future__ import print_function
import json
import argparse
import sys
from functools import partial

import langid

from MultiprocessFiles import MultiprocessFiles


def filter_classify_lang_line(lang, langid_min_prob, replacements, line):
    """ With langid calculates probability of text being in which language.

    Args:
        line: JSON object
            represents one tweet
        replacements: dictionary
            entities to be replaced

    Returns:
        only tweets with the selected language probability higher than
        langid_min_prob.
    """
    try:
        tweet = json.loads(line)
    except:
        return None

    if tweet is None:
        return None

    tokens = tweet['text'].split()
    list_replacements = replacements.values()
    tokens = [x for x in tokens if x not in list_replacements and
              x.isalpha() and x.lower() != u'rt']

    if not tokens:
        return None

    text = u' '.join(tokens)

    # Check if identified language is the expected language
    lid, prob = langid.classify(text)  # text without properties
    if lid != lang:
        return None

    # Filter based on langid minimum probability
    if prob < langid_min_prob:
        return None

    return tweet


def main():
    """ main """
    lang_codes = ['en']
    langid_min_prob = 0.7
    replacements = json.load(open('replacements.json'))

    parser = argparse.ArgumentParser()
    parser.add_argument('tweet_infiles',
                        help='input file paths comma seperated')
    parser.add_argument('dest_files', help='output file paths comma seperated')
    parser.add_argument('-l', '--lang_codes')
    parser.add_argument('-p', '---langid_min_prob', type=float,
                        help='outputs only tweets that have langid_min_prob \
                              or higher probability')
    parser.add_argument('-n', '--num_jobs', type=int, default=0,
                        help='number of worker processes to use. Default: \
                              number of cores')
    args = parser.parse_args()

    tweet_files = args.tweet_infiles.split(',')
    dest_files = args.dest_files.split(',')

    if args.lang_codes:
        lang_codes = args.lang_codes.split(',')

    if not len(tweet_files) == len(dest_files):
        print('tweet_files and dest_files are different sizes')
        sys.exit(0)

    if not len(dest_files) == len(lang_codes):
        print('different number of files and language codes')
        sys.exit(0)

    if args.langid_min_prob:
        langid_min_prob = args.langid_min_prob

    for source, dest, lang in zip(tweet_files, dest_files, lang_codes):
        func = partial(filter_classify_lang_line, lang, langid_min_prob,
                       replacements)
        multiprocess = MultiprocessFiles(source, dest, func,
                                         num_procs=args.num_jobs,
                                         queue_size=2000)
        multiprocess.run()


if __name__ == '__main__':
    main()
