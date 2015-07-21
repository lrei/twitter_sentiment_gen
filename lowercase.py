"""This module lowercases tweets' text.

Reads tweet's text from infile and writes lowercased text to outfile,
processes Line Delimited JSON files.
"""

from __future__ import print_function
from __future__ import division
import json
import argparse

from MultiprocessFiles import MultiprocessFiles


def lower_line(line):
    """ returns lowered line """
    try:
        tweet = json.loads(line)
    except:
        return None

    tweet['text'] = tweet['text'].lower()

    return tweet


def main():
    """ main """
    parser = argparse.ArgumentParser()
    parser.add_argument('input_tweet_file')
    parser.add_argument('output_file')
    args = parser.parse_args()

    infile = args.input_tweet_file
    outfile = args.output_file

    multiprocess = MultiprocessFiles(infile, outfile, lower_line, num_procs=0,
                                     queue_size=200000)
    multiprocess.run()


if __name__ == '__main__':
    main()
