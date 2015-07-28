"""
Select random lines from a gzipped file and write them to another file
"""

from __future__ import print_function

import sys
import argparse
import gzip
import random
from progressbar import ProgressBar, Bar, Percentage


def count_lines(infile):
    """ Count lines in a gzip JSON LD file """
    n_lines = 0
    with gzip.open(infile, 'r') as source:
        for _ in source:
            n_lines += 1

    return n_lines


def select_nrandom(infile, outfile, n):
    """
    Selects random lines from a file
    """
    n_lines = count_lines(infile)
    selected_lines = random.sample(range(0, n_lines), n)
    last_line = max(selected_lines)

    print("%s -> %s (%d [%d] -> %d)" % (infile, outfile, n_lines,
                                        last_line, n))
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=last_line).start()
    counter = 0

    with gzip.open(outfile, 'w') as destination:
        with gzip.open(infile, 'r') as source:
            for line in source:
                # check if line is selected
                if counter in selected_lines:
                    # "selected" line is written to file
                    destination.write(line)

                # update display
                counter += 1
                pbar.update(counter)

                # check end condition
                if counter > last_line:
                    # all "selected" lines have been written
                    pbar.finish()
                    break

    return


def main():
    """ main """
    n = 10000

    parser = argparse.ArgumentParser()
    parser.add_argument('tweet_infiles', help='input files comma seperated')
    parser.add_argument('dest_files', help='output files comma seperated')
    parser.add_argument('-n', '--number', type=int,
                        help='selects simple tokenizer instead of twokenizer')
    args = parser.parse_args()

    tweet_files = args.tweet_infiles.split(',')
    dest_files = args.dest_files.split(',')

    if not len(tweet_files) == len(dest_files):
        print('tweet_files and dest_files are different sizes')
        sys.exit(0)

    if args.number:
        n = args.number

    for source, dest in zip(tweet_files, dest_files):
        select_nrandom(source, dest, n)

if __name__ == '__main__':
    main()
