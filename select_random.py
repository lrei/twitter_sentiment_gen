"""
Select random lines from a gzipped file and write them to another file
"""

import sys
import argparse
import gzip
import random


def count_lines(infile):
    """
    Count lines in a gzip JSON LD file
    """
    n_lines = 0
    with gzip.open(infile, 'r') as source:
        for line in source:
            n_lines += 1

    return n_lines


def select_nrandom(infile, outfile, n):
    n_lines = count_lines(infile)
    selected_lines = random.sample(range(0, n_lines), n)
    last_line = max(selected_lines)

    counter = 0
    with gzip.open(outfile, 'w') as destination:
        with gzip.open(infile, 'r') as source:
            for line in source:
                if counter in selected_lines:
                    destination.write(line + '\n')
                counter += 1
                if counter > last_line:
                    break
    return


def main():
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
