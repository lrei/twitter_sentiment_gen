"""
Select random lines from a gzipped file and write them to another file
"""

from __future__ import print_function

import sys
import argparse
import gzip
import random
from progressbar import ProgressBar, Bar, Percentage


def naive_count_lines(infile):
    """ Count lines in a gzip JSON LD file """
    n = 0

    with gzip.open(infile, 'r') as source:
        for _, n in enumerate(source, 1):
            pass

    return n


def buf_count_lines(filename):
    """
    Buffered count
    """
    f = gzip.open(filename, 'r')
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read  # loop optimization

    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)

    return lines


def select_nrandom(infile, outfile, n):
    """
    Selects random lines from a file
    """
    count_lines = buf_count_lines
    n_lines = count_lines(infile)
    selected_lines = sorted(random.sample(range(0, n_lines), n))
    last_line = selected_lines[-1]

    print("%s -> %s (%d [last: %d] -> %d)" % (infile, outfile, n_lines,
                                              last_line, n))
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=last_line).start()
    counter = 0

    with gzip.open(outfile, 'w') as destination:
        with gzip.open(infile, 'r') as source:
            for line in source:

                # check if line is selected
                if counter == selected_lines[0]:
                    # "selected" line is written to file
                    destination.write(line)
                    selected_lines = selected_lines[1:]

                    # update display
                    pbar.update(counter)

                # update line counter
                counter += 1

                # check end condition
                if not selected_lines:
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
