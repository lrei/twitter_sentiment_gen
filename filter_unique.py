import argparse
import gzip
import json


def make_unique(infile, outfile, tweet_ids):
    fout = gzip.open(outfile, 'w')
    with gzip.open(infile, 'r') as source:
        for line in source:
            try:
                tweet = json.loads(line)
            except:
                continue

            if tweet['id'] in tweet_ids:
                continue

            if 'retweet_id' in tweet and tweet['retweet_id'] in tweet_ids:
                continue

            tweet_ids.add(tweet['id'])
            if 'retweet_id' in tweet:
                tweet_ids.add(tweet['retweet_id'])

            tweet_string = json.dumps(tweet) + '\n'
            fout.write(tweet_string)

    fout.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('infile')
    parser.add_argument('outfile')
    args = parser.parse_args()
    tweet_ids = set()
    make_unique(args.infile, args.outfile, tweet_ids)


if __name__ == '__main__':
    main()
