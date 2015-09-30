import argparse
import gzip
import json
import codecs


def main():
    """
    Makes train.tsv from pos.txt, neg.txt and news tweets (.json.gz format).
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('neg_file', help='txt file with negative tweets')
    parser.add_argument('neu_file', help='.json.gz file with neutral tweets')
    parser.add_argument('pos_file', help='txt file with positive tweets')
    parser.add_argument('output_file')
    args = parser.parse_args()

    train_file = codecs.open(args.output_file, 'w', encoding='utf-8')

    with open(args.neg_file, 'r') as source:
        for line in source:
            train_file.write(line.decode(encoding='utf-8').strip() + u'\t' +
                             u'NEGATIVE' + u'\n')

    with gzip.open(args.neu_file, 'r') as source:
        for line in source:
            try:
                tweet = json.loads(line)
            except:
                continue

            train_file.write(tweet['text'].strip() + u'\t' + u'NEUTRAL' + u'\n')

    with open(args.pos_file, 'r') as source:
        for line in source:
            train_file.write(line.decode(encoding='utf-8').strip() + u'\t' +
                             u'POSITIVE' + u'\n')
    train_file.close()


if __name__ == "__main__":
    main()

