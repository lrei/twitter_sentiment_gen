'''
Simple tweet text preprocessing module
This assumes only text is available.
'''

from __future__ import print_function
import sys
import re
import codecs
import itertools
import multiprocessing


re_user = re.compile(r'(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9]+)')
re_tok = re.compile(r'\w+|[^\w\s]+')


def simple_replace_user(unicode_text, replace_with=u'USER'):
    ''' Simple regex replacement for twitter @user'''
    return re_user.sub(replace_with, unicode_text)


def word_tokenize(unicode_text):
    ''' Simple tokenization function: breaks text on regex word boundaries '''
    return u' '.join(re_tok.findall(unicode_text))


def preprocess_tweet(unicode_text):
    ''' Preprocess a single tweet '''
    # lower
    unicode_text = unicode_text.lower()
    # replace username
    unicode_text = simple_replace_user(unicode_text)
    # tokenize
    unicode_text = word_tokenize(unicode_text)
    return unicode_text


def preprocess_tweet_lines(lines, pool):
    ''' Parallel preprocess multiple tweets (as a list of tweets) '''
    lines = pool.map(preprocess_tweet, lines)
    return lines


def preprocess_tweet_file(input_fname, output_fname):
    ''' Preprocess an entire file '''
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    # read input
    f_in = codecs.open(input_fname, 'r', encoding='utf-8')
    lines = preprocess_tweet_lines(f_in.readlines(), pool)
    f_in.close()

    # write to output
    with codecs.open(output_fname, 'w', encoding='utf-8') as f_out:
        for line in lines:
            f_out.write(line + u'\n')


def preprocess_tweet_file_chunked(input_fname, output_fname, size=1000000):
    ''' Preprocess an entire file '''
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    n_chunk = 0
    # read input
    f_out = codecs.open(output_fname, 'w', encoding='utf-8')
    with codecs.open(input_fname, 'r', encoding='utf-8') as f_in:
        while True:
            lines = list(itertools.islice(f_in, size))
            if not lines:
                break  # EOF
            lines = preprocess_tweet_lines(lines, pool)
            # write to output
            for line in lines:
                f_out.write(line + u'\n')
            n_chunk += 1
            print('Chunk %d (%dM lines/chunk)' % (n_chunk, size/1000000))
    f_out.close()


if __name__ == '__main__':
    preprocess_tweet_file_chunked(sys.argv[1], sys.argv[2])
