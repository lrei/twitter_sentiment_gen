"""Reads and writes Line Delimited JSON files in parallel processing."""

from __future__ import print_function
import multiprocessing
import gzip
import json
import time


class MultiprocessFiles:
    """Reads and writes Line Delimited JSON files in parallel processing.

    Attributes:
        num_procs: int
            number of processors to work on
        queue_size: int
            size of multiprocessing queue
        workq: multiprocessing queue
            queue to read from
        writeq: multiprocessing queue
            queue to write from
        infile: string
            path to file from where to read tweets
        outfile: string
            path to file to write tweets to
        work_func: function
            function to apply to tweets
    """

    def __init__(self, infile, outfile, work_func, num_procs=0,
                 queue_size=2000, verbose=False):
        if num_procs == 0:
            self.num_procs = multiprocessing.cpu_count()
        else:
            self.num_procs = num_procs
        if verbose:
            print('using %d procs' % (self.num_procs))
        self.queue_size = self.num_procs * queue_size
        self.workq = multiprocessing.Queue(self.queue_size)
        self.writeq = multiprocessing.Queue()
        self.infile = infile
        self.outfile = outfile
        self.work_func = work_func

    def reader(self):
        """ Reads from file and adds JSON objects to multiprocessing queue. """
        with gzip.open(self.infile, 'r') as source:
            for line in source.readlines():
                # add to queue
                self.workq.put(line)

        for _ in range(self.num_procs):
            self.workq.put(-1)

    def worker(self):
        """ Takes JSON object from workq and applies function to it.
        At the end puts JSON object to writeq.
        """
        while True:
            entry = self.workq.get(block=True)
            if type(entry) == int:
                if entry < 0:
                    break

            # process line
            tweet = self.work_func(entry)
            if tweet is not None:
                tweet_string = json.dumps(tweet) + '\n'
                self.writeq.put(tweet_string)

        # exit
        self.writeq.put(-1)

    def writer(self):
        """ Takes JSON object from queue and writes it to outfile. """
        start_time = time.time()
        counter = 0
        with gzip.open(self.outfile, 'a') as destination:
            while True:
                tweet = self.writeq.get(block=True)
                if type(tweet) == int:
                    if tweet == - 1:
                        self.num_procs = self.num_procs - 1
                        if self.num_procs == 0:
                            break
                else:
                    destination.write(tweet)
                    destination.flush()
                    counter += 1
                    if counter % (4 * self.queue_size) == 0:
                        end_time = time.time()
                        processed_per_second = (counter / (end_time -
                                                start_time)) / 1000
                        print('total processed lines = %dk lines/s = %dk' 
                               % (int(counter / 1000), 
                                  int(processed_per_second)))

    def run(self):
        """ Runs reader, num_procs workers and writer. """
        # start procs
        procs = []
        proc = multiprocessing.Process(target=self.reader)
        proc.start()
        procs.append(proc)

        for _ in xrange(self.num_procs):
            proc = multiprocessing.Process(target=self.worker)
            proc.start()
            procs.append(proc)

        proc = multiprocessing.Process(target=self.writer)
        proc.start()
        procs.append(proc)
        # wait for processes to finish
        [proc.join() for proc in procs]
