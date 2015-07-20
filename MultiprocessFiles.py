from __future__ import print_function
import multiprocessing
import gzip
import json
import time

class MultiprocessFiles:
    
    def __init__(self, infile, outfile, work_func, num_procs=0, queue_size=200000):
        if num_procs == 0:
            self.num_procs = multiprocessing.cpu_count()
        else:
            self.num_procs = num_procs
        self.queue_size = self.num_procs * queue_size
        self.workq = multiprocessing.Queue(self.queue_size)
        self.writeq = multiprocessing.Queue()
        self.infile = infile
        self.outfile = outfile
        self.work_func = work_func
        
    

    def reader(self):
        with gzip.open(self.infile, 'r') as source:
            for line in source:
                # add to queue      
                self.workq.put(line)
                
        for ii in range(self.num_procs):
            self.workq.put(-1)

    
    
    def worker(self):
        while True:
            entry = self.workq.get(block=True)
            if type(entry) == int:
                if entry < 0:
                    break
                    
            # process line
            tweet = self.work_func(entry)
            if tweet is not None:
                tweet_string = json.dumps(tweet)  + '\n'
                self.writeq.put(tweet_string)
                
        # exit
        self.writeq.put(-1)
            

    def writer(self):
        start_time = time.time()
        counter = 0
        with gzip.open(self.outfile, 'a') as destination:
            while True:
                tweet = self.writeq.get(block=True)
                if type(tweet) == int:
                    if tweet == -1:
                        self.num_procs = self.num_procs -1
                        if self.num_procs == 0:
                            break
                else:
                    destination.write(tweet)
                    counter += 1
                    if counter % 2*self.queue_size == 0:
                        end_time = time.time()
                        processed_per_second = (counter / (end_time - start_time)) / 1000
                        print('total processed lines = %dk' % (int(counter / 1000)))
                        print('processed lines per second = %dk' % int(processed_per_second))
                    
    
  
    
    def run(self):           
        # start procs
        procs = []
        proc = multiprocessing.Process(target=self.reader)
        proc.start()
        procs.append(proc)
        
        for i in xrange(self.num_procs):
            proc = multiprocessing.Process(target=self.worker)
            proc.start()
            procs.append(proc)
    
        proc = multiprocessing.Process(target=self.writer)
        proc.start()
        procs.append(proc)
        # wait for processes to finish
        [proc.join() for proc in procs]

       
            
        