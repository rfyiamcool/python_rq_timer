#coding:utf-8

import os
import logging
import sys
import signal
import redis
import time
import multiprocessing

from rq import Worker, Queue, Connection
from delay import DelayedJob

listen = ['high', 'default', 'low']
redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)

def sigint_handler(signum,frame):
    for i in pid_list:
        os.kill(i,signal.SIGKILL)
    logging.info("exit...")
    sys.exit()


def soon_worker():
    logging.info('this is worker')
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()

def delay_worker():
    while True:
        print 'Enqueued %d Jobs.' % delayed_jobs.enqueue_delayed_jobs()
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print "Shutting Down"
            break

pid_list = []
signal.signal(signal.SIGINT,sigint_handler)

#redis_connection = Redis()
delayed_jobs = DelayedJob(conn)

if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=5)
    pool.apply_async(delay_worker,)
    for i in xrange(3):
        pool.apply_async(soon_worker,)
    for i in multiprocessing.active_children():
        print i
        pid_list.append(i.pid)
    pid_list.append(os.getpid())
    pool.close()
    pool.join()

