# -*- coding: utf-8 -*-

from delay import DelayedJob
from redis import Redis
import time

redis_connection = Redis()
delayed_jobs = DelayedJob(redis_connection)

while True:
    print 'Enqueued %d Jobs.' % delayed_jobs.enqueue_delayed_jobs()
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print "Shutting Down"
        break
