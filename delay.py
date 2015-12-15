# -*- coding: utf-8 -*-

try:
    import cPickle as pickle
except ImportError:
    import pickle

import time
import uuid

from rq import Queue


class DelayedJob(object):

    def __init__(self, redis):
        self.redis = redis

    def _now(self):
        '''Get the current time, as an integer UTC timestamp.'''
        return int(time.mktime(time.gmtime()))

    def delay(self, queue, job, seconds, *args, **kwargs):
        '''Delay a queue job by a number of seconds.'''
        self.redis.zadd('queue:delayed', pickle.dumps({'job': job, 'queue': queue, 'args': args, 'kwargs': kwargs, 'id': uuid.uuid1().hex}), self._now() + seconds)

    def enqueue_delayed_jobs(self, now=None):
        '''Enqueue and clear out ready delayed jobs.'''
        if not now:
            now = self._now()

        jobs = self.redis.zrangebyscore('queue:delayed', 0, now)

        for pickled_job in jobs:
            job = pickle.loads(pickled_job)
            Queue(job['queue'], connection=self.redis).enqueue(job['job'], *job['args'], **job['kwargs'])
            self.redis.zrem('queue:delayed', pickled_job)

        return len(jobs)

    def seconds(self, queue, seconds, job, *args, **kwargs):
        return self.delay(queue, job, seconds, *args, **kwargs)

    def minutes(self, queue, minutes, job, *args, **kwargs):
        return self.delay(queue, job, 60 * minutes, *args, **kwargs)

    def hours(self, queue, hours, job, *args, **kwargs):
        return self.delay(queue, job, 3600 * hours, *args, **kwargs)

    def days(self, queue, days, job, *args, **kwargs):
        return self.delay(queue, job, 86400 * days, *args, **kwargs)
