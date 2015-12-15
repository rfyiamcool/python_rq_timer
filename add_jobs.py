# -*- coding: utf-8 -*-

from delay import DelayedJob
from redis import Redis
import jobs

redis_connection = Redis()
delayed_jobs = DelayedJob(redis_connection)

print "Delay jobs.add(7, 13) on default queue for 30 seconds."
delayed_jobs.seconds('default', 30, jobs.add, 7, 13)

print "Delay jobs.multiply(5, 2) on high queue for 5 seconds."
delayed_jobs.seconds('high', 5, jobs.multiply, 5, 2)
