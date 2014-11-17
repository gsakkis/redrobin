#!/usr/bin/env python

import argparse
import itertools as it
import logging
import multiprocessing
import random
import threading
import time

import redis
import redrobin


def worker(jobs):
    scheduler = redrobin.ThrottlingScheduler(connection=redis.StrictRedis(db=REDIS_DB))
    for job in jobs:
        proxy = scheduler.next()
        logging.info("%s started using %s", job, proxy)
        time.sleep(random.random())
        logging.info("%s finished using %s", job, proxy)


def spawn_threads(num_threads, num_jobs):
    threads = []
    job_fmt = 'job' + multiprocessing.current_process().name[7:] + '.{}'
    jobs = it.imap(job_fmt.format, it.islice(it.count(1), num_jobs))
    for i in range(1, num_threads + 1):
        thread = threading.Thread(name='thread{}'.format(i), target=worker,
                                  kwargs={'jobs': jobs})
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()


def spawn_processes(num_processes, num_threads, num_jobs):
    processes = []
    for i in range(1, num_processes + 1):
        process = multiprocessing.Process(name='process{}'.format(i),
                                          target=spawn_threads,
                                          kwargs={'num_threads': num_threads,
                                                  'num_jobs': num_jobs})
        processes.append(process)
        process.start()
    for process in processes:
        process.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', '--processes', type=int, default=2,
                        help='Number of worker processes')
    parser.add_argument('-t', '--threads', type=int, default=3,
                        help='Number of threads per worker process')
    parser.add_argument('-j', '--jobs', type=int, default=15,
                        help='Number of jobs per worker process')
    parser.add_argument('-r', '--resources', type=int, default=5,
                        help='Number of resources to load balance')
    parser.add_argument('--throttle', type=float,
                        help='Throtting of resources, or randomized if not given')
    parser.add_argument('--db', type=int, default=10,
                        help='Redis database number')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(relativeCreated)6d [%(processName)s:%(threadName)s:%(name)s] %(message)s'
    )
    REDIS_DB = args.db
    connection = redis.StrictRedis(db=REDIS_DB)
    resources = ['resource{}'.format(i) for i in range(1, args.resources + 1)]
    if args.throttle is None:
        throttles = [0.3 + 0.1 * i for i in range(args.resources)]
        resources = dict(zip(resources, throttles))
    else:
        resources = dict.fromkeys(resources, args.throttle)
    print "Resource throttles: {}".format(sorted(resources.items()))

    scheduler = redrobin.ThrottlingScheduler(connection=connection)
    scheduler.clear()
    scheduler.update(resources)
    spawn_processes(args.processes, args.threads, args.jobs)
