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


def run(jobs):
    for job in jobs:
        logging.info("%s started", job)
        proxy = SCHEDULER.next()
        logging.info("%s got %s", job, proxy)
        time.sleep(random.random())
        logging.info("%s finished", job)


def spawn_threads(num_threads, num_jobs):
    proc_name = multiprocessing.current_process().name
    if proc_name == 'MainProcess':
        job_fmt = 'job{}'
    else:
        job_fmt = 'job' + proc_name[7:] + '.{}'
    jobs = it.imap(job_fmt.format, it.islice(it.count(1), num_jobs))
    if num_threads == 0:
        return run(jobs)

    threads = []
    for i in range(1, num_threads + 1):
        thread = threading.Thread(name='thread{}'.format(i), target=run,
                                  kwargs={'jobs': jobs})
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()


def spawn_processes(num_processes, num_threads, num_jobs):
    if num_processes == 0:
        return spawn_threads(num_threads, num_jobs)

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
    connection = redis.StrictRedis(db=args.db)
    resources = ['resource{}'.format(i) for i in range(1, args.resources + 1)]
    if args.throttle is None:
        throttles = [0.3 + 0.1 * i for i in range(args.resources)]
        resources = dict(zip(resources, throttles))
        print "Resource throttles: {}".format(sorted(resources.items()))
        SCHEDULER = redrobin.ThrottlingScheduler(resources, connection=connection)
    else:
        print "Resources: {}".format(resources)
        if args.throttle:
            SCHEDULER = redrobin.ThrottlingRoundRobinScheduler(args.throttle,
                                                               resources,
                                                               connection=connection)
        else:
            SCHEDULER = redrobin.RoundRobinScheduler(resources, connection=connection)

    spawn_processes(args.processes, args.threads, args.jobs)
