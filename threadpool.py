import Queue
import threading

"""
A simple thread pool implementation intended to work with python versions 2.3+.

Goals:
    * Work with python 2.3+
    * Do not rely on Queue.task_done()
    * Be interruptible w/ Ctrl-C for console programs

Test:
    Running this python module directly will run a series of simple tests.
    Ex: python threadpool.py
"""

class ThreadPoolWorker(threading.Thread):
    """A subclass of threading.Thread that does the work for a
    given pool."""
    def __init__(self, pool):
        threading.Thread.__init__(self)
        self.pool = pool
        self.daemon = True

    def run(self):
        while not self.pool.stopping:
            func, args, kwargs, callback = self.pool._queue.get()
            rv = func(*args, **kwargs)
            if callback: callback(rv)
            self.pool.task_done()
        self.pool.worker_done()
        
class ThreadPool(object):
    """A thread pool object, the real workhorse of this module.

    Methods:
        __init__(size, name):

            Arguments:
                size - the number of threads in the pool
                name - a name to give to the pool

        task_add(func, args, kwargs, callback):

            Arguments:
                func - callable object to execute on the thread
                args - a list of the positional arguments for 'func'
                kwargs - dict of the kwargs for 'func'
                callback - an optional callback to run after func completes
                    ** Note remember this runs on the same thread as 'func' so
                    the thread will not pickup a new task until the callback
                    completes also.

                    The callback will be passed the return value of 'func'.

        task_done():
            This should not be called directly and should only be called by the
            run() method of the Worker class.  Because in python 2.3 Queue.Queue
            does not have a task_done() method I implemented it in the pool
            instead.

        worker_done():
            This should not be called directly and should only be called by the
            run() method of the Worker class.  If a thread is in a 'stopping'
            state it will not pickup another task from the queue and instead
            return.  Before it returns it calls worker_done() to decrement
            pool.active_workers.  If pool.active_workers == 0 then the pool is
            stopped.

        start():
            Start the ThreadPool working on its queue.

        join():
            Block until the ThreadPool has finished all of its work.
"""

    def __init__(self, size, name):
        self.stopping = False 
        self.active_workers = 0
        self.name = name
        self.unfinished_tasks = 0
        self.mutex = threading.Lock()
        self.all_tasks_done = threading.Condition(self.mutex)

        self._queue = Queue.Queue()
        self._workers = []
        for i in range(size):
            worker = ThreadPoolWorker(self)
            worker.setName('%s-%d' % (name, i))
            self._workers.append(worker)

    def task_add(self, func, args = [], kwargs = {}, callback = None):
        self.mutex.acquire()
        try:
            self.unfinished_tasks+=1
            self._queue.put((func, args, kwargs, callback))
        finally:
            self.mutex.release()

    add_task = task_add

    def task_done(self):
        self.all_tasks_done.acquire()
        try:
            self.unfinished_tasks-=1
            if self.unfinished_tasks == 0:
                self.all_tasks_done.notify_all()
        finally:
            self.all_tasks_done.release()

    def worker_done(self):
        self.mutex.acquire()
        self.active_workers-=1
        self.mutex.release()

    def start(self):
        for w in self._workers:
            w.start()
            self.mutex.acquire()
            self.active_workers+=1
            self.mutex.release()

    def join(self):
        self.all_tasks_done.acquire()
        try:
            while self.unfinished_tasks:
                self.all_tasks_done.wait(0.1)
        finally:
            self.all_tasks_done.release()

if __name__ == '__main__':
    import random
    import sys
    import time

    print_lock = threading.RLock()
    def done_frobnicating(rv):
        """A simple callback for frobnicate()"""
        with print_lock: print "done_frobnicating: task: %d rv: %s" % (rv)

    def frobnicate(i, pool, maxsleep = 10, otherpool = None):
            """A simple function to call on a thread."""
            with print_lock: print "frobnicating: pool: %s task: %d" % (pool.name, i)

            # If frobnicate was passed 'otherpool' approx 50% of the time start
            # a new task on that thread.
            if otherpool and random.random() < 0.5:
                otherpool.add_task(frobnicate, [i+20, otherpool], { 'maxsleep': maxsleep }, done_frobnicating)

            # Return the task number and a random value between 0 and 4
            time.sleep(random.random() * maxsleep)
            return (i, round(random.random() * 4))

    # Create a couple of pools
    pool = ThreadPool(5, 'TestPool')
    pool2 = ThreadPool(5, 'OtherPool')

    # Add 10 things to the first pool and pass 'otherpool' to frobnicate also
    # test callback by calling 'done_frobnicating'
    for i in range(10):
        pool.add_task(frobnicate, [i, pool], {'maxsleep': 5, 'otherpool': pool2}, done_frobnicating)

    # Add 5 more tasks to the pool without 'otherpool' and without a callback.
    for i in range(11,15):
        pool.add_task(frobnicate, [i, pool], {'maxsleep': 2 })

    # Start each pool, then join pool and pool2 respectively.
    try:
        pool.start()
        pool2.start()
        pool.join()
        with print_lock: print "pool is done..."
        pool2.join()
        with print_lock: print "pool2 is done..."
    except KeyboardInterrupt:
        print ("\rGot Ctrl-C, asking threads to stop, press Ctrl-C again to "
            "exit immediately...")
        pool.stopping = True
        pool2.stopping = True

        while pool.active_workers > 0 and pool2.active_workers > 0:
            time.sleep(0.1)

        sys.exit(1)

    # The end result should show:
    # Tasks 0-10 on pool 'TestPool' with a callback
    # Tasks 11-15 on pool 'TestPool' without a callback
    # Tasks 20+ on pool 'OtherPool' with a callback
