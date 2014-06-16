__author__ = 'martin'

import threading
import collections
import time


from Config import Config


class PatternMessageReceiver(threading.Thread):

    def __init__(self, queue):
        threading.Thread.__init__(self)
        assert isinstance(queue, collections.deque)
        self._queue = queue

    def run(self):
        sleepForSecs = Config.ThreadPool.TIME_SLEEP_ON_EMPTY_QUEUE
        while True:

            queueLen = len(self._queue)
            while queueLen == 0:
                time.sleep(sleepForSecs)
                queueLen = len(self._queue)
            try:
                for i in range(queueLen):
                    pattern, message = self._queue.popleft()
                    if callable(pattern):
                        pattern(message)
                    else:
                        if pattern.acceptMessage(message):
                            pattern.receiveMessage(message)
            except IndexError:
                pass


class ThreadPool():

    instance = None

    @staticmethod
    def getInstance():
        """

        :rtype : ThreadPool
        """
        if ThreadPool.instance is None:
            return ThreadPool()
        else:
            return ThreadPool.instance

    def __init__(self):
        ThreadPool.instance = self
        self._queues = []
        self._queuePointer = -1
        self._numThreads = 0
        self._prepareWorkers(Config.ThreadPool.NUM_THREADS_INITIAL)

    def _nextQueue(self):
        """

        :rtype : collections.deque
        """
        queuePointer = self._queuePointer
        queuePointer += 1
        if queuePointer >= self._numThreads:
            queuePointer = 0
        self._queuePointer = queuePointer
        return self._queues[queuePointer]

    def _prepareWorkers(self, numWorkers=None):
        if self._numThreads < Config.ThreadPool.NUM_THREADS_MAX:
            if numWorkers is None:
                numWorkers = min(Config.ThreadPool.NUM_THREADS_INCREMENT, Config.ThreadPool.NUM_THREADS_MAX - self._numThreads)
            if numWorkers > 0:
                newQueues = [collections.deque() for i in range(numWorkers)]
                self._queues.extend(newQueues)
                for queue in newQueues:
                    PatternMessageReceiver(queue).start()
                    self._numThreads += 1

    def enqueueTask(self, message, pattern):
        item = pattern, message
        self._nextQueue().append(item)