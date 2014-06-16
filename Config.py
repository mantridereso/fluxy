__author__ = 'martin'


class Config:

    class ZMQ:

        PLATFORM_ADDRESS = "127.0.0.1"
        PLATFORM_MANAGER_PORT = "6008"
        LOCAL_ADDRESS = "127.0.0.1"
        PORT_RANGES = [(11000, 12000), (14000, 16000)]

    class ThreadPool:

        NUM_THREADS_MAX = 100
        NUM_THREADS_INITIAL = 30
        NUM_THREADS_INCREMENT = 5
        HWM_OPEN_TASKS_PER_THREAD = 10
        TIME_SLEEP_ON_EMPTY_QUEUE = 0.02