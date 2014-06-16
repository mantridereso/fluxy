__author__ = 'martin'

import cProfile
import re
import prime_numbers
import yappi


yappi.start(True,True)

prime_numbers.runPrimeTesting()

yappi.get_func_stats().print_all()
yappi.get_thread_stats().print_all()