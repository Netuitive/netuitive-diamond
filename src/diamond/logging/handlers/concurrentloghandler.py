# coding=utf-8

from concurrent_log_handler import ConcurrentRotatingFileHandler as CRFH
import sys


class ConcurrentRotatingFileHandler(TRFH):

    def flush(self):
        try:
            super(ConcurrentRotatingFileHandler, self).flush()
        except IOError:
            sys.stderr.write('ConcurrentRotatingFileHandler received a IOError!')
            sys.stderr.flush()
