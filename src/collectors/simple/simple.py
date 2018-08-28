# coding=utf-8

"""
The SimpleCollector collects one utilization metric for CPU, MEM, Disk I/O, and Disk Usage

"""

import diamond.collector


class SimpleCollector(diamond.collector.Collector):

    def __init__(self, config=None, handlers=[], name=None, configfile=None):
        super(SimpleCollector, self).__init__(
            config, handlers, name, configfile)

    def get_default_config_help(self):
        return super(SimpleCollector, self).get_default_config_help()

    def get_default_config(self):
        return super(SimpleCollector, self).get_default_config()

    def collect(self):
        return True
