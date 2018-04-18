# coding=utf-8

"""
The BaseCollector collects CPU/MEM/Disk/Network metrics

#### Dependencies

 * CPUCollector
 * MemoryCollector
 * LoadAverageCollector
 * NetworkCollector
 * DiskUsageCollector
 * DiskSpaceCollector
 * VMStatCollector

"""

import diamond.collector
from cpu import CPUCollector
from memory import MemoryCollector
from loadavg import LoadAverageCollector
from network import NetworkCollector
from diskusage import DiskUsageCollector
from diskspace import DiskSpaceCollector
from vmstat import VMStatCollector


class BaseCollector(diamond.collector.Collector):

    def __init__(self, config=None, handlers=[], name=None, configfile=None):
        super(BaseCollector, self).__init__(config, handlers, name, configfile)
        self.cpu_collector = CPUCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        self.memory_collector = MemoryCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        self.loadavg_collector = LoadAverageCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        self.network_collector = NetworkCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        self.diskusage_collector = DiskUsageCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        self.diskspace_collector = DiskSpaceCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        self.vmstat_collector = VMStatCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)


    def get_default_config_help(self):
        config_help = super(BaseCollector, self).get_default_config_help()
        config_help.update({
            'simple': 'run simple mode on of its sub collectors',
        })
        return config_help

    def get_default_config(self):
        config = super(BaseCollector, self).get_default_config()
        return config

    def collect(self):
        self.cpu_collector.collect()
        self.memory_collector.collect()
        self.loadavg_collector.collect()
        self.network_collector.collect()
        self.diskusage_collector.collect()
        self.diskspace_collector.collect()
        self.vmstat_collector.collect()
        return True
