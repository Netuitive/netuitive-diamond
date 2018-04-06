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
        cpu_collector = CPUCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        cpu_collector.collect()
        memory_collector = MemoryCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        memory_collector.collect()
        loadavg_collector = LoadAverageCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        loadavg_collector.collect()
        network_collector = NetworkCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        network_collector.collect()
        diskusage_collector = DiskUsageCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        diskusage_collector.collect()
        diskspace_collector = DiskSpaceCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        diskspace_collector.collect()
        vmstat_collector = VMStatCollector(config=self.config, configfile=self.configfile, handlers=self.handlers)
        vmstat_collector.collect()
        return True
