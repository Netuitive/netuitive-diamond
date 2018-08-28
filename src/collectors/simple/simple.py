# coding=utf-8

"""
The SimpleCollector collects one utilization metric for CPU, MEM, Disk I/O, and Disk Usage

"""

import diamond.collector
import os
import time
import psutil

class SimpleCollector(diamond.collector.Collector):

    LastCollectTime = None

    PROC_STAT = '/proc/stat'
    PROC_MEM = '/proc/meminfo'
    PROC_DISKSTATS = '/proc/diskstats'

    def __init__(self, config=None, handlers=[], name=None, configfile=None):
        super(SimpleCollector, self).__init__(config, handlers, name, configfile)


    def get_default_config_help(self):
        return super(SimpleCollector, self).get_default_config_help()

    def get_default_config(self):
        config = super(SimpleCollector, self).get_default_config()
        config.update({
            'path': 'netuitive.linux',
        })
        return config

    def collect(self):
        if os.access(self.PROC_STAT, os.R_OK):
            file = open(self.PROC_STAT)
            lines = file.read().splitlines()
            file.close()

            for line in lines:
                if line.startswith('cpu '):
                    elements = line.split()
                    self.collect_cpu_proc(elements)
        else:
            total_time = psutil.cpu_times()
            self.collect_cpu_psutil(total_time)

        # Memory collection only exists for /proc/meminfo
        if os.access(self.PROC_MEM, os.R_OK):
            file = open(self.PROC_MEM)
            lines = file.read().splitlines()
            file.close()

            self.collect_memory_proc(lines)

        if os.access(self.PROC_DISKSTATS, os.R_OK):
            file = open(self.PROC_DISKSTATS)
            lines = file.read().splitlines()
            file.close()

            self.collect_disk_stats_proc(lines)

        return True

    def collect_cpu_proc(self, elements):
        # Compute all CPU usage values from /proc/stat counter values
        user = self.derivative('cpu.total.user', long(elements[1]), diamond.collector.MAX_COUNTER)
        nice = self.derivative('cpu.total.nice', long(elements[2]), diamond.collector.MAX_COUNTER)
        system = self.derivative('cpu.total.system', long(elements[3]), diamond.collector.MAX_COUNTER)
        idle = self.derivative('cpu.total.idle', long(elements[4]), diamond.collector.MAX_COUNTER)
        iowait = self.derivative('cpu.total.iowait', long(elements[5]), diamond.collector.MAX_COUNTER)
        irq = self.derivative('cpu.total.irq', long(elements[6]), diamond.collector.MAX_COUNTER)
        softirq = self.derivative('cpu.total.softirq', long(elements[7]), diamond.collector.MAX_COUNTER)
        steal = self.derivative('cpu.total.steal', long(elements[8]), diamond.collector.MAX_COUNTER)
        guest = self.derivative('cpu.total.guest', long(elements[9]), diamond.collector.MAX_COUNTER)
        guest_nice = self.derivative('cpu.total.guest_nice', long(elements[10]), diamond.collector.MAX_COUNTER)

        total = sum([user, nice, system, idle, iowait, irq, softirq, steal, guest, guest_nice])

        # Derivatives take one cycle to warm up
        if total != 0:
            self.publish('cpu.total.utilization.percent', (total - idle) / total * 100)

    def collect_cpu_psutil(self, total_time):
        # Compute all CPU usage values from psutil counter values
        user = self.derivative('cpu.total.user', total_time.user, diamond.collector.MAX_COUNTER)
        nice = self.derivative('cpu.total.nice', total_time.nice, diamond.collector.MAX_COUNTER)
        system = self.derivative('cpu.total.system', total_time.system, diamond.collector.MAX_COUNTER)
        idle = self.derivative('cpu.total.idle', total_time.idle, diamond.collector.MAX_COUNTER)

        total = sum([user, nice, system, idle])

        # Derivatives take one cycle to warm up
        if total != 0:
            self.publish('cpu.total.utilization.percent', (total - idle) / total * 100)

    def collect_memory_proc(self, lines):
        # Compute and convert all memory usage in bytes
        total = self.memory_proc_line_to_bytes(lines[0])
        free = self.memory_proc_line_to_bytes(lines[1])
        buffers = self.memory_proc_line_to_bytes(lines[3])
        cached = self.memory_proc_line_to_bytes(lines[4])

        self.publish('memory.utilizationpercent', 100 - 100 * (buffers + cached + free) / total)

    # Convert a /proc/meminfo line to a byte value
    def memory_proc_line_to_bytes(self, line):
        name, value, units = line.split()
        name = name.rstrip(':')
        value = int(value)
        return diamond.convertor.binary.convert(value=value, oldUnit=units, newUnit='byte')

    def collect_disk_stats_proc(self, lines):
        # Get the latest collection time for the devisor in the disk I/O calculation
        CollectTime = time.time()
        time_delta = CollectTime - self.LastCollectTime if self.LastCollectTime else float(self.config['interval'])
        self.LastCollectTime = CollectTime

        # Compute the I/O usage in ms for all devices during the collection period
        devices = [line for line in lines if not line.split()[2].startswith('ram') and not line.split()[2].startswith('loop')]
        io_milliseconds = map(self.disk_stats_proc_line_to_io, devices)

        # Take the maximum utilization during the period
        max_util = max(map(lambda ms: ms / time_delta / 10.0, io_milliseconds))

        # Derivatives take one cycle to warm up, though 0 utilization is often a reality
        self.publish('iostat.max_util_percentage', max_util)

    # Convert a /proc/diskstats line to an I/O sample value
    def disk_stats_proc_line_to_io(self, line):
        columns = line.split()
        return self.derivative('iostat.' + columns[2], float(columns[12]), diamond.collector.MAX_COUNTER)
