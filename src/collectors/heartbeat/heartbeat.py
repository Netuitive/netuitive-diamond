# coding=utf-8

"""
Send a value of 1 as a heartbeat every time this collector is invoked.

#### Dependencies
None

#### Usage
Add the collector config as :

enabled = True
path = netuitive

Metrics are collected as :
    - metrics.heartbeat

Netuitive Change History
========================

DVG     2016/11/14      Initial version.

"""

import diamond.collector


class HeartbeatCollector(diamond.collector.Collector):

    def collect(self):
        self.publish_gauge("metrics.heartbeat", 1)