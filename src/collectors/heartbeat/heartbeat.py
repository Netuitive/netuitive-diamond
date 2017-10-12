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

from diamond.utils.config import load_config as load_server_config

try:
    import netuitive
except ImportError:
    netuitive = None


class HeartbeatCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(HeartbeatCollector, self).__init__(*args, **kwargs)

        self.hostname = self.get_hostname()
        self.interval = self.config['interval']

        if not netuitive:
            self.log.error('netuitive import failed. Heartbeat collector disabled')
            self.enabled = False
            return

        try:
            self.version = self._get_version()
            self.api = netuitive.Client(self.config['url'], self.config['api_key'], self.version)
        except Exception as e:
            self.log.debug(e)

    def collect(self):
        check = netuitive.Check('heartbeat', self.hostname, self.interval)
        self.api.post_check(check)
