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
        self.ttl = self.config['ttl']
        self.connection_timeout = 5

        if not netuitive:
            self.log.error('netuitive import failed. Heartbeat collector disabled')
            self.enabled = False
            return

        try:
            self.version = self._get_version()
            if self.config['netuitive_connection_timeout']:
                self.connection_timeout = int(self.config['netuitive_connection_timeout'])

            self.api = netuitive.Client(url=self.config['netuitive_url'],
                                        api_key=self.config['netuitive_api_key'],
                                        agent=self.version,
                                        connection_timeout=self.connection_timeout)
        except Exception as e:
            self.log.debug(e)

    def collect(self):
        check = netuitive.Check('heartbeat', self.hostname, self.ttl)
        self.api.post_check(check)
