"""
The PortCheckCollector checks ports listed in config file.

##### Dependencies

* psutil

Example config file PortCheckCollector.conf

```
enabled = True
ttl = 150

[port]
[[echo]]
number = 8080
protocol = tcp
```

"""

from collections import defaultdict
import diamond.collector

try:
    import psutil
except ImportError:
    psutil = None

try:
    import netuitive
except ImportError:
    netuitive = None


def get_port_stats(port, protocol):
    """
    Iterate over connections and count states for specified port
    :param port: port for which stats are collected
    :return: Counter with port states
    """
    cnts = defaultdict(int)
    for c in psutil.net_connections(protocol):
        c_port = c.laddr[1]
        if c_port != port:
                continue
        if protocol == 'udp':
                status = 'listen'
        if protocol == 'tcp':
                status = c.status.lower()
        cnts[status] += 1
    return cnts


class PortCheckCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(PortCheckCollector, self).__init__(*args, **kwargs)
        self.hostname = self.get_hostname()
        self.ttl = self.config['ttl']

        self.ports = {}
        for port_name, cfg in self.config['port'].items():
            port_cfg = {}
            for key in ('number',):
                port_cfg[key] = cfg.get(key, [])
            for key in ('protocol',):
                port_cfg[key] = cfg.get(key, [])
            self.ports[port_name] = port_cfg

        if not netuitive:
            self.log.error('netuitive import failed. PortCheckCollector disabled')
            self.enabled = False
            return

        try:
            self.version = self._get_version()
            self.api = netuitive.Client(self.config['netuitive_url'], self.config['netuitive_api_key'], self.version)
        except Exception as e:
            self.log.debug(e)

    def get_default_config_help(self):
        config_help = super(PortCheckCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        config = super(PortCheckCollector, self).get_default_config()
        config.update({
            'path': 'port',
            'port': {},
            'protocol': 'tcp'
        })
        return config

    def collect(self):
        """
        Overrides the Collector.collect method
        """

        if psutil is None:
            self.log.error('Unable to import module psutil')
            return {}

        for port_name, port_cfg in self.ports.iteritems():
            port = int(port_cfg['number'])
            if port_cfg['protocol'] == []:
                protocol = 'tcp'
            else:
                protocol = str(port_cfg['protocol'])
            stats = get_port_stats(port, protocol)
            for stat_name, stat_value in stats.iteritems():
                if stat_name == 'listen' and stat_value >= 1:
                    check_name = '%s.%d' % (port_name, port)
                    check = netuitive.Check(check_name, self.hostname, self.ttl)
                    self.api.post_check(check)
