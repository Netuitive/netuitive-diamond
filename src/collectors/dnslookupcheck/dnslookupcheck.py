"""
The DNSLookupCheckCollector does a DNS lookup and returns a check
##### Dependencies
* socket

Example config file DNSLookupCheckCollector.conf
```
enabled = True
ttl = 150

dnsAddressList = ('www.google.com', 'www.yahoo.com')
```
"""

from collections import defaultdict
import diamond.collector

try:
  import socket
except ImportError:
  socket = None
  self.log.error('Unable to import module socket')

try:
    import netuitive
except ImportError:
    netuitive = None


class DNSLookupCheckCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(DNSLookupCheckCollector, self).__init__(*args, **kwargs)
        self.hostname = self.get_hostname()
        self.ttl = self.config['ttl']

        if not netuitive:
            self.log.error('netuitive import failed. dnslookupcheck disabled')
            self.enabled = False
            return

        try:
            self.version = self._get_version()
            self.api = netuitive.Client(self.config['netuitive_url'], self.config['netuitive_api_key'], self.version)
        except Exception as e:
            self.log.debug(e)

    def get_default_config_help(self):
        config_help = super(DNSLookupCheckCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        config = super(DNSLookupCheckCollector, self).get_default_config()
        config.update({
            'path': 'port',
            'port': {},
        })
        return config

    def collect(self):
        """
        Overrides the Collector.collect method
        """

        #check to see if the dns name returns an IP address
        for dnsAddress in self.config['dnsAddressList']:
            try:
                addr = socket.gethostbyname(dnsAddress)
                check = netuitive.Check(dnsAddress, self.hostname, self.ttl)
                self.api.post_check(check)
            except socket.gaierror:
                self.log.error ('cannot resolve hostname')
