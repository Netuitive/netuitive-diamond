#!/usr/bin/python
# coding=utf-8
##########################################################################

from test import unittest
import configobj

from diamond.collector import Collector


class BaseCollectorTest(unittest.TestCase):

    def test_SetCustomHostname(self):
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'custom.localhost',
        }
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

    def test_SetHostnameViaShellCmd(self):
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'echo custom.localhost',
            'hostname_method': 'shell',
        }
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

    def test_MergeNetuitiveHandlerConfig(self):
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'custom.localhost',
        }
        config['handlers'] = {}
        config['handlers']['NetuitiveHandler'] = {
            'url': 'https://api.app.netuitive.com/ingest/infrastructure',
            'api_key': '3bd5b41c0cbbbe3e8a1eefb16a6f8c58',
        }
        c = Collector(config, [])
        c.merge_config(config['handlers']['NetuitiveHandler'], prefix='netuitive_')
        self.assertEquals('https://api.app.netuitive.com/ingest/infrastructure', c.config['netuitive_url'])
        self.assertEquals('3bd5b41c0cbbbe3e8a1eefb16a6f8c58', c.config['netuitive_api_key'])

