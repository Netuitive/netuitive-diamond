#!/usr/bin/python
# coding=utf-8
##########################################################################

from test import unittest
import configobj
from diamond.server import Server


class ServerTest(unittest.TestCase):

    def test_BaseCollectorEnabled(self):

        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['BaseCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['CPUCollector'] = {
            'enabled': 'False',
        }
        config['collectors']['DiskSpaceCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['DiskUsageCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['LoadAverageCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['MemoryCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['VMStatCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['NetworkCollector'] = {
            'enabled': 'True',
        }

        server = Server(None)
        server.mangage_base_collectors(config)

        self.assertEqual(len(config['collectors']), 8)
        self.assertEqual(config['collectors']['BaseCollector']['enabled'], 'True')

        self.assertEqual(config['collectors']['CPUCollector']['enabled'], 'False')
        self.assertEqual(config['collectors']['DiskSpaceCollector']['enabled'], 'False')
        self.assertEqual(config['collectors']['DiskUsageCollector']['enabled'], 'False')
        self.assertEqual(config['collectors']['LoadAverageCollector']['enabled'], 'False')
        self.assertEqual(config['collectors']['MemoryCollector']['enabled'], 'False')
        self.assertEqual(config['collectors']['VMStatCollector']['enabled'], 'False')
        self.assertEqual(config['collectors']['NetworkCollector']['enabled'], 'False')

    def test_BaseCollectorDisabled(self):

        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}

        config['collectors']['CPUCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['DiskSpaceCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['DiskUsageCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['LoadAverageCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['MemoryCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['VMStatCollector'] = {
            'enabled': 'True',
        }
        config['collectors']['NetworkCollector'] = {
            'enabled': 'True',
        }

        server = Server(None)
        server.mangage_base_collectors(config)

        self.assertEqual(len(config['collectors']), 7)

        self.assertEqual(config['collectors']['CPUCollector']['enabled'], 'True')
        self.assertEqual(config['collectors']['DiskSpaceCollector']['enabled'], 'True')
        self.assertEqual(config['collectors']['DiskUsageCollector']['enabled'], 'True')
        self.assertEqual(config['collectors']['LoadAverageCollector']['enabled'], 'True')
        self.assertEqual(config['collectors']['MemoryCollector']['enabled'], 'True')
        self.assertEqual(config['collectors']['VMStatCollector']['enabled'], 'True')
        self.assertEqual(config['collectors']['NetworkCollector']['enabled'], 'True')



