# coding=utf-8

from test import CollectorTestCase
from test import get_collector_config

from base import BaseCollector


class TestBaseCollector(CollectorTestCase):

    TEST_CONFIG = {
        'simple' : True,
    }

    def setUp(self):
        config = get_collector_config('BaseCollector', self.TEST_CONFIG)
        self.collector = BaseCollector(config, None)

    def test_import(self):
        self.assertTrue(BaseCollector)

    def test_subcollectors(self):
        self.assertTrue(hasattr(self.collector, 'cpu_collector'))
        self.assertTrue(hasattr(self.collector, 'memory_collector'))
        self.assertTrue(hasattr(self.collector, 'loadavg_collector'))
        self.assertTrue(hasattr(self.collector, 'network_collector'))
        self.assertTrue(hasattr(self.collector, 'diskusage_collector'))
        self.assertTrue(hasattr(self.collector, 'diskspace_collector'))
        self.assertTrue(hasattr(self.collector, 'vmstat_collector'))