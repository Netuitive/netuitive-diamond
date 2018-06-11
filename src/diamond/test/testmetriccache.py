#!/usr/bin/python
# coding=utf-8
##########################################################################

from test import unittest

from diamond.metric import Metric
from diamond.metriccache import MetricCache


class TestMetricCache(unittest.TestCase):

    def testFindMetric(self):
        cache = MetricCache()
        cache.add(Metric('network.eth0.rx_byte', 1000))
        cache.add(Metric('network.eth0.tx_byte', 0))

        metrics = cache.find('network.*.rx_byte')

        self.assertEqual(1, len(metrics), 'there should be two matching metrics')
        self.assertEqual('network.eth0.rx_byte', metrics[0].path)
        self.assertEqual(1000, metrics[0].value)

    def testAvg(self):
        cache = MetricCache()
        cache.add(Metric('network.eth0.rx_byte', 0))
        cache.add(Metric('network.eth1.rx_byte', 1000))

        metric = cache.avg('network.*.rx_byte', 'network.rx_byte.avg')

        self.assertEqual('network.rx_byte.avg', metric.path)
        self.assertEqual(500, metric.value)

    def testSum(self):
        cache = MetricCache()
        cache.add(Metric('network.eth0.rx_byte', 500))
        cache.add(Metric('network.eth1.rx_byte', 1000))

        metric = cache.sum('network.*.rx_byte', 'network.rx_byte.total')

        self.assertEqual('network.rx_byte.total', metric.path)
        self.assertEqual(1500, metric.value)

    def testMax(self):
        cache = MetricCache()
        cache.add(Metric('network.eth0.rx_byte', 500))
        cache.add(Metric('network.eth1.rx_byte', 1000))

        metric = cache.max('network.*.rx_byte', 'network.rx_byte.max')

        self.assertEqual('network.rx_byte.max', metric.path)
        self.assertEqual(1000, metric.value)

    def testMin(self):
        cache = MetricCache()
        cache.add(Metric('network.eth0.rx_byte', 500))
        cache.add(Metric('network.eth1.rx_byte', 1000))

        metric = cache.min('network.*.rx_byte', 'network.rx_byte.min')

        self.assertEqual('network.rx_byte.min', metric.path)
        self.assertEqual(500, metric.value)