#!/usr/bin/python
# coding=utf-8
##########################################################################

import json

from mock import patch

from diamond.collector import Collector
from netuitivedocker import NetuitiveDockerCollector
from test import CollectorTestCase
from test import get_collector_config
from test import unittest


##########################################################################

class TestNetuitiveDockerCollector(CollectorTestCase):

    def setUp(self):
        config = get_collector_config('NetuitiveDockerCollector', {
            'interval': 10
        })
        self.collector = NetuitiveDockerCollector(config, None)

    @patch.object(Collector, 'publish')
    @patch('docker.Client.stats')
    @patch('docker.Client.images')
    @patch('docker.Client.containers')
    def test_docker_container_stats_processing(self, client_containers_mock, client_images_mock, client_container_stats_mock, collector_publish_mock):
        client_containers_mock.return_value = json.load(self.getFixture('docker_containers'))
        client_images_mock.return_value = json.load(self.getFixture('docker_images'))
        client_container_stats_mock.return_value.next.return_value = json.dumps(json.load(self.getFixture('docker_container_stats')))

        self.collector.collect()

        metrics = {
            'counts.running': 1,
            'counts.stopped': 0,
            'counts.all_containers': 1,
            'counts.images': 0,
            'counts.dangling_images': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.failcnt': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.limit': 1073741824,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.max_usage': 343592960,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.usage': 341712896,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.active_anon': 152625152,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.active_file': 10948608,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.cache': 46673920,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.dirty': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.hierarchical_memory_limit': 1073741824,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.hierarchical_memsw_limit': 2147483648,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.inactive_anon': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.inactive_file': 35725312,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.mapped_file': 5922816,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.pgfault': 176602,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.pgmajfault': 154,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.pgpgin': 164612,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.pgpgout': 119021,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.rss': 152625152,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.rss_huge': 12582912,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.swap': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_active_anon': 152625152,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_active_file': 10948608,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_cache': 46673920,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_dirty': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_inactive_anon': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_inactive_file': 35725312,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_mapped_file': 5922816,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_pgfault': 176602,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_pgmajfault': 154,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_pgpgin': 164612,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_pgpgout': 119021,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_rss': 152625152,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_rss_huge': 12582912,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_swap': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_unevictable': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.total_writeback': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.unevictable': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.writeback': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage0': 1026097122891,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage1': 1361517474952,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage2': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage3': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage4': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage5': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage6': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage7': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage8': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage9': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage10': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage11': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage12': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage13': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage14': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.total_usage': 2387614597843,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.usage_in_kernelmode': 265790000000,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.usage_in_usermode': 1889720000000,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.system_cpu_usage': 2340427730000000,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.throttling_data.periods': 261043,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.throttling_data.throttled_periods': 19516,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.throttling_data.throttled_time': 1104996575045,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth0.rx_bytes': 1570713649,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth0.rx_packets': 15317254,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth0.rx_errors': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth0.rx_dropped': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth0.tx_bytes': 27458,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth0.tx_packets': 627,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth0.tx_errors': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth0.tx_dropped': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth1.rx_bytes': 144561531,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth1.rx_packets': 1133379,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth1.rx_errors': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth1.rx_dropped': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth1.tx_bytes': 479507552,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth1.tx_packets': 1112684,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth1.tx_errors': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth1.tx_dropped': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth2.rx_bytes': 16470,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth2.rx_packets': 229,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth2.rx_errors': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth2.rx_dropped': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth2.tx_bytes': 2608,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth2.tx_packets': 36,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth2.tx_errors': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.network.eth2.tx_dropped': 0
        }

        self.assertPublishedMany(collector_publish_mock, metrics)

    @patch.object(Collector, 'publish')
    @patch('docker.Client.stats')
    @patch('docker.Client.images')
    @patch('docker.Client.containers')
    def test_docker_container_stats_processing_simple(self, client_containers_mock, client_images_mock, client_container_stats_mock, collector_publish_mock):
        client_containers_mock.return_value = json.load(self.getFixture('docker_containers'))
        client_images_mock.return_value = json.load(self.getFixture('docker_images'))
        client_container_stats_mock.return_value.next.return_value = json.dumps(json.load(self.getFixture('docker_container_stats')))

        config = get_collector_config('NetuitiveDockerCollector', {
            'interval': 10,
            'simple': True
        })

        collector = NetuitiveDockerCollector(config, None)
        collector.collect()

        skipped_metrics = {
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.active_anon': 152625152,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.active_file': 10948608,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.cache': 46673920,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.dirty': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.hierarchical_memory_limit': 1073741824,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.hierarchical_memsw_limit': 2147483648,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.inactive_anon': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.inactive_file': 35725312,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.mapped_file': 5922816,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.pgfault': 176602,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.pgmajfault': 154,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.pgpgin': 164612,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.pgpgout': 119021,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.rss': 152625152,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.rss_huge': 12582912,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.swap': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.unevictable': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.memory.stats.writeback': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage0': 1026097122891,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage1': 1361517474952,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage2': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage3': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage4': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage5': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage6': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage7': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage8': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage9': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage10': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage11': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage12': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage13': 0,
            'acm.1.88j768jw74704kgc51jd96ajg.cpu.cpu_usage.percpu_usage14': 0,
        }
        self.assertUnpublishedMany(collector_publish_mock, skipped_metrics)


##########################################################################
if __name__ == "__main__":
    unittest.main()