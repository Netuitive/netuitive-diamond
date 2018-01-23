from test import CollectorTestCase
from test import get_collector_config
from test import run_only

from mock import call, Mock, patch
from unittest import TestCase

from portcheck import get_port_stats, PortCheckCollector

def run_only_if_netuitive_is_available(func):
    try:
        import netuitive
    except ImportError:
        netuitive = None
    return run_only(func, lambda: netuitive is not None)


class PortCheckCollectorTestCase(CollectorTestCase):

    TEST_CONFIG = {
        'hostname' : 'localhost',
        'netuitive_url': 'http://localhost',
        'netuitive_api_key': "1234567890",
        'ttl': 120,
        'port': {
            'something1': {
                'number': 5222,
            },
            'something2': {
                'number': 8888,
            }
        }
    }

    def setUp(self):
        config = get_collector_config('PortCheckCollector',
                                      self.TEST_CONFIG)

        self.collector = PortCheckCollector(config, None)

    def test_import(self):
        self.assertTrue(PortCheckCollector)

    @run_only_if_netuitive_is_available
    @patch('netuitive.Check')
    @patch('netuitive.Client.post_check')
    @patch('portcheck.get_port_stats')
    def test_collect(self, get_port_stats_mock, netuitive_client_post_check_mock, netuitive_check_mock):

        get_port_stats_mock.return_value = {'listen': 1}

        self.collector.collect()
        get_port_stats_mock.assert_has_calls([call(5222), call(8888)],
                                             any_order=True)
        netuitive_check_mock.assert_has_calls([call('something1.5222', 'localhost', 120), call('something2.8888', 'localhost', 120)],
                                              any_order=True)

class GetPortChecksTestCase(TestCase):

    @patch('portcheck.psutil.net_connections')
    def test_get_port_stats(self, net_connections_mock):

        ports = [Mock() for _ in range(5)]

        ports[0].laddr = (None, 5222)
        ports[0].status = 'ok'
        ports[1].laddr = ports[2].laddr = ports[3].laddr = (None, 8888)
        ports[1].status = 'ok'
        ports[2].status = 'OK'
        ports[3].status = 'bad'
        ports[4].laddr = (None, 9999)

        net_connections_mock.return_value = ports

        cnts = get_port_stats(5222)

        self.assertEqual(net_connections_mock.call_count, 1)
        self.assertEqual(cnts, {'ok': 1})

        cnts = get_port_stats(8888)

        self.assertEqual(net_connections_mock.call_count, 2)
        self.assertEqual(cnts, {'ok': 2, 'bad': 1})
