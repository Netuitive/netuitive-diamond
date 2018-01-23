#!/usr/bin/python
# coding=utf-8
##########################################################################

import os
from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from test import run_only
from mock import call, patch

from processcheck import ProcessCheckCollector

##########################################################################


def run_only_if_psutil_is_available(func):
    try:
        import psutil
    except ImportError:
        psutil = None
    return run_only(func, lambda: psutil is not None)


def run_only_if_netuitive_is_available(func):
    try:
        import netuitive
    except ImportError:
        netuitive = None
    return run_only(func, lambda: netuitive is not None)


class TestProcessCheckCollector(CollectorTestCase):
    TEST_CONFIG = {
        'hostname' : 'localhost',
        'netuitive_url': 'http://localhost',
        'netuitive_api_key': "1234567890",
        'ttl': 150,
        'process': {
            'foo': {
                'name': '^foo$',
            },
            'bar': {
                'exe': '^\/usr\/bin\/bar',
            },
            'noprocess': {
                'name': 'noproc',
            }
        }
    }

    def setUp(self):
        config = get_collector_config('ProcessCheckCollector',
                                      self.TEST_CONFIG)

        self.collector = ProcessCheckCollector(config, None)

    def test_import(self):
        self.assertTrue(ProcessCheckCollector)

    @run_only_if_netuitive_is_available
    @run_only_if_psutil_is_available
    @patch('netuitive.Check')
    @patch('netuitive.Client.post_check')
    @patch.object(os, 'getpid')
    def test(self, getpid_mock, netuitive_client_post_check_mock, netuitive_check_mock):
        process_info_list = [
            {
                'exe': '/usr/bin/foo',
                'name': 'foo',
                'pid': 9998,
                'status': 'running'
            },
            {
                'exe': '/usr/bin/bar',
                'name': 'bar',
                'pid': 9996,
                'status': 'sleeping'
            }
        ]

        class ProcessMock:

            def __init__(self, pid, name, exe, status=None):
                self.pid = pid
                self.name = name
                self.exe = exe
                if status is not None:
                    self.status = status

                self.cmdline = [self.exe]
                self.create_time = 0

            def as_dict(self, attrs=None, ad_value=None):
                from collections import namedtuple
                user = namedtuple('user', 'real effective saved')
                group = namedtuple('group', 'real effective saved')
                return {
                    'status': self.status,
                    'pid': self.pid,
                    'cmdline': [self.exe],
                    'create_time': 0,
                    'ppid': 0,
                    'username': 'root',
                    'name': self.name,
                    'exe': self.exe,
                    'uids': user(real=0, effective=0, saved=0),
                    'gids': group(real=0, effective=0, saved=0)}

        process_iter_mock = (ProcessMock(
            pid=x['pid'],
            name=x['name'],
            exe=x['exe'],
            status=x['status'])
                             for x in process_info_list)

        patch_psutil_process_iter = patch('psutil.process_iter',
                                          return_value=process_iter_mock)
        patch_psutil_process_iter.start()
        self.collector.collect()
        netuitive_check_mock.assert_has_calls([call('foo', 'localhost', 150), call('bar', 'localhost', 150)],
                                              any_order=True)
        patch_psutil_process_iter.stop()

##########################################################################
if __name__ == "__main__":
    unittest.main()
