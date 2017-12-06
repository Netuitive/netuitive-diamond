# coding=utf-8

"""
A Diamond collector that checks process defined in it's
config file by matching them with their executable filepath or the process name.

Example config file ProcessCheckCollector.conf

```
enabled=True
ttl=150
[process]
[[postgres]]
exe=^\/usr\/lib\/postgresql\/+d.+d\/bin\/postgres$
name=^postgres,^pg

```
exe and name are both lists of comma-separated regexps.

"""

import re

import diamond.collector
import diamond.convertor

try:
    import psutil
    psutil
except ImportError:
    psutil = None

try:
    import netuitive
except ImportError:
    netuitive = None


def match_process(pid, name, cmdline, exe, cfg):
    """
    Decides whether a process matches with a given process descriptor

    :param pid: process pid
    :param exe: process executable
    :param name: process name
    :param cmdline: process cmdline
    :param cfg: the dictionary from processes that describes with the
        process group we're testing for
    :return: True if it matches
    :rtype: bool
    """
    for exe_re in cfg['exe']:
        if exe_re.search(exe):
            return True
    for name_re in cfg['name']:
        if name_re.search(name):
            return True
    for cmdline_re in cfg['cmdline']:
        if cmdline_re.search(' '.join(cmdline)):
            return True
    return False


def get_value(process, name):
    result = getattr(process, name)
    try:
        return result()
    except TypeError:
        return result


class ProcessCheckCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(ProcessCheckCollector, self).__init__(*args, **kwargs)

        self.hostname = self.get_hostname()
        self.ttl = self.config['ttl']

        if not netuitive:
            self.log.error('netuitive import failed. ProcessCheckCollector disabled')
            self.enabled = False
            return

        try:
            self.version = self._get_version()
            self.api = netuitive.Client(self.config['url'], self.config['api_key'], self.version)
        except Exception as e:
            self.log.debug(e)


    def process_config(self):
        super(ProcessCheckCollector, self).process_config()
        """
        prepare self.processes, which is a descriptor dictionary in
        pg_name: {
            exe: [regex],
            name: [regex],
            cmdline: [regex],
            procs: [psutil.Process]
        }
        """
        self.processes = {}
        self.processes_info = {}
        for pg_name, cfg in self.config['process'].items():
            pg_cfg = {}
            for key in ('exe', 'name', 'cmdline'):
                pg_cfg[key] = cfg.get(key, [])
                if not isinstance(pg_cfg[key], list):
                    pg_cfg[key] = [pg_cfg[key]]
                pg_cfg[key] = [re.compile(e) for e in pg_cfg[key]]
            self.processes[pg_name] = pg_cfg
            self.processes_info[pg_name] = {}

    def get_default_config_help(self):
        config_help = super(ProcessCheckCollector,
                            self).get_default_config_help()
        config_help.update({
            'process': ("A subcategory of settings inside of which each "
                        "collected process has it's configuration"),
        })
        return config_help

    def get_default_config(self):
        """
        Default settings are:
            path: 'process'
        """
        config = super(ProcessCheckCollector, self).get_default_config()
        config.update({
            'path': 'process',
            'process': {},
        })
        return config

    def save_process_info(self, pg_name, process_info):
        for key, value in process_info.iteritems():
            if key in self.processes_info[pg_name]:
                self.processes_info[pg_name][key] += value
            else:
                self.processes_info[pg_name][key] = value

    def collect_process_info(self, process):
        try:
            pid = get_value(process, 'pid')
            name = get_value(process, 'name')
            cmdline = get_value(process, 'cmdline')
            try:
                exe = get_value(process, 'exe')
            except psutil.AccessDenied:
                exe = ""
            for pg_name, cfg in self.processes.items():
                if match_process(pid, name, cmdline, exe, cfg):
                    pi = {}
                    status = get_value(process, 'status')
                    self.log.debug("process %s has its status as %s", pg_name, status)
                    pi.update({'up': 1 if status not in [psutil.STATUS_DEAD, psutil.STATUS_STOPPED] else 0})
                    self.log.debug("process %s has its process info as %s", pg_name, pi)
                    self.save_process_info(pg_name, pi)
        except psutil.NoSuchProcess, e:
            self.log.info("Process exited while trying to get info: %s", e)

    def collect(self):
        """
        Check each process defined under the
        `process` subsection of the config file
        """
        if not psutil:
            self.log.error('Unable to import psutil, no process check performed')
            return None

        for process in psutil.process_iter():
            self.collect_process_info(process)

        # check results
        for pg_name, counters in self.processes_info.iteritems():
            if counters and counters['up'] > 0:
                # send check ttl for the process
                check = netuitive.Check(pg_name, self.hostname, self.ttl)
                self.api.post_check(check)

            # reinitialize process info
            self.processes_info[pg_name] = {}
