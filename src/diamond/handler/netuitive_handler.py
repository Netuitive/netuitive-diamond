"""
handler to flush stats to [Netuitive](http://www.netuitive.com)
# Dependencies

# Configuration
Enable handler

  * handlers = diamond.handler.netuitive_cloud.NetuitiveHandler

  * url = https://api.app.netuitive.com/ingest/infrastructure
  * api_key = NETUITIVE_API_KEY
  * tags = tag1:tag1val, tag2:tag2val

"""

from Handler import Handler
import logging
import re
import platform
import datetime
import time
import os
import json
import urllib2
from diamond.util import get_diamond_version
from diamond.utils.config import str_to_bool
from diamond.utils.config import load_config as load_server_config

try:
    import psutil
except ImportError:
    psutil = None

try:
    import netuitive
except ImportError:
    netuitive = None

try:
    import docker
except ImportError:
    docker = None


def get_human_readable_size(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return(("%3.2f %s%s" % (num, unit, suffix)).strip())
        num /= 1024.0
    return(("%.2f %s%s" % (num, 'Y', suffix)).strip())


def get_azure_uuid(starting_uuid):

    ret = None

    try:
        a = starting_uuid.lower().split('-')
        b = str(a[0][6:8]) + str(a[0][4:6]) + str(a[0][2:4]) + str(a[0][0:2])
        c = str(a[1][2:4]) + str(a[1][0:2])
        d = str(a[2][2:4]) + str(a[2][0:2])
        e = str(a[3])
        f = str(a[4])
        ret = '{0}-{1}-{2}-{3}-{4}'.format(b, c, d, e, f).rstrip()

    except Exception as e:
        logging.debug(e)

    return(ret)


def check_lsb():

    if os.path.isfile("/etc/lsb-release"):
        try:
            _distributor_id_file_re = re.compile(
                "(?:DISTRIB_ID\s*=)\s*(.*)", re.I)
            _release_file_re = re.compile(
                "(?:DISTRIB_RELEASE\s*=)\s*(.*)", re.I)
            _codename_file_re = re.compile(
                "(?:DISTRIB_CODENAME\s*=)\s*(.*)", re.I)
            with open("/etc/lsb-release", "rU") as etclsbrel:
                for line in etclsbrel:
                    m = _distributor_id_file_re.search(line)
                    if m:
                        _u_distname = m.group(1).strip()
                    m = _release_file_re.search(line)
                    if m:
                        _u_version = m.group(1).strip()
                    m = _codename_file_re.search(line)
                    if m:
                        _u_id = m.group(1).strip()
                if _u_distname and _u_version:
                    return (_u_distname, _u_version, _u_id)
        except Exception as e:
            logging.debug(e)
            return(None)

    else:
        return(None)


class NetuitiveHandler(Handler):

    def __init__(self, config=None):
        """
        initialize Netuitive api and populate agent host metadata
        """

        if not netuitive:
            self.log.error('netuitive import failed. Handler disabled')
            self.enabled = False
            return

        try:
            Handler.__init__(self, config)

            logging.debug("initialize Netuitive handler")

            self.version = self._get_version()
            self.api = netuitive.Client(self.config['url'], self.config[
                                        'api_key'], self.version)

            self.element = netuitive.Element(
                location=self.config.get('location'))

            self.batch_size = int(self.config['batch'])

            self.max_backlog_multiplier = int(
                self.config['max_backlog_multiplier'])

            self.trim_backlog_multiplier = int(
                self.config['trim_backlog_multiplier'])

            self._add_sys_meta()
            self._add_docker_meta()
            self._add_azure_meta()
            self._add_config_tags()
            self._add_config_relations()
            self._add_collectors()

            self.flush_time = 0
            self.aws_meta_state = 0

            try:
                self.config['write_metric_fqns'] = str_to_bool(self.config['write_metric_fqns'])

            except KeyError, e:
                self.log.warning('write_metric_fqns missing from the config')
                self.config['write_metric_fqns'] = False

            if self.config['write_metric_fqns']:
                self.metric_fqns_path = self.config['metric_fqns_path']
                truncate_fqn_file = open(self.metric_fqns_path, "w")
                truncate_fqn_file.close()

            logging.debug(self.config)

        except Exception as e:
            logging.exception('NetuitiveHandler: init - %s', str(e))

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(NetuitiveHandler, self).get_default_config_help()

        config.update({
            'url': 'NetuitiveCloud url to send data to',
            'api_key': 'Datasource api key',
            'tags': 'Netuitive Tags',
            'relations': 'Netuitive child Element',
            'location': 'the location of this Element',
            'batch': 'How many to store before sending to the graphite server',
            'max_backlog_multiplier': 'how many batches to store before trimming',
            'trim_backlog_multiplier': 'Trim down how many batches',
        })
        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """

        config = super(NetuitiveHandler, self).get_default_config()

        config.update({
            'url': 'https://api.app.netuitive.com/ingest/infrastructure',
            'api_key': 'apikey',
            'tags': None,
            'relations': None,
            'location': None,
            'batch': 100,
            'max_backlog_multiplier': 5,
            'trim_backlog_multiplier': 4,
        })
        return config

    def __del__(self):
        pass

    def _get_version(self):
        """
        Return version string
        """

        ret = 'Diamond_' + get_diamond_version().rstrip()
        if os.path.isfile('/opt/netuitive-agent/version-manifest.txt'):
            with open('/opt/netuitive-agent/version-manifest.txt', 'r') as f:
                v = f.readline()

            f.close()

            ret = v.replace(' ', '_').lower().rstrip()

        return(ret)

    def _add_sys_meta(self):
        try:

            self.element.add_attribute('platform', platform.system())
            self.element.add_attribute('agent', self.version)

            if psutil:
                self.element.add_attribute('cpus', psutil.cpu_count())
                mem = psutil.virtual_memory()
                self.element.add_attribute(
                    'ram', get_human_readable_size(mem.total))
                self.element.add_attribute('ram bytes', mem.total)
                self.element.add_attribute(
                    'boottime', str(datetime.datetime.fromtimestamp(psutil.boot_time())))

            if platform.system().startswith('Linux'):
                if check_lsb() is None:
                    supported_dists = platform._supported_dists + ('system',)
                    dist = platform.linux_distribution(
                        supported_dists=supported_dists)

                else:
                    dist = check_lsb()

                self.element.add_attribute('distribution_name', str(dist[0]))
                self.element.add_attribute(
                    'distribution_version', str(dist[1]))
                if dist[2] != '':
                    self.element.add_attribute('distribution_id', str(dist[2]))

        except Exception as e:
            logging.debug(e)
            pass

    def _add_docker_meta(self):
        if docker:
            try:

                cc = docker.Client(
                    base_url='unix://var/run/docker.sock', version='auto')
                dockerver = cc.version()

                for k, v in dockerver.items():
                    logging.debug('docker_' + k + ' = ' + v)

                    if type(v) is list:
                        vl = ', '.join(v)
                        v = vl
                    self.element.add_attribute('docker_' + k, v)

            except Exception as e:
                logging.debug(e)
                pass

    def _add_aws_meta(self):
        url = 'http://169.254.169.254/latest/dynamic/instance-identity/document'

        if self.aws_meta_state < 1:
        
            try:
                request = urllib2.Request(url)
                resp = urllib2.urlopen(request, timeout=1).read()
                j = json.loads(resp)

                if j:
                    for k, v in j.items():
                        if type(v) is list:
                            vl = ', '.join(v)
                            v = vl
                            
                        self.element.add_attribute(k, v)

                        if k.lower() == 'instanceid':
                            instanceid = v

                        if k.lower() == 'region':
                            region = v

                        if k.lower() == 'accountid':
                            accountid = v
                    self.aws_meta_state = 1

                    try:
                        # old fqn format
                        child = '{0}:{1}'.format(region, instanceid)
                        self.element.add_relation(child)

                        # new fqn format
                        child = '{0}:EC2:{1}:{2}'.format(accountid, region, instanceid)
                        self.element.add_relation(child)


                    except Exception as e:
                        pass

            except Exception as e:
                logging.debug('Couldnt get AWS metadata')
                pass

    def _add_azure_meta(self):
        url = 'http://169.254.169.254/metadata/v1/InstanceInfo'
        uuid_file = '/sys/devices/virtual/dmi/id/product_uuid'

        try:

            if os.path.isfile(uuid_file) and os.access(uuid_file, os.R_OK):

                request = urllib2.Request(url)
                resp = urllib2.urlopen(request, timeout=1).read()
                j = json.loads(resp)

                with open(uuid_file) as fp:
                    for line in fp:
                        my_uuid = get_azure_uuid(line)
                        if my_uuid is not None:
                            break

                try:
                    child = 'VirtualMachine:{0}'.format(my_uuid)
                    self.element.add_relation(child)

                except Exception as e:
                    pass

        except Exception as e:
            logging.debug(e)
            pass

    def _add_config_tags(self):
        tags = self.config.get('tags')

        if tags is not None:
            if type(tags) is list:
                for k, v in dict(tag.split(":") for tag in tags).iteritems():
                    self.element.add_tag(k.strip(), v.strip())

            if type(tags) is str:
                self.element.add_tag(tags.split(":")[0], tags.split(":")[1])

    def _add_config_relations(self):
        relations = self.config.get('relations')

        if relations is not None:
            if type(relations) is list:
                for r in relations:
                    self.element.add_relation(r.strip())

            if type(relations) is str:
                self.element.add_relation(relations.strip())

    def _add_collectors(self):
        try:

            cf = "/opt/netuitive-agent/conf/netuitive-agent.conf"

            enabled_collectors = []

            if os.path.isfile(cf):
                serverconf = load_server_config(cf)

            c = serverconf['collectors']

            for k, v in c.iteritems():
                if v.get('enabled', False):
                    logging.debug(k + ' is enabled')
                    enabled_collectors.append(k.replace('Collector', ''))

            enabled_collectors.sort()
            collectors = ', '.join(enabled_collectors)

            self.element.add_tag('n.collectors', collectors)

            self.element.add_tag('variant', 'SIMPLE' if c.get('SimpleCollector') and str_to_bool(c.get('SimpleCollector').get('enabled')) else 'FULL')

        except Exception as e:
            logging.error(e)
            pass

    def write_metric_fqns(self):
        with open(self.metric_fqns_path,'a+') as metric_fqn_file:
            master_fqn_list = [line.strip() for line in metric_fqn_file.readlines()]
            candidate_fqn_list = [metric.id for metric in [metric for metric in self.element.metrics]]
            new_metric_fqns = list(set(candidate_fqn_list).difference(set(master_fqn_list)))

            # Only write if there are new metric fqns.
            if len(new_metric_fqns) > 0:
                for fqn in new_metric_fqns:
                    metric_fqn_file.write("{}\n".format(fqn))
        metric_fqn_file.closed

    def process(self, metric):
        metricId = metric.getCollectorPath() + '.' + metric.getMetricPath()

        self.element.add_sample(
            metricId, metric.timestamp, metric.value, metric.metric_type, host=metric.host)

        logging.debug(
            'length of self.element.samples: ' + str(len(self.element.samples)))

        if len(self.element.samples) >= self.batch_size:
            logging.debug('flushing data due to exceeding batch_size')
            self.flush()

    def flush(self):
        logging.debug('sending data')

        if self.element.id is None:
            logging.warn('element id not set. nothing to post.')
            return

        try:

            # Don't let too many metrics back up
            if len(self.element.metrics) >= (
                    self.batch_size * self.max_backlog_multiplier):
                trim_offset = (self.batch_size *
                               self.trim_backlog_multiplier * -1)
                logging.warn('NetuitiveHandler: Trimming backlog. Removing' +
                             ' oldest %d and keeping newest %d metrics',
                             len(self.element.metrics) - abs(trim_offset),
                             abs(trim_offset))
                self.element.metrics = self.element.metrics[trim_offset:]

            self._add_aws_meta()

            self.api.post(self.element)
            if self.config['write_metric_fqns']:
                self.write_metric_fqns()

            self.element.clear_samples()

            elapsed = int(time.time()) - self.flush_time
            if elapsed > 900 or self.flush_time == 0:

                toff = self.api.check_time_offset()
                if toff not in range(-300, 300):
                    logging.error('local time is {0} seconds out '
                                  'of sync with the server'.format(toff))

                self.flush_time = int(time.time())

                if self.api.disabled is False:
                    logging.info('NetuitiveHandler: Data posted successfully. ' +
                                 'Next log message in 15 minutes.')

        except urllib2.HTTPError as e:
            if e.code in self.api.kill_codes:
                logging.exception('NetuitiveHandler: flush - %s', str(e))

        except Exception as e:
            logging.exception('NetuitiveHandler: flush - %s', str(e))
