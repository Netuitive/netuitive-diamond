"""
Originally from https://github.com/lesaux/diamond-DockerContainerCollector
"""

try:
    import docker
except ImportError:
    docker = None
import threading
import diamond.collector
try:
    import json
except ImportError:
    import simplejson as json
from diamond.collector import str_to_bool


class NetuitiveDockerCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(NetuitiveDockerCollector, self).__init__(*args, **kwargs)
        if not docker:
            self.log.error('docker import failed. NetuitiveDockerCollector disabled')
            self.enabled = False
            return
        self.client = docker.Client(
            base_url='unix://var/run/docker.sock', version='auto')

    def get_default_config_help(self):
        config_help = super(
            NetuitiveDockerCollector, self).get_default_config_help()
        config_help.update({
            'simple': 'Only collect total metrics for CPU, Memory',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(NetuitiveDockerCollector, self).get_default_config()
        config.update({
            'path':     'containers',
            'simple':   'False'
        })
        return config

    def flatten_dict(self, d):
        def items():
            for key, value in d.items():
                if isinstance(value, dict):
                    for subkey, subvalue in self.flatten_dict(value).items():
                        yield key + "." + subkey, subvalue
                else:
                    yield key, value
        return dict(items())

    def collect(self):

        def filter_metric(metric):
            if not str_to_bool(self.config['simple']):
                return True
            else:
                if metric.startswith('memory.stats.'):
                    if not metric.startswith('total', 13):
                        return False
            return True

        def print_metric(cc, name):
            data = cc.stats(name)
            metrics = json.loads(data.next())
            if name.find("/") != -1:
                name = name.rsplit('/', 1)[1]
            # memory metrics
            self.memory = self.flatten_dict(metrics['memory_stats'])
            for key, value in self.memory.items():
                if value is not None:
                    metric = 'memory.' + key
                    if filter_metric(metric):
                        metric_name = name + "." + metric
                        self.publish_gauge(metric_name, value)
            # cpu metrics
            self.cpu = self.flatten_dict(metrics['cpu_stats'])
            for key, value in self.cpu.items():
                if value is not None:
                    # percpu_usage is a list, we'll deal with it after
                    if type(value) == int:
                        metric_name = name + ".cpu." + key
                        self.publish_counter(metric_name, value)
                    # dealing with percpu_usage
                    if type(value) == list and not str_to_bool(self.config['simple']):
                        self.length = len(value)
                        for i in range(self.length):
                            self.value = value
                            self.metric_name = name + ".cpu." + key + str(i)
                            self.publish_counter(
                                self.metric_name, self.value[i])
            
            # network metrics
            self.network = None

            if 'network' in metrics:
                self.network = self.flatten_dict(metrics['network'])

            if 'networks' in metrics:
                self.network = self.flatten_dict(metrics['networks'])

            if self.network is not None:
                for key, value in self.network.items():
                    if value is not None:
                        metric_name = name + ".network." + key
                        self.publish_counter(metric_name, value)
            
            # blkio metrics
            self.blkio = self.flatten_dict(metrics['blkio_stats'])
            for key, value in self.blkio.items():
                if value is not None:
                    metric_name = name + ".blkio." + key
                    self.publish_counter(metric_name, value)

        dockernames = [i['Names'] for i in self.client.containers()]

        running_containers = len(self.client.containers())
        all_containers = len(self.client.containers(all=True))
        stopped_containers = (all_containers - running_containers)

        image_count = len(set(self.client.images(quiet=True)))
        dangling_image_count = len(
            set(self.client.images(quiet=True, all=True, filters={'dangling': True})))

        self.publish('counts.running', running_containers)
        self.publish('counts.stopped', stopped_containers)

        self.publish('counts.all_containers', all_containers)
        self.publish('counts.images', image_count)
        self.publish('counts.dangling_images', dangling_image_count)

        threads = []

        for dname in dockernames:
            t = threading.Thread(target=print_metric, args=(self.client, dname[0][1:]))
            threads.append(t)
            t.start()

        for thread in threads:
            thread.join()
