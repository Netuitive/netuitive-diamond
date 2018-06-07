"""
Originally from https://github.com/lesaux/diamond-DockerContainerCollector
"""

try:
    import docker
except ImportError:
    docker = None
from multiprocessing.pool import ThreadPool
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
            'threadpool_size': 'Number of threads used for container metrics data collection'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(NetuitiveDockerCollector, self).get_default_config()
        config.update({
            'path':     'containers',
            'simple':   'False',
            'threadpool_size': 20
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

        def extract_dockernames(dockernames):
            container_names = []
            for dockername in dockernames:
                container_names.append(dockername[0][1:])
            return container_names

        def print_metric(name):
            data = self.client.stats(name)
            metrics = json.loads(data.next())
            if name.find("/") != -1:
                name = name.rsplit('/', 1)[1]
            # memory metrics
            memory = self.flatten_dict(metrics['memory_stats'])
            for key, value in memory.items():
                if value is not None:
                    metric = 'memory.' + key
                    if filter_metric(metric):
                        metric_name = name + "." + metric
                        self.publish_gauge(metric_name, value)
            # cpu metrics
            cpu = self.flatten_dict(metrics['cpu_stats'])
            for key, value in cpu.items():
                if value is not None:
                    # percpu_usage is a list, we'll deal with it after
                    if type(value) == int:
                        metric_name = name + ".cpu." + key
                        self.publish_counter(metric_name, value)
                    # dealing with percpu_usage
                    if type(value) == list and not str_to_bool(self.config['simple']):
                        for i in range(len(value)):
                            metric_name = name + ".cpu." + key + str(i)
                            self.publish_counter(metric_name, value[i])
            
            # network metrics
            network = None

            if 'network' in metrics:
                network = self.flatten_dict(metrics['network'])

            if 'networks' in metrics:
                network = self.flatten_dict(metrics['networks'])

            if network is not None:
                for key, value in network.items():
                    if value is not None:
                        metric_name = name + ".network." + key
                        self.publish_counter(metric_name, value)
            
            # blkio metrics
            #blkio = self.flatten_dict(metrics['blkio_stats'])
            #for key, value in blkio.items():
            #    if value is not None:
            #        metric_name = name + ".blkio." + key
            #        self.publish_counter(metric_name, value)

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

        self.log.debug("start collecting docker container metrics using a threadpool of %d workers" % (int(self.config['threadpool_size'])))
        pool = ThreadPool(processes=int(self.config['threadpool_size']))
        containers = extract_dockernames(dockernames)
        pool.map(print_metric, containers)
        pool.close()
        pool.join()

