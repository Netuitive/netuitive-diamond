import diamond.collector
import urllib2
try:
    import json
except ImportError:
    import simplejson as json


class ConsulCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        return super(ConsulCollector, self).get_default_config_help()

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(ConsulCollector, self).get_default_config()
        config.update({
            'url': 'http://localhost:8500',
            'path': 'consul'
        })
        return config

    def collect_node_metrics(self):
        url = self.config['url']
        try:
            response = urllib2.urlopen(url + '/v1/catalog/nodes')
            nodes = json.load(response)

            self.publish('catalog.total_nodes', len(nodes))

            # Get the health for each node
            node_healths = map(self.get_node_health, nodes)
            # Filter down to nodes with health responses
            up_node_healths = [node for node in node_healths if node]
            # Calculate the max status for the checks on each node
            max_statuses = map(self.get_check_max_status, up_node_healths)

            # Increment the metrics for how many nodes are of each status
            self.publish('catalog.nodes_up', len(up_node_healths))
            self.publish('catalog.nodes_critical', len(
                [status for status in max_statuses if status == 'critical']))
            self.publish('catalog.passing', len(
                [status for status in max_statuses if status == 'passing']))
            self.publish('catalog.warning', len(
                [status for status in max_statuses if status == 'warning']))
        except Exception, err:
            self.log.error("%s: %s", url, err)
            return False

    def collect_service_metrics(self):
        url = self.config['url']
        try:
            services = list(json.load(urllib2.urlopen(
                url + '/v1/catalog/services')).keys())

            service_status = {'up': 0, 'passing': 0,
                              'warning': 0, 'critical': 0}

            for service in services:
                nodes_for_service = self.get_nodes_for_service(service)

                # Find the maximum status for the service on each node
                node_max_statuses = map(lambda node: self.get_check_max_status(
                    node['Checks']), nodes_for_service)

                # Calculate the max status of the service across nodes
                service_max_status = self.get_max_status(node_max_statuses)

                # Increment the count for the service being up and for it's status
                service_status['up'] += 1
                service_status[service_max_status] += 1

            for key in service_status.keys():
                self.publish('service.services_' + key, service_status[key])
        except Exception, err:
            self.log.error("%s: %s", url, err)
            return False


    # Get the health details of a given node
    def get_node_health(self, node):
        url = self.config['url']
        return json.load(urllib2.urlopen(url + '/v1/health/node/' + node['Node']))

    # Get the health details of a each node a service lives on for a given service
    def get_nodes_for_service(self, service):
        url = self.config['url']
        return json.load(urllib2.urlopen(url + '/v1/health/service/' + service))

    # Get the maximum status for a list of checks
    def get_check_max_status(self, checks):
        statuses = map(lambda check: check['Status'], checks)
        return self.get_max_status(statuses)

    # Return the highest status from a list of services
    def get_max_status(self, statuses):
        if 'critical' in statuses:
            return 'critical'
        if 'warning' in statuses:
            return 'warning'
        return 'passing'

    def collect(self):
        self.collect_node_metrics()
        self.collect_service_metrics()
