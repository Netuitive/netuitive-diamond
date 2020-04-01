# coding=utf-8

"""
Collect metrics from Puppet DB Dashboard

# Dependencies

 * urllib2
 * json
 * datetime

"""

import urllib2
import diamond.collector
from diamond.convertor import time as time_convertor
from datetime import datetime

try:
    import json
except ImportError:
    import simplejson as json


class PuppetDBCollector(diamond.collector.Collector):

    PATHS = {
        'memory':
            "metrics/v1/mbeans/java.lang:type=Memory",
        'queue.AwaitingRetry':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.awaiting-retry",
        'queue.CommandParseTime':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.command-parse-time",
        'queue.Depth':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.depth",
        'queue.Discarded':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.discarded",
        'queue.Fatal':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.fatal",
        'queue.Invalidated':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.invalidated",
        'queue.MessagePersistenceTime':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.message-persistence-time",
        'queue.Processed':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.processed",
        'queue.ProcessingTime':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.processing-time",
        'queue.QueueTime':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.queue-time",
        'queue.Retried':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.retried",
        'queue.RetryCounts':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.retry-counts",
        'queue.Seen':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.seen",
        'queue.Size':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.size",
        'processing-time':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.processing-time",
        'processed':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.processed",
        'retried':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.retried",
        'discarded':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.discarded",
        'fatal':
            "metrics/v1/mbeans/puppetlabs.puppetdb.mq:" +
            "name=global.fatal",
        'commands.service-time':
            "metrics/v1/mbeans/puppetlabs.puppetdb." +
            "http:name=/pdb/cmd/v1.service-time",
        'resources.service-time':
            "metrics/v1/mbeans/puppetlabs.puppetdb." +
            "http:name=/pdb/query/v4/resources.service-time",
        'gc-time':
            "metrics/v1/mbeans/puppetlabs.puppetdb.storage:" +
            "name=gc-time",
        'duplicate-pct':
            "metrics/v1/mbeans/puppetlabs.puppetdb.storage:" +
            "name=duplicate-pct",
        'pct-resource-dupes':
            "metrics/v1/mbeans/puppetlabs.puppetdb." +
            "population:name=pct-resource-dupes",
        'num-nodes':
            "metrics/v1/mbeans/puppetlabs.puppetdb." +
            "population:name=num-nodes",
        'num-resources':
            "metrics/v1/mbeans/puppetlabs.puppetdb." +
            "population:name=num-resources",
        'avg-resources-per-node':
            "metrics/v1/mbeans/puppetlabs.puppetdb." +
            "population:name=avg-resources-per-node",
    }

    def get_default_config_help(self):
        config_help = super(PuppetDBCollector,
                            self).get_default_config_help()
        config_help.update({
            'host': 'Hostname to collect from',
            'port': 'Port number to collect from',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(PuppetDBCollector, self).get_default_config()
        config.update({
            'host': 'localhost',
            'port': 8080,
            'path': 'PuppetDB',
        })
        return config

    def count_nodes(self):
        now = datetime.utcnow()
        try:
            url = "http://%s:%s/%s" % (
                self.config['host'], int(self.config['port']), "pdb/query/v4/nodes")
            response = urllib2.urlopen(url)
        except Exception, e:
            self.log.error('Couldn\'t connect to puppetdb: %s -> %s', url, e)
            return {}
        nodes = json.load(response)

        try:
            url = "http://%s:%s/%s" % (
                self.config['host'], int(self.config['port']), "pdb/query/v4/events?query=%5B%22%3D%22%2C%22latest_report%3F%22%2Ctrue%5D")
            response = urllib2.urlopen(url)
        except Exception, e:
            self.log.error('Couldn\'t connect to puppetdb: %s -> %s', url, e)
            return {}
        event_counts = json.load(response)

        stats = {
            'failures': 0,
            'skips': 0,
            'successes': 0,
            'noops': 0
        }

        for node in nodes:
            status = [event['status'] for event in event_counts
                      if event['certname'] == node['certname']]
            if 'failure' in status:
                stats['failures'] += status.count('failure')
            if 'skipped' in status:
                stats['skips'] += status.count('skipped')
            if 'success' in status:
                stats['successes'] += status.count('success')
            if 'noop' in status:
                stats['noops'] += status.count('noop')

        return stats

    def fetch_metrics(self, url):
        try:
            url = "http://%s:%s/%s" % (
                self.config['host'], int(self.config['port']), url)
            response = urllib2.urlopen(url)
        except Exception, e:
            self.log.error('Couldn\'t connect to puppetdb: %s -> %s', url, e)
            return {}
        return json.load(response)

    def collect(self):
        nodestats = self.count_nodes()
        rawmetrics = {}
        for subnode in self.PATHS:
            path = self.PATHS[subnode]
            rawmetrics[subnode] = self.fetch_metrics(path)

        self.publish_gauge('num_resources',
                           rawmetrics['num-resources']['Value'])
        self.publish_gauge('avg_resources_per_node',
                           rawmetrics['avg-resources-per-node']['Value'])
        self.publish_gauge('catalog_duplicate_pct',
                           rawmetrics['duplicate-pct']['Value'])
        self.publish_gauge(
            'resources_service_time',
            time_convertor.convert(
                rawmetrics['resources.service-time']['50thPercentile'],
                rawmetrics['resources.service-time']['DurationUnit'],
                'seconds'))
        self.publish_gauge(
            'enqueueing_service_time',
            time_convertor.convert(
                rawmetrics['commands.service-time']['50thPercentile'],
                rawmetrics['commands.service-time']['DurationUnit'],
                'seconds'))

        self.publish_gauge('processed', rawmetrics['processed']['Count'])
        self.publish_gauge(
            'DB_Compaction',
            time_convertor.convert(
                rawmetrics['gc-time']['50thPercentile'],
                rawmetrics['gc-time']['DurationUnit'],
                'seconds'))
        self.publish_gauge('resource_duplicate_pct',
                           rawmetrics['pct-resource-dupes']['Value'])
        self.publish_gauge('num_nodes',
                           rawmetrics['num-nodes']['Value'])

        self.publish_gauge('queue.AwaitingRetry',
                           rawmetrics['queue.AwaitingRetry']['Count'])
        self.publish_gauge(
            'queue.CommandParseTime',
            time_convertor.convert(
                rawmetrics['queue.CommandParseTime']['50thPercentile'],
                rawmetrics['queue.CommandParseTime']['DurationUnit'],
                'seconds'))
        self.publish_gauge('queue.Depth',
                           rawmetrics['queue.Depth']['Count'])
        self.publish_counter('queue.Discarded',
                             rawmetrics['queue.Discarded']['Count'])
        self.publish_counter('queue.Fatal',
                             rawmetrics['queue.Fatal']['Count'])
        self.publish_counter('queue.Invalidated',
                             rawmetrics['queue.Invalidated']['Count'])
        self.publish_gauge(
            'queue.MessagePersistenceTime',
            time_convertor.convert(
                rawmetrics['queue.MessagePersistenceTime']['50thPercentile'],
                rawmetrics['queue.MessagePersistenceTime']['DurationUnit'],
                'seconds'))
        self.publish_counter('queue.Processed',
                             rawmetrics['queue.Processed']['Count'])
        self.publish_gauge(
            'queue.ProcessingTime',
            time_convertor.convert(
                rawmetrics['queue.ProcessingTime']['50thPercentile'],
                rawmetrics['queue.ProcessingTime']['DurationUnit'],
                'seconds'))
        self.publish_gauge('queue.QueueTime',
                           rawmetrics['queue.QueueTime']['50thPercentile'])
        self.publish_counter('queue.Retried',
                             rawmetrics['queue.Retried']['Count'])
        self.publish_gauge('queue.RetryCounts',
                           rawmetrics['queue.RetryCounts']['50thPercentile'])
        self.publish_counter('queue.Seen',
                             rawmetrics['queue.Seen']['Count'])
        self.publish_gauge('queue.Size',
                           rawmetrics['queue.Size']['50thPercentile'])

        self.publish_gauge('memory.NonHeapMemoryUsage.used',
                           rawmetrics['memory']['NonHeapMemoryUsage']['used'])
        self.publish_gauge(
            'memory.NonHeapMemoryUsage.committed',
            rawmetrics['memory']['NonHeapMemoryUsage']['committed'])
        self.publish_gauge('memory.HeapMemoryUsage.used',
                           rawmetrics['memory']['HeapMemoryUsage']['used'])
        self.publish_gauge('memory.HeapMemoryUsage.committed',
                           rawmetrics['memory']['HeapMemoryUsage']['committed'])
        self.publish_gauge('nodes.status.unchanged', nodestats['unchanged'])
        self.publish_gauge('nodes.status.changed', nodestats['changed'])
        self.publish_gauge('nodes.status.failed', nodestats['failed'])
        self.publish_gauge('nodes.status.unreported', nodestats['unreported'])
        self.publish_gauge('nodes.status.noops', nodestats['noops'])
