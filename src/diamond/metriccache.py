# coding=utf-8

import re
from diamond.metric import Metric


class MetricCache(object):
    """
    This serves as a metrics cache that a computed/aggregate metric can be created
    """
    def __init__(self):
        self.metrics = []

    def add(self, metric):
        self.metrics.append(metric)

    def find(self, match):
        """
        :param match: a regex that is used to search metric paths in cache
        :return: a list of metrics in the cache that matches the given 'match' regex
        """
        found = []
        metric_pattern = re.compile(match)
        for metric in self.metrics:
            matches = metric_pattern.match(metric.path)
            if matches:
                found.append(metric)
        return found

    def avg(self, match, name):
        """
        :param match:
        :param name: aggregated metric name
        :return: a new Metric with an average value of all the values of matched metrics
        """
        metrics = self.find(match)
        if len(metrics) == 0:
            return None
        avg = sum(map(lambda m: m.value, metrics)) / len(metrics);
        return Metric(name, avg)

    def sum(self, match, name):
        """
        :param match: a regex that is used to search metric paths in cache
        :param name: aggregated metric name
        :return: a new Metric with a sum value of all the values of matched metrics
        """
        metrics = self.find(match)
        if len(metrics) == 0:
            return None
        return Metric(name, sum(map(lambda m: m.value, metrics)))

    def max(self, match, name):
        """
        :param match: a regex that is used to search metric paths in cache
        :param name: aggregated metric name
        :return: a new Metric with a max value of all the values of matched metrics
        """
        metrics = self.find(match)
        if len(metrics) == 0:
            return None
        return Metric(name, max(map(lambda m: m.value, metrics)))

    def min(self, match, name):
        """
        :param match: a regex that is used to search metric paths in cache
        :param name: aggregated metric name
        :return: a new Metric with a min value of all the values of matched metrics
        """
        metrics = self.find(match)
        if len(metrics) == 0:
            return None
        return Metric(name, min(map(lambda m: m.value, metrics)))


