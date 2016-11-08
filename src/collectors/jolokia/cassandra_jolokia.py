# coding=utf-8

"""
Collects Cassandra JMX metrics from the Jolokia Agent.  Extends the
JolokiaCollector to interpret Histogram beans with information about the
distribution of request latencies.

#### Example Configuration
CassandraJolokiaCollector uses a regular expression to determine which
attributes represent histograms. This regex can be overridden by providing a
`histogram_regex` in your configuration.  You can also override `percentiles` to
collect specific percentiles from the histogram statistics.  The format is shown
below with the default values.

CassandraJolokiaCollector.conf

```
    percentiles '50,95,99'
    histogram_regex '.*HistogramMicros$'
```

Netuitive Change History
    2016/09/19 DVG - Override the clean_up() function to clean metric names in a
                     Cassandra-specific way. See comments inline for more detail.
    2016/09/29 DVG - Added support for java.* MBeans as well as the older Cassandra
                     MBeans.
    2016/11/08 DVG - Bug fix: Call to super-class clean_up() method coded incorrectly.
                     Bug fix: Don't assume that the value for the "type" key has a dot.
                     Improved error handling such that messages will be very detailed,
                     and any unhandled exceptions will not kill processing.

"""

from jolokia import JolokiaCollector
import math
import string
import re

class CassandraJolokiaCollector(JolokiaCollector):
    # override to allow setting which percentiles will be collected

    def get_default_config_help(self):
        config_help = super(CassandraJolokiaCollector,
                            self).get_default_config_help()
        config_help.update({
            'percentiles':
            'Comma separated list of percentiles to be collected '
            '(e.g., "50,95,99").',
            'histogram_regex':
            'Filter to only process attributes that match this regex'
        })
        return config_help

    # override to allow setting which percentiles will be collected
    def get_default_config(self):
        config = super(CassandraJolokiaCollector, self).get_default_config()
        config.update({
            'percentiles': ['50', '95', '99'],
            'histogram_regex': '.*HistogramMicros$'
        })
        return config

    def __init__(self, *args, **kwargs):
        super(CassandraJolokiaCollector, self).__init__(*args, **kwargs)
        self.offsets = self.create_offsets(91)
        self.update_config(self.config)

    def update_config(self, config):
        if 'percentiles' in config:
            self.percentiles = map(int, config['percentiles'])
        if 'histogram_regex' in config:
            self.histogram_regex = re.compile(config['histogram_regex'])

    # override: Interpret beans that match the `histogram_regex` as histograms,
    # and collect percentiles from them.
    def interpret_bean_with_list(self, prefix, values):
        if not self.histogram_regex.match(prefix):
            return

        buckets = values
        offsets = self.offsets
        for percentile in self.percentiles:
            value = self.compute_percentile(offsets, buckets, percentile)
            cleaned_key = self.clean_up("%s.p%s" % (prefix, percentile))
            self.publish(cleaned_key, value)

    # Adapted from Cassandra docs:
    # https://bit.ly/13M5JPE
    # The index corresponds to the x-axis in a histogram.  It represents buckets
    # of values, which are a series of ranges. Each offset includes the range of
    # values greater than the previous offset and less than or equal to the
    # current offset. The offsets start at 1 and each subsequent offset is
    # calculated by multiplying the previous offset by 1.2, rounding up, and
    # removing duplicates. The offsets can range from 1 to approximately 25
    # million, with less precision as the offsets get larger.
    def compute_percentile(self, offsets, buckets, percentile_int):
        non_zero_points_sum = sum(buckets)
        if non_zero_points_sum is 0:
            return 0
        middle_point_index = math.floor(
            non_zero_points_sum * (percentile_int / float(100)))

        points_seen = 0
        for index, bucket in enumerate(buckets):
            points_seen += bucket
            if points_seen >= middle_point_index:
                return round((offsets[index] - offsets[index - 1]) / 2)

    # Returns a list of offsets for `n` buckets.
    def create_offsets(self, bucket_count):
        last_num = 1
        offsets = [last_num]

        for index in range(bucket_count):
            next_num = round(last_num * 1.2)
            if next_num == last_num:
                next_num += 1
            offsets.append(next_num)
            last_num = next_num

        return offsets
    
    ########
    #
    # The clean_up() function takes the metric name and cleans it up to make it
    # more presentable.  The default Jolokia JMX collector only does some minimal
    # work in this regard (replacing '=' and ':" with '_" and things like that),
    # whereas for Cassandra we need to actually rearrange the ordering of the 
    # JMX keys to make the metrics fit into a hierarchy.  Therefore, we override
    # the clean_up method to do just that.
    #
    # 2016-09-19 DVG
    #
    ########

    def clean_up(self, text):

        try:

            # The metric name as it comes in has two main parts separated by a colon.
            s = string.split(text, ':')

            # The first part tells us the name of the MBean; the second part contains all of the JMX keys.
            base = s[0]
            jmx_keys = s[1]

            # Some metrics also have a statistic (i.e., "Min", "Max", "OneMinuteRate", "Value")
            # Not all metrics have this, but those that do will always have it at the end of
            # the list of JMX keys, separated by a dot.  Since the keys are separated by commas, 
            # the first thing we do is look for the last comma in the string.
            statistic = ''
            last_comma_index = string.rfind(jmx_keys, ',')

            # It shouldn't even be the case, but check in case there were no commas
            if (-1 != last_comma_index):
                    # Now find the last dot that occurs in this final key's value.
                    # (NOTE: We can't just search the entire JMX keys string for the last dot,
                    # because if the metric in question does not have a statstic at the end,
                    # and one of the other keys has dots in it, then we won't parse things correctly.)
                    last_dot_index = string.rfind(jmx_keys, '.', last_comma_index)
                    # If there wasn't a dot, this just means that this metric didn't have a statistic on the end
                    if (-1 != last_dot_index):
                            # If there was a dot, parse out the statistic, and strip it off the jmx_keys
                            statistic = jmx_keys[last_dot_index+1:]
                            jmx_keys = jmx_keys[:last_dot_index]

            # Parse the jmx_keys string to split everything out into an array of name-value pairs.
            kvps = string.split(jmx_keys, ',')

            # Initialize the variables that will be used to hold the values from the JXM keys
            m_type = ''
            keyspace = ''
            name = ''
            scope = ''
            colfam = ''
            path = ''

            # Loop through the array of name-value pairs.
            for i in range(len(kvps)):
                    # Split the pair on the equal sign such that kvp[0] is the key and kvp[1] is the value
                    kvp = string.split(kvps[i], '=')
                    
                    # Check the name of the key, and set the appropriate variable accordingly.
                    # In most cases, we'll replace any dots in the values with dashes. We don't do this
                    # for the "type" key, though, as in some cases we need to do further parsing first.
                    if (kvp[0].lower() == 'type'):
                            m_type = kvp[1]
                    elif (kvp[0].lower() == 'keyspace'):
                            keyspace = '.' + string.replace(kvp[1], '.', '-')
                    elif (kvp[0].lower() == 'name'):
                            name = '.' + string.replace(kvp[1], '.', '-')
                    elif (kvp[0].lower() == 'scope'):
                            scope = '.' + string.replace(kvp[1], '.', '-')
                    elif (kvp[0].lower() == 'columnfamily'):
                            colfam = '.' + string.replace(kvp[1], '.', '-')
                    elif (kvp[0].lower() == 'path'):
                            path = '.' + string.replace(kvp[1], '.', '-')
                    else:
                            self.log.error('Unknown key name: %s - This key will be IGNORED in the construction of the metric name.  Full bean name is: %s', kvps[i], text)
                            continue

            # Now we need to build the metric name, which happens in slightly different ways depending on:
            #   1) Which MBean the metric was retrieved from; and
            #   2) What type of metric it is.

            # Is this from the org.apache.cassandra.metrics MBean (Cassandra 2+)?
            if (base == 'org.apache.cassandra.metrics'):
                # If the metric type is a column family metric, then the metric is either:
                #   1) Associated with a particluar table in a particular keyspace; or
                #   2) An aggregate across all tables and keyspaces in the Cassandra node.
                if (m_type == 'ColumnFamily') or (m_type == 'ColumnFamilies'):
                        if (keyspace != ''):
                                # If the keyspace is not blank, it's a metric for a specific keyspace and table
                                metric_name = 'Keyspace._Keyspaces' + keyspace + '._Tables' + scope + name
                        else:
                                # Otherwise, it's a global aggregate
                                metric_name = 'Keyspace' + scope + name
                # If the metric type is a keyspace metric, then the metric is either:
                #   1) An aggregate across all tables in a particular keyspace; or
                #   2) An aggregate across all tables and keyspaces in the Cassandra node.
                elif (m_type == 'Keyspace'):
                        if (keyspace != ''):
                                # If the keyspace is not blank, it's an aggrgate metric for a specific keyspace
                                metric_name = m_type + '._Keyspaces' + keyspace + scope + name
                        else:
                                # Otherwise, it's a global aggregate
                                metric_name = 'Keyspace' + scope + name
                # If the metric is a thread pool metric, there is an additional 'path' component to add to the metric name
                elif (m_type == 'ThreadPool'):
                         metric_name = m_type + path + scope + name
                # All other metrics are constructed simply as type + scope + name
                else:
                        metric_name = m_type + scope + name
            
            elif (base == 'org.apache.cassandra.internal'): 
                # Is this from the "internal" MBean (Cassandra 1+)?  All of these are metrics related to the
                # thread pools. The value for the "type" key will actually be of the form "<pool-name>.<metric>"
                metric_name = 'ThreadPools.' + m_type
            
            elif (base == 'org.apache.cassandra.request'): 
                # Is this from the "request" MBean (Cassandra 1+)?  All of these are metrics related to the thread
                # pools for request/response. The value for the "type" key will actually be of the form "<pool-name>.<metric>"
                metric_name = 'ThreadPools.' + m_type
            
            elif (base == 'org.apache.cassandra.db'): 
                # Is this from the "db" MBean (Cassandra 1+)?  

                # If the "type" starts with "ColumnFamilies", then the metric is related to column families 
                # (i.e., tables).  In this case:
                #    - The value for "keyspace" will be the name of the specific keyspace;
                #    - The value for "columnfamily" will be the name of the specific table; and
                #    - The value for the "type" key will take one of the following forms:
                #       1) "ColumnFamilies.<metricname>" or 
                #       2) "ColumnFamilies.<metricname>.<stat>"
                if (m_type[0:14] == 'ColumnFamilies'):
                    metric_name = 'Keyspace._Keyspaces' + keyspace + '._Tables' + colfam + m_type[14:]
                else:
                    # Typically, the value for the "type" key contains a dot. We split on the dot so that we can
                    # make any changes to the left-hand portion to provide naming consistency between v2 and v3 beans.
                    s = string.split(m_type, '.')

                    # Make sure we map to names that are compatible with the v2 MBean
                    if (s[0] == 'Caches'):
                        metric_name = 'Cache'
                    elif (s[0] == 'Commitlog'):
                        metric_name = 'CommitLog'
                    elif (s[0] == 'CompactionManager'):
                        metric_name = 'Compaction'

                    # If there was, in fact, a dot, reattach the right-hand side
                    if (len(s) > 1):
                        metric_name = metric_name + '.' + s[1]
            
            elif (base == 'org.apache.cassandra.net'):
                # Is this from the "net" MBean (Cassandra 1+)?  All of these are metrics related to networking.
                # Their format and organization is the most complicated of the Cassandra 1 MBeans. 

                # One metric type is 'FailureDetector'; we will present these as FailureDetector.<metric>
                if (m_type[0:15] == 'FailureDetector'):
                    metric_name = m_type
                else:
                    # The other metric type is MessagingService
                    # First, strip "MessagingService." from the start of the type
                    m_type = m_type[17:]

                    # At this point, we have two formats of metric to consider:
                    #    1) DroppedMessages.xxx or RecentlyDroppedMessages.xxx (where xxx is the metric name)
                    #    2) xxx.127.0.0.1 (where xxx is the metric name and 127.0.0.1 could be any IP address or hostname)

                    if (m_type[0:15] == 'DroppedMessages' or m_type[0:23] == 'RecentlyDroppedMessages'):
                        # Group both of these under 'DroppedMessage'
                        metric_name = 'DroppedMessage.' + m_type[string.find(m_type, '.')+1:]
                    else:
                        # The metric is everything up to the first dot; the host is everything afterwards; we
                        # just need to make sure that we convert any dots in the host string into dashes.
                        i = string.find(m_type, '.')
                        metric = m_type[0:i]
                        host = string.replace(m_type[i+1:], '.', '-')
                        metric_name = "Connection." + host + '.' + metric

            elif  (base[0:4] == 'java'):
                    # Is this from the "java" MBean?  All of these are metrics related to the JVM.
                    metric_name = 'jvm.' + m_type
            else:
                # Use the default Jolokia clean_up if it's not an MBean we've accounted for
                self.log.warning('Unknown MBean for Cassandra: %s - default JMX naming strategy will be followed.  Full bean name is: %s', kvps[i], text)
                return(super(CassandraJolokiaCollector, self).clean_up(text))

            # If there is a statistic, we append that to the metric name.
            # (NOTE: We don't append the statistic if it's 'Value', since metrics with 'Value' don't have
            # any other statistics, so this only results in the user having an extra layer to click through
            # in the metric name hierarchy.)
            if (statistic != '') and (statistic != 'Value'):
                    metric_name = metric_name + '.' + statistic

            # Finally, return the new metric name.
            return metric_name                        

    # IF any unexpected exceptions occur, log the full details, then return an empty string for the metric name in order to skip it
    except Exception as e:
        self.log.error("An unhandled exception has occurred. The full bean name is: %s - This metric will be SKIPPED, but processing will continue.  The full exception text is:\n%s", text, str(e))
        return ""