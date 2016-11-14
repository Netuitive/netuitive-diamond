"""
Collects Kafka JMX metrics from the Jolokia Agent.  Extends the
JolokiaCollector to reformat the Kafka MBean names into easier to
understand metric names.

Additionally, we also connect to Zookeeper to collect certain 
metrics that are critical to Kafka monitoring.  This is done in
two ways:
    1) By extending the ProcessCollector, we can execute a command-
        line function against the Zookeeper instance to extract 
        infomation about the consumer groups and their corresponding
        consumer lag.  This replaces and improves upon the 
        KafkaConsumerLagCollector.
    2) By extending the ZookeeperCollector, we can collect the 
        key Zookeeper metrics for Kafka without requiring the user
        to configure a separate collector. This does not replace
        the Zookeeper collector, however; users with a Zookeeper
        instance being used for something other than Kafka can still
        use the Zookeeper collector on its own. 

Netuitive Change History
    2016/10/26 DVG - Initial version.
"""

from jolokia import JolokiaCollector
from zookeeper import ZookeeperCollector
from diamond.collector import ProcessCollector
import math
import string
import re

class KafkaJolokiaCollector(JolokiaCollector, ProcessCollector, ZookeeperCollector):

    ########
    #
    # The get_default_config_help() function is over-ridden to provide help on the config
    # options specific to this colector.
    #
    # 2016-11-03 DVG
    #
    ########

    def get_default_config_help(self):

        # Get the parent class config help object.
        config_help = super(KafkaJolokiaCollector, self).get_default_config_help()
         
        # Update it....
        config_help.update({
            'bin': 'The path to the kafka-run-class.sh script.  Default is /opt/kafka/bin/kafka-run-class.sh',
            'version': 'The version of Kafka. Two values are accepted here. 8 for Kafka versions 8 and earlier, or 9 for Kafka versions 9 or later. The default is 9.',
            'zookeeper': 'Zookeper host and port number. If no port number is given, defaults to 2181. If nothing is given, defaults to localhost:2181',
            'consumer_groups': 'Comma-separated list of consumer groups. This is only required for Kafka versions 8 and earlier; with Kafka 9 and higher we can discover these dynamically.',
            'topics': 'Comma-separated list of consumer topics. This is only required for Kafka versions 8 and earlier; with Kafka 9 and higher we can discover these dynamically. If not specified, default is all topics.'
        })

        # .... and return it.
        return config_help


    ########
    #
    # The get_default_config() function is over-ridden to provide the default values for the config
    # options specific to this colector.
    #
    # 2016-11-03 DVG
    #
    ########

    def get_default_config(self):
        """
        Returns the default collector settings
        """

        # Get the parent class default config class
        config = super(KafkaJolokiaCollector, self).get_default_config()

        # Update it....
        config.update({
            'path': 'kafka',
            'bin': '/opt/kafka/bin/kafka-run-class.sh',
            'version': '8',
            'zookeeper': 'localhost:2181'
        })

        # ....and return it.
        return config


    ########
    #
    # The collect() function is over-ridden to allow us to collect certain Zookeeper
    # metrics in addition to the Kafka ones. We do this because those Zookeeper
    # metrics are crucial for effective monitoring of Kafka.
    #
    # 2016-10-26 DVG
    #
    ########

    def collect(self):

        ####
        #
        # The first thing we need to get from Zookeeper are the consumer offsets
        # and lags. Consumer lag is a critical metric to track for Kafka, and 
        # while Kakfa itself stores the broker offset, it has no knowledge of
        # the consumer offset (and hence no knowledge of the lag).
        #
        # This code improves upon and replaces the existing KafkaConsumerLagCollector,
        # which required you to specify the consumer groups and topics in the config file. 
        # While this is still necessary for Kafka 8 and earlier, Kafka 9 introduced the 
        # means to discover the consumer groups and topics. 
        #
        ####

        try:
            # The "zookeeper" parameter in the config file specifies the Zookeeper host
            # and port to collect from.  
            zookeeper = self.config.get('zookeeper')
            
            # If the port is not specified, we assume 2181.
            if (-1 == string.find(zookeeper, ':')):
                zookeeper = zookeeper + ':2181'

            # Get the version number from the configuration
            k_ver = self.config.get('version')

            # Call a different collection routine for the consumer lag metrics, depending on the version of Kafka
            if (k_ver == '8'):
                self.collect_consumer_lag_8(zookeeper)
            elif (k_ver == '9'):
                self.collect_consumer_lag_9(zookeeper)
            else:
                raise ValueError('The value "' + k_ver + '" given for the "version" parameter is not valid. Accepted values are "8" for Kafka versions 8 and earlier, or "9" for Kafka versions 9 and later.')

        except Exception as e:
            self.log.error('Failed to collect consumer lag metrics from Zookeeper; ensure that your Kafka version is set correctly in the config file. These metrics will be SKIPPED, but processing will continue.  The full exception text is:\n%s', str(e))

        ###
        # 
        # Now that we have collected the consumer group information from Zookeeper,
        # the next steps are to collect the Zookeeper server stats and all of the
        # Kafka performance metrics.
        #
        # For the Zookeeper server stats, we would like to collect via JMX for 
        # consistency, but 2 of the metrics are not published to JMX. Therefore, 
        # we will invoke the ZookeeperCollector, which uses a command line utility
        # to collect the server stats.
        #
        # For the Kakfa stats, well, that's the main purpose of this class, so we
        # invoke the super-class collector on Jolokia to kick off the JMX collection.
        #
        ###

        try:
            # Explicitly run the Zookeeper collector
            ZookeeperCollector.collect(self)

        except Exception as e:
            self.log.error('Failed to collect Zookeeper metrics. These metrics will be SKIPPED, but processing will continue.  The full exception text is:\n%s', str(e))


        try:
            # And now run the Jolokia collector to get the JMX metrics
            super(KafkaJolokiaCollector, self).collect()

        except Exception as e:
            self.log.error('Failed to collect Kafka metrics via the Jolokia JMX bridge. These metrics will be SKIPPED, but processing will continue.  The full exception text is:\n%s', str(e))


    ########
    #
    # The collect_consumer_lag_9() function collects the consumer lag metrics for Kafka
    # versions 9 and above. Version 9 introduced the kafka.admin.ConsumerGroupCommand
    # class which allows us to discover the consumer groups and the topics they are 
    # using, rather than requiring them to be specified in the config file up front.
    #
    # 2016-11-03 DVG
    #
    ########

    def collect_consumer_lag_9(self, zookeeper):

        # Set up the first call to ConsumerGroupCommand, a call to list the consumer groups.
        cmd = [
            'kafka.admin.ConsumerGroupCommand',
            '--list',
            '--zookeeper',
            zookeeper
        ]

        # Run the command (via the run_command function of the ProcessCollector)
        raw_output = self.run_command(cmd)

        # Assuming we get output, process it.
        if raw_output is not None:

            # Loop through each line of the outpt we got. Each line will contain the name of exactly one consumer group.
            for i, c_group in enumerate(raw_output[0].split('\n')):

                # If the line is blank, continue to the next line.
                if c_group == '':
                    continue

                # Now prepare the second command, which will get the metrics for the current consumer group.
                cmd2 = [
                    'kafka.admin.ConsumerGroupCommand',
                    '--describe',
                    '--group',
                    c_group,
                    '--zookeeper',
                    zookeeper
                ]

                # Run the command and get the raw output.
                raw_output2 = self.run_command(cmd2)

                # If we didn't get anything, log an error and continue on to the next consumer group.
                if raw_output2 is None:
                    self.log.error('No output returned for consumer group ' + c_group)
                    continue

                ###
                #
                # The output here is typically one line with column headers followed by one or more
                # lines of statistics for the consumer group.  There will be one line for each
                # partition of each topic that the consumer is listening on.
                #
                # There may, however, be an error message returned instead of the metrics we want.
                #
                ###

                # Loop through each line of the output.
                for i2, line in enumerate(raw_output2[0].split('\n')):
    
                    # If the line is blank, or if it is the header line, continue to the next line. 
                    if (line == '' or line[0:5] == 'GROUP'):
                        continue
            
                    # Split the line on commas to get the details (the metrics we want).
                    details = string.split(line, ', ')

                    # And process the results
                    self.process_result_row(details)

    ########
    #
    # The collect_consumer_lag_8() function collects the consumer lag metrics for Kafka
    # versions 8 and below. Version 8 did not have a way to discover the consumer groups 
    # or the topics they are using, hence we require them to be specified in the config 
    # file up front.
    #
    # 2016-11-03 DVG, from code originally by Shawn Butts
    #
    ########

    def collect_consumer_lag_8(self, zookeeper):
        try:

            # Get the list of consumer groups, and the list of topics. We split the
            # consumer groups into a list that we can walk through, since each call to
            # Zookeeper can only specify one group. Topics, however, can have multiple
            # specified, so we just remove whitespace and keep it as a string.
            consumer_groups = self.config.get('consumer_groups').split(',')
            topics = self.config.get('topics').replace(' ', '')

            # Get any arguments specified in the config file
            config_args = self.config.get('args').split(' ')

            # Loop through the list of consumer groups; we need to make one call per group
            for consumer_group in consumer_groups:

                # Build the arguments for the command that we will be executing.  The actual
                # program or script will be specified by the "bin" parameter in the config file.
                # Here, we will build the list with the arguments to pass.
                args = []
                args += config_args
                args += [
                    'kafka.tools.ConsumerOffsetChecker',
                    '--group',
                    consumer_group,
                    '--zookeeper',
                    zookeeper + '/kafka'
                ]

                # If topics were specified in the config file, add the --topic argument.
                # If not, the command will default to retrieving the metrics for all topics.
                if topics:
                    args += '--topic ' + topics

                # Execute the command and get the raw output.
                raw_output = self.run_command(args)

                # If there is no output for this consumer group, continue on to the next one.
                if raw_output is None:
                    continue

                for i, output in enumerate(raw_output[0].split('\n')):

                    self.log.error('#### output = ' + output)

                    # If there is no output, or if it's the header line, continue to the next line
                    if i == 0 or output is None or output == '':
                        continue

                    items = output.split(' ')
                    details = [item for item in items if item]

                    self.process_result_row(details)

        except Exception as e:
            self.log.error(e)








    def process_result_row(self, details):

        for i in range(0, len(details)):
            self.log.error('#### %i - %s', i, details[i])

        # If there are less than 7, assume this is an error message and not data.
        # Presumably we will not get an error message with 6 or more commas! :)
        # If this happens, skip over this consumer group, and move on to the next.
        if (len(details) < 7):
            self.log.error('Error processing consumer group - %s', details[0])
            return

        ###
        #
        # Each line contains multiple metrics. First, we construct the common base for each metric name.
        #
        ###

        # Each metric will start with 'zookeeper.consumer_groups'
        metric_base = 'zookeeper.consumer_groups'

        # Next up is the consumer group name
        metric_base = metric_base + '.' + details[0]

        # Followed by the topic name
        metric_base = metric_base + '.' + details[1]

        # Followed by the partition number, which we preface with "partition-" for readability
        metric_base = metric_base + '.partition-' + details[2]

        ###
        #
        # And now for each of the actual metric names
        #
        ###

        # 1) Consumer offet
        metric_name = metric_base + '.consumer_offset'
        value = details[3]
        self.publish(metric_name, value)
        
        # 2) Broker offset
        metric_name = metric_base + '.broker_offset'
        value = details[4]
        self.publish(metric_name, value)

        # 3) Consumer lag (which is broker offset minus consumer offset)
        metric_name = metric_base + '.consumer_lag'
        value = details[5]
        self.publish(metric_name, value)

        # 4) Owner - This column from the Zookeeper results has a string with the name of the 
        # consumer group's owner, or the value 'none'.  We make this into a binary 0/1 to 
        # indicate whether or not the consumer group has an owner.  
        metric_name = metric_base + '.has_owner'

        if (details[6].lower() == 'none'):
            value = 0
        else:
            value = 1

        self.publish(metric_name, value)    




















    ########
    #
    # The clean_up() function takes the metric name and cleans it up to make it
    # more presentable.  The default Jolokia JMX collector only does some minimal
    # work in this regard (replacing '=' and ':" with '_" and things like that),
    # whereas for Kafka we need to actually rearrange the ordering of the JMX
    # keys to make the metrics fit into a hierarchy.  Therefore, we override the
    # clean_up function to do just that.
    #
    # 2016-10-26 DVG
    #
    ########

    def clean_up(self, text):

        # The MBean name has two main parts separated by a colon.
        s = string.split(text, ':')

        # The first part tells us the domain of the MBean; the second part contains all of the JMX keys.
        domain = s[0]
        jmx_keys = s[1]

        # Use the domain, but minus the "kafka" at the start, since the Jolokia collector will add this
        if (domain[0:5] == 'kafka'):
            domain = domain[6:]
        else:
            # Use the default Jolokia clean_up if it's not a Kafka domain (most likely it's Java)
            return super(KafkaJolokiaCollector, self).clean_up(text)

        # Parse the jmx_keys string to split everything out into an array of name-value pairs.
        kvps = string.split(jmx_keys, ',')

        # Initialize the variables that will be used to hold the values from the JXM keys.
        # Note that there are a lot of keys, but not all of them apply to all metric types.
        m_type = ''
        name = ''
        request = ''
        topic = ''
        partition = ''
        broker=''
        delayed_op=''
        network_processor=''
        processor=''
        client=''
        broker_host=''
        broker_port=''
        group=''
        thread=''
        fetcher=''

        # Loop through the array of name-value pairs.
        for i in range(len(kvps)):
            # Split the pair on the equal sign such that kvp[0] is the key and kvp[1] is the value
            kvp = string.split(kvps[i], '=')
            
            # Check the name of the key, and set the appropriate variable accordingly.
            # In most cases, we'll replace any dots in the values with dashes. We don't do this
            # for the "type" key, though, as in some cases we need to do further parsing first.
            if (kvp[0].lower() == 'type'):
                    m_type = kvp[1]
            elif (kvp[0].lower() == 'name'):
                    name = '.' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'request'):
                    request = '.' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'topic'):
                    topic = '.' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'partition'):
                    partition = '.' + 'partition-' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'broker-id'):
                    broker = '.' + 'broker-' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'networkprocessor'):
                    network_processor = '.' + 'networkprocessor-' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'processor'):
                    processor = '.' + 'processor-' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'clientid' or kvp[0].lower() == 'client-id'):
                    client = '.' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'delayedoperation'):
                    delayed_op = '.' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'brokerhost'):
                    broker_host = '.' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'brokerport'):
                    broker_port = string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'groupid'):
                    group = '.' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'threadid'):
                    broker = '.' + 'thread-' + string.replace(kvp[1], '.', '-')
            elif (kvp[0].lower() == 'fetchertype'):
                    fetcher = '.' + string.replace(kvp[1], '.', '-')
            else:
                    self.log.error('Unknown key name %s for MBean %s', kvps[i], text)

        # The type parameter will occassionally have additional qualifiers to the metric, such
        # as a statistic, which should get appended after the metric name.
        remainder=''
        dot_index = string.find(m_type, '.')
        if (-1 != dot_index):
            # If there was a dot, parse out the remainder, and strip it off the jmx_keys
            remainder = m_type[dot_index+1:]
            m_type = m_type[:dot_index]

        # Next we do some mapping of the types for consistent capitalization/punctuation
        # as well as for better grouping.
        if (-1 != string.find(m_type.lower(), 'logcleaner')):
            m_type = 'LogCleaner'
        elif ((-1 != string.find(m_type.lower(), 'socket')) | (m_type.lower()=='processor')):
            m_type = 'SocketServer'
        elif (-1 != string.find(m_type.lower(), 'controller-channel-metrics')):
            m_type = 'ControllerStats'
        elif (-1 != string.find(m_type.lower(), 'kafka-metrics-count')):
            m_type = 'KafkaServer'
        elif (m_type.lower() == 'partition'):
            m_type = ''
        elif ((m_type.lower() == 'controllerstats') or (m_type.lower() == 'kafkacontroller')):
            m_type = ''
            domain='controller'
        elif (m_type.lower() == 'log'):
            m_type = 'Topics'
        elif (m_type.lower() == 'socketserver'):
            domain = 'network'

        # Create a single string from the broker_host and broker port, if they were provided.
        broker_hp=''
        if (broker_host != '' and broker_port != ''):
            broker_hp = broker_host + '-' + broker_port

        ###
        #
        # And now it's time to compose the metric name.
        #
        ###

        # Start with the domain
        metric_name = domain

        # Next is the type (if any)
        if (m_type != ''):
            metric_name = domain + '.' + m_type 

        # BrokenTopicMetrics contains metrics for each topic.  If no topic is specified, the metrics 
        # are the aggregates across all topics; group them together accordingly.
        if (m_type == 'BrokerTopicMetrics') and (topic == ''):
            topic = '._all'

        # Append all of the intermediate components. Not all of these will be present, but those that 
        # are are in the correct hierarchical ordering. 
        metric_name = metric_name + request + broker_hp + group + client + thread
        metric_name = metric_name + delayed_op + topic + partition + broker + processor + network_processor 
        
        # Now append the name portion to the metric name.
        metric_name = metric_name + name

        # If these are RequestMetrics, then the only statistic to keep is the average (mean)
        if (m_type == 'RequestMetrics') or (name == '.LeaderElectionRateAndTimeMs'):
            # If the remainder is 'Mean', return the metric name, otherwise throw it out
            if (remainder == 'Mean'):
                metric_name = metric_name
            else:
                metric_name = ''
        else:
            # Since these are NOT RequestMetrics, append the remainder unless it is a Value or Count 'statistic'.
            if (remainder != '') and (remainder != 'Value') and (remainder != 'Count'):
                metric_name = metric_name + '.' + remainder
            else:
                metric_name = metric_name

        # Finally, return the new metric name.
        return metric_name                        
