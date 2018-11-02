import diamond.collector


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

    def collect(self):
      self.log.info('Collecting for Consul')
