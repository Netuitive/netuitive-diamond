# Netuitive's Diamond

This is [Netuitive's](http://www.netuitive.com/) fork of the [Diamond](https://github.com/python-diamond) project.

This repos is intended to be used as part of our [agent build](https://github.com/Netuitive/omnibus-netuitive-agent) process.


##Guide to install agent to local docker container:

1. Log to netuitive and go to Integraion section
2. Choose "Docker" item and copy API KEY


###  Building agent rpm/deb
1. Clone the omnibus project `git clone https://github.com/Netuitive/omnibus-netuitive-agent`
2. Run `bundle install` in the project repo
3. Turn off s3_caching in omnibus.rb: https://github.com/Netuitive/omnibus-netuitive-agent/blob/master/omnibus.rb#L33
4. Update the cacerts version and checksum in cacerts.rb: /config/software/cacerts.rb
   Use https://curl.haxx.se/docs/caextract.html to get pem file. Generate md5 hash for pem file.  
5.Update cacert.rb like   
   `version "2018.06.20" do`  
   `source md5: "247294da2f913ad84609b3bdd85c16f3"`
6. Update the default_version in netuitive-diamond.rb to specify your branch: https://github.com/Netuitive/omnibus-netuitive-agent/blob/master/config/software/netuitive-diamond.rb#L2
7. Run `make rpm` or `make deb` as appropriate for your local platform
8. You should get rpm/deb package in your `/dist` root folder

### Build docker image with agent installed
1. Clone docker-agent project  `git clone https://github.com/Netuitive/docker-agent project`. 
2. Copy your rpm/deb to root folder
3. Add to DOCKERFILE your package like 
  `ADD <package_name.rpm> netuitive-agent.rpm` 
4. Set APIHOST to environment you want to connect to ie 'api.app.metricly.com'  
5. If you use Kinematic you can set such parameters at Settings area  
6. Run `docker run -d -p 8125:8125/udp --name <your docker container name> -e DOCKER_HOSTNAME=“your_host” -e APIKEY=<your API key> -v /proc:/host_proc:ro -v /var/run/docker.sock:/var/run/docker.sock:ro netuitive-agent`  
   Example to run locally `docker run -d -p 8125:8125/udp --name containert-test -e DOCKER_HOSTNAME=“localhost” -e APIKEY=<your API key> -v /proc:/host_proc:ro -v /var/run/docker.sock:/var/run/docker.sock:ro netuitive-agent`
   
### Modify netuitive-diamond sources
1. Run `docker exec -i <your_docker_container_name> bash`  
   Example `docker exec -i netuitive-agent-test bash`
2. Sources located at '/opt/netuitive-agent'
3. You can copy new version of python files using docker `cp` command  
   Example `docker cp /netuitivedocker.py  netuitive-agent-test:/opt/netuitive-agent/collectors/netuitivedocker/netuitivedocker.py`
4. Restart docker container to apply changes

 