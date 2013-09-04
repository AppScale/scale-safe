#!/usr/bin/ruby -w


$:.unshift File.join(File.dirname(__FILE__))
require 'helperfunctions'


# To support the Google App Engine Datastore API in a way that is
# database-agnostic, App Engine applications store and retrieve data
# via the DatastoreServer. The server inherits this name from the storage
# format of requests in the Datastore API: Datastore Buffers.
module DatastoreServer


  # The first port that should be used to host DatastoreServers.
  STARTING_PORT = 4000


  # The port that we should run nginx on, to load balance requests to the
  # various DatastoreServers running on this node.
  PROXY_PORT = 3999


  # The port that nginx should be listening to for non-encrypted requests to
  # the DatastoreServers.
  LISTEN_PORT_NO_SSL = 8888


  # The port that nginx should be listening to for encrypted requests to the
  # DatastoreServers.
  LISTEN_PORT_WITH_SSL = 8443


  # The name that nginx should use as the identifier for the DatastoreServer when it
  # we write its configuration files.
  NAME = "as_datastore_server"

  # If we fail to get the number of processors we set our default number of 
  # datastore servers to this value.
  DEFAULT_NUM_SERVERS = 3

  # Google service account for Google Cloud Datastore.
  SERVICE_ACCOUNT = "399068749927-11atpdlu60fv73ip8jnhvftr3kt7kd8l@developer.gserviceaccount.com"

  # Google private key for Google Cloud Datastore.
  PRIVATE_KEY_FILE = "/root/2574a2a5f891af1afb67c13de3be28648a46833f-privatekey.p12"

  # We have two modes of operation for Google Cloud Datastore, 
  # read only mode and read/write mode.
  READ_ONLY = 'READ_ONLY'
  READ_WRITE = 'READ_WRITE'

  # Starts a Datastore Server on this machine. We don't want to monitor
  # it ourselves, so just tell god to start it and watch it.
  def self.start(master_ip, db_local_ip, my_ip, table, zklocations)
    datastore_server = self.get_executable_name(table)
    ports = self.get_server_ports(table)

    env_vars = { 
      'APPSCALE_HOME' => APPSCALE_HOME,
      "MASTER_IP" => master_ip, 
      "LOCAL_DB_IP" => db_local_ip,
      "DATASTORE_SERVICE_ACCOUNT" => SERVICE_ACCOUNT,
      "DATASTORE_PRIVATE_KEY_FILE" => PRIVATE_KEY_FILE,
      "GCD_DB_PERMISSIONS" => READ_WRITE
    }
  
    ports.each { |port|
      start_cmd = "/usr/bin/python2.6 #{datastore_server} -p #{port} " +
          "--no_encryption --type #{table} -z \'#{zklocations}\' "
      # stop command doesn't work, relies on terminate.rb
      stop_cmd = "pkill -9 datastore_server"
      GodInterface.start(:datastore_server, start_cmd, stop_cmd, port, env_vars)
    }
  end


  # Stops the Datastore Buffer Server running on this machine. Since it's
  # managed by god, just tell god to shut it down.
  def self.stop(table)
     GodInterface.stop(:datastore_server)
  end


  # Restarts the Datastore Buffer Server on this machine by doing a hard
  # stop (killing it) and starting it.
  def self.restart(master_ip, my_ip, table, zklocations)
    self.stop()
    self.start(master_ip, my_ip, table, zklocations)
  end


  # Number of servers is based on the number of CPUs.
  def self.number_of_servers()
    # If this is NaN then it returns 0
    num_procs = `cat /proc/cpuinfo | grep processor | wc -l`.to_i
    if num_procs == 0
      return DEFAULT_NUM_SERVERS
    else 
      return num_procs
    end
  end


  # Returns a list of ports that should be used to host DatastoreServers.
  def self.get_server_ports(table)
    num_datastore_servers = self.number_of_servers()

    server_ports = []
    num_datastore_servers.times { |i|
      server_ports << STARTING_PORT + i
    }
    return server_ports
  end

  
  def self.is_running(my_ip)
    `curl http://#{my_ip}:#{PROXY_PORT}` 
  end 


  # Return the name of the executable of the datastore server.
  def self.get_executable_name(table)
    return "#{APPSCALE_HOME}/AppDB/datastore_server.py"
  end
end

