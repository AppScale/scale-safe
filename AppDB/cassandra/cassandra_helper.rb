# Programmer: Navraj Chohan <nlake44@gmail.com>
require 'djinn'
require 'djinn_job_data'
require 'helperfunctions'


# A Fixnum that indicates which port the Thrift service binds to, by default.
# Note that this class does not dictate what port it binds to - change this
# constant and the template file that dictates to change this port.
THRIFT_PORT = 9160


# A String that indicates where we write the process ID that Cassandra runs
# on at this machine.
PID_FILE = "/var/appscale/appscale-cassandra.pid"


# A String that indicates where we install Cassandra on this machine.
CASSANDRA_DIR = "#{APPSCALE_HOME}/AppDB/cassandra"


# A String that indicates where the Cassandra binary is located on this
# machine.
CASSANDRA_EXECUTABLE = "#{CASSANDRA_DIR}/cassandra/bin/cassandra"


# Determines where the closest UserAppServer runs in this AppScale deployment.
# For Cassandra, multiple UserAppServers can be running, so we defer this
# calculation elsewhere.
#
# Returns:
#   A String that names the private FQDN or IP address where a UserAppServer
#   runs in this AppScale deployment.
def get_uaserver_ip()
  Djinn.get_nearest_db_ip
end


# Determines if a UserAppServer should run on this machine.
#
# Args:
#   job: A DjinnJobData that indicates if the node runs a Database role.
#
# Returns:
#   true if the given node runs a Database role, and false otherwise.
def has_soap_server?(job)
  if job.is_db_master? or job.is_db_slave?
    return true
  else
    return false
  end
end


# Calculates the token that should be set on this machine, which dictates how
# data should be partitioned between machines.
#
# Args:
#   master_ip: A String corresponding to the private FQDN or IP address of the
#     machine hosting the Database Master role.
#   slave_ips: An Array of Strings, where each String corresponds to a private
#     FQDN or IP address of a machine hosting a Database Slave role.
# Returns:
#   A Fixnum that corresponds to the token that should be used on this machine's
#   Cassandra configuration.
def get_local_token(master_ip, slave_ips)
  return if master_ip == HelperFunctions.local_ip

  slave_ips.each_with_index { |ip, index|
    # This token generation was taken from:
    # http://www.datastax.com/docs/0.8/install/cluster_init#cluster-init
    if ip == HelperFunctions.local_ip
      # Add one to offset the master
      return (index + 1)*(2**127)/(1 + slave_ips.length)
    end
  }
end


# Writes all the configuration files necessary to start Cassandra on this
# machine.
#
# Args:
#   master_ip: A String corresponding to the private FQDN or IP address of the
#     machine hosting the Database Master role.
#   slave_ips: An Array of Strings, where each String corresponds to a private
#     FQDN or IP address of a machine hosting a Database Slave role.
def setup_db_config_files(master_ip, slave_ips)
  source_dir = "#{CASSANDRA_DIR}/templates"
  dest_dir = "#{CASSANDRA_DIR}/cassandra/conf"

  all_ips = [master_ip, slave_ips].flatten
  local_token = get_local_token(master_ip, slave_ips)

  files_to_config = Djinn.log_run("ls #{source_dir}").split
  files_to_config.each{ |filename|
    full_path_to_read = File.join(source_dir, filename)
    full_path_to_write = File.join(dest_dir, filename)
    File.open(full_path_to_read) { |source_file|
      contents = source_file.read
      contents.gsub!(/APPSCALE-LOCAL/, HelperFunctions.local_ip)
      contents.gsub!(/APPSCALE-MASTER/, master_ip)
      contents.gsub!(/APPSCALE-TOKEN/, "#{local_token}")
      contents.gsub!(/REPLICATION/, @creds["replication"])
      contents.gsub!(/APPSCALE-JMX-PORT/, "7070")              
      File.open(full_path_to_write, "w+") { |dest_file|
        dest_file.write(contents)
      }
    }
  }
end


# Starts Cassandra on this machine. Because this machine runs the DB Master
# role, it starts Cassandra first.
def start_db_master()
  @state = "Starting up Cassandra on the head node"
  Djinn.log_info("Starting up Cassandra as master")
  start_cassandra
end


# Starts Cassandra on this machine. This is identical to starting Cassandra as a
# Database Master role, with the extra step of waiting for the DB Master to boot
# Cassandra up.
def start_db_slave()
  @state = "Waiting for Cassandra to come up"
  Djinn.log_info("Starting up Cassandra as slave")

  HelperFunctions.sleep_until_port_is_open(Djinn.get_db_master_ip, THRIFT_PORT)
  Kernel.sleep(5)
  start_cassandra
end


# Starts Cassandra, and waits for it to start the Thrift service locally.
def start_cassandra()
  Djinn.log_run("pkill ThriftBroker")
  if @creds['clear_datastore']
    Djinn.log_info("Erasing datastore contents")
    Djinn.log_run("rm -rf /opt/appscale/cassandra*")
    Djinn.log_run("rm /var/log/appscale/cassandra/system.log")
  end

  Djinn.log_run("#{CASSANDRA_EXECUTABLE} start -p #{PID_FILE}")
  HelperFunctions.sleep_until_port_is_open(HelperFunctions.local_ip,
    THRIFT_PORT)
end


# Kills Cassandra on this machine.
# TODO: Consider a more graceful way to do this.
def stop_db_master
  Djinn.log_info("Stopping Cassandra master")
  Djinn.log_run("cat #{PID_FILE} | xargs kill -9")
end


# Kills Cassandra on this machine.
# TODO: Consider a more graceful way to do this.
def stop_db_slave
  Djinn.log_info("Stopping Cassandra slave")
  Djinn.log_run("cat #{PID_FILE} | xargs kill -9")
end
