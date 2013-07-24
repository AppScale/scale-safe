require 'djinn'


# The version of Hadoop we are currently running (here, it's the Cloudera
# Hadoop distribution).
HADOOP_VER = "0.20.2-cdh3u3"


# The location on the local file system where we've installed Hadoop to.
HADOOP_LOC = "#{APPSCALE_HOME}/AppDB/hadoop-" + HADOOP_VER 


# The port that the Hadoop Distributed File System runs on, by default.
HDFS_PORT = 9000


# TODO(cgb): Find out from Raj what this is and why it's needed.
ENABLE_HADOOP_SINGLE_NODE = true


# Writes the configuration files needed to start the Hadoop Distributed File
# System and Hadoop MapReduce, which for us just involves grabbing a number of
# Hadoop template config files, replacing constant strings with the IPs of the
# machines to contact, and writing the new files.
def setup_hadoop_config(master_ip, replication)
  source_dir = "#{APPSCALE_HOME}/AppDB/hadoop/templates"
  dest_dir = File.join(HADOOP_LOC, "conf")
  my_public = HelperFunctions.local_ip

  files_to_config = `ls #{source_dir}`.split
  files_to_config.each do |filename|
    full_path_to_read = File.join(source_dir, filename)
    full_path_to_write = File.join(dest_dir, filename)
    File.open(full_path_to_read) do |source_file|
      contents = source_file.read
      contents.gsub!(/APPSCALE-MASTER/, master_ip)
      contents.gsub!(/APPSCALE-LOCAL/, my_public)
      contents.gsub!(/REPLICATION/, replication)

      File.open(full_path_to_write, "w+") do |dest_file|
        dest_file.write(contents)
      end
    end
  end

  # By default, the Cloudera Hadoop distribution disallows running Hadoop as
  # root. We hack it to re-enable running Hadoop as root.
  # TODO(cgb): Investigate what it would take to not run Hadoop as root.
  Djinn.log_run("cp #{source_dir}/hadoop #{HADOOP_LOC}/bin")
end

def start_hadoop_master
  # if there is hadoop, stop first
#  stop_hadoop_master
  Djinn.log_debug(`rm -rfv #{APPSCALE_HOME}/AppDB/hadoop-#{HADOOP_VER}/logs/*`)
  Djinn.log_debug("cleaning up the hadoop data folder.")
  Djinn.log_debug(`rm -rf /tmp/hadoop*`)
  Djinn.log_debug(`rm -rf /var/appscale/hadoop`)

  Djinn.log_debug(`#{HADOOP_LOC}/bin/hadoop namenode -format`)
  Djinn.log_debug(`#{HADOOP_LOC}/bin/hadoop-daemon.sh start namenode`)
  if ENABLE_HADOOP_SINGLE_NODE
    Djinn.log_debug(`#{HADOOP_LOC}/bin/hadoop-daemon.sh start datanode`)
  end
  wait_on_hadoop
  Djinn.log_debug(`#{HADOOP_LOC}/bin/hadoop-daemon.sh start jobtracker`)
  if ENABLE_HADOOP_SINGLE_NODE
    Djinn.log_debug(`#{HADOOP_LOC}/bin/hadoop-daemon.sh start tasktracker`)
  end
end

def wait_on_hadoop
#  command = "python #{APPSCALE_HOME}/AppDB/hadoop/wait_on_hadoop.py"
#  Djinn.log_debug(`#{command}`)
  # TODO: this should handle timeout error.
  until `lsof -i :#{HDFS_PORT} -t`.length > 0
    Djinn.log_debug("waiting for HDFS coming up: port #{HDFS_PORT}")
    sleep(5)
  end
  loop do
    report = `#{HADOOP_LOC}/bin/hadoop dfsadmin -report | awk '/^Datanodes/{print $3}'`
    break if report.length > 0 && report.to_i > 0
    Djinn.log_debug("waiting for DataNode connecting")
    sleep(5)
  end
end

def stop_hadoop_master
  if ENABLE_HADOOP_SINGLE_NODE
    Djinn.log_run("#{HADOOP_LOC}/bin/hadoop-daemon.sh stop tasktracker")
    Djinn.log_run("#{HADOOP_LOC}/bin/hadoop-daemon.sh stop datanode")
  end
  Djinn.log_run("#{HADOOP_LOC}/bin/hadoop-daemon.sh stop jobtracker")
  Djinn.log_run("#{HADOOP_LOC}/bin/hadoop-daemon.sh stop namenode")
end

def start_hadoop_slave
  Djinn.log_run("rm -rfv #{APPSCALE_HOME}/AppDB/hadoop-#{HADOOP_VER}/logs/*")
  Djinn.log_debug("cleaning up the hadoop data folder.")
  Djinn.log_run("rm -rf /tmp/hadoop*")
  Djinn.log_run("rm -rf /var/appscale/hadoop")
#  stop_hadoop_slave
  Djinn.log_run("#{HADOOP_LOC}/bin/hadoop-daemon.sh start datanode")
  Djinn.log_run("#{HADOOP_LOC}/bin/hadoop-daemon.sh start tasktracker")
end

def stop_hadoop_slave
  Djinn.log_run("#{HADOOP_LOC}/bin/hadoop-daemon.sh stop tasktracker")
  Djinn.log_run("#{HADOOP_LOC}/bin/hadoop-daemon.sh stop datanode")
end
