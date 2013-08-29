#!/usr/bin/ruby -w

# Imports within Ruby's standard libraries
require 'logger'
require 'monitor'
require 'net/http'
require 'net/https'
require 'openssl'
require 'socket'
require 'soap/rpc/driver'
require 'syslog'
require 'timeout'
require 'yaml'


# Imports for RubyGems
require 'rubygems'
require 'httparty'
require 'json'
require 'zookeeper'


# Imports for AppController libraries
$:.unshift File.join(File.dirname(__FILE__), "lib")
require 'app_controller_client'
require 'app_manager_client'
require 'taskqueue_client'
require 'blobstore'
require 'custom_exceptions'
require 'ejabberd'
require 'error_app'
require 'cron_helper'
require 'godinterface'
require 'haproxy'
require 'helperfunctions'
require 'infrastructure_manager_client'
require 'datastore_server'
require 'nginx'
require 'taskqueue'
require 'apichecker'
require 'user_app_client'
require 'zkinterface'

NO_OUTPUT = false

# Path to the appscale-tools installation.
APPSCALE_TOOLS_HOME = "/usr/local/appscale-tools/"

# This lock makes it so that global variables related to apps are not updated 
# concurrently, preventing race conditions. 
APPS_LOCK = Monitor.new()


$:.unshift File.join(File.dirname(__FILE__), "..", "AppDB", "zkappscale")
require "zookeeper_helper"

# A HTTP client that assumes that responses returned are JSON, and automatically
# loads them, returning the result. Raises a NoMethodError if the host/URL is 
# down or otherwise unreachable.
class JSONClient
  include HTTParty

  # Assume the response is JSON and load it accordingly.
  parser(
    Proc.new do |body, format|
      JSON.load(body)
    end
  )
end


# The string that should be returned to the caller if they call a publicly
# exposed SOAP method but provide an incorrect secret.
BAD_SECRET_MSG = "false: bad secret"


# Regular expression to determine if a file is a .tar.gz file.
TAR_GZ_REGEX = /\.tar\.gz$/


# The maximum number of seconds that we should wait when deploying Google App
# Engine applications via the AppController.
APP_UPLOAD_TIMEOUT = 180


# The location on the local file system where we store information about
# where ZooKeeper clients are located, used to backup and restore 
# AppController information.
ZK_LOCATIONS_FILE = "/etc/appscale/zookeeper_locations.json"


# Djinn (interchangeably known as 'the AppController') automatically
# configures and deploys all services for a single node. It relies on other
# Djinns or the AppScale Tools to tell it what services (roles) it should
# be hosting, and exposes these methods via a SOAP interface (as is provided
# in DjinnServer).
class Djinn
  # An Array of DjinnJobData objects, each of which containing information about
  # a node in the currently running AppScale deployment.
  attr_accessor :nodes
  

  # A Hash containing all the parameters needed to configure any service
  # on any node. At a minimum, this is all the information from the AppScale
  # Tools, including information about database parameters and the roles
  # for all nodes.
  attr_accessor :creds
  

  # An Array of Strings, each of which corresponding to the name of an App
  # Engine app that should be loaded.
  attr_accessor :app_names
  
  
  # An Array of Strings, each of which corresponding to the name of an App
  # Engine app that has been loaded on this node.
  attr_accessor :apps_loaded


  # An Array of Strings, each of which corresponding to the name of an App
  # Engine app that should be restarted on this node.
  attr_accessor :apps_to_restart


  # A boolean that is used to let remote callers know when this AppController
  # is done initializing itself, but not necessarily done starting or 
  # stopping roles.
  attr_accessor :done_initializing


  # A boolean that is used to let remote callers know when this AppController
  # is done starting all the services it is responsible for.
  attr_accessor :done_loading


  # The port that nginx will listen to for the next App Engine app that is
  # uploaded into the system.
  attr_accessor :nginx_port
  

  # The port that haproxy will listen to for the next App Engine app that is
  # uploaded into the system.
  attr_accessor :haproxy_port
  
  
  # The public IP address (or FQDN) that the UserAppServer can be found at,
  # initally set to a dummy value to tell callers not to use it until a real
  # value is set.
  attr_accessor :userappserver_public_ip 


  # The public IP address (or FQDN) that the UserAppServer can be found at,
  # initally set to a dummy value to tell callers not to use it until a real
  # value is set.
  attr_accessor :userappserver_private_ip
  
  
  # The human-readable state that this AppController is in.
  attr_accessor :state
  
 
  # A boolean that is used to let remote callers start the shutdown process
  # on this AppController, which will cleanly shut down and terminate all
  # services on this node.
  attr_accessor :kill_sig_received 


  # An Integer that indexes into @nodes, to return information about this node.
  attr_accessor :my_index
  
  
  # The number of dev_appservers that should run for every App Engine
  # application.
  attr_accessor :num_appengines
  
  
  # A boolean that indicates if we are done restoring state from a previously
  # running AppScale deployment.
  attr_accessor :restored


  # A Hash that lists the status of each Google App Engine API made available
  # within AppScale. Keys are the names of the APIs (e.g., memcache), and
  # values are the statuses of those APIs (e.g., running).
  attr_accessor :api_status


  # An Array that lists the CPU, disk, and memory usage of each machine in this
  # AppScale deployment. Used as a cache so that it does not need to be
  # generated in response to AppDashboard requests.
  attr_accessor :all_stats


  # An integer timestamp that corresponds to the last time this AppController
  # has updated @nodes, which we use to compare with a similar timestamp in
  # ZooKeeper to see when data in @nodes has changed on other nodes.
  attr_accessor :last_updated


  # A Hash that contains information about each Google App Engine application
  # running in this deployment. It includes information about the nginx and
  # haproxy ports the app uses, as well as the language the app is written
  # in.
  attr_accessor :app_info_map


  # A lock that should be used whenever we modify internal state that can be
  # modified by more than one thread at a time.
  attr_accessor :state_change_lock


  # A Hash that maps the names of Google App Engine apps running in this AppScale
  # deployment to the total number of requests that haproxy has processed.
  attr_accessor :total_req_rate


  # A Hash that maps the names of Google App Engine apps running in this AppScale
  # deployment to the last time we sampled the total number of requests that
  # haproxy has processed. When combined with total_req_rate, we can infer the
  # average number of requests per second that come in for each App Engine
  # application.
  attr_accessor :last_sampling_time


  # A Time that corresponds to the last time this machine added or removed nodes
  # in this AppScale deployment. Adding or removing nodes can happen in response
  # to autoscaling requests, or (eventually) to recover from faults.
  attr_accessor :last_scaling_time


  # The port that the AppController runs on by default
  SERVER_PORT = 17443


  # The port that SSH connections are hosted over, by default.
  SSH_PORT = 22


  # A boolean that should be used when we are waiting for a specific port
  # to open, and only if that port needs SSL to talk over it.
  USE_SSL = true


  # A boolean that indicates whether or not we should turn the firewall on,
  # and continuously keep it on. Should definitely be on for releases, and
  # on whenever possible.
  FIREWALL_IS_ON = true


  # The location on the local filesystem where AppScale-related configuration
  # files are written to.
  CONFIG_FILE_LOCATION = "/etc/appscale"


  # The location on the local filesystem where the AppController writes
  # information about the status of App Engine APIs, which the AppDashboard
  # will read and display to users.
  HEALTH_FILE = "#{CONFIG_FILE_LOCATION}/health.json"


  # The location on the local filesystem where the AppController periodically
  # writes its state to, and recovers its state from if it crashes.
  STATE_FILE = "#{CONFIG_FILE_LOCATION}/appcontroller-state.json"

  
  # The location on the local filesystem where the AppController writes
  # the location of all the nodes which are taskqueue nodes.
  TASKQUEUE_FILE = "#{CONFIG_FILE_LOCATION}/taskqueue_nodes"


  APPSCALE_HOME = ENV['APPSCALE_HOME']


  # The location on the local filesystem where we save data that should be
  # persisted across AppScale deployments. Currently this is Cassandra data,
  # ZooKeeper data, and Google App Engine apps that users upload.
  PERSISTENT_MOUNT_POINT = "/opt/appscale"


  # The location where we can find the Python 2.7 executable, included because
  # it is not the default version of Python installed on AppScale VMs.
  PYTHON27 = "/usr/local/Python-2.7.3/python"


  # The message that we display to the user if they call a SOAP-accessible
  # function with a malformed input (e.g., of the wrong class or format).
  BAD_INPUT_MSG = JSON.dump({'success' => false, 'message' => 'bad input'})


  # The message to display to users if they try to add nodes to a one node
  # deployment, which currently is not supported.
  CANT_SCALE_FROM_ONE_NODE = JSON.dump({
    'success' => false, 
    'message' => "can't scale up from a one node deployment"
  })


  # The message that we display to the user if they want to scale up services
  # in an Xen/KVM deployment but don't have enough open nodes to do so.
  NOT_ENOUGH_OPEN_NODES = JSON.dump({'success' => false, 
    'message' => 'not enough open nodes'})


  # The options that should be used when invoking wget, so that the
  # AppController can automatically probe a site to see if it's up.
  WGET_OPTIONS = "--tries=1000 --no-check-certificate -q -O /dev/null"


  # How often we should attempt to increase the number of AppServers on a
  # given node.
  SCALEUP_TIME_THRESHOLD = 60  # seconds
  
  
  # How often we should attempt to decrease the number of AppServers on a
  # given node.
  SCALEDOWN_TIME_THRESHOLD = 300  # seconds


  # The size of the rotating buffers that we use to keep information on
  # the request rate and number of enqueued requests.
  NUM_DATA_POINTS = 10


  # The minimum number of AppServers (for all applications) that should be run
  # on this node.
  MIN_APPSERVERS_ON_THIS_NODE = 1


  # The maximum number of AppServers (for all applications) that should be run
  # on this node.
  MAX_APPSERVERS_ON_THIS_NODE = 10


  # The position in the haproxy profiling information where the name of
  # the service (e.g., the frontend or backend) is specified.
  SERVICE_NAME_INDEX = 1


  # The position in the haproxy profiling information where the number of
  # enqueued requests is specified.
  REQ_IN_QUEUE_INDEX = 2


  # The position in the haproxy profiling information where the total number of
  # requests seen for a given app is specified.
  TOTAL_REQUEST_RATE_INDEX = 48


  # Scales up the number of AppServers used to host an application if the
  # request rate rises above this value.
  SCALEUP_REQUEST_RATE_THRESHOLD = 5


  # Scales down the number of AppServers used to host an application if the
  # request rate falls below this value.
  SCALEDOWN_REQUEST_RATE_THRESHOLD = 2


  # The minimum number of requests that have to sit in haproxy's wait queue for
  # an App Engine application before we will scale up the number of AppServers 
  # that serve that application.
  SCALEUP_QUEUE_SIZE_THRESHOLD = 5


  # The path to the file where we will store information about AppServer
  # scaling decisions.
  AUTOSCALE_LOG_FILE = "/var/log/appscale/autoscale.log"
  

  # CPU limits that determine when to stop adding AppServers on a node. Because
  # AppServers in different languages consume different amounts of CPU, set
  # different limits per language.
  MAX_CPU_FOR_APPSERVERS = {'python' => 80.00, 'python27' => 90.00, 'java' => 75.00, 'go' => 70.00}


  # Memory limits that determine when to stop adding AppServers on a node. 
  # Because AppServers in different languages consume different amounts of 
  # memory, set different limits per language.
  MAX_MEM_FOR_APPSERVERS = {'python' => 90.00, 'python27' => 90.00, 'java' => 95.00, 'go' => 90.00}


  # A regular expression that can be used to match any character that is not
  # acceptable to use in a hostname:port string, used to filter out unacceptable
  # characters from user input.
  NOT_FQDN_REGEX = /[^\w\d\.:\/_-]/


  # A regular expression that can be used to match any character that is not
  # acceptable to use in a hostname:port string, while also allowing the +
  # character to be used. This is used to filter out unacceptable characters
  # from user input where the plus sign is acceptable.
  NOT_FQDN_OR_PLUS_REGEX = /[^\w\d\.\+:\/_-]/


  # A regular expression that can be used to match any character that is not
  # acceptable to use in a e-mail address, used to filter out unacceptable
  # characters from user input.
  NOT_EMAIL_REGEX = /[^\w\d_@-]/


  # An Integer that determines how many log messages we should send at a time
  # to the AppDashboard, for later viewing.
  LOGS_PER_BATCH = 25


  # Creates a new Djinn, which holds all the information needed to configure
  # and deploy all the services on this node.
  def initialize()
    # The password, or secret phrase, that is required for callers to access
    # methods exposed via SOAP.
    @@secret = HelperFunctions.get_secret()

    # An Array of Hashes, where each Hash contains a log message and the time
    # it was logged.
    @@logs_buffer = []

    STDOUT.sync = true
    @@log = Logger.new(STDOUT)
    @@log.level = Logger::DEBUG

    @nodes = []
    @my_index = nil
    @creds = {}
    @app_names = []
    @apps_loaded = []
    @apps_to_restart = []
    @kill_sig_received = false
    @done_initializing = false
    @done_loading = false
    @nginx_port = Nginx::START_PORT
    @haproxy_port = HAProxy::START_PORT
    @appengine_port = 20000
    @userappserver_public_ip = "not-up-yet"
    @userappserver_private_ip = "not-up-yet"
    @state = "AppController just started"
    @num_appengines = 1
    @restored = false
    @api_status = {}
    @all_stats = []
    @last_updated = 0
    @state_change_lock = Monitor.new()
    @app_info_map = {}

    @scaling_in_progress = false
    @last_decision = {}
    @initialized_apps = {}
    @total_req_rate = {}
    @last_sampling_time = {}
    @last_scaling_time = Time.now.to_i
  end


  # A SOAP-exposed method that callers can use to determine if this node
  # has received information from another node and is starting up.
  def is_done_initializing(secret)
    if valid_secret?(secret)
      return @done_initializing
    else
      return BAD_SECRET_MSG
    end
  end


  # A SOAP-exposed method that callers use to determine if this node has
  # finished starting all the roles it should run when it initially starts.
  def is_done_loading(secret)
    if valid_secret?(secret)
      return @done_loading
    else
      return BAD_SECRET_MSG
    end
  end


  # A SOAP-exposed method that callers can use to get information about what
  # roles each node in the AppScale deployment are running.
  def get_role_info(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    all_nodes = []
    @nodes.each { |node|
      all_nodes << node.to_hash()
    }

    return JSON.dump(all_nodes)
  end


  def kill(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end
    @kill_sig_received = true
    
    if is_hybrid_cloud? 
      Thread.new {
        Kernel.sleep(5)
        HelperFunctions.terminate_hybrid_vms(creds)
      }
    elsif is_cloud?
      Thread.new {
        Kernel.sleep(5)
        infrastructure = creds["infrastructure"]
        keyname = creds["keyname"]
        HelperFunctions.terminate_all_vms(infrastructure, keyname)
      }
    else
      # in xen/kvm deployments we actually want to keep the boxes
      # turned on since that was the state they started in

      if my_node.is_login?
        stop_ejabberd 
      end

      if my_node.is_shadow? or my_node.is_appengine?
        ApiChecker.stop 
      end

      #maybe_stop_taskqueue_worker("apichecker")
      maybe_stop_taskqueue_worker(AppDashboard::APP_NAME)

      jobs_to_run = my_node.jobs
      commands = {
        "load_balancer" => "stop_app_dashboard",
        "appengine" => "stop_appengine",
        "db_master" => "stop_db_master",
        "db_slave" => "stop_db_slave",
        "zookeeper" => "stop_zookeeper"
      }

      my_node.jobs.each { |job|
        if commands.include?(job)
          Djinn.log_info("About to run [#{commands[job]}]")
          send(commands[job].to_sym)
        else
          Djinn.log_info("Unable to find command for job #{job}. Skipping it.")
        end
      }

      if has_soap_server?(my_node)
        stop_soap_server
        stop_datastore_server
      end

      TaskQueue.stop if my_node.is_taskqueue_master?
      TaskQueue.stop if my_node.is_taskqueue_slave?
      TaskQueue.stop_flower if my_node.is_login?

      stop_app_manager_server
      stop_infrastructure_manager
    end

    GodInterface.shutdown
    FileUtils.rm_rf(STATE_FILE)

    if @creds['alter_etc_resolv'].downcase == "true"
      HelperFunctions.restore_etc_resolv()
    end

    return "OK"  
  end
 

  # Validates and sets the instance variables that Djinn needs before it can
  # begin configuring and deploying services on a given node (and if it is the
  # first Djinn, starting up the other Djinns).
  def set_parameters(djinn_locations, database_credentials, app_names, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    if djinn_locations.class != String
      msg = "Error: djinn_locations wasn't a String, but was a " +
        djinn_locations.class.to_s
      Djinn.log_error(msg)
      return msg
    end
    locations = JSON.load(djinn_locations)

    if database_credentials.class != Array
      msg = "Error: database_credentials wasn't an Array, but was a " +
        database_credentials.class.to_s
      Djinn.log_error(msg)
      return msg
    end

    if app_names.class != Array
      msg = "Error: app_names wasn't an Array, but was a " + 
        app_names.class.to_s
      Djinn.log_error(msg)
      return msg
    end

    # credentials is an array that we're converting to
    # hash tables, so we need to make sure that every key maps to a value
    # e.g., ['foo', 'bar'] becomes {'foo' => 'bar'}
    # so we need to make sure that the array has an even number of elements
        
    if database_credentials.length % 2 != 0
      error_msg = "Error: DB Credentials wasn't of even length: Len = " + \
        "#{database_credentials.length}"
      Djinn.log_error(error_msg)
      return error_msg
    end

    possible_credentials = Hash[*database_credentials]
    if !valid_format_for_credentials(possible_credentials)
      return "Error: Credential format wrong"
    end

    keyname = possible_credentials["keyname"]
    @nodes = Djinn.convert_location_array_to_class(locations, keyname)
    @creds = possible_credentials
    @app_names = app_names
    
    convert_fqdns_to_ips
    @creds = sanitize_credentials

    find_me_in_locations
    if @my_index.nil?
      return "Error: Couldn't find me in the node map"
    end

    ENV['EC2_URL'] = @creds['ec2_url']

    if @creds['ec2_access_key'].nil?
      @creds['ec2_access_key'] = @creds['EC2_ACCESS_KEY']
      @creds['ec2_secret_key'] = @creds['EC2_SECRET_KEY']
      @creds['ec2_url'] = @creds['EC2_URL']
    end

    if @creds['alter_etc_resolv'].downcase == "true"
      HelperFunctions.alter_etc_resolv()
    end

    if @creds['clear_datastore'].class == String
      @creds['clear_datastore'] = @creds['clear_datastore'].downcase == "true"
    end
    Djinn.log_debug("clear_datastore is set to #{@creds['clear_datastore']}, " +
      "of class #{@creds['clear_datastore'].class.name}")

    if @creds['verbose'].downcase == "false"
      @@log.level = Logger::INFO
    end

    begin
      @creds['zone'] = JSON.load(@creds['zone'])
    rescue JSON::ParserError
    end

    Djinn.log_run("mkdir -p /opt/appscale/apps")

    return "OK"
  end


  # Validates and sets the list of applications that should be loaded on this
  # node.
  def set_apps(app_names, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    if app_names.class != Array
      return "app names was not an Array but was a #{app_names.class}"
    end

    @app_names = app_names
    return "App names is now #{@app_names.join(', ')}"
  end

  # Gets the status of the current node in the AppScale deployment
  # 
  # Args:
  #   secret: The shared key for authentication
  # Returns:
  #   A string with the current node's status
  # 
  def status(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    stats = get_stats(secret)

    stats_str = <<-STATUS
    Currently using #{stats['cpu']} Percent CPU and #{stats['memory']} Percent Memory
    Hard disk is #{stats['disk']} Percent full
    Is currently: #{stats['roles'].join(', ')}
    Database is at #{stats['db_location']}
    Is in cloud: #{stats['cloud']}
    Current State: #{stats['state']}
    STATUS

    if my_node.is_appengine?
      app_names = []
      stats['apps'].each { |k, v|
        app_names << k
      }

      stats_str << "    Hosting the following apps: #{app_names.join(', ')}\n"

      stats['apps'].each { |app_name, is_loaded|
        next if !is_loaded
        next if app_name == "none"
        if !@app_info_map[app_name]['appengine'].nil?
          stats_str << "    The number of AppServers for app #{app_name} is: " +
            "#{@app_info_map[app_name]['appengine'].length}\n"
        end
      }
    end
 
    return stats_str
  end

  # Upload a Google App Engine application into this AppScale deployment.
  # 
  # Args:
  #   tgz_file: A String, with the path to the tar.gz file containing the app.
  #   email: A String with the email address of the user that will own this application.
  #   secret: A String with the shared key for authentication.
  # Returns:
  #   A String that indicates if the application was successfully uploaded, and
  #   if not, the reason why the upload failed.
  def upload_tgz_file(tgz_file, email, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    if !tgz_file.match(TAR_GZ_REGEX)
      tgz_file_old = tgz_file
      tgz_file = "#{tgz_file_old}.tar.gz"
      File.rename(tgz_file_old, tgz_file)
    end

    begin
      keyname = @creds['keyname']
      Timeout.timeout(APP_UPLOAD_TIMEOUT) do
        command = "#{APPSCALE_TOOLS_HOME}/bin/appscale-upload-app --file " +
          "#{tgz_file} --email #{email} --keyname #{keyname} 2>&1"
        output = Djinn.log_run("#{command}").chomp
        File.delete(tgz_file)
        if output.include?("uploaded successfully")
          result = "true"
        else
          result = output
        end
      end
    rescue Timeout::Error
      Djinn.log_error("Uploading App Engine app timed out: #{output}")
      result = "The request has timed out. Large applications should be " +
        "uploaded via the command line."
    end
    return result
  end    

  # Gets the statistics of all the nodes in the AppScale deployment.
  # 
  # Args:
  #   secret: A string with the shared key for authentication.
  # Returns:
  #   A JSON string with the statistics of the nodes.
  def get_stats_json(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    return JSON.dump(@all_stats)
  end


  # Updates our locally cached information about the CPU, memory, and disk
  # usage of each machine in this AppScale deployment.
  def update_node_info_cache()
    @all_stats = []
    @nodes.each { |node|
      ip = node.private_ip
      acc = AppControllerClient.new(ip, @@secret)

      begin
        @all_stats << acc.get_stats(@@secret)
      rescue FailedNodeException
        Djinn.log_warn("Failed to get status update from node at #{ip}, so " +
          "not adding it to our cached info.")
      end
    }
  end


  # Gets the database information of the AppScale deployment.
  # 
  # Args:
  #   secret: A string with the shared key for authentication.
  # Returns:
  #   A JSON string with the database information.
  def get_database_information(secret)
    tree = { :table => @creds["table"], :replication => @creds["replication"],
      :keyname => @creds["keyname"] }
    return JSON.dump(tree)
  end

  # Gets the statistics of only this node.
  # 
  # Args:
  #   secret: A string with the shared key for authentication.
  # Returns:
  #   A Hash with the statistics of this node.
  def get_stats(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    usage = HelperFunctions.get_usage
    mem = sprintf("%3.2f", usage['mem'])

    jobs = my_node.jobs or ["none"]
    # don't use an actual % below, or it will cause a string format exception
    stats = {
      'ip' => my_node.public_ip,
      'cpu' => usage['cpu'],
      'memory' => mem,
      'disk' => usage['disk'],
      'roles' => jobs,
      'db_location' => @userappserver_public_ip,
      'cloud' => my_node.cloud,
      'state' => @state
    }

    stats['apps'] = {}
    @app_names.each { |name|
      stats['apps'][name] = @apps_loaded.include?(name)
    }
    return stats
  end


  # Runs the Groomer service that the Datastore provides, which cleans up
  # deleted entries and generates statistics about the entities stored for each
  # application.
  #
  # Args:
  #   secret: A String with the shared key for authentication.
  # Returns:
  #   'OK' if the groomer was invoked, and BAD_SECRET_MSG if the user failed to
  #   authenticate correctly.
  def run_groomer(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    Thread.new {
      run_groomer_command = "cd /root/appscale/AppDB && python groomer.py"
      if my_node.is_db_master?
        Djinn.log_run(run_groomer_command)
      else
        db_master = get_db_master
        HelperFunctions.run_remote_command(db_master.private_ip,
          run_groomer_command, db_master.ssh_key, NO_OUTPUT)
      end
    }

    return 'OK'
  end


  # Removes an application and stops all AppServers hosting this application.
  #
  # Args:
  #   app_name: The application to stop
  #   secret: Shared key for authentication
  #
  def stop_app(app_name, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    app_name.gsub!(/[^\w\d\-]/, "")
    Djinn.log_info("Shutting down app named [#{app_name}]")
    result = ""
    Djinn.log_run("rm -rf /var/apps/#{app_name}")
   
    # app shutdown process can take more than 30 seconds
    # so run it in a new thread to avoid 'execution expired'
    # error messages and have the tools poll it 
    Thread.new {
      # Tell other nodes to shutdown this application
      if @app_names.include?(app_name) and !my_node.is_appengine?
        @nodes.each { |node|
          next if node.private_ip == my_node.private_ip
          if node.is_appengine? or node.is_login?
            ip = node.private_ip
            acc = AppControllerClient.new(ip, @@secret)

            begin
              result = acc.stop_app(app_name)
              Djinn.log_debug("Removing application #{app_name} from #{ip} " +
                "returned #{result}")
            rescue FailedNodeException
              Djinn.log_warn("Could not remove application #{app_name} from " +
                "#{ip} - moving on to other nodes.")
            end
          end
        }
      end

      # Contact the soap server and remove the application
      if (@app_names.include?(app_name) and !my_node.is_appengine?) or @nodes.length == 1
        ip = HelperFunctions.read_file("#{CONFIG_FILE_LOCATION}/masters")
        uac = UserAppClient.new(ip, @@secret)
        result = uac.delete_app(app_name)
        Djinn.log_debug("(stop_app) Delete app: #{ip} returned #{result} (#{result.class})")
      end
     
      # may need to stop XMPP listener
      if my_node.is_login? 
        pid_files = HelperFunctions.shell("ls #{CONFIG_FILE_LOCATION}/xmpp-#{app_name}.pid").split
        unless pid_files.nil? # not an error here - XMPP is optional
          pid_files.each { |pid_file|
            pid = HelperFunctions.read_file(pid_file)
            Djinn.log_run("kill -9 #{pid}")
          }

          result = "true"
        end
        stop_xmpp_for_app(app_name)
      end

      Djinn.log_debug("(stop_app) Maybe stopping taskqueue worker")
      maybe_stop_taskqueue_worker(app_name)
      Djinn.log_debug("(stop_app) Done maybe stopping taskqueue worker")

      APPS_LOCK.synchronize {
        if my_node.is_appengine?
          Djinn.log_debug("(stop_app) Calling AppManager for app #{app_name}")
          app_manager = AppManagerClient.new()
          if !app_manager.stop_app(app_name)
            Djinn.log_error("(stop_app) ERROR: Unable to stop app #{app_name}")
          else
            Djinn.log_info("(stop_app) AppManager shut down app #{app_name}")
          end

          Nginx.remove_app(app_name)
          HAProxy.remove_app(app_name)
          Nginx.reload
          ZKInterface.remove_app_entry(app_name, my_node.public_ip)

          # If this node has any information about AppServers for this app,
          # clear that information out.
          if !@app_info_map[app_name].nil?
            @app_info_map.delete(app_name)
          end

          result = "true"
        end

        @apps_loaded = @apps_loaded - [app_name]    
        @app_names = @app_names - [app_name]

        if @apps_loaded.empty?
          @apps_loaded << "none"
        end

        if @app_names.empty?
          @app_names << "none"
        end
      } # end of lock
    } # end of thread

    return "true"
  end

  # Stop taskqueue worker on this local machine.
  # 
  # Args:
  #   app: The application ID
  def maybe_stop_taskqueue_worker(app)
    if my_node.is_taskqueue_master? or my_node.is_taskqueue_slave?
      Djinn.log_info("Stopping TaskQueue workers for app #{app}")
      tqc = TaskQueueClient.new()
      result = tqc.stop_worker(app) 
      Djinn.log_info("Stopped TaskQueue workers for app #{app}: #{result}")
    end
  end

  # Start taskqueue worker on this local machine.
  # 
  # Args:
  #   app: The application ID
  def maybe_start_taskqueue_worker(app)
    if my_node.is_taskqueue_master? or my_node.is_taskqueue_slave?
      tqc = TaskQueueClient.new()
      result = tqc.start_worker(app) 
      Djinn.log_info("Starting TaskQueue worker for app #{app}: #{result}")
    end
  end


  def update(app_names, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end
    
    Djinn.log_info("Received request to update with apps: [#{app_names.join(', ')}]")
    current_apps_uploaded = @apps_loaded
    apps = @app_names - app_names + app_names
    apps.uniq!
    Djinn.log_debug("Will set apps to: [#{apps.join(', ')}]")
    
    # Begin by starting any new App Engine apps.
    @nodes.each_index { |index|
      ip = @nodes[index].private_ip
      acc = AppControllerClient.new(ip, @@secret)
      begin
        result = acc.set_apps(apps)
        Djinn.log_debug("Set apps at #{ip} returned #{result}.")
      rescue FailedNodeException
        Djinn.log_warn("Couldn't tell #{ip} to run new Google App Engine apps" +
          " - skipping for now.")
      end
    }

    # Next, restart any apps that have new code uploaded.
    apps_to_restart = current_apps_uploaded & app_names
    Djinn.log_debug("Apps to restart are #{apps_to_restart}")
    if !apps_to_restart.empty?
      apps_to_restart.each { |appid|
        ZKInterface.clear_app_hosters(appid)
        location = "/opt/appscale/apps/#{appid}.tar.gz"
        ZKInterface.add_app_entry(appid, my_node.public_ip, location)
      }

      @nodes.each_index { |index|
        ip = @nodes[index].private_ip
        acc = AppControllerClient.new(ip, @@secret)
        begin
          result = acc.set_apps_to_restart(apps_to_restart)
          Djinn.log_debug("Set apps to restart at #{ip} returned #{result} as class #{result.class}")
        rescue FailedNodeException
          Djinn.log_warn("Couldn't tell #{ip} to restart Google App Engine " +
            "apps - skipping for now.")
        end
      }
    end

    Djinn.log_debug("Done updating apps!")
    # now that another app is running we can take out 'none' from the list
    # if it was there (e.g., run-instances with no app given)
    @app_names = @app_names - ["none"]
    
    return "OK"
  end

  # Adds the list of apps that should be restarted to this node's list of apps
  # that should be restarted.
  #
  # Args:
  #   apps_to_restart: An Array of Strings, where each String is an appid
  #     corresponding to an application that should be restarted.
  #   secret: The String that authenticates the caller.
  # Returns:
  #   A String indicating that the SOAP call succeeded, or the reason why it
  #   did not.
  def set_apps_to_restart(apps_to_restart, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    APPS_LOCK.synchronize {
      @apps_to_restart += apps_to_restart
      @apps_to_restart.uniq!
    }
    Djinn.log_debug("Apps to restart is now [#{@apps_to_restart.join(', ')}]")

    return "OK"
  end

  def get_all_public_ips(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    public_ips = []
    @nodes.each { |node|
      public_ips << node.public_ip
    }
    return JSON.dump(public_ips)
  end

  def job_start(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    start_infrastructure_manager
    data_restored, need_to_start_jobs = restore_appcontroller_state

    if data_restored
      parse_creds
    else
      erase_old_data
      wait_for_data
      parse_creds
    end

    if need_to_start_jobs
      change_job
    end

    @done_loading = true
    write_our_node_info
    wait_for_nodes_to_finish_loading(@nodes)

    while !@kill_sig_received do
      @state = "Done starting up AppScale, now in heartbeat mode"
      write_database_info
      update_firewall
      write_memcache_locations
      write_zookeeper_locations
      update_api_status
      flush_log_buffer

      update_local_nodes

      if my_node.is_shadow?
        # Since we now backup state to ZK, don't make everyone do it.
        # The Shadow has the most up-to-date info, so let it handle this
        backup_appcontroller_state
      end

      # Login nodes host the AppDashboard app, which has links to each
      # of the apps running in AppScale. Update the files it reads to
      # reflect the most up-to-date info.
      if my_node.is_login?
        @nodes.each { |node|
          get_status(node)
        }

        update_node_info_cache()
      end

      #ensure_all_roles_are_running

      # TODO: consider only calling this if new apps are found
      start_appengine
      restart_appengine_apps
      scale_appservers_on_this_node
      scale_appservers_across_nodes
      send_instance_info_to_dashboard
      Kernel.sleep(20)
    end
  end


  # Examines the list of applications that need to be updated, and restarts them
  # on the same ports that they were previously running on.
  def restart_appengine_apps
    # use a copy of @apps_to_restart here since we delete from it in
    # setup_appengine_application
    apps = @apps_to_restart
    apps.each { |app_name|
      if !my_node.is_login?  # this node has the new app - don't erase it here
        Djinn.log_info("Removing old version of app #{app_name}")
        Djinn.log_run("rm -fv /opt/appscale/apps/#{app_name}.tar.gz")
      end
      Djinn.log_info("About to restart app #{app_name}")
      APPS_LOCK.synchronize {
        setup_appengine_application(app_name, is_new_app=false)
      }
    }
  end


  # Starts the InfrastructureManager service on this machine, which exposes
  # a SOAP interface by which we can dynamically add and remove nodes in this
  # AppScale deployment.
  def start_infrastructure_manager
    if HelperFunctions.is_port_open?("localhost", 
      InfrastructureManagerClient::SERVER_PORT, HelperFunctions::USE_SSL)

      Djinn.log_debug("InfrastructureManager is already running locally - " +
        "don't start it again.")
      return
    end

    start_cmd = "python2.6 #{APPSCALE_HOME}/InfrastructureManager/infrastructure_manager_service.py"
    stop_cmd = "pkill -9 infrastructure_manager_service"
    port = [InfrastructureManagerClient::SERVER_PORT]
    env = {
      'APPSCALE_HOME' => APPSCALE_HOME,
      'EC2_HOME' => ENV['EC2_HOME'],
      'JAVA_HOME' => ENV['JAVA_HOME']
    }

    GodInterface.start(:iaas_manager, start_cmd, stop_cmd, port, env)
    Djinn.log_info("Started InfrastructureManager successfully!")
  end


  def stop_infrastructure_manager
    Djinn.log_info("Stopping InfrastructureManager")
    GodInterface.stop(:iaas_manager)
  end


  def write_cloud_info()
    cloud_info = {
      'is_cloud?' => is_cloud?(), 
      'is_hybrid_cloud?' => is_hybrid_cloud?()
    }

    HelperFunctions.write_json_file("#{CONFIG_FILE_LOCATION}/cloud_info.json", cloud_info)
  end


  def get_online_users_list(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    online_users = []

    login_node = get_login
    ip = login_node.public_ip
    key = login_node.ssh_key
    raw_list = `ssh -i #{key} -o StrictHostkeyChecking=no root@#{ip} 'ejabberdctl connected-users'`
    raw_list.split("\n").each { |userdata|
      online_users << userdata.split("/")[0]
    }

    return online_users
  end

  def done_uploading(appname, location, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    if File.exists?(location)
      ZKInterface.add_app_entry(appname, my_node.public_ip, location)
      result = "success"
    else
      result = "The #{appname} app was not found at #{location}."
    end

    Djinn.log_debug(result)
    return result
  end

  def is_app_running(appname, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    hosters = ZKInterface.get_app_hosters(appname, @creds['keyname'])
    hosters_w_appengine = []
    hosters.each { |node|
      hosters_w_appengine << node if node.is_appengine?
    }
 
    app_running = !hosters_w_appengine.empty?
    Djinn.log_debug("Is app #{appname} running? #{app_running}")
    return app_running
  end


  def add_role(new_role, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    # new roles may run indefinitely in the background, so don't block
    # on them - just fire and forget
    Thread.new {
      start_roles = new_role.split(":")
      start_roles.each { |role|
        # only start roles that we aren't already running
        # e.g., don't start_appengine if we already are, as this
        # will create two threads loading apps
        if my_node.jobs.include?(role)
          Djinn.log_info("Already running role #{role}, not invoking again")
        else
          Djinn.log_info("Adding and starting role #{role}")
          my_node.add_roles(role)
          send("start_#{role}".to_sym)
        end
      }
    }

    return "OK"
  end

  def remove_role(old_role, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    my_node.remove_roles(old_role)
    stop_roles = old_role.split(":")
    stop_roles.each { |role|
      Djinn.log_info("Removing and stopping role #{role}")
      send("stop_#{role}".to_sym)
    }
    return "OK"
  end


  # This SOAP-exposed method dynamically scales up a currently running
  # AppScale deployment. For virtualized clusters, this assumes the
  # user has given us a list of IP addresses where AppScale has been
  # installed to, and for cloud deployments, we assume that the user
  # wants to use the same credentials as for their current deployment.
  # Args:
  #   ips_hash: A Hash that maps roles (e.g., appengine, database) to the
  #     IP address (in virtualized deployments) or unique identifier (in
  #     cloud deployments) that should run that role.
  #   secret: A String password that is used to authenticate the request
  #     to add nodes to the deployment.
  # Returns:
  #   BAD_SECRET_MSG: If the secret given does not match the secret for
  #     this AppScale deployment.
  #   BAD_INPUT_MSG: If ips_hash was not a Hash.
  #   Otherwise, returns a Hash that maps IP addresses to the roles that
  #     will be hosted on them (the inverse of ips_hash).
  def start_roles_on_nodes(ips_hash, secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    ips_hash = JSON.load(ips_hash)
    if ips_hash.class != Hash
      Djinn.log_warn("Was expecting ips_hash to be a Hash, not " +
        "a #{ips_hash.class}")
      return BAD_INPUT_MSG
    end

    if @nodes.length == 1
      Djinn.log_warn("Can't scale up in a one node deployment.")
      return CANT_SCALE_FROM_ONE_NODE
    end

    Djinn.log_info("Received a request to start additional roles on " +
      "new machines, with the following placement strategy: " +
      "#{ips_hash.inspect}")

    # ips_hash maps roles to IPs, but the internal format here maps
    # IPs to roles, so convert to the right format
    ips_to_roles = {}
    ips_hash.each { |role, ip_or_ips|
      if ip_or_ips.class == String
        ips = [ip_or_ips]  # just one IP
      else
        ips = ip_or_ips  # a list of IPs
      end

      ips.each { |ip|
        if ips_to_roles[ip].nil?
          ips_to_roles[ip] = []
        end
        ips_to_roles[ip] << role
      }
    }

    Thread.new {
      if is_cloud?
        start_new_roles_on_nodes_in_cloud(ips_to_roles)
      else
        start_new_roles_on_nodes_in_xen(ips_to_roles)
      end
    }

    return ips_to_roles
  end


  # This method acquires virtual machines from a cloud IaaS and adds them
  # to the currently running AppScale deployment. The new machines are then
  # assigned the roles given to us by the caller.
  # Args:
  #   ips_to_roles: A Hash that maps machines to the roles that should be
  #     started on them. As we have not yet spawned the machines, we do not
  #     have IP addresses for them, so any unique identifier can be used in
  #     lieu of IP addresses.
  # Returns:
  #   An Array of Strings, where each String contains information about the
  #     public IP address, private IP address, and roles that the new machines
  #     have taken on.
  def start_new_roles_on_nodes_in_cloud(ips_to_roles)
    Djinn.log_info("Starting new roles in cloud with following info: " +
      "#{ips_to_roles.inspect}")

    keyname = @creds['keyname']
    num_of_vms = ips_to_roles.keys.length
    roles = ips_to_roles.values
    disks = Array.new(size=num_of_vms, obj=nil)  # no persistent disks
    Djinn.log_info("Need to spawn up #{num_of_vms} VMs")
    imc = InfrastructureManagerClient.new(@@secret)

    begin
      new_nodes_info = imc.spawn_vms(num_of_vms, @creds, roles, disks)
    rescue AppScaleException => exception
      HelperFunctions.log_and_crash("Couldn't spawn #{num_of_vms} VMs with " +
        "roles #{roles} because: #{exception.message}")
    end

    # initialize them and wait for them to start up
    Djinn.log_debug("info about new nodes is " +
      "[#{new_nodes_info.join(', ')}]")

    add_nodes(new_nodes_info)
    update_hosts_info()

    if my_node.is_login?
      regenerate_nginx_config_files()
    end

    return new_nodes_info
  end


  # This method takes a list of IP addresses that correspond to machines
  # with AppScale installed on them, that have passwordless SSH already
  # set up (presumably by appscale-add-instances). The machines are added
  # to the currently running AppScale deployment, and are then assigned
  # the roles given to us by the caller.
  # Args:
  #   ips_to_roles: A Hash that maps machines to the roles that should be
  #     started on them. Machines are uniquely identified by their IP
  #     address, which is assumed to be reachable from any node in the
  #     AppScale deployment.
  # Returns:
  #   An Array of Strings, where each String contains information about the
  #     public IP address, private IP address, and roles that the new machines
  #     have taken on.
  def start_new_roles_on_nodes_in_xen(ips_to_roles)
    Djinn.log_info("Starting new roles in virt with following info: " +
      "#{ips_to_roles.inspect}")

    nodes_info = []
    keyname = @creds['keyname']
    ips_to_roles.each { |ip, roles|
      Djinn.log_info("Will add roles #{roles.join(', ')} to new " +
        "node at IP address #{ip}")
      nodes_info << {
        "public_ip" => ip,
        "private_ip" => ip,
        "jobs" => roles,
        "disk" => nil
      }
    }

    add_nodes(nodes_info)
    update_hosts_info()

    if my_node.is_login?
      regenerate_nginx_config_files()
    end

    return nodes_info
  end


  # Starts the given roles by using open nodes, spawning new nodes, or some
  # combination of the two. 'nodes_needed' should be an Array, where each 
  # item is an Array of the roles to start on each node.
  def start_new_roles_on_nodes(nodes_needed, instance_type, secret)
     if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    if nodes_needed.class != Array
      Djinn.log_error("Was expecting nodes_needed to be an Array, not " +
        "a #{nodes_needed.class}")
      return BAD_INPUT_MSG
    end

    Djinn.log_info("Received a request to acquire nodes with roles " +
      "#{nodes_needed.join(', ')}, with instance type #{instance_type} for " +
      "new nodes")

    vms_to_use = []
    ZKInterface.lock_and_run {
      num_of_vms_needed = nodes_needed.length

      @nodes.each_with_index { |node, index|
        if node.is_open?
          Djinn.log_info("Will use node #{node} to run new roles")
          node.jobs = nodes_needed[vms_to_use.length]
          vms_to_use << node

          if vms_to_use.length == nodes_needed.length
            Djinn.log_info("Only using open nodes to run new roles")
            break
          end
        end
      }

      vms_to_spawn = nodes_needed.length - vms_to_use.length
    
      if vms_to_spawn > 0 and !is_cloud?
        Djinn.log_error("Still need #{vms_to_spawn} more nodes, but we " +
        "aren't in a cloud environment, so we can't acquire more nodes - " +
        "failing the caller's request.")
        return NOT_ENOUGH_OPEN_NODES
      end

      if vms_to_spawn > 0
        Djinn.log_info("Need to spawn up #{vms_to_spawn} VMs")
        # Make sure the user has said it is ok to add more VMs before doing so.
        allowed_vms = Integer(@creds['max_images']) - @nodes.length
        if allowed_vms < vms_to_spawn
          Djinn.log_info("Can't spawn up #{vms_to_spawn} VMs, because that " +
            "would put us over the user-specified limit of #{@creds['max']} " +
            "VMs. Instead, spawning up #{allowed_vms}.")
          vms_to_spawn = allowed_vms
          if vms_to_spawn.zero?
            Djinn.log_error("Reached the maximum number of VMs that we " +
              "can use in this cloud deployment, so not spawning more nodes.")
            return "Reached maximum number of VMs we can use."
          end
        end

        disks = Array.new(size=vms_to_spawn, obj=nil)  # no persistent disks

        # start up vms_to_spawn vms as open
        imc = InfrastructureManagerClient.new(@@secret)
        begin
          new_nodes_info = imc.spawn_vms(vms_to_spawn, @creds, "open", disks)
        rescue AppScaleException => exception
          HelperFunctions.log_and_crash("Couldn't spawn #{vms_to_spawn} VMs " +
            "with roles open because: #{exception.message}")
        end


        # initialize them and wait for them to start up
        Djinn.log_debug("info about new nodes is " +
          "[#{new_nodes_info.join(', ')}]")
        add_nodes(new_nodes_info)

        # add information about the VMs we spawned to our list, which may
        # already have info about the open nodes we want to use
        new_nodes = Djinn.convert_location_array_to_class(new_nodes_info,
          @creds['keyname'])
        vms_to_use << new_nodes
        vms_to_use.flatten!
      end
    }

    wait_for_nodes_to_finish_loading(vms_to_use)
    
    nodes_needed.each_index { |i|
      Djinn.log_info("Adding roles #{nodes_needed[i].join(', ')} " +
        "to virtual machine #{vms_to_use[i]}")
      ZKInterface.add_roles_to_node(nodes_needed[i], vms_to_use[i],
        @creds['keyname'])
    }

    wait_for_nodes_to_finish_loading(vms_to_use)

    return "OK"
  end


  # Given an Array of Strings containing information about machines with
  # AppScale installed on them, copies over deployment-specific files
  # and starts the AppController on them. Each AppController is then
  # instructed to start a specific set of roles, and join the existing
  # AppScale deployment.
  # Args:
  #   node_info: An Array of Strings, where each String has information
  #     about a node to add to the current AppScale deployment (e.g.,
  #     IP addresses, roles to run).
  def add_nodes(node_info)
    keyname = @creds['keyname']
    new_nodes = Djinn.convert_location_array_to_class(node_info, keyname)

    # Since an external thread can modify @nodes, let's put a lock around
    # it to prevent race conditions.
    @state_change_lock.synchronize {
      @nodes.concat(new_nodes)
      Djinn.log_debug("Changed nodes to #{@nodes}")
    }

    update_firewall
    initialize_nodes_in_parallel(new_nodes)
  end


  # Cleans out temporary files that may have been written by a previous
  # AppScale deployment.
  def erase_old_data()
    Djinn.log_run("rm -rf /tmp/h*")
    Djinn.log_run("rm -f ~/.appscale_cookies")
    Djinn.log_run("rm -f #{APPSCALE_HOME}/.appscale/status-*")
    Djinn.log_run("rm -f #{APPSCALE_HOME}/.appscale/database_info")

    Nginx.clear_sites_enabled
    HAProxy.clear_sites_enabled
    Djinn.log_run("echo '' > /root/.ssh/known_hosts") # empty it out but leave the file there
    CronHelper.clear_crontab
  end


  def wait_for_nodes_to_finish_loading(nodes)
    Djinn.log_info("Waiting for nodes to finish loading")

    nodes.each { |node|
      if ZKInterface.is_node_done_loading?(node.public_ip)
        Djinn.log_info("Node at #{node.public_ip} has finished loading.")
        next
      else
        Djinn.log_info("Node at #{node.public_ip} has not yet finished " +
          "loading - will wait for it to finish.")
        Kernel.sleep(30)
        retry
      end
    }

    Djinn.log_info("Nodes have finished loading")
    return
  end


  # This method logs a message that is useful to know when debugging AppScale,
  # but is too extraneous to know when AppScale normally runs.
  #
  # Messages are logged both to STDOUT as well as to @@logs_buffer, which is
  # sent to the AppDashboard for viewing via a web UI.
  #
  # Args:
  #   message: A String containing the message to be logged.
  def self.log_debug(message)
    @@log.debug(message)
    self.log_to_buffer(Logger::DEBUG, message)
  end


  # This method logs a message that is useful to know when AppScale normally
  # runs.
  #
  # Args:
  #   message: A String containing the message to be logged.
  def self.log_info(message)
    @@log.info(message)
    self.log_to_buffer(Logger::INFO, message)
  end


  # This method logs a message that is useful to know when the AppController
  # experiences an unexpected event.
  #
  # Args:
  #   message: A String containing the message to be logged.
  def self.log_warn(message)
    @@log.warn(message)
    self.log_to_buffer(Logger::WARN, message)
  end


  # This method logs a message that corresponds to an erroneous, but
  # recoverable, event.
  #
  # Args:
  #   message: A String containing the message to be logged.
  def self.log_error(message)
    @@log.error(message)
    self.log_to_buffer(Logger::ERROR, message)
  end


  # This method logs a message that immediately precedes the death of this
  # AppController.
  #
  # Args:
  #   message: A String containing the message to be logged.
  def self.log_fatal(message)
    @@log.fatal(message)
    self.log_to_buffer(Logger::FATAL, message)
  end


  # Appends this log message to a buffer, which will be periodically sent to
  # the AppDashbord.
  #
  # Only sends the message if it has content (as some empty messages are the
  # result of exec'ing commands that produce no output), and if its log level
  # is at least as great as the log level that we want to capture logs for.
  #
  # Args:
  #   level: An Integer in the set of Logger levels (e.g., Logger::DEBUG,
  #     Logger::INFO) that indicates the severity of this log message.
  #   message: A String containing the message to be logged.
  def self.log_to_buffer(level, message)
    return if message.empty?
    return if level < @@log.level
    time = Time.now
    @@logs_buffer << {
      'timestamp' => time.to_i,
      'level' => level + 1,  # Python and Java are one higher than Ruby
      'message' => message
    }
    return
  end

  
  # Logs and runs the given command, which is assumed to be trusted and thus
  # needs no filtering on our part. Obviously this should not be executed by
  # anything that the user could inject input into. Returns the output of 
  # the command that was executed.
  def self.log_run(command)
    Djinn.log_debug(command)
    output = `#{command}`
    Djinn.log_debug(output)
    return output
  end


  # This method converts an Array of Strings (where each String contains all the
  # information about a single node) to an Array of DjinnJobData objects, which
  # provide convenience methods that make them easier to operate on than just
  # raw String objects.
  def self.convert_location_array_to_class(nodes, keyname)
    array_of_nodes = []
    nodes.each { |node|
      converted = DjinnJobData.new(node, keyname)
      array_of_nodes << converted
    }
    
    return array_of_nodes
  end


  # This method is the opposite of the previous method, and is needed when an
  # AppController wishes to pass node information to other AppControllers via
  # SOAP (as SOAP accepts Arrays and Strings but not DjinnJobData objects).
  def self.convert_location_class_to_array(djinn_locations)
    if djinn_locations.class != Array
      HelperFunctions.log_and_crash("Locations should be an Array, not a " +
        "#{djinn_locations.class}")
    end
    
    djinn_loc_array = []
    djinn_locations.each { |location|
      djinn_loc_array << location.to_hash
    }
    
    return JSON.dump(djinn_loc_array)
  end
    
  def get_login
    @nodes.each { |node|
      return node if node.is_login?
    }

    Djinn.log_fatal("Couldn't find a login node in the following nodes: " +
      "#{@nodes.join(', ')}")
    HelperFunctions.log_and_crash("No login nodes found.")
  end

  def get_shadow
    @nodes.each { |node|
      return node if node.is_shadow?
    }

    Djinn.log_fatal("Couldn't find a shadow node in the following nodes: " +
      "#{@nodes.join(', ')}")
    HelperFunctions.log_and_crash("No shadow nodes found.")
  end

  def get_db_master
    @nodes.each { |node|
      return node if node.is_db_master?
    }

    Djinn.log_fatal("Couldn't find a db master node in the following nodes: " +
      "#{@nodes.join(', ')}")
    HelperFunctions.log_and_crash("No db master nodes found.")
  end

  def self.get_db_master_ip
    masters_file = File.expand_path("#{CONFIG_FILE_LOCATION}/masters")
    master_ip = HelperFunctions.read_file(masters_file)
    return master_ip
  end

  def self.get_db_slave_ips
    slaves_file = File.expand_path("#{CONFIG_FILE_LOCATION}/slaves")
    slave_ips = File.open(slaves_file).readlines.map { |f| f.chomp! }
    slave_ips = [] if slave_ips == [""]
    return slave_ips
  end

  def self.get_nearest_db_ip()
    db_ips = self.get_db_slave_ips
    db_ips << self.get_db_master_ip
    db_ips.compact!
    
    local_ip = HelperFunctions.local_ip
    Djinn.log_debug("DB IPs are [#{db_ips.join(', ')}]")
    if db_ips.include?(local_ip)
      # If there is a local database then use it
      local_ip
    else
      # Otherwise just select one randomly
      db_ips.sort_by { rand }[0]
    end
  end

  def get_all_appengine_nodes()
    ae_nodes = []
    @nodes.each { |node|
      if node.is_appengine?
        ae_nodes << node.private_ip
      end
    }
    return ae_nodes
  end

  def get_load_balancer_ip()
    @nodes.each { |node|
      if node.is_load_balancer?
        return node.public_ip
      end
    }
  end

  def valid_secret?(secret)
    @@secret = HelperFunctions.get_secret
    if secret != @@secret
      failed_match_msg = "Incoming secret [#{secret}] failed to match " + \
        " known secret [#{@@secret}]"
      Djinn.log_error(failed_match_msg)
    end
    return secret == @@secret
  end

  def set_uaserver_ips()
    Djinn.log_info("Setting uaserver public/private ips")

    Djinn.log_debug("My node is #{my_node}")
    # set it to this node if we run the uaserver
    if my_node.is_db_master? or my_node.is_db_slave?
      Djinn.log_info("Running the UserAppServer locally - setting uaserver" +
        " IPs to (pub)#{my_node.public_ip}, (pri)#{my_node.private_ip}")
      @userappserver_public_ip = my_node.public_ip
      @userappserver_private_ip = my_node.private_ip
      return
    end

    # otherwise just set it to an arbitrary db node's ips
    @nodes.each { |node|
      Djinn.log_debug("looking at node #{node} as a possible uaserver")
      if node.is_db_master? or node.is_db_slave?
        Djinn.log_info("Found a UserAppServer at (pub) #{node.public_ip}, " +
          "(pri) #{node.private_ip}")
        @userappserver_public_ip = node.public_ip
        @userappserver_private_ip = node.private_ip
        return
      end
    }

    Djinn.log_fatal("[get uaserver ip] Couldn't find a UAServer.")
    HelperFunctions.log_and_crash("[get uaserver ip] Couldn't find a UAServer.")
  end
  
  def get_public_ip(private_ip)
    return private_ip unless is_cloud?
    
    keyname = @creds["keyname"]
    infrastructure = @creds["infrastructure"]    

    Djinn.log_debug("Looking for #{private_ip}")
    private_ip = HelperFunctions.convert_fqdn_to_ip(private_ip)
    Djinn.log_debug("[converted] Looking for #{private_ip}")
    @nodes.each { |node|
      node_private_ip = HelperFunctions.convert_fqdn_to_ip(node.private_ip)
      node_public_ip = HelperFunctions.convert_fqdn_to_ip(node.public_ip)

      if node_private_ip == private_ip or node_public_ip == private_ip
        return node_public_ip
      end
    }

    Djinn.log_fatal("get public ip] Couldn't convert private " +
      "IP #{private_ip} to a public address.")
    HelperFunctions.log_and_crash("[get public ip] Couldn't convert private " +
      "IP #{private_ip} to a public address.")
  end

  def get_status(node)
    ip = node.private_ip
    ssh_key = node.ssh_key
    acc = AppControllerClient.new(ip, @@secret)

    begin
      if !acc.is_done_loading?
        Djinn.log_info("Node at #{ip} is not done loading yet - will try " +
          "again later.")
        return
      end
    rescue FailedNodeException
      Djinn.log_warn("Node at #{ip} is not responding to loading requests " +
        "- will try again later.")
    end

    begin
      status_file = "#{CONFIG_FILE_LOCATION}/status-#{ip}.json"
      stats = acc.get_stats()
      json_state = JSON.dump(stats)
      HelperFunctions.write_file(status_file, json_state)

      if !my_node.is_login?
        login_ip = get_login.private_ip
        HelperFunctions.scp_file(status_file, status_file, login_ip, ssh_key)
      end
    rescue FailedNodeException
      Djinn.log_warn("Node at #{ip} is not responding to status requests " +
        "- will try again later.")
      return
    end
  end

  # Collects all AppScale-generated logs from all machines, and places them in
  # a tarball in the AppDashboard running on this machine. This enables users
  # to download it for debugging purposes.
  #
  # Args:
  #   secret: A String password that is used to authenticate SOAP callers.
  def gather_logs(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end

    uuid = HelperFunctions.get_random_alphanumeric()
    Djinn.log_info("Generated uuid #{uuid} for request to gather logs.")

    Thread.new {
      # Begin by copying logs on all machines to this machine.
      local_log_dir = "/tmp/#{uuid}"
      remote_log_dir = "/var/log/appscale"
      FileUtils.mkdir_p(local_log_dir)
      @nodes.each { |node|
        this_nodes_logs = "#{local_log_dir}/#{node.private_ip}"
        FileUtils.mkdir_p(this_nodes_logs)
        Djinn.log_run("scp -r -i #{node.ssh_key} -o StrictHostkeyChecking=no " +
          "2>&1 root@#{node.private_ip}:#{remote_log_dir} #{this_nodes_logs}")
      }

      # Next, tar.gz it up in the dashboard app so that users can download it.
      dashboard_log_location = "/var/apps/appscaledashboard/app/static/download-logs/#{uuid}.tar.gz"
      Djinn.log_info("Done gathering logs - placing logs at " +
        dashboard_log_location)
      Djinn.log_run("tar -czf #{dashboard_log_location} #{local_log_dir}")
      FileUtils.rm_rf(local_log_dir)
    }

    return uuid
  end


  def write_database_info()
    table = @creds["table"]
    replication = @creds["replication"]
    keyname = @creds["keyname"]
    
    tree = { :table => table, :replication => replication, :keyname => keyname }
    db_info_path = "#{CONFIG_FILE_LOCATION}/database_info.yaml"
    File.open(db_info_path, "w") { |file| YAML.dump(tree, file) }
    
    num_of_nodes = @nodes.length
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/num_of_nodes", "#{num_of_nodes}\n")
  end


  def update_firewall
    all_ips = []
    @nodes.each { |node|
      all_ips << node.private_ip
    }
    all_ips << "\n"
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/all_ips", all_ips.join("\n"))
    Djinn.log_debug("Letting the following IPs through the firewall: " +
      all_ips.join(', '))

    # Re-run the filewall script here since we just wrote the all_ips file
    if FIREWALL_IS_ON
      Djinn.log_run("bash #{APPSCALE_HOME}/firewall.conf")
    end
  end


  def backup_appcontroller_state()
    state = {'@@secret' => @@secret }

    instance_variables.each { |k|
      v = instance_variable_get(k)
      if k == "@nodes"
        v = Djinn.convert_location_class_to_array(@nodes)
      elsif k == "@my_index" or k == "@api_status"
        # Don't back up @my_index - it's a node-specific pointer that
        # indicates which node is "our node" and thus should be regenerated
        # via find_me_in_locations. Also don't worry about @api_status - it
        # can take up a lot of space and can easily be regenerated with new
        # data.
        next
      end

      state[k] = v
    }

    HelperFunctions.write_local_appcontroller_state(state)
    ZKInterface.write_appcontroller_state(state)
  end


 
  # Restores the state of each of the instance variables that the AppController
  # holds by pulling it from ZooKeeper (previously populated by the Shadow
  # node, who always has the most up-to-date version of this data).
  #
  # Returns:
  #   Two booleans, that indicate if (1) data was restored to this AppController
  #   from either ZooKeeper or locally, and (2) if we need to start the roles
  #   on this machine or not.
  def restore_appcontroller_state()
    Djinn.log_info("Restoring AppController state from local file")

    restoring_from_local = true
    if !File.exists?(ZK_LOCATIONS_FILE)
      Djinn.log_info("No recovery data found - skipping recovery process")
      return false, restoring_from_local
    end

    zookeeper_data = HelperFunctions.read_json_file(ZK_LOCATIONS_FILE)
    json_state = {}
    zookeeper_data['locations'].each { |ip|
      begin
        Djinn.log_info("Restoring AppController state from ZK at #{ip}")
        Timeout.timeout(10) do
          ZKInterface.init_to_ip(HelperFunctions.local_ip(), ip)
          json_state = ZKInterface.get_appcontroller_state()
        end
      rescue Exception => e
        Djinn.log_warn("Saw exception of class #{e.class} from #{ip}, " +
          "trying next ZooKeeper node")
        next
      end

      Djinn.log_info("Got data #{json_state.inspect} successfully from #{ip}")
      restoring_from_local = false
      break
    }

    if json_state.empty?
      Djinn.log_warn("Couldn't get data from any ZooKeeper node - instead " +
        "restoring from local data.")
      json_state = HelperFunctions.get_local_appcontroller_state()
      Djinn.log_info("Got data #{json_state} successfully from local" +
        " backup")
      restoring_from_local = true

      # Because we're restoring from our local state, that means we need to
      # start up Cassandra and ZooKeeper. The user may have told us to erase
      # all data on initial startup, but we don't want to erase any data we've
      # accumulated in the meanwhile.
      json_state['@creds']['clear_datastore'] = false

      # Similarly, if the machine was halted, then no App Engine apps are
      # running, so we need to start them all back up again.
      json_state['@nginx_port'] = Nginx::START_PORT
      json_state['@haproxy_port'] = HAProxy::START_PORT
      json_state['@appengine_port'] = 20000
      json_state['@apps_loaded'] = []
  end

    @@secret = json_state['@@secret']
    keyname = json_state['@creds']['keyname']

    json_state.each { |k, v|
      next if k == "@@secret"
      if k == "@nodes"
        v = Djinn.convert_location_array_to_class(JSON.load(v), keyname)
      end

      instance_variable_set(k, v)
    }

    # Now that we've restored our state, update the pointer that indicates
    # which node in @nodes is ours
    find_me_in_locations

    return true, restoring_from_local
  end


  # Updates the file that says where all the ZooKeeper nodes are
  # located so that this node has the most up-to-date info if it needs to
  # restore the data down the line.
  def write_zookeeper_locations
    zookeeper_data = { 'last_updated_at' => @last_updated,
      'locations' => []
    }

    @nodes.each { |node|
      if node.is_zookeeper?
        zookeeper_data['locations'] << node.private_ip
      end
    }

    HelperFunctions.write_json_file(ZK_LOCATIONS_FILE, zookeeper_data)
  end

  # Gets the status of the APIs of the AppScale deployment.
  # 
  # Args:
  #   secret: A string with the shared key for authentication.
  # Returns:
  #   A JSON string with the status of the APIs.
  def get_api_status(secret)
    if !valid_secret?(secret)
      return BAD_SECRET_MSG
    end
    begin
      return HelperFunctions.read_file(HEALTH_FILE)
    rescue Errno::ENOENT
      Djinn.log_warn("Couldn't read our API status - generating it now.")
      update_api_status()
      begin
        return HelperFunctions.read_file(HEALTH_FILE)
      rescue Errno::ENOENT
        Djinn.log_warn("Couldn't generate API status at this time.")
        return ''
      end
    end
  end

  # Contacts the API Checker application to learn which Google App Engine APIs
  # are running, which have failed, and which are in an unknown state. To
  # determine if an API is alive, we keep a running tally of its state and see
  # if it was alive for a majority of the times we checked up on it.
  # TODO(cgb): Consider only using 'running' if it was alive on every check and
  # add a 'degraded' state for cases where it was not alive on a check.
  #
  # Returns:
  #   A JSON-encoded Hash that maps each API name to its state (e.g., running,
  #   failed).
  def generate_api_status()
    return
    if my_node.is_appengine?
      apichecker_host = my_node.private_ip
    else
      apichecker_host = get_shadow.private_ip
    end

    apichecker_url = "http://#{apichecker_host}:#{ApiChecker::SERVER_PORT}/health/all"

    retries_left = 3
    begin
      response = Net::HTTP.get_response(URI.parse(apichecker_url))
      data = JSON.load(response.body)
    rescue Exception => e
      Djinn.log_error("Couldn't get API status from host at #{apichecker_url}")
      data = {}

      if retries_left > 0
        Kernel.sleep(5)
        retries_left -= 1
        retry
      else
        Djinn.log_warn("ApiChecker at #{apichecker_host} appears to be down - will " +
          "try again later.")
        return
      end
    end

    majorities = {}

    data.each { |k, v|
      @api_status[k] = [] if @api_status[k].nil?
      @api_status[k] << v

      # If not enough API statuses are known yet, pad it with 'running' to avoid
      # accidentally claiming that an API has failed without sufficient evidence
      # of its failure.
      @api_status[k] = HelperFunctions.shorten_to_n_items(10, @api_status[k],
        "running")
      majorities[k] = HelperFunctions.find_majority_item(@api_status[k])
    }

    json_state = JSON.dump(majorities)
    return json_state
  end


  # Writes a file to the local filesystem that indicates if each Google App
  # Engine API is currently running fine, experiences errors, or is in an
  # unknown state.
  def update_api_status()
    HelperFunctions.write_file(HEALTH_FILE, generate_api_status())
  end


  # Backs up information about what this node is doing (roles, apps it is
  # running) to ZooKeeper, for later recovery or updates by other nodes.
  def write_our_node_info
    # Since more than one AppController could write its data at the same 
    # time, get a lock before we write to it.
    ZKInterface.lock_and_run {
      @last_updated = ZKInterface.add_ip_to_ip_list(my_node.public_ip)
      ZKInterface.write_node_information(my_node, @done_loading)
    }

    return
  end


  # Returns the buffer that contains all logs yet to be sent to the Admin
  # Console for viewing.
  #
  # Returns:
  #   An Array of Hashes, where each Hash has information about a single log
  #     line.
  def self.get_logs_buffer()
    return @@logs_buffer
  end


  # Sends all of the logs that have been buffered up to the Admin Console for
  # viewing in a web UI.
  def flush_log_buffer()
    APPS_LOCK.synchronize {
      loop {
        break if @@logs_buffer.empty?
        encoded_logs = JSON.dump({
          'service_name' => 'appcontroller',
          'host' => my_node.public_ip,
          'logs' => @@logs_buffer.shift(LOGS_PER_BATCH),
        })

        begin
          url = URI.parse("https://#{get_login.public_ip}/logs/upload")
          http = Net::HTTP.new(url.host, url.port)
          http.use_ssl = true
          response = http.post(url.path, encoded_logs,
            {'Content-Type'=>'application/json'})
        rescue Exception
          # Don't crash the AppController because we weren't able to send over
          # the logs - just continue on.
        end
      }
    }
  end


  # Sends information about the AppServer processes hosting App Engine apps on
  # this machine to the AppDashboard, for later viewing.
  def send_instance_info_to_dashboard
    APPS_LOCK.synchronize {
      instance_info = []
      @app_info_map.each_pair { |appid, app_info|
        next if app_info['appengine'].nil?
        app_info['appengine'].each { |port|
          instance_info << {
            'appid' => appid,
            'host' => my_node.public_ip,
            'port' => port,
            'language' => app_info['language']
          }
        }
      }

      begin
        url = URI.parse("https://#{get_login.public_ip}/apps/stats/instances")
        http = Net::HTTP.new(url.host, url.port)
        http.use_ssl = true
        response = http.post(url.path, JSON.dump(instance_info),
          {'Content-Type'=>'application/json'})
        Djinn.log_debug("Done sending instance info to AppDashboard!")
        Djinn.log_debug("Instance info is: #{instance_info.inspect}")
        Djinn.log_debug("Response is #{response.body}")
      rescue OpenSSL::SSL::SSLError, NotImplementedError, Errno::EPIPE, Errno::ECONNRESET
        retry
      rescue Exception => exception
        # Don't crash the AppController because we weren't able to send over
        # the instance info - just continue on.
        Djinn.log_warn("Couldn't send instance info to AppDashboard because" +
          " of a #{exception.class} exception.")
      end
    }
  end


  # Informs the AppDashboard that the named AppServer is no longer running, so
  # that it no longer displays that AppServer in its instance information.
  #
  # Args:
  #   appid: A String that names the application whose AppServer was removed.
  #   port: An Integer that identifies the port that the AppServer was removed
  #     off of. We don't pass in the corresponding IP address or FQDN because we
  #     assume it was on this machine.
  def delete_instance_from_dashboard(appid, port)
    APPS_LOCK.synchronize {
      begin
        instance_info = [{
          'appid' => appid,
          'host' => my_node.public_ip,
          'port' => port
        }]

        url = URI.parse("https://#{get_login.public_ip}/apps/stats/instances")
        http = Net::HTTP.new(url.host, url.port)
        http.use_ssl = true
        request = Net::HTTP::Delete.new(url.path)
        request.body = JSON.dump(instance_info)
        response = http.request(request)
        Djinn.log_debug("Done sending instance info to AppDashboard!")
        Djinn.log_debug("Instance info is: #{instance_info.inspect}")
        Djinn.log_debug("Response is #{response.body}")
      rescue Exception => exception
        # Don't crash the AppController because we weren't able to send over
        # the instance info - just continue on.
        Djinn.log_warn("Couldn't delete instance info to AppDashboard because" +
          " of a #{exception.class} exception.")
      end
    }
  end


  # Queries ZooKeeper to see if our local copy of @nodes is out of date and
  # should be regenerated with up to date data from ZooKeeper. If data on
  # our node has changed, this starts or stops the necessary roles.
  def update_local_nodes
    ZKInterface.lock_and_run {
      # See if the ZooKeeper data is newer than ours - if not, don't
      # update anything and return.
      zk_ips_info = ZKInterface.get_ip_info()
      if zk_ips_info["last_updated"] <= @last_updated
        return "NOT UPDATED"
      else
        Djinn.log_info("Updating data from ZK. Our timestamp, " +
          "#{@last_updated}, was older than the ZK timestamp, " +
          "#{zk_ips_info['last_updated']}")
      end

      all_ips = zk_ips_info["ips"]
      new_nodes = []
      all_ips.each { |ip|
        new_nodes << DjinnJobData.new(ZKInterface.get_job_data_for_ip(ip),
          @creds['keyname'])
      }

      old_roles = my_node.jobs
      @nodes = new_nodes
      find_me_in_locations
      new_roles = my_node.jobs

      Djinn.log_info("My new nodes are [#{@nodes.join(', ')}], and my new " +
        "node is #{my_node}")

      # Since we're about to possibly load and unload roles, set done_loading
      # for our node to false, so that other nodes don't erroneously send us
      # additional roles to do while we're in this state where lots of side
      # effects are happening.
      @done_loading = false
      ZKInterface.set_done_loading(my_node.public_ip, false)
   
      roles_to_start = new_roles - old_roles
      if !roles_to_start.empty?
        Djinn.log_info("Need to start [#{roles_to_start.join(', ')}] " +
          "roles on this node")
        roles_to_start.each { |role|
          Djinn.log_info("Starting role #{role}")

          # When starting the App Engine role, we need to make sure that we load
          # all the App Engine apps on this machine.
          if role == "appengine"
            @apps_loaded = []
          end
          send("start_#{role}".to_sym)
        }
      end

      roles_to_stop = old_roles - new_roles
      if !roles_to_stop.empty?
        Djinn.log_info("Need to stop [#{roles_to_stop.join(', ')}] " +
          "roles on this node")
        roles_to_stop.each { |role|
          send("stop_#{role}".to_sym)
        }
      end

      # And now that we're done loading/unloading roles, set done_loading for
      # our node back to true.
      ZKInterface.set_done_loading(my_node.public_ip, true)
      @done_loading = true

      @last_updated = zk_ips_info['last_updated']

      # Finally, since the node layout changed, there may be a change in the
      # list of AppServers, so update nginx / haproxy accordingly.
      if my_node.is_login?
        regenerate_nginx_config_files()
      end
    }

    return "UPDATED"
  end


  # Each node has a responsibility to check up on other nodes and make sure
  # they are still running, and if not, to remedy it somehow.
  # Returns an Array of the roles that this process started.
  def ensure_all_roles_are_running
    roles_to_add = []
    ZKInterface.lock_and_run {
      ip_info = ZKInterface.get_ip_info()
      ip_info['ips'].each { |ip|
        if !ZKInterface.is_node_done_loading?(ip)
          next
        end

        if ZKInterface.is_node_live?(ip)
          next
        else
          failed_job_data = ZKInterface.get_job_data_for_ip(ip)
          failed_node = DjinnJobData.new(failed_job_data, @creds['keyname'])
          roles_to_add << failed_node.jobs

          remove_app_hosting_data_for_node(ip)
          remove_node_from_local_and_zookeeper(ip)
          Djinn.log_info("Will recover [#{failed_node.jobs.join(', ')}] " +
            " roles that were being run by the failed node at #{ip}")
        end
      }

      if !roles_to_add.empty?
        start_new_roles_on_nodes(roles_to_add, @creds['instance_type'],
          @@secret)
      end
    }

    return roles_to_add
  end


  def remove_app_hosting_data_for_node(ip)
    instances_to_delete = ZKInterface.get_app_instances_for_ip(ip)
    uac = UserAppClient.new(@userappserver_private_ip, @@secret)
    instances_to_delete.each { |instance|
      Djinn.log_info("Deleting app instance for app #{instance['app_name']}" +
        " located at #{instance['ip']}:#{instance['port']}")
      uac.delete_instance(instance['app_name'], instance['ip'],
        instance['port'])
    }
  end


  # Removes information associated with the given IP address from our local
  # cache (@nodes) as well as the remote node storage mechanism (in ZooKeeper).
  def remove_node_from_local_and_zookeeper(ip)
    # First, remove our local copy
    index_to_remove = nil
    @nodes.each_index { |i|
      if @nodes[i].public_ip == ip
        index_to_remove = i
        break
      end
    }
    @nodes.delete(@nodes[index_to_remove])

    # Then remove the remote copy
    ZKInterface.remove_node_information(ip)
    @last_updated = ZKInterface.remove_ip_from_ip_list(ip)
  end


  def wait_for_data()
    loop {
      break if got_all_data
      if @kill_sig_received
        Djinn.log_fatal("Received kill signal, aborting startup")
        HelperFunctions.log_and_crash("Received kill signal, aborting startup")
      else
        Djinn.log_info("Waiting for data from the load balancer or cmdline tools")
        Kernel.sleep(5)
      end
    }

  end

  def parse_creds
    if @creds["appengine"]
      @num_appengines = Integer(@creds["appengine"])
    end

    keypath = @creds['keyname'] + ".key"
    Djinn.log_debug("Keypath is #{keypath}, keyname is #{@creds['keyname']}")
    my_key_dir = "#{CONFIG_FILE_LOCATION}/keys/#{my_node.cloud}"
    my_key_loc = "#{my_key_dir}/#{keypath}"
    Djinn.log_debug("Creating directory #{my_key_dir} for my ssh key #{my_key_loc}")
    FileUtils.mkdir_p(my_key_dir)
    Djinn.log_run("cp #{CONFIG_FILE_LOCATION}/ssh.key #{my_key_loc}")
        
    if is_cloud?
      # for euca
      ENV['EC2_ACCESS_KEY'] = @creds["ec2_access_key"]
      ENV['EC2_SECRET_KEY'] = @creds["ec2_secret_key"]
      ENV['EC2_URL'] = @creds["ec2_url"]

      # for ec2
      cloud_keys_dir = File.expand_path("#{CONFIG_FILE_LOCATION}/keys/cloud1")
      ENV['EC2_PRIVATE_KEY'] = "#{cloud_keys_dir}/mykey.pem"
      ENV['EC2_CERT'] = "#{cloud_keys_dir}/mycert.pem"
    end

    write_database_info
    update_firewall
  end

  def got_all_data()
    return false if @nodes == []
    return false if @creds == {}
    return false if @app_names == []
    return true
  end
  

  # If running in a cloud environment, we may be dealing with public and
  # private FQDNs instead of IP addresses, which makes it hard to find out
  # which node is our node (since we find our node by IP). This method
  # looks through all the nodes we currently know of and converts any private
  # FQDNs we see to private IPs.
  def convert_fqdns_to_ips()
    if is_cloud?
      Djinn.log_debug("In a cloud deployment, so converting FQDNs -> IPs")
    else
      Djinn.log_debug("Not in a cloud deployment, so not converting FQDNs -> IPs")
      return
    end

    if @creds["hostname"] =~ /#{FQDN_REGEX}/
      begin
        @creds["hostname"] = HelperFunctions.convert_fqdn_to_ip(@creds["hostname"])
      rescue Exception => e
        Djinn.log_fatal("Failed to convert main hostname #{@creds['hostname']}")
        HelperFunctions.log_and_crash("Failed to convert main hostname #{@creds['hostname']}")
      end
    end
    
    @nodes.each { |node|
      # Resolve the private FQDN to a private IP, but don't resolve the public
      # FQDN, as that will just resolve to the private IP.

      pri = node.private_ip
      if pri =~ /#{FQDN_REGEX}/
        begin
          node.private_ip = HelperFunctions.convert_fqdn_to_ip(pri)
        rescue Exception => e
          node.private_ip = node.public_ip
        end
      end
    }
  end

 
  # Searches through @nodes to try to find out which node is ours. Strictly
  # speaking, we assume that our node is identifiable by private IP.
  def find_me_in_locations()
    @my_index = nil
    all_local_ips = HelperFunctions.get_all_local_ips()
    @nodes.each_index { |index|
      if all_local_ips.include?(@nodes[index].private_ip)
        @my_index = index
        HelperFunctions.set_local_ip(@nodes[index].private_ip)
        return
      end
    }
    Djinn.log_fatal("Can't find my node in @nodes: #{@nodes}. " +
      "My local IPs are: #{all_local_ips.join(', ')}")
    HelperFunctions.log_and_crash("Can't find my node in @nodes: #{@nodes}. " +
      "My local IPs are: #{all_local_ips.join(', ')}")
  end


  # Checks to see if the credentials given to us (a Hash) have all the keys that
  # other methods expect to see.
  def valid_format_for_credentials(possible_credentials)
    required_fields = ["table", "hostname", "ips", "keyname"]
    required_fields.each { |field|
      if !possible_credentials[field]
        return false
      end
    }

    return true
  end
  
  def sanitize_credentials()
    newcreds = {}
    @creds.each { |key, val|
      if key == 'ips'
        newcreds[key] = val
        next
      end

      next unless key.class == String
      newkey = key.gsub(NOT_EMAIL_REGEX, "")
      if newkey.include? "_key"
        if val.class == String
          newval = val.gsub(NOT_FQDN_OR_PLUS_REGEX, "")
        else
          newval = val
        end
      else
        if val.class == String
          newval = val.gsub(NOT_FQDN_REGEX, "")
        else
          newval = val
        end
      end
      newcreds[newkey] = newval
    }
    return newcreds
  end
    
  def change_job()
    my_data = my_node
    jobs_to_run = my_data.jobs
    
    Djinn.log_debug("Pre-loop: #{@nodes.join('\n')}")
    if my_node.is_shadow?
      # TODO(cgb): Check to make sure the machines aren't already
      # initialized before attempting to start up AppScale on them.
      spawn_and_setup_appengine
      loop {
        @everyone_else_is_done = true
        @nodes.each_index { |index|
          unless index == @my_index
            ip = @nodes[index].private_ip
            acc = AppControllerClient.new(ip, @@secret)
            begin
              if !acc.is_done_initializing?
                Djinn.log_info("Node at #{ip} is not done initializing yet - " +
                  "will check back later.")
                @everyone_else_is_done = false
              end
            rescue FailedNodeException
              Djinn.log_warn("Node at #{ip} is not responding to initializing" +
                " queries - will check back later.")
              @everyone_else_is_done = false
            end
          end
        }
        break if @everyone_else_is_done
        Djinn.log_info("Waiting on other nodes to come online")
        Kernel.sleep(5)
      }
    end

    initialize_server

    write_memcache_locations
    write_apploadbalancer_location
    find_nearest_taskqueue
    write_taskqueue_nodes_file
    setup_config_files
    set_uaserver_ips 
    write_hypersoap
    start_api_services()

    # since apichecker does health checks on the app engine apis, 
    # start it up there.

    #apichecker_ip = get_shadow.public_ip
    #apichecker_private_ip = get_shadow.private_ip
    #apichecker_ip = my_node.public_ip if my_node.is_appengine?
    #apichecker_private_ip = my_node.private_ip if my_node.is_appengine?
    #ApiChecker.init(apichecker_ip, apichecker_private_ip,  @@secret)

    #if my_node.is_shadow? or my_node.is_appengine?
    #  ApiChecker.start(get_login.public_ip, @userappserver_private_ip)
    #end

    # Start the AppDashboard.
    if my_node.is_login?
      update_node_info_cache()
      start_app_dashboard(get_login.public_ip, @userappserver_private_ip)
    end

    Djinn.log_info("Starting taskqueue worker for #{AppDashboard::APP_NAME}")
    maybe_start_taskqueue_worker(AppDashboard::APP_NAME)

    Djinn.log_info("Starting cron service for #{AppDashboard::APP_NAME}")
    CronHelper.update_cron(get_login.public_ip, AppDashboard::LISTEN_PORT,
      AppDashboard::APP_LANGUAGE, AppDashboard::APP_NAME)

    #maybe_start_taskqueue_worker("apichecker")

    if my_node.is_login?
      TaskQueue.start_flower()
    end

    # appengine is started elsewhere
  end


  # Starts all of the services that this node has been assigned to run.
  # Also starts all services that all nodes run in an AppScale deployment.
  def start_api_services()
    # ejabberd uses uaserver for authentication
    # so start it after we find out the uaserver's ip
    threads = []
    if my_node.is_login?
      threads << Thread.new {
        start_ejabberd()
      }
    end

    @done_initializing = true

    # start zookeeper
    threads << Thread.new {
      if my_node.is_zookeeper?
        configure_zookeeper(@nodes, @my_index)
        start_zookeeper
      end

      ZKInterface.init(my_node, @nodes)
    }

    if my_node.is_memcache?
      threads << Thread.new {
        start_memcache()
      }
    end

    if my_node.is_db_master?
      threads << Thread.new {
        start_db_master()
        # create initial tables
        if my_node.is_db_master?
          prime_database
        end

        # Always colocate the Datastore Server and UserAppServer (soap_server).
        if has_soap_server?(my_node)
          @state = "Starting up SOAP Server and Datastore Server"
          start_datastore_server()
          start_soap_server()
          HelperFunctions.sleep_until_port_is_open(HelperFunctions.local_ip, UserAppClient::SERVER_PORT)
        end

        # If we're starting AppScale with data from a previous deployment, we
        # may have to clear out all the registered app instances from the
        # UserAppServer (since nobody is currently hosting any apps).
        if not @creds['clear_datastore']
          erase_app_instance_info
        end
      }
    end

    if my_node.is_db_slave?
      threads << Thread.new {
        start_db_slave()

        # Currently we always run the Datastore Server and SOAP
        # server on the same nodes.
        if has_soap_server?(my_node)
          @state = "Starting up SOAP Server and Datastore Server"
          start_datastore_server()
          start_soap_server()
          HelperFunctions.sleep_until_port_is_open(HelperFunctions.local_ip,
            UserAppClient::SERVER_PORT)
        end
      }
    end

    # All nodes have application managers
    threads << Thread.new {
      start_app_manager_server()
    }

    if my_node.is_appengine?
      threads << Thread.new {
        start_blobstore_server()
      }
    end

    if my_node.is_taskqueue_master?
      threads << Thread.new {
        start_taskqueue_master()
      }
    elsif my_node.is_taskqueue_slave?
      threads << Thread.new {
        start_taskqueue_slave()
      }
    end

    # App Engine apps rely on the above services to be started, so
    # join all our threads here
    Djinn.log_info("Waiting for all services to finish starting up")
    threads.each { |t| t.join() }
    Djinn.log_info("API services have started on this node")

  end


  # Creates database tables in the underlying datastore to hold information
  # about the users that interact with AppScale clouds, and about the
  # applications that AppScale hosts (including data that the apps themselves
  # read and write).
  #
  # Raises:
  #   SystemExit: If the database could not be primed for use with AppScale,
  #     after ten retries.
  def prime_database
    table = @creds['table']
    prime_script = "#{APPSCALE_HOME}/AppDB/#{table}/prime_#{table}.py"
    retries = 10
    loop {
      Djinn.log_run("APPSCALE_HOME='#{APPSCALE_HOME}' MASTER_IP='localhost' " +
        "LOCAL_DB_IP='localhost' python2.6 #{prime_script} " +
        "#{@creds['replication']}; echo $? > /tmp/retval")
      retval = `cat /tmp/retval`.to_i
      return if retval.zero?
      Djinn.log_warn("Failed to prime database. #{retries} retries left.")
      Kernel.sleep(5)
      retries -= 1
      break if retries.zero?
    }

    Djinn.log_fatal("Failed to prime #{table}. Cannot continue.")
    HelperFunctions.log_and_crash("Failed to prime #{table}.")
  end


  # Contacts the UserAppServer to get a list of apps that it believes are
  # running in this AppScale cloud, and instructs it to delete each entry
  # present.
  def erase_app_instance_info
    uac = UserAppClient.new(@userappserver_private_ip, @@secret)
    app_list = uac.get_all_apps()
    my_public = my_node.public_ip

    Djinn.log_info("All apps are [#{app_list.join(', ')}]")
    app_list.each { |app|
      if uac.does_app_exist?(app)
        Djinn.log_debug("App #{app} is enabled, so stopping it.")
        hosts = uac.get_hosts_for_app(app)
        Djinn.log_debug("[Stop appengine] hosts for #{app} is [#{hosts.join(', ')}]")
        hosts.each { |host|
          Djinn.log_debug("[Stop appengine] deleting instance for app #{app} at #{host}")
          ip, port = host.split(":")
          uac.delete_instance(app, ip, port)
        }

        Djinn.log_info("Finished deleting instances for app #{app}")
      else
        Djinn.log_debug("App #{app} wasnt enabled, skipping it")
      end
    }
  end


  def start_blobstore_server
    db_local_ip = @userappserver_private_ip
    BlobServer.start(db_local_ip, DatastoreServer::LISTEN_PORT_NO_SSL)
    BlobServer.is_running(db_local_ip)

    return true
  end


  def start_taskqueue_master
    TaskQueue.start_master()      
    return true
  end


  def start_taskqueue_slave
    # All slaves connect to the master to start
    master_ip = nil
    @nodes.each { |node|
      master_ip = node.private_ip if node.is_taskqueue_master?
    }

    TaskQueue.start_slave(master_ip)
    return true
  end

  # Starts the application manager which is a SOAP service in charge of 
  # starting and stopping applications.
  def start_app_manager_server
    @state = "Starting up AppManager"
    env_vars = {}
    start_cmd = ["/usr/bin/python2.6 #{APPSCALE_HOME}/AppManager/app_manager_server.py"]
    stop_cmd = "pkill -9 app_manager_server"
    port = [AppManagerClient::SERVER_PORT]
    GodInterface.start(:appmanagerserver, start_cmd, stop_cmd, port, env_vars)
  end

  def start_soap_server
    db_master_ip = nil
    @nodes.each { |node|
      db_master_ip = node.private_ip if node.is_db_master?
    }
    HelperFunctions.log_and_crash("db master ip was nil") if db_master_ip.nil?

    db_local_ip = @userappserver_private_ip
            
    table = @creds['table']

    env_vars = {}

    env_vars['APPSCALE_HOME'] = APPSCALE_HOME
    env_vars['MASTER_IP'] = db_master_ip
    env_vars['LOCAL_DB_IP'] = db_local_ip

    if table == "simpledb"
      env_vars['SIMPLEDB_ACCESS_KEY'] = @creds['SIMPLEDB_ACCESS_KEY']
      env_vars['SIMPLEDB_SECRET_KEY'] = @creds['SIMPLEDB_SECRET_KEY']
    end

    start_cmd = ["/usr/bin/python2.6 #{APPSCALE_HOME}/AppDB/soap_server.py",
            "-t #{table} -s #{HelperFunctions.get_secret}"].join(' ')
    stop_cmd = "pkill -9 soap_server"
    port = [4343]

    GodInterface.start(:uaserver, start_cmd, stop_cmd, port, env_vars)
  end 

  def start_datastore_server
    db_master_ip = nil
    my_ip = my_node.public_ip
    @nodes.each { |node|
      db_master_ip = node.private_ip if node.is_db_master?
    }
    HelperFunctions.log_and_crash("db master ip was nil") if db_master_ip.nil?

    table = @creds['table']
    zoo_connection = get_zk_connection_string(@nodes)
    DatastoreServer.start(db_master_ip, @userappserver_private_ip, my_ip, table, zoo_connection)
    HAProxy.create_datastore_server_config(my_node.private_ip, DatastoreServer::PROXY_PORT, table)
    Nginx.create_datastore_server_config(my_node.private_ip, DatastoreServer::PROXY_PORT)
    Nginx.restart()

    # TODO check the return value
    DatastoreServer.is_running(my_ip)
  end

  def stop_blob_server
    BlobServer.stop
    Djinn.log_run("pkill -f blobstore_server")
  end 

  def stop_soap_server
    GodInterface.stop(:uaserver)
  end 

  # Stops the AppManager service
  #
  def stop_app_manager_server
    GodInterface.stop(:appmanagerserver)
  end 

  def stop_datastore_server
    DatastoreServer.stop(@creds['table']) 
  end
  
  def is_hybrid_cloud?
    if @creds["infrastructure"].nil?
      false
    else
      @creds["infrastructure"] == "hybrid"
    end
  end

  def is_cloud?
    !@creds["infrastructure"].nil?
  end

  def restore_from_db?
    @creds['restore_from_tar'] || @creds['restore_from_ebs']
  end

  def spawn_and_setup_appengine()
    # should also make sure the tools are on the vm and the envvars are set

    table = @creds['table']

    nodes = JSON.load(@creds["ips"])
    appengine_info = spawn_appengine(nodes)

    @state = "Copying over needed files and starting the AppController on the other VMs"
    
    keyname = @creds["keyname"] 
    appengine_info = Djinn.convert_location_array_to_class(appengine_info, keyname)
    @nodes.concat(appengine_info)
    find_me_in_locations
    write_database_info
    update_firewall
    
    creds = @creds.to_a.flatten
    initialize_nodes_in_parallel(appengine_info)
  end

  def spawn_appengine(nodes)
    Djinn.log_debug("nodes is #{nodes.join(', ')}")
    return [] if nodes.length.zero?

    appengine_info = []
    if is_cloud?
      @state = "Spawning up #{nodes.length} virtual machines"
      roles = nodes.map { |node| node['jobs'] }
      disks = nodes.map { |node| node['disk'] }

      # since there's only one cloud, call it cloud1 to tell us
      # to use the first ssh key (the only key)
      imc = InfrastructureManagerClient.new(@@secret)
      begin
        appengine_info = imc.spawn_vms(nodes.length, @creds, roles, disks)
      rescue AppScaleException => exception
        HelperFunctions.log_and_crash("Couldn't spawn #{nodes.length} VMs " +
          "with roles #{roles} because: #{exception.message}")
      end
    else
      nodes.each { |node|
        appengine_info << {
          'public_ip' => node['ip'],
          'private_ip' => node['ip'],
          'jobs' => node['jobs'],
          'instance_id' => 'i-SGOOBARZ',
          'disk' => nil
        }
      }
    end

    Djinn.log_debug("Received appengine info: #{appengine_info.join(', ')}")
    return appengine_info
  end

  def initialize_nodes_in_parallel(node_info)
    threads = []
    node_info.each { |slave|
      threads << Thread.new {
        initialize_node(slave)
      }
    }

    threads.each { |t| t.join }
  end

  def initialize_node(node)
    copy_encryption_keys(node)
    validate_image(node)
    rsync_files(node)
    start_appcontroller(node)
  end

  def validate_image(node)
    ip = node.public_ip
    key = node.ssh_key
    HelperFunctions.ensure_image_is_appscale(ip, key)
    HelperFunctions.ensure_version_is_supported(ip, key)
    HelperFunctions.ensure_db_is_supported(ip, @creds["table"], key)
  end

  def copy_encryption_keys(dest_node)
    ip = dest_node.private_ip
    Djinn.log_info("Copying SSH keys to node at IP address #{ip}")
    ssh_key = dest_node.ssh_key

    HelperFunctions.sleep_until_port_is_open(ip, SSH_PORT)
    Kernel.sleep(3)

    if ["ec2", "euca"].include?(@creds["infrastructure"])
      options = "-o StrictHostkeyChecking=no -o NumberOfPasswordPrompts=0"
      enable_root_login = "sudo cp /home/ubuntu/.ssh/authorized_keys /root/.ssh/"
      Djinn.log_run("ssh -i #{ssh_key} #{options} 2>&1 ubuntu@#{ip} '#{enable_root_login}'")
    elsif @creds["infrastructure"] == "gce"
      options = "-o StrictHostkeyChecking=no -o NumberOfPasswordPrompts=0"
      enable_root_login = "sudo cp /home/#{@creds['gce_user']}/.ssh/authorized_keys /root/.ssh/"
      Djinn.log_run("ssh -i #{ssh_key} #{options} 2>&1 #{@creds['gce_user']}@#{ip} '#{enable_root_login}'")
    end

    secret_key_loc = "#{CONFIG_FILE_LOCATION}/secret.key"
    cert_loc = "#{CONFIG_FILE_LOCATION}/certs/mycert.pem"
    key_loc = "#{CONFIG_FILE_LOCATION}/certs/mykey.pem"
    pub_key = File.expand_path("~/.ssh/id_rsa.pub")

    HelperFunctions.scp_file(secret_key_loc, secret_key_loc, ip, ssh_key)
    HelperFunctions.scp_file(cert_loc, cert_loc, ip, ssh_key)
    HelperFunctions.scp_file(key_loc, key_loc, ip, ssh_key)
    scp_ssh_key_to_ip(ip, ssh_key, pub_key)

    cloud_keys_dir = File.expand_path("#{CONFIG_FILE_LOCATION}/keys/cloud1")
    make_dir = "mkdir -p #{cloud_keys_dir}"

    cloud_private_key = "#{cloud_keys_dir}/mykey.pem"
    cloud_cert = "#{cloud_keys_dir}/mycert.pem"

    HelperFunctions.run_remote_command(ip, make_dir, ssh_key, NO_OUTPUT)
    HelperFunctions.scp_file(ssh_key, ssh_key, ip, ssh_key)
    HelperFunctions.scp_file(cloud_private_key, cloud_private_key, ip, ssh_key)
    HelperFunctions.scp_file(cloud_cert, cloud_cert, ip, ssh_key)

    # Finally, on GCE, we need to copy over the user's credentials, in case
    # nodes need to attach persistent disks.
    return if @creds["infrastructure"] != "gce"

    client_secrets = '/etc/appscale/client_secrets.json'
    gce_oauth = '/etc/appscale/oauth2.dat'

    if File.exists?(client_secrets)
      HelperFunctions.scp_file(client_secrets, client_secrets, ip, ssh_key)
    end

    HelperFunctions.scp_file(gce_oauth, gce_oauth, ip, ssh_key)
  end

 
  # Copies over SSH keys to ~/.ssh on the given machine, enabling that
  # machine to log in to itself or any other AppScale VM without being
  # prompted for a password. Note that since this copies keys to ~./ssh,
  # it will overwrite any keys that already exist there.
  # Args:
  #   ip: The IP address to copy SSH keys to.
  #   private_key: The SSH private key that should be copied over.
  #   public_key: The SSH public key that should be copied over.
  def scp_ssh_key_to_ip(ip, private_key, public_key)
    HelperFunctions.scp_file(private_key, "~/.ssh/id_rsa", ip,
      private_key)
    # this is needed for EC2 integration.
    HelperFunctions.scp_file(private_key, "~/.ssh/id_dsa", ip,
      private_key)
    HelperFunctions.scp_file(public_key, "~/.ssh/id_rsa.pub", ip,
      private_key)
  end


  def rsync_files(dest_node)
    controller = "#{APPSCALE_HOME}/AppController"
    server = "#{APPSCALE_HOME}/AppServer"
    loadbalancer = "#{APPSCALE_HOME}/AppDashboard"
    appdb = "#{APPSCALE_HOME}/AppDB"
    loki = "#{APPSCALE_HOME}/Loki"
    app_manager = "#{APPSCALE_HOME}/AppManager"
    iaas_manager = "#{APPSCALE_HOME}/InfrastructureManager"
    xmpp_receiver = "#{APPSCALE_HOME}/XMPPReceiver"

    ssh_key = dest_node.ssh_key
    ip = dest_node.private_ip
    options = "-e 'ssh -i #{ssh_key}' -arv --filter '- *.pyc'"

    HelperFunctions.shell("rsync #{options} #{controller}/* root@#{ip}:#{controller}")
    HelperFunctions.shell("rsync #{options} #{server}/* root@#{ip}:#{server}")
    HelperFunctions.shell("rsync #{options} #{loadbalancer}/* root@#{ip}:#{loadbalancer}")
    HelperFunctions.shell("rsync #{options} --exclude='logs/*' --exclude='cassandra/cassandra/*' #{appdb}/* root@#{ip}:#{appdb}")
    HelperFunctions.shell("rsync #{options} #{loki}/* root@#{ip}:#{loki}")
    HelperFunctions.shell("rsync #{options} #{app_manager}/* root@#{ip}:#{app_manager}")
    HelperFunctions.shell("rsync #{options} #{iaas_manager}/* root@#{ip}:#{iaas_manager}")
    HelperFunctions.shell("rsync #{options} #{xmpp_receiver}/* root@#{ip}:#{xmpp_receiver}")
  end

  def setup_config_files()
    @state = "Setting up database configuration files"

    master_ip = []
    slave_ips = []

    # load datastore helper
    # TODO: this should be the class or module
    table = @creds['table']
    # require db_file
    begin
      require "#{APPSCALE_HOME}/AppDB/#{table}/#{table}_helper"
    rescue Exception => e
      backtrace = e.backtrace.join("\n")
      Djinn.log_fatal("Unable to find #{table} helper." +
        " Please verify datastore type: #{e}\n#{backtrace}")
      HelperFunctions.log_and_crash("Unable to find #{table} helper." +
        " Please verify datastore type: #{e}\n#{backtrace}")
    end
    FileUtils.mkdir_p("#{APPSCALE_HOME}/AppDB/logs")

    @nodes.each { |node| 
      master_ip = node.private_ip if node.jobs.include?("db_master")
      slave_ips << node.private_ip if node.jobs.include?("db_slave")
    }

    Djinn.log_debug("Master is at #{master_ip}, slaves are at #{slave_ips.join(', ')}")

    my_public = my_node.public_ip
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/my_public_ip", "#{my_public}\n")

    my_private = my_node.private_ip
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/my_private_ip", "#{my_private}\n")
   
    head_node_ip = get_public_ip(@creds['hostname'])
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/head_node_ip", "#{head_node_ip}\n")

    login_ip = get_login.public_ip
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/login_ip", "#{login_ip}\n")
    
    login_private_ip = get_login.private_ip
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/login_private_ip", "#{login_private_ip}\n")

    masters_file = "#{CONFIG_FILE_LOCATION}/masters"
    HelperFunctions.write_file(masters_file, "#{master_ip}\n")

    if @nodes.length  == 1
      Djinn.log_info("Only saw one machine, therefore my node is " +
        "also a slave node")
      slave_ips = [ my_private ]
    end
    
    slave_ips_newlined = slave_ips.join("\n")
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/slaves", "#{slave_ips_newlined}\n")

    # Invoke datastore helper function
    setup_db_config_files(master_ip, slave_ips)

    update_hosts_info()

    # use iptables to lock down outside traffic
    # nodes can talk to each other on any port
    # but only the outside world on certain ports
    #`iptables --flush`
    if FIREWALL_IS_ON
      Djinn.log_run("bash #{APPSCALE_HOME}/firewall.conf")
    end
  end


  # Writes a file to the local filesystem that contains the IP addresses of
  # all machines running memcached. AppServers read this file periodically to
  # get an up-to-date list of the nodes running the memcache service, which can
  # change if AppScale scales up or down.
  def write_memcache_locations()
    memcache_ips = []
    @nodes.each { |node|
      memcache_ips << node.private_ip if node.is_memcache?
    }
    Djinn.log_debug("Memcache servers are at #{memcache_ips.join(', ')}")
    memcache_file = "#{CONFIG_FILE_LOCATION}/memcache_ips"
    memcache_contents = memcache_ips.join("\n")
    HelperFunctions.write_file(memcache_file, memcache_contents)
  end


  # Writes a file to the local filesystem that contains the IP address
  # of a machine that runs the AppDashboard. AppServers use this file
  # to know where to send users to log in. Because users have to be able
  # to access this IP address, we use the public IP here instead of the
  # private IP.
  def write_apploadbalancer_location()
    login_file = "#{CONFIG_FILE_LOCATION}/appdashboard_public_ip"
    login_ip = get_login.public_ip()
    HelperFunctions.write_file(login_file, login_ip)
  end


  # Writes a file to the local filesystem that contains the IP
  # address of the 'nearest' machine running the TaskQueue service.
  # 'Nearest' is defined as being this node's IP if our node runs TQ,
  # or a random node that runs TQ otherwise.
  def find_nearest_taskqueue()
    rabbitmq_ip = nil
    if my_node.is_taskqueue_master? or my_node.is_taskqueue_slave?
      rabbitmq_ip = my_node.private_ip
    end

    if rabbitmq_ip.nil?
      rabbitmq_ips = []
      @nodes.each { |node|
        if node.is_taskqueue_master? or node.is_taskqueue_slave?
          rabbitmq_ips << node.private_ip
        end
      }
      Djinn.log_debug("TaskQueue servers are at #{rabbitmq_ips.join(', ')}")

      # pick one at random
      rabbitmq_ip = rabbitmq_ips.sort_by { rand }[0]
    end

    Djinn.log_debug("AppServers on this node will connect to TaskQueue " +
      "at #{rabbitmq_ip}")
    rabbitmq_file = "#{CONFIG_FILE_LOCATION}/rabbitmq_ip"
    rabbitmq_contents = rabbitmq_ip
    HelperFunctions.write_file(rabbitmq_file, rabbitmq_contents)
  end
 
  # Writes a file to the local file system that tells the taskqueue master
  # all nodes which are taskqueue nodes. 
  def write_taskqueue_nodes_file
    taskqueue_ips = []
    @nodes.each { |node|
      taskqueue_ips << node.private_ip if node.is_taskqueue_master? or node.is_taskqueue_slave?
    }
    taskqueue_contents = taskqueue_ips.join("\n")
    HelperFunctions.write_file(TASKQUEUE_FILE,  taskqueue_contents)
  end

  # Updates files on this machine with information about our hostname
  # and a mapping of where other machines are located.
  def update_hosts_info()
    all_nodes = ""
    @nodes.each_with_index { |node, index|
      all_nodes << "#{HelperFunctions.convert_fqdn_to_ip(node.private_ip)} appscale-image#{index}\n"
    }
    
    new_etc_hosts = <<HOSTS
127.0.0.1 localhost.localdomain localhost
127.0.1.1 localhost
::1     ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
ff02::3 ip6-allhosts
#{all_nodes}
HOSTS

    etc_hosts = "/etc/hosts"
    File.open(etc_hosts, "w+") { |file| file.write(new_etc_hosts) }    

    etc_hostname = "/etc/hostname"
    my_hostname = "appscale-image#{@my_index}"
    File.open(etc_hostname, "w+") { |file| file.write(my_hostname) }

    Djinn.log_run("/bin/hostname #{my_hostname}")
  end


  # Writes new nginx configuration files for the App Engine applications
  # hosted in this deployment. Callers should invoke this method whenever
  # there is a change in the number of machines hosting App Engine apps.
  def regenerate_nginx_config_files()
    Djinn.log_debug("Regenerating nginx config files for App Engine apps")
    my_public = my_node.public_ip
    my_private = my_node.private_ip
    login_ip = get_login.private_ip

    Djinn.log_debug("@app_info_map is #{@app_info_map.inspect}")
    @apps_loaded.each { |app|  
      Djinn.log_debug("Regenerating nginx config for app #{app}")
      app_number = @app_info_map[app]['nginx'] - Nginx::START_PORT
      proxy_port = HAProxy.app_listen_port(app_number)
      Nginx.write_fullproxy_app_config(app, app_number, my_public,
        my_private, proxy_port, login_ip, get_all_appengine_nodes())
    }
    Djinn.log_debug("Done writing new nginx config files!")
    Nginx.reload()
  end


  def write_hypersoap()
    HelperFunctions.write_file("#{CONFIG_FILE_LOCATION}/hypersoap", @userappserver_private_ip)
  end

  def my_node()
    if @my_index.nil?
      find_me_in_locations
    end

    if @my_index.nil?
      Djinn.log_debug("My index is nil - is nodes nil? #{@nodes.nil?}")
      if @nodes.nil?
        Djinn.log_debug("My nodes is nil also, timing error? race condition?")
      else
        Djinn.log_fatal("Couldn't find our position in #{@nodes}")
        HelperFunctions.log_and_crash("Couldn't find our position in #{@nodes}")
      end
    end

    @nodes[@my_index]
  end
  
  # Perform any necessary initialization steps before we begin starting up
  # services.
  def initialize_server
    my_public_ip = my_node.public_ip
    head_node_ip = get_public_ip(@creds['hostname'])

    HAProxy.initialize_config
    Nginx.initialize_config

    if my_node.disk
      imc = InfrastructureManagerClient.new(@@secret)

      device_name = imc.attach_disk(@creds, my_node.disk, my_node.instance_id)
      loop {
        if File.exists?(device_name)
          Djinn.log_info("Device #{device_name} exists - mounting it.")
          break
        else
          Djinn.log_info("Device #{device_name} does not exist - waiting for " +
            "it to exist.")
          Kernel.sleep(1)
        end
      }

      Djinn.log_run("rm -rf #{PERSISTENT_MOUNT_POINT}")
      Djinn.log_run("mkdir #{PERSISTENT_MOUNT_POINT}")
      mount_output = Djinn.log_run("mount -t ext4 #{device_name} " +
        "#{PERSISTENT_MOUNT_POINT} 2>&1")
      if mount_output.empty?
        Djinn.log_info("Mounted persistent disk #{device_name}, without " +
          "needing to format it.")
        Djinn.log_run("mkdir -p #{PERSISTENT_MOUNT_POINT}/apps")
        return
      end

      Djinn.log_info("Formatting persistent disk #{device_name}")
      Djinn.log_run("mkfs.ext4 -F #{device_name}")

      Djinn.log_info("Mounting persistent disk #{device_name}")
      Djinn.log_run("mount -t ext4 #{device_name} #{PERSISTENT_MOUNT_POINT} " +
        "2>&1")
      Djinn.log_run("mkdir -p #{PERSISTENT_MOUNT_POINT}/apps")
    end
  end

  def start_appcontroller(node)
    ip = node.private_ip
    ssh_key = node.ssh_key

    remote_home = HelperFunctions.get_remote_appscale_home(ip, ssh_key)
    env = {
      'APPSCALE_HOME' => APPSCALE_HOME,
      'EC2_HOME' => ENV['EC2_HOME'],
      'JAVA_HOME' => ENV['JAVA_HOME']
    }
    start = "ruby #{remote_home}/AppController/djinnServer.rb"
    stop = "ruby #{remote_home}/AppController/terminate.rb"

    # remove any possible appcontroller state that may not have been
    # properly removed in non-cloud runs
    remove_state = "rm -rf #{CONFIG_FILE_LOCATION}/appcontroller-state.json"
    HelperFunctions.run_remote_command(ip, remove_state, ssh_key, NO_OUTPUT)

    GodInterface.start_god(ip, ssh_key)
    Kernel.sleep(1)

    begin
      GodInterface.start(:controller, start, stop, SERVER_PORT, env, ip, ssh_key)
      HelperFunctions.sleep_until_port_is_open(ip, SERVER_PORT, USE_SSL, 60)
    rescue Exception => except
      backtrace = except.backtrace.join("\n")
      remote_start_msg = "[remote_start] Unforeseen exception when " + \
        "talking to #{ip}: #{except}\nBacktrace: #{backtrace}"
      Djinn.log_warn(remote_start_msg)
      retry
    end

    Djinn.log_debug("Sending data to #{ip}")
    acc = AppControllerClient.new(ip, @@secret)

    loc_array = Djinn.convert_location_class_to_array(@nodes)
    credentials = @creds.to_a.flatten

    begin
      result = acc.set_parameters(loc_array, credentials, @app_names)
      Djinn.log_info("Setting parameters on node at #{ip} returned #{result}")
    rescue FailedNodeException
      Djinn.log_error("Couldn't set parameters on node at #{ip}.")
      HelperFunctions.log_and_crash("Couldn't set parameters on node at #{ip}")
    end
  end

  def is_running?(name)
    !`ps ax | grep #{name} | grep -v grep`.empty?
  end

  def start_memcache()
    @state = "Starting up memcache"
    Djinn.log_info("Starting up memcache")
    start_cmd = "/usr/bin/memcached -m 32 -p 11211 -u root"
    stop_cmd = "pkill memcached"
    GodInterface.start(:memcached, start_cmd, stop_cmd, [11211])
  end

  def stop_memcache()
    GodInterface.stop(:memcached)
  end

  def start_ejabberd()
    @state = "Starting up XMPP server"
    my_public = my_node.public_ip
    Ejabberd.stop
    Djinn.log_run("rm -f /var/lib/ejabberd/*")
    Ejabberd.write_auth_script(my_public, get_db_master.private_ip, @@secret)
    Ejabberd.write_config_file(my_public)
    Ejabberd.start
  end

  def stop_ejabberd()
    Ejabberd.stop
  end

  # Start the AppDashboard web service which allows users to login,
  # upload and remove apps, and view the status of the AppScale deployment.
  #
  # Args:
  #  login_ip: A string wth the ip of the login node.
  #  uaserver_ip: A string with the ip of the UserAppServer.
  def start_app_dashboard(login_ip, uaserver_ip)
    @state = "Starting up Load Balancer"
    Djinn.log_info("Starting up Load Balancer")

    my_public = my_node.public_ip
    my_private = my_node.private_ip
    HAProxy.create_app_load_balancer_config(my_public, my_private, 
      AppDashboard::PROXY_PORT)
    Nginx.create_app_load_balancer_config(my_public, my_private, 
      AppDashboard::PROXY_PORT)
    AppDashboard.start(login_ip, uaserver_ip, my_public, my_private, @@secret)
    HAProxy.start
    Nginx.restart
  end

  # Stop the AppDashboard web service.
  def stop_app_dashboard()
    Djinn.log_info("Shutting down AppDashboard")
    AppDashboard.stop
  end

  def start_shadow()
    Djinn.log_info("Starting Shadow role")
  end

  def stop_shadow()
    Djinn.log_info("Stopping Shadow role")
  end

  #
  # Swaps out an application with one that relays an error message to the 
  # developer. It will take the application that currently exists in the 
  # application folder, deletes it, and places a templated app that prints out the 
  # given error message. 
  #
  # Args: 
  #   app_name: Name of application to construct an error application for
  #   err_msg: A String message that will be displayed as 
  #            the reason why we couldn't start their application.
  def place_error_app(app_name, err_msg)
    Djinn.log_info("Placing error application for #{app_name} because of: #{err_msg}")
    ea = ErrorApp.new(app_name, err_msg)
    ea.generate() 
  end

  def start_appengine()
    @state = "Preparing to run AppEngine apps if needed"
    db_private_ip = nil
    @nodes.each { |node|
      if node.is_db_master? or node.is_db_slave?
        if HelperFunctions.is_port_open?(node.private_ip, 4343,
          HelperFunctions::USE_SSL)
          Djinn.log_debug("UAServer port open on #{node.private_ip} - using it!")
          db_private_ip = node.private_ip
          break
        else
          Djinn.log_debug("UAServer port not open on #{node.private_ip} - skipping!")
          next
        end
      end
    }
    if db_private_ip.nil?
      Djinn.log_warn("Couldn't find a live db node - falling back to UAServer IP")
      db_private_ip = @userappserver_private_ip
    end

    Djinn.log_debug("Starting appengine - pbserver is at [#{db_private_ip}]")

    uac = UserAppClient.new(db_private_ip, @@secret)
    if @restored == false
      Djinn.log_info("Need to restore")
      app_list = uac.get_all_apps()
      app_list.each { |app|
        if uac.does_app_exist?(app)
          Djinn.log_debug("App #{app} is enabled, so restoring it")
          @app_names = @app_names + [app]
        else
          Djinn.log_debug("App #{app} is not enabled, moving on")
        end
      }

      @app_names.uniq!
      Djinn.log_debug("Decided to restore these apps: [#{@app_names.join(', ')}]")
      @restored = true
    end

    APPS_LOCK.synchronize {
      apps_to_load = @app_names - @apps_loaded - ["none"]
      apps_to_load.each { |app|
        setup_appengine_application(app, is_new_app=true)
      }
    } # end of synchronize
  end


  # Performs all of the preprocessing needed to start an App Engine application
  # on this node. This method then starts the actual app by calling the AppManager.
  #
  # Args:
  #   app: A String containing the appid for the app to start.
  #   is_new_app: true if the application to start has never run on this node
  #     before, and false if it has (e.g., we're loading new code for this app).
  def setup_appengine_application(app, is_new_app)
    uac = UserAppClient.new(@userappserver_private_ip, @@secret)
    app_data = uac.get_app_data(app)
    loop {
      Djinn.log_info("Waiting for app data to have instance info for app named #{app}: #{app_data}")

      app_data = uac.get_app_data(app)
      if app_data[0..4] != "Error"
        break
      end
      Kernel.sleep(5)
    }

    my_public = my_node.public_ip
    my_private = my_node.private_ip
    app_language = app_data.scan(/language:(\w+)/).flatten.to_s
    
    if is_new_app
      @app_info_map[app] = {}
      @app_info_map[app]['language'] = app_language
    end

    shadow = get_shadow
    shadow_ip = shadow.private_ip
    ssh_key = shadow.ssh_key
    app_dir = "/var/apps/#{app}/app"
    app_path = "/opt/appscale/apps/#{app}.tar.gz"
    FileUtils.mkdir_p(app_dir)
     
    # First, make sure we can download the app, and if we can't, throw up a
    # dummy app letting the user know there was a problem.
    if !copy_app_to_local(app)
      place_error_app(app, "ERROR: Failed to copy app: #{app}")
      app_language = "python"
    end

    # Next, make sure their app has an app.yaml or appengine-web.xml in it,
    # since the following code assumes it is present. If it is not there
    # (which can happen if the scp fails on a large app), throw up a dummy
    # app.
    if !HelperFunctions.app_has_config_file?(app_path)
      place_error_app(app, "ERROR: No app.yaml or appengine-web.xml for app " +
        app)
      app_language = "python"
    end

    HelperFunctions.setup_app(app)

    if is_new_app
      maybe_start_taskqueue_worker(app)
    end

    if is_new_app
      nginx_port = @nginx_port
    else
      nginx_port = @app_info_map[app]['nginx']
    end
    app_number = nginx_port - Nginx::START_PORT

    # TODO(cgb): Make sure we don't add the same cron lines in twice for the same
    # app, and only start xmpp if it isn't already started
    if my_node.is_shadow?
      CronHelper.update_cron(my_public, nginx_port, app_language, app)
      start_xmpp_for_app(app, nginx_port, app_language)
    end

    # We only need a new full proxy config file for new apps, on the machine
    # that runs the login service (but not in a one node deploy, where we don't
    # do a full proxy config).
    if is_new_app and my_node.is_login? and !my_node.is_appengine?
      write_full_proxy_nginx_file(app)
    end

    if my_node.is_appengine?
      begin
        static_handlers = HelperFunctions.parse_static_data(app)
        Djinn.log_run("chmod -R +r #{HelperFunctions.get_cache_path(app)}")
      rescue Exception => e
        # This specific exception may be a json parse error
        error_msg = "ERROR: Unable to parse app.yaml file for #{app}." + \
                    " Exception of #{e.class} with message #{e.message}" 
        place_error_app(app, error_msg)
        static_handlers = []
      end

      proxy_port = HAProxy.app_listen_port(app_number)
      login_ip = get_login.private_ip
      success = Nginx.write_app_config(app, app_number, my_public, my_private,
        proxy_port, static_handlers, login_ip)
      if !success
        error_msg = "ERROR: Failure to create valid nginx config file " + \
                    "for application #{app}."
        place_error_app(app, error_msg)
      end

      # send a warmup request to the app to get it loaded - can shave a
      # number of seconds off the initial request if it's java or go
      # go provides a default warmup route
      # TODO: if the user specifies a warmup route, call it instead of /
      warmup_url = "/"

      app_manager = AppManagerClient.new()
      # TODO(cgb): What happens if the user updates their env vars between app
      # deploys?
      if is_new_app
        @app_info_map[app]['appengine'] = []
        @num_appengines.times { |index|
          Djinn.log_info("Starting #{app_language} app #{app} on " +
            "#{HelperFunctions.local_ip}:#{@appengine_port}")
          @app_info_map[app]['appengine'] << @appengine_port

          xmpp_ip = get_login.public_ip

          pid = app_manager.start_app(app, @appengine_port, 
            get_load_balancer_ip(), @nginx_port, app_language, 
            xmpp_ip, [Djinn.get_nearest_db_ip()],
            HelperFunctions.get_app_env_vars(app))

          if pid == -1
            place_error_app(app, "ERROR: Unable to start application " + \
                "#{app}. Please check the application logs.")
          end

          pid_file_name = "#{CONFIG_FILE_LOCATION}/#{app}-#{@appengine_port}.pid"
          HelperFunctions.write_file(pid_file_name, pid)

          @appengine_port += 1
        }
      else
        Djinn.log_info("Killing all AppServers hosting old version of application #{app}")
        result = app_manager.kill_app_instances_for_app(app)
      end

      HAProxy.update_app_config(app, app_number,
        @app_info_map[app]['appengine'], my_private)
      Nginx.reload
      HAProxy.reload

      if is_new_app
        loop {
          Kernel.sleep(5)
          success = uac.add_instance(app, my_public, @nginx_port)
          Djinn.log_debug("Add instance returned #{success}")
          if success
            # tell ZK that we are hosting the app in case we die, so that
            # other nodes can update the UserAppServer on its behalf
            ZKInterface.add_app_instance(app, my_public, @nginx_port)
            break
          end
        }

        nginx = @nginx_port
        haproxy = @haproxy_port

        # Update our local information so that we know later what ports
        # we're using to host this app on for nginx and haproxy
        @app_info_map[app]['nginx'] = @nginx_port
        @app_info_map[app]['haproxy'] = @haproxy_port

        login_ip = get_login.public_ip

        Thread.new {
          haproxy_location = "http://#{my_private}:#{haproxy}#{warmup_url}"
          nginx_location = "http://#{my_public}:#{nginx}#{warmup_url}"

          wget_haproxy = "wget #{WGET_OPTIONS} #{haproxy_location}"
          wget_nginx = "wget #{WGET_OPTIONS} #{nginx_location}"

          Djinn.log_run(wget_haproxy)
          Djinn.log_run(wget_nginx)
        }

        @nginx_port += 1
        @haproxy_port += 1

        # now doing this at the real end so that the tools will
        # wait for the app to actually be running before returning
        done_uploading(app, app_path, @@secret)
      end
    end

    APPS_LOCK.synchronize {
      if @app_names.include?("none")
        @apps_loaded = @apps_loaded - ["none"]
        @app_names = @app_names - ["none"]
      end

      if is_new_app
        @apps_loaded << app
      else
        @apps_to_restart.delete(app)
      end
    }
  end


  # Writes a nginx configuration file that tells nginx to act as a full proxy,
  # to one or more machines that host app servers.
  #
  # Args:
  #   app: A String representing the appid of the app to write an nginx config
  #     file for.
  def write_full_proxy_nginx_file(app)
    app_number = @nginx_port - Nginx::START_PORT
    proxy_port = HAProxy.app_listen_port(app_number)
    login_ip = get_login.private_ip

    success = Nginx.write_fullproxy_app_config(app, app_number, my_node.public_ip,
      my_node.private_ip, proxy_port, login_ip, get_all_appengine_nodes())
    if success
      Nginx.reload
    else
      err_msg = "ERROR: Failure to create valid nginx config file" + \
                " for application #{app} full proxy."
      place_error_app(app, err_msg)
    end

    @app_info_map[app]['nginx'] = @nginx_port
    @app_info_map[app]['haproxy'] = @haproxy_port

    @nginx_port += 1
    @haproxy_port += 1
  end


  # This method guards access to perform_scaling_for_appservers so that only 
  # one thread call it at a time. We also only perform scaling if the user 
  # wants us to, and simply return otherwise.
  def scale_appservers_on_this_node
    if !my_node.is_appengine?
      return
    end

    if @creds["autoscale"].downcase == "true"
      perform_scaling_for_appservers()
    end
  end


  # Adds or removes AppServers within a node based on the number of requests
  # that each application has received as well as the number of requests that
  # are sitting in haproxy's queue, waiting to be served.
  #
  # TODO: Accessing global state should use a lock. Failure to do so causes 
  #   race conditions where arrays are accessed using indexes that are no 
  #   longer valid. 
  #
  def perform_scaling_for_appservers()
    APPS_LOCK.synchronize {
      @apps_loaded.each { |app_name|

        next if app_name == "none"  
        initialize_scaling_info_for_app(app_name)

        # Always get scaling info, as that will send this info to the
        # AppDashboard for users to view.
        case get_scaling_info_for_app(app_name)
        when :scale_up
          Djinn.log_debug("Considering scaling up app #{app_name}.")
          try_to_scale_up(app_name)
        when :scale_down
          Djinn.log_debug("Considering scaling down app #{app_name}.")
          try_to_scale_down(app_name)
        else
          Djinn.log_debug("Not scaling app #{app_name} up or down right now.")
        end
      }
    }
  end


  # Sets up information about the request rate and number of requests in
  # haproxy's queue for the given application.
  #
  # Args:
  #   app_name: The name of the application to set up scaling info
  #   force: A boolean value that indicates if we should reset the scaling
  #     info even in the presence of existing scaling info.
  def initialize_scaling_info_for_app(app_name, force=false)
    return if @initialized_apps[app_name] and !force

    @total_req_rate[app_name] = 0
    @last_sampling_time[app_name] = Time.now.to_i

    if !@last_decision.has_key?(app_name)
      @last_decision[app_name] = 0
    end

    @initialized_apps[app_name] = true
  end
  

  # Queries haproxy to see how many requests are queued for a given application
  # and how many requests are served at a given time. Based on this information,
  # this method reports whether or not AppServers should be added, removed, or
  # if no changes are needed.
  def get_scaling_info_for_app(app_name)
    Djinn.log_debug("Getting scaling info for application #{app_name}")
  
    # Now see how many requests came in for our app and how many are enqueued
    monitor_cmd = "echo \"show info;show stat\" | " +
      "socat stdio unix-connect:/etc/haproxy/stats | grep #{app_name}"

    total_requests_seen = 0
    total_req_in_queue = 0
    time_requests_were_seen = 0
    Djinn.log_run(monitor_cmd).each { |line|
      parsed_info = line.split(',')
      if parsed_info.length < TOTAL_REQUEST_RATE_INDEX  # no request info here
        next
      end

      service_name = parsed_info[SERVICE_NAME_INDEX]

      if service_name == "FRONTEND"
        total_requests_seen = parsed_info[TOTAL_REQUEST_RATE_INDEX].to_i
        time_requests_were_seen = Time.now.to_i
        Djinn.log_debug("#{app_name} #{service_name} Requests Seen " +
          "#{total_requests_seen}")
      end

      if service_name == "BACKEND"
        total_req_in_queue = parsed_info[REQ_IN_QUEUE_INDEX].to_i
        Djinn.log_debug("#{app_name} #{service_name} Queued Currently " +
          "#{total_req_in_queue}")
      end
    }

    if time_requests_were_seen.zero?
      Djinn.log_warn("Didn't see any request data - not sure whether to scale up or down.")
      return :no_change
    end

    update_request_info(app_name, total_requests_seen, time_requests_were_seen)

    if total_req_in_queue.zero?
      Djinn.log_debug("No requests are enqueued for app #{app_name} - " +
        "advising that we scale down within this machine.")
      return :scale_down
    end

    if total_req_in_queue > SCALEUP_QUEUE_SIZE_THRESHOLD
      Djinn.log_debug("#{total_req_in_queue} requests are enqueued for app " +
        "#{app_name} - advising that we scale up within this machine.")
      return :scale_up
    end

    Djinn.log_debug("#{total_req_in_queue} requests are enqueued for app " +
      "#{app_name} - advising that don't scale either way on this machine.")
    return :no_change
  end


  # Updates internal state about the number of requests seen for the given App
  # Engine app, as well as how many requests are currently enqueued for it.
  # Some of this information is also sent to the AppDashboard for viewing by
  # users.
  #
  # Args:
  #   app_name: A String that indicates the name this Google App Engine
  #     application is registered as.
  #   total_requests_seen: An Integer that indicates how many requests haproxy
  #     has received for the given application since we reloaded it (which
  #     occurs when we start the app or add/remove AppServers).
  #   time_requests_were_seen: An Integer that represents the epoch time when we
  #     got request info from haproxy.
  def update_request_info(app_name, total_requests_seen, time_requests_were_seen)
    Djinn.log_debug("Total requests seen now is #{total_requests_seen}, last " +
      "time was #{@total_req_rate[app_name]}")
    requests_since_last_sampling = total_requests_seen - @total_req_rate[app_name]
    time_since_last_sampling = time_requests_were_seen - @last_sampling_time[app_name]
    if time_since_last_sampling.zero?
      time_since_last_sampling = 1
    end

    average_request_rate = Float(requests_since_last_sampling) / Float(time_since_last_sampling)
    if average_request_rate < 0
      Djinn.log_info("Saw negative request rate for app #{app_name}, so " +
        "resetting our haproxy stats for this app.")
      initialize_scaling_info_for_app(app_name, force=true)
      return
    end

    send_request_info_to_dashboard(app_name, time_requests_were_seen,
      average_request_rate)
    Djinn.log_debug("Total requests will be set to #{total_requests_seen} " +
      "for app #{app_name}, with last sampling time #{time_requests_were_seen}")
    @total_req_rate[app_name] = total_requests_seen
    @last_sampling_time[app_name] = time_requests_were_seen
  end


  def try_to_scale_up(app_name)
    time_since_last_decision = Time.now.to_i - @last_decision[app_name]
    if @app_info_map[app_name].nil? or @app_info_map[app_name]['appengine'].nil?
      Djinn.log_info("Not scaling up app #{app_name}, since we aren't " +
        "hosting it anymore.")
      return
    end

    appservers_running = @app_info_map[app_name]['appengine'].length

    if appservers_running >= MAX_APPSERVERS_ON_THIS_NODE
      Djinn.log_info("The maximum number of AppServers for this app " +
        "are already running, so don't add any more on this machine.")
      ZKInterface.request_scale_up_for_app(app_name, my_node.private_ip)
      return
    end

    Djinn.log_info("Adding a new AppServer on this node for #{app_name}")
    add_appserver_process(app_name)
    initialize_scaling_info_for_app(app_name, force=true)
    @last_decision[app_name] = Time.now.to_i
  end


  def try_to_scale_down(app_name)
    time_since_last_decision = Time.now.to_i - @last_decision[app_name]
    if @app_info_map[app_name].nil? or @app_info_map[app_name]['appengine'].nil?
      Djinn.log_debug("Not scaling down app #{app_name}, since we aren't " +
        "hosting it anymore.")
      return
    end

    appservers_running = @app_info_map[app_name]['appengine'].length

    if appservers_running <= MIN_APPSERVERS_ON_THIS_NODE
      Djinn.log_debug("The minimum number of AppServers for this app " +
        "are already running, so don't remove any more off this machine.")
      ZKInterface.request_scale_down_for_app(app_name, my_node.private_ip)
      return
    end

    Djinn.log_info("Removing an AppServer on this node for #{app_name}")
    remove_appserver_process(app_name)
    initialize_scaling_info_for_app(app_name, force=true)
    @last_decision[app_name] = Time.now.to_i
  end


  # Starts a new AppServer for the given application.
  # TODO(cgb): This is mostly copy-pasta'd from start_appengine - consolidate
  # this somehow
  #
  # Args:
  #   app: Name of the application for which we're adding a process instance
  #
  def add_appserver_process(app)
    # Starting a appserver instance on request to scale the application 
    @state = "Adding an AppServer for #{app}"

    uac = UserAppClient.new(@userappserver_private_ip, @@secret)
    app_manager = AppManagerClient.new()

    warmup_url = "/"

    app_data = uac.get_app_data(app)
    
    Djinn.log_debug("Get app data for #{app} said [#{app_data}]")

    loop {
      Djinn.log_info("Waiting for app data to have instance info for app named #{app}: #{app_data}")

      app_data = uac.get_app_data(app)
      if app_data[0..4] != "Error"
        break
      end
      Kernel.sleep(5)
    }
    
    app_language = app_data.scan(/language:(\w+)/).flatten.to_s
    my_public = my_node.public_ip
    my_private = my_node.private_ip

    app_is_enabled = uac.does_app_exist?(app)
    Djinn.log_debug("is app #{app} enabled? #{app_is_enabled}")
    if app_is_enabled == "false"
      return  
    end

    nginx_port = @app_info_map[app]['nginx']
    haproxy_port = @app_info_map[app]['haproxy']
    @app_info_map[app]['appengine'] << @appengine_port

    app_number = nginx_port - Nginx::START_PORT

    my_private = my_node.private_ip
    Djinn.log_debug("port apps error contains - #{@app_info_map[app]['appengine']}")
    HAProxy.update_app_config(app, app_number, @app_info_map[app]['appengine'],
      my_private)     

    Djinn.log_debug("Adding #{app_language} app #{app} on #{HelperFunctions.local_ip}:#{@appengine_port} ")

    xmpp_ip = get_login.public_ip

    pid = app_manager.start_app(app, @appengine_port, 
            get_load_balancer_ip(), nginx_port, app_language, 
            xmpp_ip, [Djinn.get_nearest_db_ip()],
            HelperFunctions.get_app_env_vars(app))

    if pid == -1
      Djinn.log_error("ERROR: Unable to start application #{app} on port #{@appengine_port}.")
      next
    end
    pid_file_name = "#{CONFIG_FILE_LOCATION}/#{app}-#{@appengine_port}.pid"
    HelperFunctions.write_file(pid_file_name, pid)

    @appengine_port += 1

    # Nginx.reload 
    HAProxy.reload

    Thread.new {
      haproxy_location = "http://#{my_private}:#{haproxy_port}#{warmup_url}"
      nginx_location = "http://#{my_public}:#{nginx_port}#{warmup_url}"

      wget_haproxy = "wget #{WGET_OPTIONS} #{haproxy_location}"
      wget_nginx = "wget #{WGET_OPTIONS} #{nginx_location}"
 
      Djinn.log_run(wget_haproxy)
      Djinn.log_run(wget_nginx)
    }
  end


  # Terminates a random AppServer that hosts the specified App Engine app.
  #
  # Args:
  #   app: The name of the application for which we're removing a 
  #        process instance
  #
  def remove_appserver_process(app)
    @state = "Stopping an AppServer to free unused resources"
    Djinn.log_debug("Deleting appserver instance to free up unused resources")

    uac = UserAppClient.new(@userappserver_private_ip, @@secret)
    app_manager = AppManagerClient.new()
    warmup_url = "/"

    my_public = my_node.public_ip
    my_private = my_node.private_ip
    app_number = @app_info_map[app]['nginx'] - Nginx::START_PORT

    app_data = uac.get_app_data(app)

    Djinn.log_debug("Get app data for #{app} said [#{app_data}]")

    app_is_enabled = uac.does_app_exist?(app)
    Djinn.log_debug("is app #{app} enabled? #{app_is_enabled}")
    if app_is_enabled == "false"
      return
    end

    # Select a random AppServer to kill.
    ports = @app_info_map[app]['appengine']
    port = ports[rand(ports.length)]

    if !app_manager.stop_app_instance(app, port)
      Djinn.log_error("Unable to stop instance on port #{port} app #{app_name}")
    end

    # Delete the port number from the app_info_map
    @app_info_map[app]['appengine'].delete(port)

    HAProxy.update_app_config(app, app_number, @app_info_map[app]['appengine'],
      my_private)
    HAProxy.reload

    # And tell the AppDashboard that the AppServer has been killed.
    delete_instance_from_dashboard(app, port)
  end 
 

  # Tells the AppDashboard how many requests were served for the named
  # application at the given time, so that it can display this info to users
  # graphically.
  #
  # Args:
  #   app_id: A String that indicates which application id we are storing
  #     request info for.
  #   timestamp: An Integer that indicates the epoch time when we measured the
  #     request rate for the given application.
  #   request_rate: An Integer that indicates how many requests were served for
  #     the given application in the last second since we queried it.
  # Returns:
  #   true if the request info was successfully sent, and false otherwise.
  def send_request_info_to_dashboard(app_id, timestamp, request_rate)
    Djinn.log_debug("Sending a log with request rate #{app_id}, timestamp " +
      "#{timestamp}, request rate #{request_rate}")
    encoded_request_info = JSON.dump({
      'timestamp' => timestamp,
      'request_rate' => request_rate
    })

    begin
      url = URI.parse("https://#{get_login.public_ip}/apps/json/#{app_id}")
      http = Net::HTTP.new(url.host, url.port)
      http.use_ssl = true
      response = http.post(url.path, encoded_request_info,
        {'Content-Type'=>'application/json'})
      return true
    rescue OpenSSL::SSL::SSLError, NotImplementedError, Errno::EPIPE, Errno::ECONNRESET
      retry
    rescue Exception
      # Don't crash the AppController because we weren't able to send over
      # the request info - just inform the caller that we couldn't send it.
      Djinn.log_info("Couldn't send request info for app #{app_id} to #{url}")
      return false
    end
  end


  def scale_appservers_across_nodes()
    return if !my_node.is_login?
    # TODO(cgb): Do we need to get the apps lock here?
    Djinn.log_debug("Seeing if we need to spawn new AppServer nodes")

    appservers = @nodes.select { |node| node.is_appengine? }
    num_of_appservers = appservers.length

    nodes_needed = []
    all_scaling_requests = {}
    @apps_loaded.each { |appid|
      scaling_requests = ZKInterface.get_scaling_requests_for_app(appid)
      all_scaling_requests[appid] = scaling_requests
      ZKInterface.clear_scaling_requests_for_app(appid)
      scale_up_requests = scaling_requests.select { |item| item == "scale_up" }
      num_of_scale_up_requests = scale_up_requests.length

      # Spawn an additional node if (1) more than one node is receiving too much
      # web traffic, or (2) there's only one node, and it's receiving too much
      # web traffic.
      if num_of_scale_up_requests > 1 or (num_of_scale_up_requests > 0 and 
        num_of_appservers == 1)
        Djinn.log_info("Nodes at #{scale_up_requests.join(' and ')} have " + 
          "requested more AppServers for app #{appid}, so adding a node.")
        nodes_needed << ["memcache", "taskqueue_slave", "appengine"]
      end
    }

    if nodes_needed.empty?
      Djinn.log_debug("Not adding any new AppServers at this time. Checking " +
        "to see if we need to scale down.")
      return examine_scale_down_requests(all_scaling_requests)
    end

    if Time.now.to_i - @last_scaling_time < SCALEUP_TIME_THRESHOLD
      Djinn.log_info("Not scaling up right now, as we recently scaled " +
        "up or down.")
      return 0
    end

    Djinn.log_info("Need to spawn #{nodes_needed.length} new AppServers.")
    added_nodes = start_new_roles_on_nodes(nodes_needed,
      @creds['instance_type'], @@secret)

    if added_nodes != "OK"
      Djinn.log_error("Was not able to add #{nodes_needed.length} new nodes" +
        " because: #{added_nodes}")
      return 0
    end

    regenerate_nginx_config_files()
    @last_scaling_time = Time.now.to_i
    return nodes_needed.length
  end


  # Searches through the requests to scale up and down each application in this
  # AppScale deployment, and determines if machines need to be terminated due
  # to excess capacity.
  #
  # Args:
  #   all_scaling_votes: A Hash that maps each appid (a String) to the votes
  #     received to scale the app up or down (an Array of Strings).
  # Returns:
  #   An Integer that indicates how many nodes were added to this AppScale
  #   deployment. A negative number indicates that that many nodes were
  #   removed from this AppScale deployment.
  def examine_scale_down_requests(all_scaling_votes)
    # First, only scale down in cloud environments.
    if !is_cloud?
      Djinn.log_debug("Not scaling down VMs, because we aren't in a cloud.")
      return 0
    end

    if @nodes.length <= Integer(@creds['min_images'])
      Djinn.log_debug("Not scaling down VMs right now, as we are at the " +
        "minimum number of nodes the user wants to use.")
      return 0
    end

    # Second, only consider scaling down if nobody wants to scale up.
    @apps_loaded.each { |appid|
      scale_ups = all_scaling_votes[appid].select { |vote| vote == "scale_up" }
      if scale_ups.length > 0
        Djinn.log_info("Not scaling down VMs, because app #{appid} wants to scale" +
          " up.")
        return 0
      end
    }

    # Third, only consider scaling down if we get two votes to scale down on
    # the same app, just like we do for scaling up.
    scale_down_threshold_reached = false
    @apps_loaded.each { |appid|
      scale_downs = all_scaling_votes[appid].select { |vote| vote == "scale_down" }
      if scale_downs.length > 1
        Djinn.log_info("Got #{scale_downs.length} votes to scale down app " +
          "#{appid}, so considering removing VMs.")
        scale_down_threshold_reached = true
      end
    }

    if !scale_down_threshold_reached
      Djinn.log_info("Not scaling down VMs right now, as not enough nodes have " +
        "requested it.")
      return 0
    end

    # Also, don't scale down if we just scaled up or down.
    if Time.now.to_i - @last_scaling_time < SCALEDOWN_TIME_THRESHOLD
      Djinn.log_info("Not scaling down VMs right now, as we recently scaled " +
        "up or down.")
      return 0
    end

    # Finally, find a node to remove and remove it.
    node_to_remove = nil
    @nodes.each { |node|
      if node.jobs == ["memcache", "taskqueue_slave", "appengine"]
        Djinn.log_info("Removing node #{node}")
        node_to_remove = node
        break
      end
    }

    if node_to_remove.nil?
      Djinn.log_warn("Tried to scale down but couldn't find a node to remove.")
      return 0
    end

    remove_app_hosting_data_for_node(node_to_remove.public_ip)
    remove_node_from_local_and_zookeeper(node_to_remove.public_ip)
    imc = InfrastructureManagerClient.new(@@secret)
    imc.terminate_instances(@creds, node_to_remove.instance_id)
    regenerate_nginx_config_files()
    @last_scaling_time = Time.now.to_i
    return -1
  end


  def stop_appengine()
    Djinn.log_info("Shutting down AppEngine")

    erase_app_instance_info
    Nginx.reload

    APPS_LOCK.synchronize { 
      @app_names = []
      @apps_loaded = []
      @restored = false
    }
  
    Djinn.log_run("pkill -f dev_appserver")
    Djinn.log_run("pkill -f DevAppServerMain")
  end

  # Returns true on success, false otherwise
  def copy_app_to_local(appname)
    app_dir = "/var/apps/#{appname}/app"
    app_path = "/opt/appscale/apps/#{appname}.tar.gz"

    if File.exists?(app_path)
      Djinn.log_debug("I already have a copy of app #{appname} - won't grab it remotely")
      return true
    else
      Djinn.log_debug("I don't have a copy of app #{appname} - will grab it remotely")
    end

    nodes_with_app = []
    retries_left = 10
    loop {
      nodes_with_app = ZKInterface.get_app_hosters(appname, @creds['keyname'])
      break if !nodes_with_app.empty?
      Djinn.log_info("[#{retries_left} retries left] Waiting for a node to " +
        "have a copy of app #{appname}")
      Kernel.sleep(5)
      retries_left -=1
      if retries_left.zero?
        Djinn.log_warn("Nobody appears to be hosting app #{appname}")
        return false
      end
    }

    # Try 3 times on each node known to have this application
    nodes_with_app.each { |node|
      ssh_key = node.ssh_key
      ip = node.public_ip
      tries = 3
      loop {
        Djinn.log_debug("Trying #{ip}:#{app_path} for the application")
        Djinn.log_run("scp -o StrictHostkeyChecking=no -i #{ssh_key} #{ip}:#{app_path} #{app_path}")
        if File.exist?("#{app_path}") == true
          Djinn.log_debug("Got a copy of #{appname} from #{ip}")
          return true 
        end
        Djinn.log_warn("Unable to get the application from #{ip}:#{app_path}! scp failed.")
        if tries > 0
          Djinn.log_debug("Trying again in 5 seconds") 
          tries = tries - 1
          Kernel.sleep(5)
        else
          Djinn.log_warn("Giving up on node #{ip} for the application")
          break
        end
      }
    }
    Djinn.log_error("Unable to get the application from any node")
    return false 
  end

  def start_xmpp_for_app(app, port, app_language)
    # create xmpp account for the app
    # for app named baz, this translates to baz@login_ip

    login_ip = get_login.public_ip
    uac = UserAppClient.new(@userappserver_public_ip, @@secret)
    xmpp_user = "#{app}@#{login_ip}"
    xmpp_pass = HelperFunctions.encrypt_password(xmpp_user, @@secret)
    uac.commit_new_user(xmpp_user, xmpp_pass, "app")

    Djinn.log_debug("Created user [#{xmpp_user}] with password [#{@@secret}] and hashed password [#{xmpp_pass}]")

    if Ejabberd.does_app_need_receive?(app, app_language)
      start_cmd = "#{PYTHON27} #{APPSCALE_HOME}/XMPPReceiver/xmpp_receiver.py #{app} #{login_ip} #{port} #{@@secret}"
      stop_cmd = "ps ax | grep '#{start_cmd}' | grep -v grep | awk '{print $1}' | xargs -d '\n' kill -9"
      watch_name = "xmpp-#{app}"
      GodInterface.start(watch_name, start_cmd, stop_cmd, 9999)
      Djinn.log_debug("App #{app} does need xmpp receive functionality")
    else
      Djinn.log_debug("App #{app} does not need xmpp receive functionality")
    end
  end

  # Stop the xmpp receiver for an applicaiton.
  # 
  # Args:
  #   app: The application ID whose XMPPReceiver we should shut down.
  def stop_xmpp_for_app(app)
    Djinn.log_info("Shutting down xmpp receiver for app: #{app}")
    watch_name = "xmpp-#{app}"
    GodInterface.remove(watch_name)
    stop_cmd = "ps ax | grep 'xmpp_receiver.py #{app}' | grep -v grep | awk '{print $1}' | xargs -d '\n' kill -9"
    Djinn.log_run(stop_cmd)
    Djinn.log_info("Done shutting down xmpp receiver for app: #{app}")
  end

  def start_open
    return
  end

  def stop_open
    return
  end
end
