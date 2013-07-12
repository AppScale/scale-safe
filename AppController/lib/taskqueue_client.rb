#!/usr/bin/ruby -w
# Programmer: Navraj Chohan <nlake44@gmail.com>

require 'base64'
require 'helperfunctions'
require 'json'
require 'net/http'
require 'timeout'

# Number of seconds to wait before timing out when doing a remote call.
# This number should be higher than the maximum time required for remote calls
# to properly execute (i.e., starting a process may take more than 2 minutes).
MAX_TIME_OUT = 180

# This is transitional glue code as we shift from ruby to python. The 
# Taskqueue server is written in python and hence we use a REST client 
# to communicate between the two services.
class TaskQueueClient

  # The connection to use and IP to connect to.
  attr_reader :conn, :ip

  # The port that the TaskQueue Server binds to.
  SERVER_PORT = 64839

  # Location of where the nearest taskqueue server is.
  NEAREST_TQ_LOCATION = '/etc/appscale/rabbitmq_ip'

  # Initialization function for TaskQueueClient
  def initialize()
    @host = HelperFunctions.read_file(NEAREST_TQ_LOCATION)
  end

  # Make a REST call out to the TaskQueue Server. 
  # 
  # Args: 
  #   timeout: The maximum time to wait on a remote call
  #   retry_on_except: Boolean if we should keep retrying the 
  #     the call
  # Returns:
  #   The result of the remote call.
  def make_call(timeout, retry_on_except, callr)
    result = ""
    Djinn.log_debug("Calling the TaskQueue Server: #{callr}")
    begin
      Timeout::timeout(timeout) do
        begin
          yield if block_given?
        end
      end
    rescue Errno::ECONNREFUSED => except
      if retry_on_except
        Djinn.log_warn("Saw a connection refused when calling #{callr}" +
          " - trying again momentarily.")
        sleep(1)
        retry
      else
        trace = except.backtrace.join("\n")
        Djinn.log_warn("We saw an unexpected error of the type #{except.class} with the following message:\n#{except}, with trace: #{trace}")
      end 
   rescue Exception => except
      if except.class == Interrupt
        abort
      end

      Djinn.log_warn("An exception of type #{except.class} was thrown: #{except}.")
      retry if retry_on_except
    end
  end
 
   # Wrapper for REST calls to the TaskQueue Server to start a
   # taskqueue worker on a taskqueue node.
   #
   # Args:
   #   app_name: Name of the application.
   # Returns:
   #   JSON response.
   def start_worker(app_name)
    config = {'app_id' => app_name, 'command' => 'update'}
    json_config = JSON.dump(config)
    response = nil
     
    make_call(MAX_TIME_OUT, false, "start_worker"){
      url = URI.parse('http://' + @host + ":#{SERVER_PORT}/startworker")
      http = Net::HTTP.new(url.host, url.port)
      response = http.post(url.path, json_config, {'Content-Type'=>'application/json'})
    }
    if response == nil:
      return {"error" => true, "reason" => "Unable to get a response"}
    end
    return JSON.load(response.body)
  end

   # Wrapper for REST calls to the TaskQueue Server to stop a
   # taskqueue worker on a taskqueue node.
   #
   # Args:
   #   app_name: Name of the application.
   # Returns:
   #   JSON response.
   def stop_worker(app_name)
    config = {'app_id' => app_name, 
              'command' => 'update'}
    json_config = JSON.dump(config)
    response = nil
    make_call(MAX_TIME_OUT, false, "stop_worker"){
      url = URI.parse('http://' + @host + ":#{SERVER_PORT}/stopworker")
      http = Net::HTTP.new(url.host, url.port)
      response = http.post(url.path, json_config, {'Content-Type'=>'application/json'})
    }
    if response == nil:
      return {"error" => true, "reason" => "Unable to get a response"}
    end
    return JSON.load(response.body)
  end

end
