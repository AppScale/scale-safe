# Programmer: Chris Bunch


$:.unshift File.join(File.dirname(__FILE__), "..", "..")
require 'neptune_manager'


$:.unshift File.join(File.dirname(__FILE__))
require 'task_engine'


require 'rubygems'
require 'httparty'


class GoogleAppEnginePushQueue
  include HTTParty
end


class TaskEngineGoogleAppEngine < TaskEngine


  attr_accessor :appid
  

  attr_accessor :appcfg_cookies
  

  attr_accessor :function


  NAME = "appengine-push-q"


  def initialize(credentials)
    if credentials.class != Hash
      raise BadConfigurationException.new
    end

    if credentials['appid'].nil?
      raise BadConfigurationException.new
    end
    @appid = credentials['appid']

    if credentials['appcfg_cookies'].nil?
      raise BadConfigurationException.new
    end
    @appcfg_cookies = credentials['appcfg_cookies']

    if credentials['function'].nil?
      raise BadConfigurationException.new
    end
    @function = credentials['function']
  end


  # Uploading the application to Google App Engine takes a non-trivially
  # long amount of time, and since the application can be invoked multiple
  # times in a row, it's unnecessary to keep re-uploading the app. Thus,
  # we skip avoiding the app if we detect that it is the same app as our
  # app - identified by its file name and function.
  def app_needs_uploading?(job_data)
    NeptuneManager.log("seeing if app needs to be uploaded to App Engine")
    return false

    # We're expecting to see the filename (minus the extension), a dot, and
    # the function name (no parens).
    extension = File.extname(job_data['@code'])
    file = File.basename(job_data['@code'], extension)
    expected = file + "." + @function

    # Oration builds apps with an /id route, that returns the file and
    # function that will be executed. We can compare that against our
    # file and function to see if they are the same (and thus if the app
    # needs to be uploaded).
    host = get_app_url(job_data)
    begin
      actual = GoogleAppEnginePushQueue.get("#{host}/id")
    rescue NoMethodError  # if the host is down
      actual = ""
    end

    NeptuneManager.log("expected is [#{expected}], actual is [#{actual}]")
    if expected == actual
      # then the app in App Engine is up to date, so don't upload a new
      # version of the app
      NeptuneManager.log("App is already in App Engine - don't upload it again")
      return false
    else
      # then the app in App Engine isn't the same as this app, so
      # do upload a new version of the app
      NeptuneManager.log("App does not match App Engine version - upload it")
      return true
    end
  end


  def self.upload_app(job_data, app_location)
    NeptuneManager.log("uploading app to app engine")

    # to avoid having to get the user's email / password, we can get
    # their appcfg_cookies file and put it in ~
    remote = job_data["@appcfg_cookies"]
    local = File.expand_path("~")
    NeptuneManager.copy_file_to_dir(remote, local, job_data)

    # TODO(cgb) - check for return val here
    appcfg = "/root/appscale/AppServer/appcfg.py"
    appid = job_data["@appid"]
    NeptuneManager.log_run("#{appcfg} update #{app_location} -A #{appid}")
  end


  def upload_app(job_data, app_location)
    NeptuneManager.log("uploading app to app engine")

    # to avoid having to get the user's email / password, we can get
    # their appcfg_cookies file and put it in ~
    remote = job_data["@appcfg_cookies"]
    local = File.expand_path("~")
    NeptuneManager.copy_file_to_dir(remote, local, job_data)

    # TODO(cgb) - check for return val here
    appcfg = "/root/appscale/AppServer/appcfg.py"
    appid = job_data["@appid"]
    NeptuneManager.log_run("#{appcfg} update #{app_location} -A #{appid}")
  end


  def get_app_url(job_data)
    #host = "http://#{job_data['@appid']}.cloudapp.net"
    host = "http://#{job_data['@appid']}.appspot.com"
    NeptuneManager.log("Google App Engine app is hosted at #{host}")
    return host
  end


  def engine_name()
    return "Google App Engine"
  end
end
