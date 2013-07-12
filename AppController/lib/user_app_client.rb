#!/usr/bin/ruby -w
# Programmer: Chris Bunch


# Imports within Ruby's standard libraries
require 'base64'
require 'openssl'
require 'soap/rpc/driver'
require 'timeout'


# AppControllers and other services need to read or write data relating to users
# and applications hosted in AppScale. Since it has to be done in a
# database-agnostic fashion, we throw up a special server that responds to SOAP
# requests for this information, called the UserAppServer. This class provides
# convenience methods for interacting with the UserAppServer.
class UserAppClient
  attr_reader :conn, :ip, :secret


  # The port that the UserAppServer binds to, by default.
  SERVER_PORT = 4343


  def initialize(ip, secret)
    @ip = ip
    @secret = secret
    
    @conn = SOAP::RPC::Driver.new("https://#{@ip}:#{SERVER_PORT}")
    @conn.add_method("change_password", "user", "password", "secret")
    @conn.add_method("commit_new_user", "user", "passwd", "utype", "secret")
    @conn.add_method("commit_new_app", "user", "appname", "language", "secret")
    @conn.add_method("commit_tar", "app_name", "tar", "secret")
    @conn.add_method("delete_app", "appname", "secret")    
    @conn.add_method("is_app_enabled", "appname", "secret")
    @conn.add_method("is_user_cloud_admin", "username", "secret")
    @conn.add_method("does_user_exist", "username", "secret")
    @conn.add_method("get_user_data", "username", "secret")
    @conn.add_method("get_app_data", "appname", "secret")
    @conn.add_method("delete_instance", "appname", "host", "port", "secret")
    @conn.add_method("get_tar", "app_name", "secret")
    @conn.add_method("add_instance", "appname", "host", "port", "secret")
    @conn.add_method("get_all_apps", "secret")
    @conn.add_method("get_all_users", "secret")
  end

  def make_call(timeout, retry_on_except, callr)
    result = ""
    Djinn.log_debug("Calling #{callr} on an UserAppServer at #{@ip}")
    begin
      Timeout::timeout(timeout) do
        begin
          yield if block_given?
        end
      end
    rescue OpenSSL::SSL::SSLError
      Djinn.log_warn("Retrying (SSL) - calling #{callr} on UserAppServer at #{@ip}")
      retry
    rescue Errno::ECONNREFUSED
      if retry_on_except
        sleep(1)
        Djinn.log_warn("Retrying (ConnRefused) - calling #{callr} on UserAppServer at #{@ip}")
        retry
      else
        abort("We were unable to establish a connection with the UserAppServer at the designated location. Is AppScale currently running?")
      end 
   rescue Exception => except
      if except.class == Interrupt
        abort
      end

      Djinn.log_warn("An exception of type #{except.class} was thrown.")
      Djinn.log_warn("Retrying - calling #{callr} on UserAppServer at #{@ip}")
      retry if retry_on_except
    end
  end
  
  def commit_new_user(user, encrypted_password, user_type, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "commit_new_user") {
      result = @conn.commit_new_user(user, encrypted_password, user_type, @secret)
    }

    if result == "true"
      puts "\nYour user account has been created successfully."
    elsif result == "false"
      abort("\nWe were unable to create your user account. Please contact your cloud administrator for further details.")
    else
      puts "\n[unexpected] Commit new user returned: [#{result}]"
    end
  end
  
  def commit_new_app(user, app_name, language, file_location)
    commit_new_app_name(user, app_name, language)
    commit_tar(app_name, file_location)
  end
  
  def commit_new_app_name(user, app_name, language, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "commit_new_app_name") {
      result = @conn.commit_new_app(user, app_name, language, @secret)
    }

    if result == "true"
      puts "We have reserved the name #{app_name} for your application."
    elsif result == "Error: appname already exist"
      puts "We are uploading a new version of the application #{app_name}."
    elsif result == "Error: User not found"
      abort("We were unable to reserve the name of your application. Please contact your cloud administrator for more information.")
    else
      puts "[unexpected] Commit new app says: [#{result}]"
    end
  end
  
  def commit_tar(app_name, file_location, retry_on_except=true)
    file = File.open(file_location, "rb")
    tar_contents = Base64.encode64(file.read)
    
    result = ""
    make_call(300, retry_on_except, "commit_tar") {
      result = @conn.commit_tar(app_name, tar_contents, @secret)
    }
 
    if result == "true"
      puts "#{app_name} was uploaded successfully."
    elsif result == "Error: app does not exist"
      abort("We were unable to upload your application. Please contact your cloud administrator for more information.")
    else
      puts "[unexpected] Commit new tar says: [#{result}]"
    end
  end
  
  def change_password(user, new_password, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "change_password") {
      result = @conn.change_password(user, new_password, @secret)
    }
        
    if result == "true"
      puts "We successfully changed the password for the given user."
    elsif result == "Error: user not found"
      puts "We were unable to locate a user with the given username."
    else
      puts "[unexpected] Got this message back: [#{result}]"
    end
  end

  def delete_app(app, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "delete_app") {
      result = @conn.delete_app(app, @secret)
    }
    
    if result == "true"
      return true
    else
      return result
    end  
  end

  def does_app_exist?(app, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "does_app_exist") {
      result = @conn.is_app_enabled(app, @secret)
    }
    
    if result == "true"
      return true
    else
      return false
    end
  end
  
  def does_user_exist?(user, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "does_user_exist") {
      result = @conn.does_user_exist(user, @secret)
    }
    
    return result
  end

  def get_user_data(username, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "get_user_data") {
      result = @conn.get_user_data(username, @secret)
    }

    return result
  end

  def get_app_data(appname, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "get_app_data") {
      result = @conn.get_app_data(appname, @secret)
    }

    return result
  end

  def delete_instance(appname, host, port, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "delete_instance") {
      result = @conn.delete_instance(appname, host, port, @secret)
    }

    return result
  end

  def get_all_apps(retry_on_except=true)
    all_apps = ""
    make_call(10, retry_on_except, "get_all_apps") {
      all_apps = @conn.get_all_apps(@secret)
    }

    app_list = all_apps.split(":")
    app_list = app_list - [app_list[0]] # first item is a dummy value
    return app_list
  end

  def get_all_users(retry_on_except=true)
    all_users = ""
    make_call(10, retry_on_except, "get_all_users") {
      all_users = @conn.get_all_users(@secret)
    }

    user_list = all_users.split(":")
    user_list = user_list - [user_list[0]]  # first item is a dummy value
    return user_list
  end

  def get_tar(appname, retry_on_except=true)
    result = ""
    make_call(300, retry_on_except, "get_tar") {
      result = @conn.get_tar(appname, @secret)
    }

    return result
  end

  def add_instance(appname, host, port, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "add_instance") {
      result = @conn.add_instance(appname, host, port, @secret)
    }

    if result == "true"
      return true
    else
      return false
    end
  end

  # This method sees if the given app is already uploaded in the system.
  # TODO(cgb): compare this with is_app_enabled? above
  def is_app_uploaded?(appname)
    all_apps = get_all_apps()
    return app_list.include?(appname)
  end

  def is_user_cloud_admin?(user, retry_on_except=true)
    result = ""
    make_call(10, retry_on_except, "is_user_cloud_admin") {
      result = @conn.is_user_cloud_admin(user, @secret)
    }
   
    if result == "true"
      return true
    else
      return false
    end
  end
 
  # This method returns an array of strings, each corresponding to a
  # ip:port that the given app is hosted at.
  def get_hosts_for_app(appname)
    app_data = get_app_data(appname)
    hosts = app_data.scan(/\nhosts:([\d\.|:]+)\n/).flatten.to_s.split(":")
    ports = app_data.scan(/\nports: ([\d|:]+)\n/).flatten.to_s.split(":")

    host_list = []

    hosts.each_index { |i|
      host_list << "#{hosts[i]}:#{ports[i]}"
    }

    return host_list
  end

  # This method finds the first user who is a cloud administrator. Since the
  # UserAppServer doesn't provide a function that does this for us, we have
  # to get a list of all the users that exist and individually query the
  # UAServer to find out who the cloud admin is.
  # TODO(cgb): Maybe cache who the cloud admin is to speed up future queries?
  def get_cloud_admin()
    user_list = get_all_users()

    user_list.each { |user|
      return user if is_user_cloud_admin?(user)
    }

    raise Exception.new("Couldn't find a cloud administrator")
  end
end
