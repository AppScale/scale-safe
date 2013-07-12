# Programmer: Navraj Chohan <nlake44@gmail.com>

import glob
import json
import os
import subprocess
import sys
import time
import unittest
import urllib

from flexmock import flexmock

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
import app_manager_server
import god_app_configuration

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../lib"))
import file_io
import appscale_info
import god_interface
import testing

class TestAppManager(unittest.TestCase):
  def test_bad_convert_config_from_json(self):
    testing.disable_logging()
    self.assertEqual(None, app_manager_server.convert_config_from_json(None))
    self.assertEqual(None, app_manager_server.convert_config_from_json("{}"))
    self.assertEqual(None, app_manager_server.convert_config_from_json("{'app_name':'test'}"))

  def test_good_convert_config_from_json(self):
    configuration = {'app_name': 'test',
                     'app_port': 2000,
                     'language': 'python',
                     'load_balancer_ip': '127.0.0.1',
                     'load_balancer_port': 8080,
                     'xmpp_ip': '127.0.0.1',
                     'dblocations': ['127.0.0.1', '127.0.0.2'],
                     'env_vars': {}}
    configuration = json.dumps(configuration)

    self.assertEqual(True, isinstance(app_manager_server.convert_config_from_json(configuration), dict))
   
  def test_start_app_badconfig(self):
    testing.disable_logging()
    self.assertEqual(app_manager_server.BAD_PID, app_manager_server.start_app({}))

  def test_start_app_badconfig2(self):
    testing.disable_logging()
    self.assertEqual(app_manager_server.BAD_PID, app_manager_server.start_app("{'app_name':'test'}"))
  
  def test_start_app_bad_appname(self):
    configuration = {'app_name': 'badName!@#$%^&*([]/.,',
                     'app_port': 2000,
                     'language': 'python',
                     'load_balancer_ip': '127.0.0.1',
                     'load_balancer_port': 8080,
                     'xmpp_ip': '127.0.0.1',
                     'dblocations': ['127.0.0.1', '127.0.0.2'],
                     'env_vars': {}}
    configuration = json.dumps(configuration)
    self.assertEqual(-1, app_manager_server.start_app(configuration)) 

  def test_start_app_goodconfig_python(self):
    configuration = {'app_name': 'test',
                     'app_port': 2000,
                     'language': 'python',
                     'load_balancer_ip': '127.0.0.1',
                     'load_balancer_port': 8080,
                     'xmpp_ip': '127.0.0.1',
                     'dblocations': ['127.0.0.1', '127.0.0.2'],
                     'env_vars': {}}
    configuration = json.dumps(configuration)

    fake_secret = "XXXXXX"
    flexmock(appscale_info).should_receive('get_private_ip')\
      .and_return('<private_ip>')
    flexmock(appscale_info).should_receive('get_secret')\
                           .and_return(fake_secret)
    flexmock(god_app_configuration).should_receive('create_config_file')\
                               .and_return('fakeconfig')
    flexmock(god_interface).should_receive('start')\
                           .and_return(True)
    flexmock(app_manager_server).should_receive('wait_on_app')\
                         .and_return(True)
    flexmock(os).should_receive('popen')\
                .and_return(flexmock(read=lambda: '12345\n'))
    flexmock(file_io).should_receive('write')\
                        .and_return()
    self.assertEqual(12345, app_manager_server.start_app(configuration))
  
  def test_start_app_goodconfig_java(self):
    configuration = {'app_name': 'test',
                     'app_port': 2000,
                     'language': 'java',
                     'load_balancer_ip': '127.0.0.1',
                     'load_balancer_port': 8080,
                     'xmpp_ip': '127.0.0.1',
                     'dblocations': ['127.0.0.1', '127.0.0.2'],
                     'env_vars': {}}
    configuration = json.dumps(configuration)

    fake_secret = "XXXXXX"
    flexmock(appscale_info).should_receive('get_private_ip')\
      .and_return('<private_ip>')
    flexmock(appscale_info).should_receive('get_secret')\
                           .and_return(fake_secret)
    flexmock(god_app_configuration).should_receive('create_config_file')\
                               .and_return('fakeconfig')
    flexmock(god_interface).should_receive('start')\
                           .and_return(True)
    flexmock(app_manager_server).should_receive('wait_on_app')\
                         .and_return(True)
    flexmock(os).should_receive('popen')\
                .and_return(flexmock(read=lambda: '12345\n'))
    flexmock(file_io).should_receive('write')\
                        .and_return()
    flexmock(subprocess).should_receive('call')\
                        .and_return(0)
    self.assertEqual(12345, app_manager_server.start_app(configuration))

  def test_start_app_failed_copy_java(self):
    configuration = {'app_name': 'test',
                     'app_port': 2000,
                     'language': 'java',
                     'load_balancer_ip': '127.0.0.1',
                     'load_balancer_port': 8080,
                     'xmpp_ip': '127.0.0.1',
                     'dblocations': ['127.0.0.1', '127.0.0.2']}
    configuration = json.dumps(configuration)

    fake_secret = "XXXXXX"
    flexmock(appscale_info).should_receive('get_private_ip')\
      .and_return('<private_ip>')
    flexmock(appscale_info).should_receive('get_secret')\
                           .and_return(fake_secret)
    flexmock(god_app_configuration).should_receive('create_config_file')\
                               .and_return('fakeconfig')
    flexmock(god_interface).should_receive('start')\
                           .and_return(True)
    flexmock(app_manager_server).should_receive('wait_on_app')\
                         .and_return(True)
    flexmock(os).should_receive('popen')\
                .and_return(flexmock(read=lambda: '12345\n'))
    flexmock(file_io).should_receive('write')\
                        .and_return()
    flexmock(subprocess).should_receive('call')\
                        .and_return(1)
    self.assertEqual(-1, app_manager_server.start_app(configuration))

  def test_choose_db_location(self):
    db_locations = ['127.0.0.1']
    self.assertEqual("127.0.0.1", app_manager_server.choose_db_location(db_locations))
    db_locations = ['127.0.0.1', '127.0.0.2']
    assert  app_manager_server.choose_db_location(db_locations) in db_locations
    self.assertRaises(ValueError, app_manager_server.choose_db_location, [])            

  def test_create_python_app_env(self):
    env_vars = app_manager_server.create_python_app_env('1', '2', '3') 
    self.assertEqual('1', env_vars['MY_IP_ADDRESS'])
    self.assertEqual('2', env_vars['MY_PORT'])
    self.assertEqual('3', env_vars['APPNAME'])
    assert 'appscale' in env_vars['APPSCALE_HOME']
    assert 0 < int(env_vars['GOMAXPROCS'])

  def test_create_java_app_env(self):
    env_vars = app_manager_server.create_java_app_env()
    assert 'appscale' in env_vars['APPSCALE_HOME']

  def test_create_java_start_cmd(self): 
    fake_secret = "XXXXXX"
    flexmock(appscale_info).should_receive('get_secret')\
      .and_return(fake_secret)
    flexmock(appscale_info).should_receive('get_private_ip')\
      .and_return('<private_ip>')
    db_locations = ['127.0.1.0', '127.0.2.0']
    app_id = 'testapp'
    cmd = app_manager_server.create_java_start_cmd(app_id,
                                            '20000',
                                            '127.0.0.2',
                                            '8080',
                                            db_locations)
    assert fake_secret in cmd
    assert app_id in cmd

  def test_create_java_stop_cmd(self): 
    fake_secret = "XXXXXX"
    port = "20000"
    flexmock(appscale_info).should_receive('get_secret')\
      .and_return(fake_secret)
    flexmock(appscale_info).should_receive('get_private_ip')\
      .and_return('<private_ip>')
    cmd = app_manager_server.create_java_stop_cmd(port)
    assert port in cmd 
    assert  fake_secret in cmd 
    assert 'kill' in cmd

    # Test with a numerial port instead of a string
    port = 20000    
    cmd = app_manager_server.create_python_stop_cmd(port, 'python')
    assert str(port) in cmd 
 
  def test_create_python_start_cmd(self): 
    fake_secret = "XXXXXX"
    flexmock(appscale_info).should_receive('get_secret')\
      .and_return(fake_secret)
    flexmock(appscale_info).should_receive('get_private_ip')\
      .and_return('<private_ip>')
    db_locations = ['127.0.1.0', '127.0.2.0']
    app_id = 'testapp'
    cmd = app_manager_server.create_python_start_cmd(app_id,
                                             '127.0.0.1',
                                             '20000',
                                             '127.0.0.2',
                                             '8080',
                                             '127.0.0.3',
                                             db_locations,
                                             'python')
    assert fake_secret in cmd
    assert app_id in cmd

  def test_create_python_stop_cmd(self): 
    fake_secret = "XXXXXX"
    port = "20000"
    flexmock(appscale_info).should_receive('get_secret')\
      .and_return(fake_secret)
    cmd = app_manager_server.create_python_stop_cmd(port, 
                                                    'python')
    assert port in cmd 
    assert fake_secret in cmd 
    assert 'kill' in cmd

    port = 20000    
    cmd = app_manager_server.create_python_stop_cmd(port, 
                                                    'python27')
    assert str(port) in cmd 
 
  def test_stop_app_instance(self):
    flexmock(subprocess).should_receive('call')\
                        .and_return(0)
    flexmock(file_io).should_receive('read')\
                        .and_return('0')
    flexmock(os).should_receive('system')\
                        .and_return(0)
    app_manager_server.stop_app('test')

  def test_kill_app_instances_for_app(self):
    flexmock(glob).should_receive('glob') \
                        .and_return(['a', 'b'])
    flexmock(subprocess).should_receive('call')\
                        .and_return(0)
    flexmock(file_io).should_receive('read')\
                        .and_return('0')
    expected = ['0', '0']
    actual = app_manager_server.kill_app_instances_for_app('test')
    self.assertEquals(expected, actual)

  def test_stop_app(self):
    flexmock(god_interface).should_receive('stop')\
                        .and_return(True)
    flexmock(os).should_receive('system')\
                        .and_return(0)
    app_manager_server.stop_app('test')

  def test_wait_on_app(self):
    port = 20000
    ip = '127.0.0.1'
    testing.disable_logging()
    flexmock(urllib).should_receive('urlopen').and_return()
    flexmock(appscale_info).should_receive('get_private_ip')\
      .and_return(ip)
    self.assertEqual(True, app_manager_server.wait_on_app(port))

    flexmock(time).should_receive('sleep').and_return()
    flexmock(urllib).should_receive('urlopen').and_raise(IOError)
    self.assertEqual(False, app_manager_server.wait_on_app(port))
     

  def test_get_pid_from_port(self):
    flexmock(os).should_receive('popen')\
                .and_return(flexmock(read=lambda: '12345\n'))
    self.assertEqual(12345, app_manager_server.get_pid_from_port(54321))
    flexmock(os).should_receive('popen')\
                .and_return(flexmock(read=lambda: ''))
    self.assertEqual(app_manager_server.BAD_PID, app_manager_server.get_pid_from_port(54321))
  
  def test_copy_modified_jars_success(self):
    app_name = 'test'
    flexmock(subprocess).should_receive('call').and_return(0)
    self.assertEqual(True, app_manager_server.copy_modified_jars(app_name))  
  
  def test_copy_modified_jars_fail_case_1(self):
    app_name = 'test'
    flexmock(subprocess).should_receive('call').and_return(0).and_return(1)
    self.assertEqual(False, app_manager_server.copy_modified_jars(app_name))

  def test_copy_modified_jars_fail_case_2(self):
    app_name = 'test'
    flexmock(subprocess).should_receive('call').and_return(1)
    self.assertEqual(False, app_manager_server.copy_modified_jars(app_name))
    
if __name__ == "__main__":
  unittest.main()
